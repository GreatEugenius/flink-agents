# Part.2 AgentPlan 数据流切换

## AgentPlan 动态更新目标

### AgentPlan 包含内容

AgentPlan 包含 Actions,ActionsByEvent, ResoureProvider 和 Config 四大内容。

*   Map<String, Action> actions:Mapping from action name to action itself

   *   key: Action Name

   *   value: 具体的 Action

      *   String name

      *   Function exec(实现方法的方法路径和名称)

      *   List\<String\> triggerConditions: 触发条件

      *   Map<String, Object> config: action 对应的配置,每次 action 执行的时候,从 ctx.getActionConfig 读取

*   Map<String, List\<Action\>> actionsByEvent: Mapping from event type string to list of actions that should be triggered by the event.

   *   key:triggerConditions

   *   value: 触发的 Action 列表(序列化的时候直接用 action 名称,防止和 actions 重复序列化)

*   Map<ResourceType, Map<String, ResourceProvider>> resourceProviders:Two-level mapping of resource type to resource name to resource provider

   *   ResourceProvider:ChatModel、EmbeddingModel、VectorStore、Tool、Prompt、Skill 等 Resource 的初始化方法

*   AgentConfiguration config(Map<String, Object> conf):整个 Flink Agents 的配置信息,可以分为框架层和用户自定义层两部分

   *   有一些 config 是框架需要的,比如 ActionStateBackend(Kafka、Fluss),ShortTermMem TTL、EventLogger 等,这些数据在算子初始化阶段就已经通过最初的 AgentPlan 声明好了,我们不希望能够通过 AgentPlan 动态更新来修改他们

   *   还有一些是用户自定义的 config,可以通过 ctx.getConfig 在 action 中获得的,这部分我们允许用户通过动态更新的方式让新的 config 生效,在 action 中加载它们。

   *   目前这两者没有很好的区分,我们在最初版本可以考虑直接**不支持用户更新 AgentConfiguration config,自动忽略这部分并文档告知。**

### 消费组件的生效 scope

横向看完 AgentPlan 有哪些内容,我们再来从 AEO 内部不同组件的角度看一下,有哪些读了 AgentPlan,在什么时机读了 AgentPlan 的什么内容,以及是否可以通过 AgentPlan 动态更新。

| 消费组件                                    | 消费的成员                                                   | 读取时机                     | scope                              | 处理方案                                                     |
| ------------------------------------------- | ------------------------------------------------------------ | ---------------------------- | ---------------------------------- | ------------------------------------------------------------ |
| OperatorStateManager                        | config:`short-term-memory.state-ttl.*`(keyed state TTL)      | open 时注册 state descriptor | 作业级                             | **原理上不可能随 plan 改**:Flink state descriptor 注册后不可变 |
| DurableExecutionManager + Kafka/Fluss store | config:`actionStateStoreBackend`、`kafka*`/`fluss*` 连接配置 | initializeState/open         | 作业级                             | store 是作业级长连接,换 backend 必须重启                     |
| jobIdentifier                               | config:`job-identifier`                                      | initializeState              | 作业级                             | 设计契约就要求跨重启稳定,绝不可随 plan 变                    |
| ActionTaskContextManager                    | config:`num-async-threads`                                   | open                         | 作业级                             | 线程池作业级                                                 |
| EventRouter(对象本身)                       | config: `eventLoggerType`/`event-log.*`、`event-listeners`   | 构造/open                    | 作业级                             | logger/listener 一次性创建;若未来要 plan 级需把生命周期改成 per-plan,暂定作业级 |
| EventRouter(路由查询)                       | actionsByEvent                                               | 每个 event                   | **plan 级**                        | 当前实现自动适配:`getActionsTriggeredBy(event, plan)`        |
| ActionTask 执行                             | actions(实现、触发条件、action config)                       | 每个 action task             | **plan 级**                        | 当前实现自动适配:选择对应的 ClassLoader,executor,无需修改    |
| ResourceCache                               | resourceProviders 全部(ChatModel/Embedding/VectorStore/Tool/Prompt/Skill/MCP) | 资源首次访问                 | **plan 级**                        | 切换时整体重建:关旧 cache、按新 plan 建新 cache(懒加载,首次访问才真正初始化) |
| PythonBridgeManager                         | plan 的 Python actions/resources、Mem0 三项配置、根据 agentPlan 创建 PythonRunnerContext | open                         | **plan 级**                        | 需要做特殊处理:<br>1. Open 逻辑需要更改<br>2. Interpreter 需要支持增加从外部文件 import 新文件<br>3. Interpreter 缓存问题(修改 action 生效问题)<br>4. pythonRunnerContext 重新创建/更新<br>5. pythonActionExecutor 重新创建/更新 |
| ActionTaskContextManager                    | action config, config(`ctx.getConfig()`)                     | 每个 action task             | **plan 级**                        | runner context 每个 action task 创建时传入当前 plan(`createAndSetRunnerContext(actionTask, key, agentPlan, resourceCache, ...)`),切换后自动读新 plan |
| BuiltInMetrics                              | actions 名称集                                               | open 注册 + 执行时懒注册     | 作业级框架指标 + per-action 懒注册 | 新 plan 的新 action 首次执行时 computeIfAbsent 自动补注册    |

### 目标与非目标

#### 目标

*   AEO 可以在运行中接收新的完整 `AgentPlan` JSON。

*   AEO 原地更新内部 plan 状态,不重启作业、不重建 job graph。

*   允许用户新增 action、修改 action 编排,并更新 action config。

*   允许用户修改 Action 的订阅逻辑。

*   允许用户修改 resource 配置,框架会根据最新的 provider 重新初始化 resource。

*   每条数据完整地由一个版本的 plan 处理:切换点之前收到的数据(含其触发的整条事件链)全部按旧 plan 执行完,切换点之后的数据按新 plan 执行。

*   失败恢复后,恢复出的在飞数据仍然用它当初对应的那个 plan 继续执行。

#### 非目标

*   允许用户修改框架相关配置并生效(作业级;唯一例外:更新自带的 `dynamic-plan.*` 键保留——artifact 位置与摘要本来就是 plan 级)。

*   跨 attempt 的严格可回放:激活 cp 失败的残余窗口内,同一条输入在失败前后可能被不同版本处理(见一致性契约;跨 subtask 全局同 cp 切换**不是**非目标——它由两阶段切换协议保证)。

### 问题拆解

"运行中原地切换"拆成三个必须回答的问题,后文各章分别回答:

1.  **生效时点**:新 plan 从哪条数据开始生效?(→ 切换时机:与 checkpoint barrier 对齐)

2.  **在飞数据**:切换时正在执行的事件链怎么办?(→ drain 语义:先按旧 plan 跑完)

3.  **切换动作自身的一致性**:切换请求怎么不丢、恢复后怎么不错位?(→ 状态与快照、恢复流程)

## AgentPlan 切换时机选择

在调研过大多数主流 Agent 框架对于动态更新配置/路由之后[《AgentPlan 动态更新主流框架调研》](https://alidocs.dingtalk.com/i/nodes/QG53mjyd800agdlKHwDkq6rE86zbX04v),我发现有一点大家做的出奇的一致,就是更新生效的时间一定是处理完在飞数据,也就是说对于已经在处理的数据还按照老方法进行处理,之后到达的新请求才按新配置/路由执行,这是为了保证 Agent 执行的可控性:一次运行只按照一个版本 agentPlan 执行完整逻辑,不会产生脑裂。所以我们更新 AgentPlan 的粒度也是 pre run 级别的。

### 切换时机与 Checkpoint 对齐

AgentPlan 切换的时机与 Checkpoint 对齐:收到新 plan 后不立即生效,而是等到下一次 checkpoint,在 **barrier 对齐点(`prepareSnapshotPreBarrier`)** 完成"drain 旧 plan 在飞数据 → 原地切换"两个动作。barrier 之前的数据全部按旧 plan 执行完毕,barrier 之后的数据按新 plan 执行,本次 checkpoint 快照里记录的就是切换后的新 plan。

选择和 checkpoint 对齐的理由:

1.  同一 subtask 内同一时间只有一个 AgentPlan 在运行,所有数据都按照一份地图路由。

   1.  同一时间只有一份 resource cache,避免创建多个同样的 resource,resource close 时间点明确(切换点,drain 后无引用)。

   2.  同一时间运行的代码是一样的,避免新老代码交替运行的潜在问题,方便排查、审计。

2.  数据从 checkpoint 恢复简单:快照里存的 plan 就是快照中在飞数据对应的 plan,恢复后直接用它继续执行,不会错位。

3.  不需要每一条数据/每条事件链记录自己所属的 plan 版本,也不需要多版本共存的引用计数和资源回收机制。

代价(已知并接受):

1.  切换发生的那次 checkpoint,barrier 会被 drain 拖慢:该 subtask 要先把在飞事件链跑完才放行 barrier,期间不消费新输入。极端情况下有约一个 checkpoint 间隔 + 最长在飞链耗时的数据堆积。**drain 的工作量是有界的**:barrier 已经把新输入挡在算子外(排队在网络 buffer),要 drain 的只是 barrier 时刻已在算子内的事件链和它们 buffer 的 pending input(见下文 drain 语义)。

2.  更新生效有延迟: 最长两个 checkpoint 间隔(两阶段协议:先搭一班 cp 持久化决定,下一班 cp 切换)。对 prompt/路由类策略调整可接受;需要秒级生效的场景不在本设计目标内。

![image.png](https://alidocs.oss-cn-zhangjiakou.aliyuncs.com/res/eLbnj1be657k9laN/img/97c59cb7-1920-4a4e-840b-7859b4f78972.png)

### 为什么生效点是 barrier 对齐点,而不是 checkpoint 完成后

早期考虑过"checkpoint 结束后(notifyCheckpointComplete)再生效,失败则回退 previousPlan"。这个方案有三个问题,因此不采用:

1.  **快照内容错位**。如果 snapshot 时已经把 currentPlan 换成新 plan、生效却推迟到 cp 完成后,那么"快照里的 plan"和"快照时实际在跑的 plan"不一致;反之如果快照存旧 plan、恢复后又需要额外规则推断"该 cp 完成即视为已切换"。切换点=barrier 则天然自洽:快照永远存"barrier 时刻起生效的 plan"。

2.  **notifyCheckpointComplete 不可靠**。Flink 对该通知只保证尽力送达,subtask 可能在收到通知前失败。依赖它做切换,会出现"cp 已完成但部分 subtask 未切换"的窗口,恢复语义变复杂(这正是 Kafka 事务型 sink 需要恢复期重提交补偿的原因)。

3.  **"先切换、事后校验、失败回退"会把未经校验的 plan 写进 checkpoint**。若 cp 完成后初始化失败回退 previousPlan,但该 cp 快照里已经记录了新 plan,此后 crash 恢复会恢复出一个无法初始化的 plan,作业陷入恢复循环。校验必须发生在 plan 进入 checkpoint 状态之前。

因此本设计把**校验和重量级准备前移到更新到达时**,把**切换本身放在 barrier 对齐点**,切换动作退化为换引用 + 关旧资源,barrier 不被 jar 加载/Python interpreter 启动这类秒级操作阻塞。副作用有两个,如实说明:

*   用户短时间连续推送多个 plan 时,中间版本的准备工作会浪费——用 newest-wins 缓解:新更新到达即废弃上一个未生效 pendingPlan 及其已准备的资源,任何时刻至多准备一份。

*   prepare 在 mailbox 线程执行,期间该 subtask 暂停消费数据:对 Python interpreter 启动这类秒级操作,表现为一次短暂反压。这是把停顿从 barrier 挪到到达时刻的权衡——不拖长 checkpoint,代价是数据路径停顿;若要两者都不停,需要异步 prepare。

## 运行时模型:currentPlan 与 pendingPlan

运行时只有两个 plan 槽位,没有 previousPlan、没有版本链:

*   **currentPlan**:当前生效的 plan。作业启动时来自 job graph 里的静态 plan;发生过切换后来自最近一次切换。随 checkpoint 持久化(见状态章节)。

*   **pendingPlan**:已通过校验和准备、等待下一次 checkpoint 生效的 plan。不持久化——它的容错由 coordinator 的 staged 状态负责(见两阶段切换协议)。

伴随每个 plan 有一个 coordinator 分配的单调递增 **planVersion**,用于新旧比较和幂等判断(重复收到 planVersion ≤ currentPlan.planVersion 的更新直接忽略)。

subtask 侧的生命周期(它不知道也不需要知道两阶段——收到指令就 prepare,下一个 barrier 就切,何时收到指令由 coordinator 的两阶段协议控制):

```plaintext
coordinator announce PlanUpdateEvent(planVersion, planJson)   ← 时机见"两阶段切换协议"
        │ handleOperatorEvent(mailbox 线程)
        ▼
  [prepare] 反序列化校验 → 构建 plan 级资源(ClassLoader / Python interpreter 预建)
        │ 失败:丢弃并告警,currentPlan 不受影响;成功:成为 pendingPlan
        ▼
  下一次 checkpoint 的 prepareSnapshotPreBarrier:
        │ ① drain:跑完本 subtask 全部在飞事件链(旧 plan)
        │ ② switch:currentPlan ← pendingPlan;重建 ResourceCache/RunnerContext 引用;关闭旧 plan 资源
        ▼
  snapshotState:快照记录切换后的 currentPlan(planVersion + canonical JSON)
        ▼
  barrier 之后的数据按新 plan 执行

```

### drain 语义

drain 直接复用现有机制,不需要新发明:

*   现有算子已经维护 subtask 级的在飞集合:`currentProcessingKeysOpState`(union list state,`OperatorStateManager.initializeOperatorStates`),key 有在飞事件链时在集合中,链跑完(含该 key buffer 的全部 pending input event)才移除。

*   现有 `waitInFlightEventsFinished()`(`ActionExecutionOperator`)就是完整的 drain 实现:`while (hasProcessingKeys()) mailboxExecutor.yield()`,目前用于 `endInput()`(有界流结束时排空)。`yield()` 逐个执行 mailbox 里排队的 action 续跑任务,包括异步 LLM 调用完成后回投的续跑 mail。

*   本设计在 `prepareSnapshotPreBarrier` 中调用同一 drain(仅当存在 pendingPlan 时)。此时执行在 task mailbox 线程上,与 `endInput` 的调用环境相同;新输入数据不是 mail,不会在 drain 期间被处理,所以 drain 工作量有界:等于 barrier 时刻的在飞链 + 它们已 buffer 的 pending input,全部按旧 plan 执行完——这正是"每条数据都以之前的 plan 执行"的落地。

*   **drain 无本地超时,阻塞直至排空;checkpoint 超时属预期内**。drain 受最长在飞链(如慢 LLM 调用)拖累会拖慢本次 checkpoint,乃至触发 cp 超时失败——用户应把 checkpoint timeout 配置到能覆盖最长在飞链。不做"超时放弃切换、顺延下一 cp":顺延会使各 subtask 生效点不一致,且 cp 后消费立即恢复、下一个 cp 大概率仍 drain 不完,切换可能永久饥饿。

*   **单更新在途——JM 侧单点决策**。coordinator 同一时间只处理一个更新: cp 进行中拒绝新请求,已有 staged 未生效也拒绝新请求(客户端稍后重试;staged 自提交被接受即存在,"未生效"覆盖 接受→持久化→announce→激活记录 全程,即整个生效延迟期)。"drain/切换进行中又来新 plan"在源头就不可能发生,不需要 subtask 侧任何分布式判定;subtask 端保留 planVersion 新旧检查仅作为重复投递的幂等兜底。

### 一致性契约

诚实地说清楚保证到什么粒度:

*   **per-key / per-run 强保证**:一条输入数据触发的整条事件链(一次 run)从头到尾只由一个版本的 plan 执行。依据:切换前必 drain,切换后才有新链;恢复时链和 plan 出自同一快照。这是主流框架共同的 pre-run 语义,也是 Agent 可控性真正需要的粒度。

*   **subtask 内强保证**:同一 subtask 内任意时刻只有一个 plan 在运行、一份 resource cache、一个 ClassLoader/interpreter 活跃。

*   **跨 subtask** **全局同 cp 切换(正常运行)**:由两阶段切换协议保证(见下一章)——切换点不再是各 subtask 依"事件先到还是 barrier 先到"临时决定的,而是 coordinator 通过两条因果链钉死的同一个 checkpoint。

*   **故障窗口的残余:跨 attempt 的重放不确定性(已知,MVP 容忍)**。切换发生在激活 cp 的 barrier 处(快照之前),barrier 过后 subtask 立即按新 plan 继续处理——此刻该 cp 是否会成功尚未可知。若它最终失败,恢复自上一完整 cp,这些已按新 plan 处理过的数据会先按旧 plan 重放(直到恢复后的重切 cp)——即同一条输入在失败前后两次 attempt 可能被不同版本处理。窗口大小取决于配置:Flink checkpoint 全局原子(所有 task + coordinator 都 ack 才算完成,部分快照作废),默认 `execution.checkpointing.tolerable-failed-checkpoints = 0` 时 cp 失败立即全局 failover,窗口被压缩到故障检测延迟的量级;调大容忍度则可延展到多个 cp 间隔。**窗口无法为零**——除非 barrier 后的数据等激活 cp 完成才消费(同步等 notify complete,流处理不可接受)。机制上的根因:barrier 不是数据流中的持久标记,不随恢复重放——它由 cp 定时器触发、注入在 source 彼时的读取位置,同一逻辑切换点在两次 attempt 中落在数据流的不同位置(恢复后 source 追赶积压,第一个 barrier 到来前可能已按旧 plan 重放大量数据)。要消除必须把切换绑到数据位置而非 cp,而 Flink 中不存在跨 source/分区/subtask 良定义的全局数据位置——cp barrier 正是它的替代品,代价即切面位置跨 attempt 不可复现。

## 两阶段切换协议:全局同 cp 生效

切换边界的所有问题(跨 subtask 歪斜、恢复二义、切换动作丢失)有同一个根源:**"在哪个 cp 切换"如果由各 subtask 在 barrier 现场按"事件先到还是 barrier 先到"临时决定,这个决定既不全局也不持久**。两阶段协议把它变成 coordinator 提前一个 cp 持久化的全局决定:

```plaintext
阶段一 stage(哪个 plan)
  更新到达 coordinator:校验 → staged = 新 plan(不向任何 subtask 发送)
  下一个 checkpoint K:coordinator 快照持久化 {active=旧, staged=新}
  K 完成(决定已持久)→ coordinator 向所有 subtask announce 切换指令

阶段二 switch(哪个 cp)
  subtask 收到指令:prepare → pendingPlan(既有逻辑,无改动)
  checkpoint K+1:所有 subtask 在 K+1 的 barrier 处 drain + 切换
  coordinator 在 K+1 快照中记录激活:{active=新, staged=空}

```

**为什么所有 subtask 必然在同一个 cp(K+1)切换**——不是"时间够长大概率都收到",而是靠确定的上下界保证的:

1.  **指令到达的下界**:announce 发生在 K 完成之后,而 K 完成意味着每个 subtask 都已做完自己的 K 快照——所以每人收到指令都在自己的 K 之后,不会有人在 K 或更早切。

2.  **K+1 barrier 的上界**:Flink 触发 cp 的顺序是"先等所有 coordinator 的快照 future 完成,才向 source 注入 barrier"。coordinator 在完成自己的 K+1 快照前,核对所有 announce 的送达确认(`sendEvent` 返回的 ack future),未确认则不放行。于是"每个 subtask 收到指令"在因果上先于"任何 K+1 barrier 存在",更先于"barrier 到达该 subtask"。

这样，每个 subtask 都在 (K, K+1) 区间收到指令,全部在 K+1 的 barrier 切换。**持久化买的则是另一半——crash 后的确定性**:staged 写进 K 的快照后,恢复到 K 的 coordinator 完整知道"有一个已宣布的切换未完成",重新 announce 即可续上;恢复到 K 之前则 staged 从未存在,世界干净。切换决定永远不会处于"发生过但没人记得"的状态。

配套规则:

*   **单更新在途**:staged 存在且未激活期间,新更新被拒绝(客户端重试)。状态机始终至多两个槽位,无版本链。

*   **激活的记录时机**:active=staged 写在 K+1 的快照里——"从 K+1 恢复"等价于"K+1 完成过"等价于"所有人已在 K+1 barrier 切换",快照内容与运行事实一致。

*   **abort 处理**:staging cp(K)abort → 决定未持久,不 announce,staged 随下一个成功 cp 再持久再宣布,切换顺延,无不一致。激活 cp(K+1)abort → 残余窗口,见一致性契约。

*   **恢复重宣(只重宣 staged)**:恢复出的 staged 必然是持久过的,`executionAttemptReady` 时重发它,续上被打断的切换;subtask 的 planVersion 检查使重发幂等。active **不重发**——生效 plan 每个 subtask 从自身 union state 恢复,两阶段协议保证任何完成的 cp 里两者必然相等。初次启动、恢复、failover 单 subtask 重启走同一条路径。

*   **代价**:生效延迟从最多 1 个 cp 间隔变为最多 2 个(stage 要先搭一班 cp);假设 `maxConcurrentCheckpoints = 1`(Flink 默认)。

## 状态与快照

新增一份算子状态,一份 coordinator 状态:

| 状态               | 位置                   | 内容                                                         | 写入时机                         | 恢复语义                                                     |
| ------------------ | ---------------------- | ------------------------------------------------------------ | -------------------------------- | ------------------------------------------------------------ |
| current-agent-plan | AEO union list state   | `{planVersion, canonicalPlanJson}`,每 subtask 至多一条               | 每次 snapshotState(发生过切换后) | union 恢复后各 subtask 看到全部条目;两阶段协议下所有条目必然同版本(无歪斜),取任意一条(实现取 max-planVersion,此时退化为去重);状态为空 = 从未切换,用 job graph 静态 plan |
| 两阶段状态         | coordinator checkpoint | `{active, staged}`(各为 planVersion + planId + canonicalJson) | coordinator snapshot             | active 回答"当前全局生效到哪个版本":①版本铸造的基,下一次更新的 planVersion = active+1;②激活事实的唯一持久记录——切换完成 = active←staged 写进激活 cp,恢复时靠它判断切换是否已完成(staged 为空即已完成)。**active 的内容从不发给 subtask**——生效 plan 各 subtask 从自身 union state 恢复。staged 存在则恢复后重新 announce,续上被打断的切换 |

设计要点:

*   **为什么不是 keyed state**:currentPlan 是 subtask 级概念而不是 per-key 概念,keyed state 会把同一份 JSON 复制到每个 key,且 rescale 时按 key group 迁移毫无意义。

*   **为什么是 union 而不是普通 list state**:扩并发时普通 list state 做 round-robin 切分,新增 subtask 可能分不到任何条目,恢复不出 currentPlan;union 让每个 subtask 都能看到全量。**两阶段协议消灭了歪斜 cp 本身**(任意 cp 里所有 subtask 版本相同,keyed operator 异常必然全局 fo、所有人从同一 cp 恢复),max 规则不再承担消歪斜职责,只是无歧义条目的去重。

*   **union 的体积代价可控**:每 subtask 只有一条(不是 POC 多版本方案的 N 版本),checkpoint 增量 ≈ 并行度 × plan JSON 大小。若规模敏感,可做 single-writer 优化(只 subtask 0 写,union 恢复语义不变)。

*   **durable execution 的跨版本缓存防护(定位:防投毒,不是可回放保证)**:ActionState 外部存储(Kafka/Fluss)不随 checkpoint 回滚。激活 cp abort 的残余窗口里,同一 `(key, seqNum, action)` 可能先后在两个版本下执行,重放时可能命中另一版本写下的缓存结果——ActionState key 纳入 planVersion 后不同版本互不复用，重放时干净重算。**如实声明**:这只防"错误复用缓存",防不了"同一条数据在 fo 前后被不同版本处理"本身;严格可回放性本设计不具备——做到它要求数据只在其 plan 决策持久化之后才被消费(等价于消费等 notify checkpoint complete，代价过高 MVP 不包括)。不使用 action store 的作业无此顾虑。

*   **备选方案(评估后未采纳):subtask 不存 plan,启动时从 coordinator 获取**。两阶段协议下 coordinator 的 `{active, staged}` 确实已是唯一真相,subtask 的 union state 在语义上冗余,理论上可删。未采纳的原因是启动时序:subtask 没有本地状态,就必须在收到 coordinator 下发的 active 之前 hold 住全部输入(operator event 与首批数据的到达顺序没有保证),平添一个启动握手/阻塞阶段;且 coordinator 需要下发 active、staged 两条消息,subtask 要区分"立即生效"和"下一个 barrier 生效"两种语义,消息协议随之分叉。保留 union state 后:subtask 启动即可处理数据,生效 plan 自己答;coordinator 只负责版本铸造、`{active, staged}` 状态机与恢复时重宣 staged,不承担向 subtask 提供生效 plan 内容的职责。

## 恢复流程

### 启动流程(初次启动与恢复统一)

subtask 侧不区分"初次启动/失败恢复/rescale 恢复"——三者走同一流程,差别只在有没有状态可读:

1.  `initializeState`:从 union state 读 current-agent-plan,取 max-planVersion 条目反序列化为 currentPlan;状态为空(初次启动或从未切换)则用 job graph 静态 plan。**后续 `open()` 里所有原来读静态 `agentPlan` 的构建逻辑(ResourceCache、BuiltInMetrics、PythonBridgeManager、EventRouter 路由)一律改读 currentPlan**。

2.  keyed state 恢复在飞事件链(actionTasks、pendingInputEvents、processingKeys),`tryResumeProcessActionTasks()` 照旧续跑。**注意:恢复出在飞链是常态,不矛盾**——drain 只发生在切换 cp,普通 cp 不 drain(否则每个 cp 都要等最慢的 LLM 调用),所以从普通 cp 恢复必然带着在飞链。它们的版本是对的:切换与快照在同一 prebarrier 原子完成,任何快照里"在飞链的生效 plan"恰好就是该快照记录的 currentPlan(切换 cp 的快照则在飞链为空);链续跑不会错位。

3.  coordinator 从自身 checkpoint 恢复  `{active, staged}`,在每个 subtask `executionAttemptReady` 时**只重宣 staged(若存在)**,续上被打断的切换。**与第 1 步分工明确**:生效 plan 由 subtask 自身状态回答(第 1 步),未完成的切换由 coordinator 回答(本步);active 不重发——两阶段协议保证任何完成的 cp 里 coordinator 的 active 与所有 subtask 快照的版本相等,重发必然是幂等 no-op。

### 逐个失败窗口走查

以 stage cp = K、激活 cp = K+1 为记号:

| crash 时点                      | 恢复来源 | coordinator 恢复出        | subtask 恢复出 | 结果                                                         |
| ------------------------------- | -------- | ------------------------- | -------------- | ------------------------------------------------------------ |
| 更新到达前                      | 任意 cp  | {active}                  | 同版本         | 与更新无关,原语义                                            |
| 更新已接受、K 完成前            | K-1      | {active=旧},staged 未持久 | 旧             | 更新丢失(从未 durable),客户端重发(幂等)                      |
| K 完成后、K+1 完成前            | K        | {active=旧, staged=新}    | 旧             | 恢复后重新 announce,在恢复后的下一个 cp 全体切换——切换被打断但不丢、不歪 |
| K+1 完成后                      | K+1      | {active=新}               | 新             | 切换已完成,快照与运行事实一致                                |
| K abort(作业未失败)             | \-       | staged 未持久,不 announce | 无感知         | 切换顺延到下一个成功 cp,无任何不一致                         |
| K+1 abort、作业继续、其后 crash | K        | {active=旧, staged=新}    | 旧             | **残余窗口**:K+1 barrier 后已按新 plan 处理的数据将按旧 plan 重放,直到恢复后的切换完成;缓存已按 planVersion 隔离。堵死需 fail-on-abort(可选强化),MVP 容忍并文档化 |

### rescale(改并发恢复)

*   currentPlan:union 全量分发 + planJson 按 planVersion 去重,新 subtask 一样恢复出 max-planVersion plan;体积与恢复放大分析见状态章节。

*   在飞链:沿用现有机制——`currentProcessingKeys` 本来就是 union state,恢复后按 key group 过滤本 subtask 拥有的 key 续跑(`tryResumeProcessActionTasks`),keyed 的 actionTasks/pendingInputEvents 随 key group 迁移。两阶段协议下任意 cp 内所有条目同版本,全体恢复出同一个 currentPlan,迁移过来的链在同一 plan 下续跑,无需任何 per-key 版本标记——这是单版本方案相对多版本 POC 最大的简化。

## Proposed Changes

### 新增:PlanUpdateCoordinator(两阶段状态机)

JM 侧 OperatorCoordinator,更新链路唯一入口、切换决策唯一持有者。状态只有 `{active, staged}` 两个槽位;一个更新从提交到生效横跨两个 checkpoint,按时间线列出每一步及承载它的回调(记 staging cp = K、激活 cp = K+1):

| 时刻 | 回调 | 动作 |
| --- | --- | --- |
| 提交 | `handleCoordinationRequest` | 接收 `PlanUpdateRequest(planJson)`(REST,Java/Python 客户端字节兼容 POC);静态校验(canonical 化 + 反序列化),失败同步拒绝;通过则 **staged = 新 plan**(planVersion = active+1),**不向任何 subtask 发送**。门禁:cp 进行中、或已有 staged 未生效 → 拒绝(单更新在途) |
| cp K 快照 | `checkpointCoordinator(K)` | 持久化 `{active=旧, staged=新}` ——**切换决定落盘** |
| cp K 完成 | `notifyCheckpointComplete(K)` | **announce**:把 staged 发给所有 subtask(决定先持久、后可见);若 K abort(`notifyCheckpointAborted`)则不发,staged 留在槽里随下一个成功 cp 再落盘再宣布 |
| cp K+1 快照 | `checkpointCoordinator(K+1)` | ① **送达门禁**:等全部 announce 的 ack future,未确认不完成快照 future——Flink 要等 coordinator 快照完成才注入 barrier,故 K+1 的 barrier 不会先于任何 subtask 收到指令而存在;② **记录激活**:active←staged,staged 清空;③ 持久化 `{active=新}` |
| K+1 barrier | (无 coordinator 动作) | 所有 subtask 在此 drain + 切换——①已保证人人手里有指令 |
| 恢复 | `resetToCheckpoint` + `executionAttemptReady` | 恢复 `{active, staged}`;staged 存在(必然持久过)→ 向每个 ready 的 subtask 重宣,续上被打断的切换;active 不重发。初次启动、恢复、单 subtask failover 走同一路径 |

注意同一个回调在两个 cp 扮演不同角色:`checkpointCoordinator` 对 K 是"落盘决定",对 K+1 是"门禁 + 记账激活"。实现并不区分 K 与 K+1,统一规则是:**快照前先清送达账;staged 已宣布即激活;总是持久化当前 `{active, staged}`**——两个角色由这组规则自然涌现,这也是状态机不需要记 cp 编号的原因。

### 修改:ActionExecutionOperatorFactory

实现 `CoordinatedOperatorFactory`,提供 coordinator provider。不设启用开关,coordinated 接线是唯一路径;存量作业停留在旧版本代码上不受影响,升级到含本特性的版本会改变算子 uid/OperatorID,旧 checkpoint 不兼容,需全新启动。

### 修改:ActionExecutionOperator

**控制面(新增的钩子):**

1.  `handleOperatorEvent(PlanUpdateEvent)`:planVersion ≤ 已知版本则忽略(幂等/重发去重);否则调 PlanVersionManager.prepare(见下),成功则记为 pendingPlan(newest-wins,废弃旧 pending 及其资源),并为新 plan 预建 Python runtime。

2.  `prepareSnapshotPreBarrier(checkpointId)`:pendingPlan 存在时,先无界 drain(复用 `waitInFlightEventsFinished`),后调 PlanVersionManager.switchToPending,释放旧 plan 的 Python runtime,刷新 durable 的 activePlanVersion。

3.  `snapshotState`:current-agent-plan union state 写入 `{planVersion, planJson}`。

4.  `initializeState`:恢复 union state 取 max-planVersion 得 currentPlan。

5.  `close`:级联关闭 PlanVersionManager(current 与 pending 的资源)。

**执行面(所有静态 `agentPlan` 读点改经 PlanVersionManager,改动面较大,逐一列出):**

| 读点                                | 原来                                                         | 现在                                                         |
| ----------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| `open()` 全部构建                   | 静态 `agentPlan`(keyed state TTL、metrics、durable store、Python bridge、context manager) | `planManager.currentPlan()`(即恢复出的生效 plan)             |
| `processElement` 输入包装           | `pythonBridge.getPythonActionExecutor()`                     | 当前 plan 的 executor(`getPythonActionExecutor(currentPlanKey)`) |
| 路由查询 `getActionsTriggeredBy`    | 静态 `agentPlan`                                             | `planManager.currentPlan()`                                  |
| 输出事件转换                        | 同上 executor                                                | 当前 plan 的 executor                                        |
| runner context 创建(每 action task) | 静态 plan + 算子级 resourceCache + 算子级 ltm                | 当前 plan + 当前 plan 的 ResourceCache + 当前 plan 的 ltm    |
| `actionTask.invoke`                 | 用户 ClassLoader + 算子级 executor                           | 当前 plan 的 ClassLoader(artifact)+ 当前 plan 的 executor    |
| BuiltInMetrics per-action 注册      | open 时按静态 plan 全量注册                                  | computeIfAbsent 懒注册,新 plan 新 action 首次执行自动补      |

算子级字段 `resourceCache`/`ltm` 取消,统一经 PlanVersionManager/PythonBridgeManager 按当前 plan 读取,杜绝切换后读到旧引用。

### 新增:PlanVersionManager

收敛 plan 生命周期为一个组件,按单版本语义实现:

1.  持有 currentPlan / pendingPlan 两槽位及各自 planVersion。

2.  prepare(planVersion, planJson):反序列化校验;合并配置(启动 config + 更新自带的 `dynamic-plan.*` 键);构建 plan 级 ClassLoader(artifact jar 分发 + sha256 校验,POC 已验证)并做 Java action 可执行性检查(Class.forName);按新 plan 建空 ResourceCache(懒加载)。同一时机,算子经 PythonBridgeManager 为新 plan 预建 Python runtime(interpreter/executor,POC 已验证多 interpreter 共存)——Python 侧生命周期归 PythonBridgeManager,PVM 只管 plan/ClassLoader/ResourceCache/状态。任一步失败即整体丢弃(关闭半成品资源),不影响 currentPlan。

3.  switchTo:换引用;按新 plan 重建 ResourceCache(懒加载,不阻塞 barrier);关闭旧 ResourceCache、旧 ClassLoader、旧 Python interpreter(drain 后无引用,可同步关闭)。

4.  getCurrentPlan/getConfig/getActionConfig:执行路径的统一读取口。

5.  快照/恢复 current-agent-plan union state。

### 修改:DurableExecutionManager / ActionStateUtil

ActionState key 纳入 planVersion(跨版本缓存隔离,防 cp abort 窗口的重放串用,见状态章节)。

### 无需修改(自动适配)

*   EventRouter 路由查询:`getActionsTriggeredBy(event, plan)` 已按次传参。

*   ActionTaskContextManager:runner context 每 task 创建时传入当前 plan/cache。

*   BuiltInMetrics:per-action computeIfAbsent 懒注册,新 action 自动补。

### 新增配置

仅有随更新提交的 plan 级键:

| 配置                                                         | 默认 | 说明                                 |
| ------------------------------------------------------------ | ---- | ------------------------------------ |
| `dynamic-plan.java-artifact.path` / `.sha256`、`dynamic-plan.python-artifact.path` / `.sha256`、`dynamic-plan.python-runtime.path`(随 plan JSON 提交) | \-   | 新代码分发与完整性校验,沿用 POC 约定 |

运行时成本盘点(支撑"不设开关"的决策),以从不推送更新的作业为基准:coordinator 是 JM 内空闲对象(active/staged 均空,backfill 不发任何事件);union state 在从未切换时不写任何条目;算子侧的常驻成本只有三样——创建时向 OperatorEventDispatcher 注册一次事件 handler、每次 checkpoint 的 prepareSnapshotPreBarrier 多一个 hasPending 判空、执行路径经 PlanVersionManager 间接读 plan(一层引用间接)。均可忽略;planVersion 比较等事件处理成本只在真的收到更新时发生。