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

*   AgentConfiguration config(Map<String, Object> conf):整个 Flink Agents 的配置信息,包含框架配置和用户自定义配置

   *   有一些 config 是框架需要的,比如 ActionStateBackend(Kafka、Fluss),ShortTermMem TTL、EventLogger 等,这些数据在算子初始化阶段就已经通过最初的 AgentPlan 声明好了,我们不希望能够通过 AgentPlan 动态更新来修改他们

   *   MVP 不在更新时区分两类 config:有效 plan 以 bootstrap plan 的整份 AgentConfiguration 为基底,只接受 Java/Python artifact 的 path 和 sha256 四个 plan 级键。因此 `ctx.getConfig()` 在切换后仍读到作业启动时的配置;可动态更新的是 Action 自身的 config,action 通过 `ctx.getActionConfig()` 读取它。

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
| PythonBridgeManager                         | plan 的 Python actions/resources、Mem0 三项配置、Python pipeline wire conversion、根据 agentPlan 创建 PythonRunnerContext | open / 激活 barrier          | **plan 级**                        | 始终只持有一个 active runtime。更新到达时只校验 artifact 元数据,不进入 CPython;激活 barrier drain 后关闭旧 runtime,通过单一 `switch_python_artifact(path_or_none)` 入口替换进程级模块图与 import path,再创建 executor/context并预解析 action 入口;Python 输入流即使只有 Java action 也保留 runtime 做输入输出转换 |
| ActionTaskContextManager                    | action config(`ctx.getActionConfig()`),作业 config(`ctx.getConfig()`) | 每个 action task             | action config 为 **plan 级**;作业 config 钉死 | Java runner context 会复用,因此 barrier drain 后必须先 close/reset V1 context;V2 首个 Java action 再用当前 plan/ResourceCache 懒建,Python context 由新 runtime 重建 |
| BuiltInMetrics                              | actions 名称集                                               | open 注册 + 执行时懒注册     | 作业级框架指标 + per-action 懒注册 | 新 plan 的新 action 首次执行时 computeIfAbsent 自动补注册    |

### 目标与非目标

#### 目标

*   AEO 可以在运行中接收新的完整 `AgentPlan` JSON。

*   AEO 原地更新内部 plan 状态,不重启作业、不重建 job graph。

*   允许用户新增 action、修改 action 编排,并更新 action config。

*   允许用户修改 Action 的订阅逻辑。

*   允许用户修改 resource 配置,框架会根据最新的 provider 重新初始化 resource。

*   对通过 `dynamic-plan.python-artifact.path` 声明的 pure-Python 用户 artifact 做完整模块图重载,使同名 action 及其 artifact 内依赖都从新 artifact 解析。

*   每条数据完整地由一个版本的 plan 处理:切换点之前收到的数据(含其触发的整条事件链)全部按旧 plan 执行完,切换点之后的数据按新 plan 执行。

*   在全部 subtask 都通过静态 prepare 的前提下,失败恢复出的在飞数据仍然用它当初对应的 plan 继续执行。prepare 无 nack 导致的环境分叉是当前 MVP 的例外,见一致性契约。

#### 非目标

*   允许用户修改框架相关配置并生效。作业级配置均钉死,包括 `dynamic-plan.python-runtime.path`;只有 Java/Python artifact 的 path 与 sha256 是 plan 级配置。

*   跨 attempt 的严格可回放:激活 cp 失败的残余窗口内,同一条输入在失败前后可能被不同版本处理(见一致性契约;在全部 subtask 静态 prepare 成功的正常路径中,跨 subtask 全局同 cp 切换**不是**非目标——它由两阶段切换协议保证)。

*   热更新 native extension、artifact 外的框架/stdlib/site-packages,以及带后台线程、`atexit` 或其他逃逸引用的用户代码。未声明 `dynamic-plan.python-artifact.path` 时也不承诺完整代码热更新。

### Python 重载的部署边界

PemJa 的多个 interpreter 共享同一 JVM 内的 CPython 进程和 `sys.modules`/`sys.path`,所以模块重载的真实作用域是进程,不是 subtask。本 MVP 把生产部署限制为:**一个 TM 只有一个 task slot,coordinated AEO 是该 JVM/CPython 进程唯一的 AEO Python owner**。`CompileUtils` 为 AEO 指定专用 slot-sharing group,`CoordinatedActionExecutionOperatorFactory` 使用 `ChainingStrategy.NEVER`,避免它和其他算子组成同一个 task;不支持 `taskmanager.numberOfTaskSlots > 1`,也不允许其他 PemJa/Python operator 与 AEO 共用该进程。

仅把 `taskmanager.numberOfTaskSlots` 设为 1 还不够:slot sharing 仍可能让多个 task 共用一个 slot,operator chaining 也可能让多个 operator 进入一个 task,因此代码同时固定专用 slot-sharing group 和 NEVER chain。专用 group 也意味着集群容量必须分别覆盖 AEO 和其他 group:对一个上下游与 AEO 并行度都为 p 的简单拓扑,通常至少需要 2p 个 slot;在每 TM 1 slot 的限制下即需要至少 2p 个 TM,否则作业会等待资源而无法完成调度。并行度大于 1 时由多个 TM 各承载一个 AEO subtask;各 TM 在同一个 checkpoint epoch 独立重载自己的 CPython 进程,不要求墙钟时刻完全一致。

两阶段协议另外要求 `execution.checkpointing.max-concurrent-checkpoints = 1`。这是部署契约,当前代码不会在作业启动时主动校验;不能仅依赖 Flink 的默认值而在生产配置中忽略它。

### 问题拆解

"运行中原地切换"拆成三个必须回答的问题,后文各章分别回答:

1.  **生效时点**:新 plan 从哪条数据开始生效?(→ 切换时机:与 checkpoint barrier 对齐)

2.  **在飞数据**:切换时正在执行的事件链怎么办?(→ drain 语义:先按旧 plan 跑完)

3.  **切换动作自身的一致性**:切换请求怎么不丢、恢复后怎么不错位?(→ 状态与快照、恢复流程)

## AgentPlan 切换时机选择

在调研过大多数主流 Agent 框架对于动态更新配置/路由之后[《AgentPlan 动态更新主流框架调研》](https://alidocs.dingtalk.com/i/nodes/QG53mjyd800agdlKHwDkq6rE86zbX04v),我发现有一点大家做的出奇的一致,就是更新生效的时间一定是处理完在飞数据,也就是说对于已经在处理的数据还按照老方法进行处理,之后到达的新请求才按新配置/路由执行,这是为了保证 Agent 执行的可控性:一次运行只按照一个版本 agentPlan 执行完整逻辑,不会产生脑裂。所以我们更新 AgentPlan 的粒度也是 pre run 级别的。

### 切换时机与 Checkpoint 对齐

AgentPlan 切换的时机与 Checkpoint 对齐:收到新 plan 后不立即生效,而是等到下一次 checkpoint,在 **barrier 对齐点(`prepareSnapshotPreBarrier`)** 完成"drain 旧 plan 在飞数据 → quiesce/关闭旧资源 → 重载 Python artifact → 原地切换"。barrier 之前的数据全部按旧 plan 执行完毕,barrier 之后的数据按新 plan 执行,本次 checkpoint 快照里记录的就是完成重载后的新 plan。

选择和 checkpoint 对齐的理由:

1.  同一 subtask 内同一时间只有一个 AgentPlan 在运行,所有数据都按照一份地图路由。

   1.  同一时间只有一份 resource cache,避免创建多个同样的 resource,resource close 时间点明确(切换点,drain 后无引用)。

   2.  同一时间运行的代码是一样的,避免新老代码交替运行的潜在问题,方便排查、审计。

2.  在全部 subtask 静态 prepare 成功的正常路径中,数据从 checkpoint 恢复简单:快照里存的 plan 就是快照中在飞数据对应的 plan,恢复后直接用它继续执行。若 prepare 因环境差异发生部分成功,union state 会出现混合版本,该保证不成立,见一致性契约。

3.  不需要每一条数据/每条事件链记录自己所属的 plan 版本,也不需要多版本共存的引用计数和资源回收机制。

代价(已知并接受):

1.  切换发生的那次 checkpoint,barrier 会被 drain 和 Python reload 拖慢:该 subtask 要先等在飞事件链按旧 plan 结束,再完成旧资源关闭、模块图清理、新 interpreter 创建和 action 入口解析才放行 barrier,期间不消费新输入。barrier 把新输入挡在算子外,因此需要 drain 的**输入集合已封闭**;但用户 action 可以继续产生事件或永不返回,所以不保证事件链必然终止。reload 耗时也取决于 artifact import 和 runtime 初始化,checkpoint timeout 应覆盖两者,但不构成完成上界。

2.  更新生效有延迟:正常路径需要两个成功 checkpoint(先持久化决定,下一个 cp 切换)。cp abort、事件链不结束、慢 artifact import 或 runtime 初始化都会延后生效,因此墙钟延迟无上界。对 prompt/路由类策略调整可接受;需要秒级生效的场景不在本设计目标内。

![image.png](https://alidocs.oss-cn-zhangjiakou.aliyuncs.com/res/eLbnj1be657k9laN/img/97c59cb7-1920-4a4e-840b-7859b4f78972.png)

### 为什么生效点是 barrier 对齐点,而不是 checkpoint 完成后

早期考虑过"checkpoint 结束后(notifyCheckpointComplete)再生效,失败则回退 previousPlan"。这个方案有三个问题,因此不采用:

1.  **快照内容错位**。如果 snapshot 时已经把 currentPlan 换成新 plan、生效却推迟到 cp 完成后,那么"快照里的 plan"和"快照时实际在跑的 plan"不一致;反之如果快照存旧 plan、恢复后又需要额外规则推断"该 cp 完成即视为已切换"。切换点=barrier 则天然自洽:快照永远存"barrier 时刻起生效的 plan"。

2.  **notifyCheckpointComplete 不可靠**。Flink 对该通知只保证尽力送达,subtask 可能在收到通知前失败。依赖它做切换,会出现"cp 已完成但部分 subtask 未切换"的窗口,恢复语义变复杂(这正是 Kafka 事务型 sink 需要恢复期重提交补偿的原因)。

3.  **Python 激活仍必须发生在快照之前**。如果先把新 plan 写进 checkpoint,等 cp 完成后才 import action,初始化失败就会留下一个可恢复但不可执行的 plan。现在的顺序是在 barrier 内先 drain 和完整重载,只有 V2 runtime 创建且声明的 action 入口全部解析成功后,才更新 currentPlan 并写快照。

因此校验被明确拆成两段:更新到达时只做 JSON、artifact 路径/sha256、Java classloader/action 及 Python 元数据等**不进入 CPython 的静态 prepare**;激活 barrier 在 drain 后才关闭 V1、清模块图和 import/function cache、创建 V2 runtime并预解析 Python action 入口。这样不会在 V1 尚有在飞执行时改共享 `sys.modules`,代价是激活 checkpoint 的 timeout 必须覆盖 **drain + reload**。如果 reload 失败,旧 runtime 已进入关闭流程,本 attempt 不做局部回退;异常使 task 和本次 checkpoint 失败,从上一个 completed checkpoint 恢复 V1,coordinator 再重宣仍在 staged 的 V2 并重试激活。

## 运行时模型:currentPlan 与 pendingPlan

运行时只有两个 plan 槽位,没有 previousPlan、没有版本链:

*   **currentPlan**:当前生效的 plan。作业启动时来自 job graph 里的静态 plan;发生过切换后来自最近一次切换。随 checkpoint 持久化(见状态章节)。

*   **pendingPlan**:已通过静态校验和 Java 侧准备、等待下一次 checkpoint 生效的 plan。此时尚未创建 Python runtime,也没有 import V2 用户模块。不持久化——它的容错由 coordinator 的 staged 状态负责(见两阶段切换协议)。

伴随每个 plan 有一个 coordinator 分配的 **planVersion**,用于新旧比较和幂等判断(重复收到 planVersion ≤ 已知版本的更新直接忽略)。已持久的 active 序列单调;接受后但 staging cp 完成前发生 JM failover 时,未持久的 planVersion 可能被重用。

subtask 侧的生命周期(它不知道也不需要知道两阶段——收到指令就 prepare,下一个 barrier 就切,何时收到指令由 coordinator 的两阶段协议控制):

```plaintext
coordinator announce PlanUpdateEvent(planVersion, planJson)   ← 时机见"两阶段切换协议"
        │ handleOperatorEvent(mailbox 线程)
        ▼
  [静态 prepare] 反序列化、artifact path/sha256、Java ClassLoader/action 校验
        │ 不创建 Python interpreter,不修改 sys.modules/sys.path
        │ 失败:丢弃并告警,currentPlan 不受影响;成功:成为 pendingPlan
        ▼
  下一次 checkpoint 的 prepareSnapshotPreBarrier:
        │ ① drain:跑完本 subtask 全部在飞事件链(旧 plan)
        │ ② 重查 V2 Java/Python artifact 元数据/摘要,quiesce 旧 executor
        │ ③ 关闭旧 ResourceCache/ClassLoader
        │ ④ switch_python_artifact(None):按 V1 roots 清模块图/path/cache,关闭旧 interpreter
        │ ⑤ 创建 V2 interpreter,初始化框架 helper,再 switch_python_artifact(V2 path)
        │    挂载 V2 artifact并预解析全部 Python action 入口
        │ ⑥ switch:currentPlan ← pendingPlan;刷新 durable activePlanVersion
        ▼
  snapshotState:快照记录切换后的 currentPlan(planVersion + canonical JSON)
        ▼
  barrier 之后的数据按新 plan 执行

```

### Python artifact 完整重载的定义

这里的"完整重载"指**替换已加载的用户 artifact 模块图**,不是 eager 执行 artifact 中的每个 `.py` 文件:

*   V1 drain 完成后,先停止 async executor并关闭可能持有 Python 引用的 plan 级资源;旧 interpreter 此时仍作为清理句柄存活。

*   直接遍历 immutable zip/whl 的 `.py` 条目:顶层 `helper.py` 得到 root `helper`,`app/.../*.py` 得到 root `app`。运行期保存当前 artifact 的 root 集合;退出 V1 时按旧 roots 删除 `root` 及 `root.*`,准备 V2 时再按新 roots 清一遍 fresh-attempt/同名残留。因为顶层 root 必须由 artifact 独占,这已覆盖入口、父包、artifact 内依赖及 V2 已删除的旧模块;`flink_agents.*` 等框架根始终保留。

*   跨语言入口只有 `switch_python_artifact(path_or_none)` 一个。它先读取新 zip/whl 的 roots;读取失败时不改任何 import 状态。随后按 `旧 roots ∪ 新 roots` 清模块,移除旧/新 artifact 的等价 `sys.path` 项,精确清理对应的 `sys.path_importer_cache` 和 `zipimport` cache,并清空 `_PYTHON_FUNCTION_CACHE`;path 非空时再把规范化路径挂到 `sys.path[0]`。这里不广播调用 `importlib.invalidate_caches()`,避免无关 finder 的回调失败把切换停在半成状态。因此旧 runtime 用 `None` 退役,V2 interpreter 在框架 helper 初始化后用新 path 挂载,不存在"artifact path 已注入、清理 helper 尚不可用"的失败窗口。

*   V2 artifact 挂载后预解析 plan 中全部 Python action 的 `(module, qualname)`。未被 plan 引用的文件不在此时执行,以后若被 V2 代码正常 import,也只会从 V2 artifact 解析。单 active runtime + barrier drain + 切换时清空 function cache 已经排除新旧 cache 共存,不再需要 plan scope 缓存键或 per-plan runtime Map。

*   `dynamic-plan.python-artifact.path` 与 `.sha256` 必须成对出现,artifact 必须是 immutable regular zip/whl;TM 静态 prepare 会先核摘要并用 `ZipFile` 验证格式。相邻两个 Python plan 直接复用同一物理路径但更换摘要会被拒绝;作业生命周期内"新内容必须使用新路径"仍是部署契约,不是一份跨 attempt 持久化的路径历史。激活前会再次核对摘要,但这不消除外部进程并发改写文件的 TOCTOU。`dynamic-plan.python-runtime.path` 属作业级配置,更新中的同名字段被 bootstrap 配置覆盖,运行中不得改变。

保证范围只包括已声明 artifact 内的 pure-Python 源码模块,artifact 的用户顶层 package/module 必须由它独占,不得与框架、stdlib 或 site-packages 的顶层名冲突。native extension 不受支持,不保证它被保留或能够安全重载;用户后台线程、`atexit`、全局注册表或其他逃逸引用可能继续持有旧对象,因此也不支持。未配置 `dynamic-plan.python-artifact.path` 的 plan 不贡献自身的用户模块 roots,只清空框架 function cache 并重建 plan runtime;这个模式可以更换 action/config 引用,但不承诺同名 Python 代码热更新。若旧 plan 声明过 artifact,退出旧 runtime 时仍会按它记录的 roots 清理旧模块和 path。在 artifact 模式下,模块 import-time side effect 会在 V2 解析时重新执行,模块全局状态也随重载重置。

### drain 语义

drain 直接复用现有机制,不需要新发明:

*   现有算子已经维护 subtask 级的在飞集合:`currentProcessingKeysOpState`(union list state,`OperatorStateManager.initializeOperatorStates`),key 有在飞事件链时在集合中,链跑完(含该 key buffer 的全部 pending input event)才移除。

*   现有 `waitInFlightEventsFinished()`(`ActionExecutionOperator`)就是完整的 drain 实现:`while (hasProcessingKeys()) mailboxExecutor.yield()`,目前用于 `endInput()`(有界流结束时排空)。`yield()` 逐个执行 mailbox 里排队的 action 续跑任务,包括异步 LLM 调用完成后回投的续跑 mail。

*   本设计在 `prepareSnapshotPreBarrier` 中调用同一 drain(仅当存在 pendingPlan 时)。此时执行在 task mailbox 线程上,与 `endInput` 的调用环境相同;新输入数据不是 mail,不会在 drain 期间被处理,因此 barrier 前已进入算子的输入集合是封闭的。它们的事件链继续按旧 plan 执行;只有当这些链全部结束后才继续切换,但用户代码可以不返回或持续产生新事件,所以 drain 不保证终止。

*   **drain 和 reload 均无本地超时;checkpoint 超时属预期内**。最长在飞链(如慢 LLM 调用)以及随后的模块清理、interpreter 初始化和入口 import 都会拖慢本次 checkpoint,乃至触发 cp 超时失败——用户应把 checkpoint timeout 配置到能覆盖 `drain + reload`。不做"超时放弃切换、顺延下一 cp":顺延会使各 subtask 生效点不一致,且 cp 后消费立即恢复、下一个 cp 大概率仍 drain 不完,切换可能永久饥饿。

*   **单更新在途——JM 侧单点决策**。coordinator 同一时间只处理一个更新: cp 进行中拒绝新请求,已有 staged 未生效也拒绝新请求(客户端稍后重试;staged 自提交被接受即存在,"未生效"覆盖 接受→持久化→announce→激活记录 全程,即整个生效延迟期)。"drain/切换进行中又来新 plan"在源头就不可能发生,不需要 subtask 侧任何分布式判定;subtask 端保留 planVersion 新旧检查仅作为重复投递的幂等兜底。

### 一致性契约

诚实地说清楚保证到什么粒度:

*   **per-key / per-run 保证(正常路径)**:一条输入数据触发的整条事件链(一次 run)从头到尾只由一个版本的 plan 执行。依据:切换前先 drain,切换后才有新链;全部 subtask 静态 prepare 成功时,恢复的链和 plan 出自同版本快照。prepare 失败但无 nack 导致的混合版本 cp 不在该保证内。

*   **subtask 内强保证(受部署前提约束)**:执行期同一 subtask 只有一个 plan、一份 resource cache、一个 ClassLoader/interpreter 活跃;切换时先 quiesce/关闭 V1,再创建 V2,不依赖多个 PemJa interpreter 提供模块隔离。该保证要求本进程只有一个 AEO Python owner。

*   **跨 subtask 全局同 cp 切换(全员 prepare 成功的正常路径)**:由两阶段切换协议保证(见下一章)——切换点不再是各 subtask 依"事件先到还是 barrier 先到"临时决定的,而是 coordinator 通过两条因果链钉死的同一个 checkpoint。

    当前 subtask 拒绝没有 nack 回执;artifact 分发或 TM 环境不一致时,coordinator 可能不知道某个 subtask 仍停在 V1,其他 subtask 却已切到 V2。这不仅失去全局同 cp 保证,还会形成混合版本 union state;恢复时实现取 max-version,可能使原先停在 V1 的 subtask 以 V2 续跑快照中的在飞链。因此环境分叉下连 per-run 恢复保证也不成立。这是已知 MVP 缺口,需要 future 的 prepare ack/nack 闭环;当前生产前提是所有 TM 的不可变 artifact 和 Python 环境一致。激活 barrier 内的 runtime/import 失败不属于这个静默拒绝窗口:它会向上抛出并使 task/checkpoint 失败。

*   **故障窗口的残余:跨 attempt 的重放不确定性(已知,MVP 容忍)**。切换发生在激活 cp 的 barrier 处(快照之前),barrier 过后 subtask 立即按新 plan 继续处理——此刻该 cp 是否会成功尚未可知。若它最终失败,恢复自上一完整 cp,这些已按新 plan 处理过的数据会先按旧 plan 重放(直到恢复后的重切 cp)——即同一条输入在失败前后两次 attempt 可能被不同版本处理。Flink checkpoint 只有在所有 task 和 coordinator 都 ack 后才全局完成,部分快照会作废;但 `execution.checkpointing.tolerable-failed-checkpoints = 0` 不等于所有 cp 失败都立即全局 failover,具体恢复范围还受失败类型和 Flink failover strategy 影响。调大容忍度可以让窗口延展到多个 cp 间隔,但默认值也不构成固定的时间上界。**窗口无法为零**——除非 barrier 后的数据等激活 cp 完成才消费(同步等 notify complete,流处理不可接受)。机制上的根因:barrier 不是数据流中的持久标记,不随恢复重放——它由 cp 定时器触发、注入在 source 彼时的读取位置,同一逻辑切换点在两次 attempt 中落在数据流的不同位置(恢复后 source 追赶积压,第一个 barrier 到来前可能已按旧 plan 重放大量数据)。要消除必须把切换绑到数据位置而非 cp,而 Flink 中不存在跨 source/分区/subtask 良定义的全局数据位置——cp barrier 正是它的替代品,代价即切面位置跨 attempt 不可复现。

## 两阶段切换协议:全局同 cp 生效

切换边界的所有问题(跨 subtask 歪斜、恢复二义、切换动作丢失)有同一个根源:**"在哪个 cp 切换"如果由各 subtask 在 barrier 现场按"事件先到还是 barrier 先到"临时决定,这个决定既不全局也不持久**。两阶段协议把它变成 coordinator 提前一个 cp 持久化的全局决定:

```plaintext
阶段一 stage(哪个 plan)
  更新到达 coordinator:校验 → staged = 新 plan(不向任何 subtask 发送)
  下一个 checkpoint K:coordinator 快照持久化 {active=旧, staged=新}
  K 完成(决定已持久)→ coordinator 向所有 subtask announce 切换指令

阶段二 switch(哪个 cp)
  subtask 收到指令:静态 prepare(不进入 CPython)→ pendingPlan
  checkpoint K+1:通过静态 prepare 的 subtask 在 K+1 的 barrier 处 drain + Python artifact reload + 切换
  coordinator 在 K+1 快照中记录激活:{active=新, staged=空}

```

**为什么在全部 subtask 静态 prepare 成功时,它们必然在同一个 cp(K+1)切换**——不是"时间够长大概率都收到",而是靠确定的上下界保证的:

1.  **指令到达的下界**:announce 发生在 K 完成之后,而 K 完成意味着每个 subtask 都已做完自己的 K 快照——所以每人收到指令都在自己的 K 之后,不会有人在 K 或更早切。

2.  **K+1 barrier 的上界**:Flink 触发 cp 的顺序是"先等所有 coordinator 的快照 future 完成,才向 source 注入 barrier"。coordinator 在完成自己的 K+1 快照前,核对所有 announce 的送达确认(`sendEvent` 返回的 ack future),未确认则不放行。于是"每个 subtask 收到指令"在因果上先于"任何 K+1 barrier 存在",更先于"barrier 到达该 subtask"。

这样,每个 subtask 都在 (K, K+1) 区间收到指令;若全员 prepare 成功,则全部在 K+1 的 barrier 切换。**持久化买的则是另一半——crash 后的确定性**:staged 写进 K 的快照后,恢复到 K 的 coordinator 完整知道"有一个已宣布的切换未完成",重新 announce 即可续上;恢复到 K 之前则 staged 从未存在,世界干净。切换决定永远不会处于"发生过但没人记得"的状态;但 coordinator 目前不知道 subtask prepare 是否成功,这是另一个独立的 MVP 缺口。

配套规则:

*   **单更新在途**:staged 存在且未激活期间,新更新被拒绝(客户端重试)。状态机始终至多两个槽位,无版本链。

*   **激活的记录时机**:active=staged 写在 K+1 的快照里。在全员 prepare 成功的正常路径下,"从 K+1 恢复"等价于"K+1 完成过"等价于"所有人已在 K+1 barrier 切换",快照内容与运行事实一致;prepare 分叉时该等价关系不成立。

*   **abort 处理**:staging cp(K)abort → 决定未持久,不 announce,staged 随下一个成功 cp 再持久再宣布,切换顺延,无不一致。激活 cp(K+1)abort → 残余窗口,见一致性契约。

*   **恢复重宣(只重宣 staged)**:恢复出的 staged 必然是持久过的,`executionAttemptReady` 时重发它,续上被打断的切换;subtask 的 planVersion 检查使重发幂等。active **不重发**——生效 plan 每个 subtask 从自身 union state 恢复。在全员 prepare 成功时,完成的 cp 中 coordinator active 与所有 subtask 版本相等;prepare 分叉时不做该保证,恢复会按 union state 的 max-version 选择。初次启动、恢复、failover 单 subtask 重启走同一条路径。

*   **代价**:正常路径从一个 cp 延后到两个成功 cp(stage 要先搭一班 cp);由于 cp abort、drain 和 reload 均可以继续延后切换,墙钟延迟无上界。协议要求 `execution.checkpointing.max-concurrent-checkpoints = 1`,当前代码不主动校验。

## 状态与快照

新增一份算子状态,一份 coordinator 状态:

| 状态               | 位置                   | 内容                                                         | 写入时机                         | 恢复语义                                                     |
| ------------------ | ---------------------- | ------------------------------------------------------------ | -------------------------------- | ------------------------------------------------------------ |
| dynamic-agent-plan-current | AEO union list state   | `{version, canonicalPlanJson}`,每 subtask 至多一条 | 每次 snapshotState(发生过切换后) | union 恢复后各 subtask 看到全部条目,实现取 max-version;全员 prepare 成功时条目同版本,max 只是去重。prepare 分叉时可以出现混合版本,max 会选最新 plan,但不保证它与旧版本 subtask 快照中的在飞链匹配;状态为空 = 从未切换,用 job graph 静态 plan |
| 两阶段状态         | coordinator checkpoint | `{active, staged}`(各为 planVersion + planId + canonicalJson) | coordinator snapshot             | active 是版本铸造的基,并记录 coordinator 认定的激活事实;在全员 prepare 成功的正常路径下,它等于所有 subtask 的生效版本。**active 的内容从不发给 subtask**——生效 plan 各 subtask 从自身 union state 恢复。staged 存在则恢复后重新 announce,续上被打断的切换 |

设计要点:

*   **为什么不是 keyed state**:currentPlan 是 subtask 级概念而不是 per-key 概念,keyed state 会把同一份 JSON 复制到每个 key,且 rescale 时按 key group 迁移毫无意义。

*   **为什么是 union 而不是普通 list state**:扩并发时普通 list state 做 round-robin 切分,新增 subtask 可能分不到任何条目,恢复不出 currentPlan;union 让每个 subtask 都能看到全量。在全员 prepare 成功的正常路径下,两阶段协议使完成的 cp 中所有条目同版本,max 只是去重。当前无 prepare nack,环境分叉仍能产生混合版本 cp;max 只能让恢复后的 plan 选择收敛,不能保证在飞链的版本兼容。恢复范围由 Flink failover strategy 决定,不假设 keyed operator 异常必然导致全局 failover。

*   **union 的体积代价可控**:每 subtask 只有一条(不是 POC 多版本方案的 N 版本),checkpoint 增量 ≈ 并行度 × plan JSON 大小。若规模敏感,可做 single-writer 优化(只 subtask 0 写,union 恢复语义不变)。

*   **durable execution 的跨版本缓存防护(定位:防投毒,不是可回放保证)**:ActionState 外部存储(Kafka/Fluss)不随 checkpoint 回滚。激活 cp abort 的残余窗口里,同一 `(key, seqNum, action)` 可能先后在两个版本下执行,重放时可能命中另一版本写下的缓存结果——ActionState key 纳入 planVersion 后不同版本互不复用，重放时干净重算。**如实声明**:这只防"错误复用缓存",防不了"同一条数据在 fo 前后被不同版本处理"本身;严格可回放性本设计不具备——做到它要求数据只在其 plan 决策持久化之后才被消费(等价于消费等 notify checkpoint complete，代价过高 MVP 不包括)。不使用 action store 的作业无此顾虑。

*   **备选方案(评估后未采纳):subtask 不存 plan,启动时从 coordinator 获取**。在全员 prepare 成功的正常路径下,coordinator 的 `{active, staged}` 足以表达全局决定,subtask 的 union state 在语义上接近冗余;但当前无 prepare nack 时,coordinator active 不一定等于每个 subtask 的实际版本。即使不考虑这个 MVP 缺口,删除 subtask 状态仍会引入启动时序问题:subtask 必须在收到 coordinator 下发的 active 之前 hold 住全部输入(operator event 与首批数据的到达顺序没有保证),平添一个启动握手/阻塞阶段;且 coordinator 需要下发 active、staged 两条消息,subtask 要区分"立即生效"和"下一个 barrier 生效"两种语义,消息协议随之分叉。保留 union state 后:subtask 启动即可处理数据,生效 plan 自己答;coordinator 只负责版本铸造、`{active, staged}` 状态机与恢复时重宣 staged,不承担向 subtask 提供生效 plan 内容的职责。

## 恢复流程

### 启动流程(初次启动与恢复统一)

subtask 侧不区分"初次启动/失败恢复/rescale 恢复"——三者走同一流程,差别只在有没有状态可读:

1.  `initializeState`:从 union state `dynamic-agent-plan-current` 读取 `{version, canonicalPlanJson}`,取 max-version 条目反序列化为 currentPlan;状态为空(初次启动或从未切换)则用 job graph 静态 plan。**后续 `open()` 里所有 plan 级构建逻辑(ResourceCache、BuiltInMetrics、PythonBridgeManager、EventRouter 路由)均以 currentPlan 为有效 plan**。

2.  keyed state 恢复在飞事件链(actionTasks、pendingInputEvents、processingKeys),`tryResumeProcessActionTasks()` 照旧续跑。**恢复出在飞链是常态**——drain 只发生在切换 cp,普通 cp 不 drain(否则每个 cp 都要等最慢的 LLM 调用)。在全员 prepare 成功的正常路径下,切换与快照在同一 prebarrier 完成,快照里在飞链的版本与 currentPlan 一致(切换 cp 的快照则在飞链为空)。prepare 分叉形成混合 union state 时,max-version 恢复可能与旧版本 subtask 的在飞链错位,当前 MVP 不保证该路径。

3.  coordinator 从自身 checkpoint 恢复 `{active, staged}`,在每个 subtask `executionAttemptReady` 时**只重宣 staged(若存在)**,续上被打断的切换。生效 plan 由 subtask 自身状态回答,未完成的切换由 coordinator 回答;active 不重发。在全员 prepare 成功的正常路径下,coordinator active 与所有 subtask 快照版本相等;prepare 分叉时不做该保证。

### 逐个失败窗口走查

以 stage cp = K、激活 cp = K+1 为记号。下表描述全部 subtask 静态 prepare 成功的正常路径;prepare 环境分叉的后果见一致性契约:

| crash 时点                      | 恢复来源 | coordinator 恢复出        | subtask 恢复出 | 结果                                                         |
| ------------------------------- | -------- | ------------------------- | -------------- | ------------------------------------------------------------ |
| 更新到达前                      | 任意 cp  | {active}                  | 同版本         | 与更新无关,原语义                                            |
| 更新已接受、K 完成前            | K-1      | {active=旧},staged 未持久 | 旧             | 更新丢失(从未 durable),客户端重发完整 JSON;内容 planId 不变,未持久的 planVersion 可能重用 |
| K 完成后、K+1 完成前            | K        | {active=旧, staged=新}    | 旧             | 恢复后重新 announce;在本表约定的全员 prepare 成功前提下,于恢复后的下一个成功 cp 全体切换——切换被打断但不丢、不歪 |
| K+1 完成后                      | K+1      | {active=新}               | 新             | 切换已完成,快照与运行事实一致                                |
| K abort(作业未失败)             | \-       | staged 未持久,不 announce | 无感知         | 切换顺延到下一个成功 cp,无任何不一致                         |
| K+1 abort、作业继续、其后 crash | K        | {active=旧, staged=新}    | 旧             | **残余窗口**:K+1 barrier 后已按新 plan 处理的数据将按旧 plan 重放,直到恢复后的切换完成;缓存已按 planVersion 隔离。堵死需 fail-on-abort(可选强化),MVP 容忍并文档化 |

### rescale(改并发恢复)

*   currentPlan:union 全量分发 + canonicalPlanJson 按 version 去重,新 subtask 一样恢复出 max-version plan;体积与恢复放大分析见状态章节。

*   在飞链:沿用现有机制——`currentProcessingKeys` 本来就是 union state,恢复后按 key group 过滤本 subtask 拥有的 key 续跑(`tryResumeProcessActionTasks`),keyed 的 actionTasks/pendingInputEvents 随 key group 迁移。全员 prepare 成功时,完成的 cp 内条目同版本,迁移过来的链在同一 plan 下续跑,无需 per-key 版本标记。prepare 分叉时这个简化不成立,当前 MVP 依赖生产环境一致性避免该路径。

## Proposed Changes

### 新增:PlanUpdateCoordinator(两阶段状态机)

JM 侧 OperatorCoordinator,更新链路唯一入口、切换决策唯一持有者。状态只有 `{active, staged}` 两个槽位;正常路径下,一个更新从提交到生效横跨两个成功 checkpoint,按时间线列出每一步及承载它的回调(记 staging cp = K、激活 cp = K+1):

| 时刻 | 回调 | 动作 |
| --- | --- | --- |
| 提交 | `handleCoordinationRequest` | 接收 `PlanUpdateRequest(planJson)`(Java `AgentPlanClient`;Python facade 经 PyFlink 调用同一 Java client);静态校验(canonical 化 + 反序列化),失败同步拒绝;通过则 **staged = 新 plan**(planVersion = active+1),**不向任何 subtask 发送**。Java 和 Python 都复用 Java `RestClusterClient` 的序列化与发送链路,没有 Python 手工构造 wire 格式的旁路。门禁:cp 进行中、或已有 staged 未生效 → 拒绝(单更新在途) |
| cp K 快照 | `checkpointCoordinator(K)` | 持久化 `{active=旧, staged=新}` ——**切换决定落盘** |
| cp K 完成 | `notifyCheckpointComplete(K)` | **announce**:把 staged 发给所有 subtask(决定先持久、后可见);若 K abort(`notifyCheckpointAborted`)则不发,staged 留在槽里随下一个成功 cp 再落盘再宣布 |
| cp K+1 快照 | `checkpointCoordinator(K+1)` | ① **送达门禁**:等全部 announce 的 ack future,未确认不完成快照 future——Flink 要等 coordinator 快照完成才注入 barrier,故 K+1 的 barrier 不会先于任何 subtask 收到指令而存在;② **记录激活**:active←staged,staged 清空;③ 持久化 `{active=新}` |
| K+1 barrier | (无 coordinator 动作) | 收到并成功 prepare 的 subtask 在此 drain + quiesce/关闭 V1 + Python artifact reload + 切换;送达门禁保证人人手里有指令,但当前没有 prepare nack,只有全员 prepare 成功时才保证全体切换 |
| 恢复 | `resetToCheckpoint` + `executionAttemptReady` | 恢复 `{active, staged}`;staged 存在(必然持久过)→ 向每个 ready 的 subtask 重宣,续上被打断的切换;active 不重发。初次启动、恢复、单 subtask failover 走同一路径 |

注意同一个回调在两个 cp 扮演不同角色:`checkpointCoordinator` 对 K 是"落盘决定",对 K+1 是"门禁 + 记账激活"。实现并不区分 K 与 K+1,统一规则是:**快照前先清送达账;staged 已宣布即激活;总是持久化当前 `{active, staged}`**——两个角色由这组规则自然涌现,这也是状态机不需要记 cp 编号的原因。

### 修改:ActionExecutionOperatorFactory

实现 `CoordinatedOperatorFactory`,提供 coordinator provider。不设启用开关,coordinated 接线是唯一路径;factory 固定 `ChainingStrategy.NEVER`,`CompileUtils` 将 AEO 放入专用 slot-sharing group,避免和其他 task/operator 共用 Python 进程。生产还必须配置每 TM 1 slot、足够的专用 group 容量、`execution.checkpointing.max-concurrent-checkpoints = 1`,并保证进程内只有一个 AEO Python owner。即使从不推送更新,NEVER chain 与专用 group 也会增加 task/network 边界和 slot 需求。存量作业停留在旧版本代码上不受影响,升级到含本特性的版本会改变算子 uid/OperatorID,旧 checkpoint 不兼容,需全新启动。

### 修改:ActionExecutionOperator

**控制面(新增的钩子):**

1.  `handleOperatorEvent(PlanUpdateEvent)`:planVersion ≤ 已知版本则忽略(幂等/重发去重);否则调 `PlanVersionManager.preparePending(planVersion, planId, canonicalPlanJson)`(见下),将 `dynamic-plan.python-runtime.path` 钉回 bootstrap 值,并校验 Java/Python artifact path/sha256 成对与 artifact 不可原地覆盖。成功则记为 pendingPlan(newest-wins,废弃旧 pending 的 Java 资源);本步骤不创建 Python interpreter、不 import 用户模块。

2.  `prepareSnapshotPreBarrier(checkpointId)`:pendingPlan 存在时,先无界 drain(复用 `waitInFlightEventsFinished`)并重查 V2 Java/Python artifact 元数据/摘要,再依次 quiesce 旧 Python executor、确认 durable/action context 已清空、close/reset 旧 Java runner context、关闭旧 ResourceCache/ClassLoader、以 `switch_python_artifact(None)` retire 旧模块图/path/cache并关闭旧 interpreter、创建新 interpreter并初始化框架 helper、以 `switch_python_artifact(V2 path)` 挂载新 artifact、创建 executor/context并预解析全部 Python action 入口;全部成功后才调 PlanVersionManager.switchToPending并刷新 durable 的 activePlanVersion。任一步失败都向上抛出,使 task/checkpoint 失败并从上一 completed cp 恢复,不在已拆除 V1 的本 attempt 内局部回退。

3.  `snapshotState`:union state `dynamic-agent-plan-current` 写入 `{version, canonicalPlanJson}`。

4.  `initializeState`:恢复 union state 取 max-version 得 currentPlan。

5.  `close`:按切换时的依赖顺序 quiesce 当前 Python executor,再关闭 ActionTaskContextManager、PlanVersionManager(current/pending 资源)和 PythonBridgeManager/interpreter。

**执行面(所有静态 `agentPlan` 读点改经 PlanVersionManager,改动面较大,逐一列出):**

| 读点                                | 原来                                                         | 现在                                                         |
| ----------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| `open()` 全部构建                   | 静态 `agentPlan`(keyed state TTL、metrics、durable store、Python bridge、context manager) | `planManager.currentPlan()`(即恢复出的生效 plan)             |
| `processElement` 输入包装           | `pythonBridge.getPythonActionExecutor()`                     | PythonBridgeManager 单一 active runtime 的 executor           |
| 路由查询 `getActionsTriggeredBy`    | 静态 `agentPlan`                                             | `planManager.currentPlan()`                                  |
| 输出事件转换                        | 同上 executor                                                | 当前 plan 的 executor                                        |
| runner context 创建(每 action task) | 静态 plan + 算子级 resourceCache + 算子级 ltm                | 当前 plan + 当前 plan 的 ResourceCache + 当前 plan 的 ltm    |
| `actionTask.invoke`                 | 用户 ClassLoader + 算子级 executor                           | 当前 plan 的 ClassLoader(artifact)+ 当前 plan 的 executor    |
| BuiltInMetrics per-action 注册      | open 时按静态 plan 全量注册                                  | computeIfAbsent 懒注册,新 plan 新 action 首次执行自动补      |

算子级字段 `resourceCache`/`ltm` 取消,统一经 PlanVersionManager/PythonBridgeManager 按当前 plan 读取,杜绝切换后读到旧引用。

### 新增:PlanVersionManager

收敛 plan 生命周期为一个组件,按单版本语义实现:

1.  持有 currentPlan / pendingPlan 两槽位及各自 planVersion。

2.  `preparePending(planVersion, planId, canonicalPlanJson)`:校验 planId 与 JSON,反序列化;以启动 config 为基底,只接受 Java/Python artifact path + sha256 四个 plan 级键(`dynamic-plan.python-runtime.path` 仍取 bootstrap 值);构建 plan 级 ClassLoader并校验 jar 摘要与 Java action 可执行性(Class.forName);按新 plan 建空 ResourceCache(懒加载)。PythonBridgeManager 同时只做 metadata/transition 校验,不创建 interpreter、不触碰 `sys.modules`/`sys.path`。任一步失败即整体丢弃半成品 pending 资源,不影响 currentPlan。

3.  激活时先由 ActionExecutionOperator 在 barrier 内关闭 current 的 plan 级资源,由 PythonBridgeManager 完成 V1 retire 和 V2 runtime/action 入口解析;之后 `switchToPending` 才换 current/pending 引用。逻辑 currentPlan 在 V2 完整可执行前不变,但 V1 物理资源已被拆除,所以后续失败必须 fail task并靠上一 completed checkpoint 恢复。

4.  `currentPlan/currentPlanVersion/currentClassLoader/currentResourceCache`:执行路径读取当前 plan 及其 Java 资源的统一入口;config 和 action config 由当前 AgentPlan 传入 runner context 后读取。Python runtime 由 PythonBridgeManager 的单一 `activeRuntime` 持有,不再通过 plan key 寻址。

5.  快照/恢复 `dynamic-agent-plan-current` union state。

### 修改:ActionTaskContextManager

Java runner context 会被同一 plan 的多个 action task 复用,并持有创建时的 AgentPlan、ResourceCache 和 long-term memory。因此激活 barrier drain 后必须断言 action-task memory/continuation/Python awaitable 映射均已清空,关闭并丢弃 V1 Java runner context;V2 首个 Java action 再按当前 plan/cache 懒建新 context。Continuation executor 是作业级资源,drain 后继续复用。

### 修改:DurableExecutionManager / ActionStateUtil

barrier 切换前断言没有遗留的 durable action context;ActionState key 纳入 planVersion(跨版本缓存隔离,防 cp abort 窗口的重放串用,见状态章节)。

### 无需修改(自动适配)

*   EventRouter 路由查询:`getActionsTriggeredBy(event, plan)` 已按次传参。

*   BuiltInMetrics:per-action computeIfAbsent 懒注册,新 action 自动补。

### 配置与部署边界

配置边界如下:

| 配置 | scope | 说明 |
| --- | --- | --- |
| `dynamic-plan.java-artifact.path` / `.sha256` | plan 级 | 必须成对出现;同一物理文件不可换摘要,激活 barrier 前复核 |
| `dynamic-plan.python-artifact.path` / `.sha256` | plan 级 | 必须成对出现;仅支持 immutable regular file,同一路径不可换摘要,新内容必须使用新物理路径 |
| `dynamic-plan.python-runtime.path` | 作业级 | 只取 bootstrap plan 的值,运行中更新不能修改 |
| `taskmanager.numberOfTaskSlots` | 部署级 | 生产要求为 1;同时要按 slot-sharing group 分别准备足够 TM/slot |
| `execution.checkpointing.max-concurrent-checkpoints` | 协议级 | 必须为 1;当前实现未主动校验 |

运行时成本盘点,以从不推送更新的作业为基准:coordinator 是 JM 内空闲对象(active/staged 均空,backfill 不发任何事件);union state 在从未切换时不写条目;算子侧新增事件 handler、checkpoint 判空和 PlanVersionManager 间接读取,这些增量较小。但总成本不能概括为"可忽略":AEO 始终 `ChainingStrategy.NEVER`并使用专用 slot-sharing group,即使从不切换 plan,也会增加 task/network 边界,并要求集群为 AEO 与其他 group 分别提供 slot。不设开关是为了保持统一接线和部署契约,而不是因为没有拓扑/资源成本。
