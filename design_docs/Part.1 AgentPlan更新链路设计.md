# Part.1 AgentPlan更新链路设计

想要支持 AgentPlan 的动态更新,首先要解决的问题是用户如何将更新的信息发送给运行中的 AEO 算子。本文档只讨论这条**外部链路**:从用户手里的一份新 AgentPlan JSON,到每个 AEO subtask 内存里出现一个待生效的 pendingPlan 为止。收到之后如何切换、如何与 checkpoint 配合、如何恢复,见 Part.2。

这条链路几乎全部由 Flink 自身组件构成(REST endpoint、OperatorCoordinator、SubtaskGateway),与 Agent 语义无关——这正是把它单独拆成一个文档的原因:边界清楚,可以独立演进。

## 链路要求

1. 不重启作业、不改 job graph 就能把完整的 AgentPlan JSON 送达每个 AEO subtask。
2. 提交是同步可反馈的:非法的 plan 要在提交时被拒绝并告知原因,而不是投出去之后作业悄悄不生效。
3. coordinator 对接受的更新给出唯一先后顺序;同一时刻只允许一个 staged 更新,并发请求不会产生歧义。
4. 更新随 checkpoint 持久化后,作业 failover、subtask 重启或扩缩容不能把它丢掉;持久化前的 JM failover 由客户端重发闭环。
5. Java 和 Python 用户都能用;Python 侧通过 PyFlink 调用同一个 Java 客户端,不要求用户手工构造 Java 序列化字节或直接拼 REST 请求。

## 通道选型:OperatorCoordinator 广播

候选方案对比:

| 通道 | 不重启 | 提交反馈 | 全局定序 | 失败恢复 | 额外依赖 | 结论 |
| --- | --- | --- | --- | --- | --- | --- |
| 重新提交作业 | ✗ | - | - | - | 无 | 这正是要消灭的现状 |
| Broadcast stream(控制流拓扑) | ✗(拓扑需提交时规划,已运行作业加不进去) | ✗(fire-and-forget) | 需自建 | 随 broadcast state | 需要一个更新源(Kafka 等) | 否决 |
| 外部存储轮询(Kafka/DB,AEO 定期拉) | ✓ | ✗(写入成功≠校验通过) | 需自建 planVersion | 需自建 | Kafka/DB 必选化 | 否决 |
| **OperatorCoordinator + REST CoordinationRequest** | ✓ | ✓(同步返回,校验失败即拒绝) | coordinator 单点铸造 planVersion | coordinator 状态随 checkpoint;事件与 cp 有 epoch 一致性 | 无(JM 内组件,Flink 原生) | **采用** |

OperatorCoordinator 是 Flink 为"JM 侧组件与算子 subtask 通信"提供的原生机制(Source 的 split 分配就用它)。它恰好逐条满足上面的要求,并且有两条别的通道给不了的保证(均经 Flink 2.2.1 源码核实,`OperatorCoordinatorHolder`/`SubtaskGatewayImpl`,2.3 待复核):

*   **事件与 checkpoint 的 epoch 一致性**:coordinator 快照前发出的事件必在该 cp 完成前确认送达(否则 cp 失败/触发 subtask failover),快照后的事件被缓冲到 subtask ack 该 cp 之后才投递。这让"更新"和"作业状态"之间没有脱节的中间态。
*   **恢复语义现成**:coordinator 状态随 checkpoint 快照(`checkpointCoordinator`/`resetToCheckpoint`);Flink 恢复后不重放事件,但 coordinator 会在每个 subtask `executionAttemptReady` 时收到通知——backfill 重发的挂载点就是它。

## 链路全景

```
 用户(Java AgentPlanClient / Python facade)
      │  POST /jobs/{jobId}/coordinators/{operatorId}
      │  body: {"serializedCoordinationRequest": base64(java-serialized PlanUpdateRequest)}
      ▼
 JM REST endpoint(Flink ClientCoordinationHandler,原生)
      ▼
 PlanUpdateCoordinator.handleCoordinationRequest
      │  ① 门禁:cp 进行中 / 已有 staged 未生效 → 同步拒绝(可重试)
      │  ② canonical 化 JSON;反序列化为 AgentPlan(静态校验,失败→同步拒绝)
      │  ③ planId = sha256(canonicalJson);planVersion = active.planVersion + 1
      │  ④ stage:staged = {planVersion, planId, json}——此刻不向任何 subtask 发送
      ▼
 返回 PlanUpdateResponse {planVersion, planId, subtasksNotified}
      ▼(之后由两阶段协议驱动,见 Part.2)
 checkpoint K 持久化 {active, staged} → K 完成 → announce 给所有 subtask
      → 全部 subtask 静态 prepare 成功时,所有 subtask 在 checkpoint K+1 的 barrier 处切换
```

wire 类型只有三个(`PlanUpdateMessages.java`),全部已实现:

*   `PlanUpdateRequest { String agentPlanJson }` — 客户端 → coordinator。
*   `PlanUpdateEvent { long planVersion, String planId, String canonicalPlanJson }` — coordinator → subtask。全量 JSON 随事件下发,subtask 不需要回源取内容。
*   `PlanUpdateResponse { long planVersion, String planId, int subtasksNotified }` — coordinator → 客户端。

## 作业侧接线

不需要任何开关或额外接线:`CompileUtils.connectToAgent` **始终**走 `CoordinatedActionExecutionOperatorFactory`——它实现 `CoordinatedOperatorFactory`,向每个 AEO 挂一个 `PlanUpdateCoordinator`,并把 AEO 注册为 operator event handler。

每个部署的 agent 都有一个稳定的 `agentName`:

*   Java `apply(agent)` 和 Python `apply(agent)` 默认使用 Agent 类的简单类名;匿名 Java 类没有简单类名,必须显式指定。
*   Java 可用 `apply("review-agent", agent)`,Python 可用 `apply(agent, name="review-agent")` 覆盖默认值;YAML/注册表路径默认使用注册名。
*   operator uid 由 `flink-agents-coordinated-operator:<agentName>` 确定,operator name 与专用 slot-sharing group 也带同一名称。同一作业内 `agentName` 必须唯一;同名会生成相同 uid,不能表示两个可独立寻址的 agent。

`AgentPlanClient` 用同一个 `agentName` 计算 uid,因此一个作业内的多个 agent 各自有 coordinator 状态与更新门禁,更新不会被广播给其他 agent。

不设 feature flag 是为了保证作业始终有统一的 coordinator 接线,不是因为成本为零。从不推送更新时,JM 内 coordinator 和 union state 的增量很小;但 AEO 始终 `NEVER` chain 且使用专用 slot-sharing group,会增加 task/network 边界,并要求集群为 AEO 与其他 group 分别提供 slot,这是为进程级 Python 重载隔离支付的固定部署成本。存量作业停留在旧版本代码上不受影响;升级到含本特性的版本会改变算子 uid/OperatorID,旧 checkpoint 不兼容,需全新启动——"老作业用老版本"是发布层面的约定,写入发布说明。

e2e 不只检查请求被 staged,而是在运行作业上按四个时点验证正常路径:提交前运行 V1、提交成功但 checkpoint K 前仍运行 V1、K 完成后仍运行 V1、K+1 完成后运行 V2。`JavaAgentPlanClientE2ETest` 在真实 MiniCluster 作业中同时部署 `target-agent` 和 `control-agent`,并验证只有指定的 target 在 K+1 后变为 V2;`agent_plan_client_test.py` 启动真实 PyFlink 流作业,通过 Python 客户端提交更新并验证同样的四个时点。这两个用例证明的是 checkpoint 都成功时的生效时序,不代表 cp abort 或 failover 路径也由它们覆盖。

## 客户端

### Java

```java
try (AgentPlanClient client = new AgentPlanClient(restAddress, restPort)) {
    AgentPlanClient.UpdateResult result =
            client.updateAgentPlan(jobId, "review-agent", agentPlanJson);
}
```

客户端接受完整 JSON,也提供直接接收 Java `AgentPlan` 的重载。`restAddress` 按 Flink `rest.address` 解释,即主机名或 IP;`restPort` 取值范围是 1–65535。客户端根据 `agentName` 定位 coordinator,封装 `RestClusterClient` 的请求、响应反序列化和异常解包。Flink `Configuration` 只是 Java 内部构造 `RestClusterClient` 时的实现细节,不是公开客户端参数;当前公开 API 也不提供 TLS、HA 或自定义 REST path 等额外连接配置。

### Python

Python API 是 Java `AgentPlanClient` 的薄 facade:

```python
with AgentPlanClient.create(rest_address, rest_port) as client:
    result = client.update_agent_plan(
        job_id, "review-agent", agent_plan_json
    )
```

`create` 把当前 Flink 版本对应的 Flink Agents jar 加入 PyFlink context classloader,再通过 `invoke_method` 把 `rest_address` 和 `rest_port` 传给同一个 Java 客户端。`update_agent_plan` 接受完整 JSON;`update_agent` 可从 Python Agent 生成 plan,未显式传 `name` 时同样使用类名。这条路径不依赖 `AgentsExecutionEnvironment` 实例,但仍然依赖 PyFlink gateway,并且必须能通过给定的 REST 地址和端口连接目标 JobManager。

### Flink 版本边界

`AgentPlanClient` 对外始终用 operator uid 寻址,版本差异收口在 dist 中同名的 `CoordinationRequestUtils`:

*   Flink 2.0–2.3 直接调用接收 uid 的 `sendCoordinationRequest` 重载。
*   Flink 1.20 的重载只接收 `OperatorID`,helper 用 `Murmur3_128(seed=0, UTF-8(operatorUid))` 计算与 Flink 构图一致的 ID 后提交。

版本选择是构建/发布层的事,不泄漏到 Java 或 Python 公开 API。

### wire 兼容性

`PlanUpdateRequest` 的全限定类名、字段名、字段类型和 `serialVersionUID` 均是冻结的协议契约;改协议 = 新增消息类型,不是修改现有类。Java 和 Python 公开客户端都复用 Java `RestClusterClient` 的同一条序列化链路,Python 侧不另行维护 Java 序列化实现。

### 提交物

每次提交是**一份完整的 AgentPlan JSON**(全量快照,不是增量 patch):actions、actionsByEvent、resourceProviders、config 一次全带。这里的"完整"只表示配置全量,不表示预先执行 artifact 中每个 Python 文件。若更新包含新代码,先把 artifact(Java jar / pure-Python zip/whl)放到 TM 可访问的路径,并在 plan config 里声明对应的 artifact path/sha256 键。Java 和 Python artifact 的 path/sha256 都必须成对出现,且它们是不可变制品:同一物理文件不允许换摘要,新内容必须使用新路径。Python artifact 中的 `.py` 条目决定它拥有的用户顶层 package/module;这些顶层名必须由该 artifact 独占,不得与框架、stdlib 或 site-packages 冲突。subtask 收到更新时只做不进入 CPython 的静态准备,不创建 interpreter、不 import 用户模块;完整模块图重载和 action 入口解析发生在激活 barrier 的 drain 之后,见 Part.2。未声明 Python artifact 的 plan 不会为自身猜测或清理用户模块根,因此不承诺代码热更新;若 V1 声明过 artifact,切出 V1 时仍会 retire 它已记录的 roots。全量提交让 planId = sha256(canonicalJson) 成为内容身份:同一份 JSON 始终得到同一 planId,但**提交操作本身不幂等**;一份已生效的 plan 再次提交会铸造新 planVersion 并再切换一次。回滚 = 把旧版 JSON 原样再提交一次(新 planVersion、同 planId)。

## 投递与定序语义

*   **定序**:coordinator 单点串行化请求,一次只允许一个 staged 更新;并发提交中一个被接受后,其他请求会因已有 staged 而被拒绝,由客户端稍后重试。已持久的 active 版本序列单调;但接受后、staging cp 完成前发生 JM failover 时,该 staged 和已返回的 planVersion 都未持久,后续请求可以重用这个版本号。

*   **单更新在途,两类拒绝**:cp 进行中提交被拒;已有 staged 未生效时提交也被拒(两阶段协议一次只处理一个更新)。两者都同步返回、可重试,切换决策收敛为 JM 单点(单测:`PlanUpdateCoordinatorTest`)。客户端把"被拒后短暂等待重试"作为标准用法。

*   **投递时机由两阶段协议决定**:接受的更新先 stage、随下一个成功 checkpoint 持久化,该 cp 完成后才 announce 给 subtask;全部 subtask 静态 prepare 成功时,再下一个成功 cp 的 barrier 处全体切换。协议细节与因果链论证见 Part.2 的"两阶段切换协议"一章。
*   **投递**:对每个 subtask 是 at-least-once(staging cp 完成后 announce + 重启后 backfill 重发),subtask 端 planVersion 检查把重复变 no-op,整体呈现幂等投递效果(单测:`DynamicPlanSwitchOperatorTest#staleOrDuplicateBackfillIsIgnored`)。
*   **backfill 是恢复的唯一机制**:Flink 恢复后不重放 operator event;coordinator 在每个 subtask `executionAttemptReady` 时重宣未完成的 staged(生效 plan 无需重发——subtask 从自身状态恢复,见 Part.2 启动流程)。subtask 的 planVersion 检查使重发幂等。恢复路径与首次投递共用同一段代码,无特殊分支(`PlanUpdateCoordinator#executionAttemptReady`)。
*   **事件与 barrier 无严格到达顺序**:operator event 走 RPC 直达,barrier 沿数据流传播,两者到达某 subtask 的先后不保证。这不影响本链路的正确性,只影响生效时点落在哪个 cp epoch——由 Part.2 的 checkpoint 对齐切换兜住。

## 响应语义:接受 ≠ 生效

客户端收到成功响应表示:**JM 静态校验通过、planVersion 已铸造、已 staged**。它不表示 Python 用户代码已经 import 或可执行,也不表示已生效。正常路径下,第一个成功 cp 持久化决定;全部 subtask 静态 prepare 成功后,第二个成功 cp 的 barrier 全体切换。cp abort、drain 不结束或 reload 变慢时,生效的墙钟延迟无上界。`subtasksNotified` 只是提交时刻已 ready 的 gateway 数,信息性字段。

诚实的边界,按失败点走查:

| 场景 | 结果 |
| --- | --- |
| 校验失败 | 同步收到异常(HTTP 层为错误响应),集群无任何变化 |
| checkpoint 进行中 / 已有 staged 未生效时提交 | 同步拒绝,提示重试;未铸 planVersion,集群无任何变化 |
| 提交成功,staging cp 完成前 JM failover | staged 从未持久,更新丢失;客户端重发完整 JSON 即可。内容的 planId 不变,但未持久的 planVersion 可能被重用 |
| staging cp 完成后任意 failover | coordinator 恢复出 {active, staged},自动重新 announce;全部 subtask 静态 prepare 成功时,在恢复后的下一个成功 cp 切换,不需要客户端动作 |
| subtask 端静态 prepare 失败(artifact 缺失/sha256 不符/Java 类加载失败/Python 元数据不合法) | 该 subtask 拒绝并打 WARN 日志 + `dynamicPlanUpdateRejected` 指标,继续跑旧 plan;**不回传客户端**。若各 TM 的 artifact/环境不一致,其他 subtask 可能已接受 pending,当前 MVP 不能阻止版本分叉(见下方 future) |
| 激活 barrier 内 Python runtime 创建或 action 入口解析失败 | 旧 runtime 已进入 quiesce/关闭流程,不能在本 attempt 本地回退;异常使 task 及本次 checkpoint 失败,受影响 task 按 Flink failover strategy 从上一个 completed checkpoint 恢复旧 plan;coordinator 保留的 staged 更新会在恢复后重宣并重试激活 |

建议客户端把"提交后轮询确认"作为标准用法:提交 → 记住返回的 planVersion/planId → 通过日志/指标(或 future 的状态查询接口)确认生效。

## 校验分层

| 层 | 位置 | 内容 | 失败表现 |
| --- | --- | --- | --- |
| 静态校验 | coordinator(JM) | JSON canonical 化、反序列化为 AgentPlan | 同步拒绝,客户端立即可见 |
| subtask 静态 prepare | 每个 subtask 收到 event 时(TM) | Java/Python artifact path/sha256 成对、文件存在性、摘要与不可原地覆盖;Java action `Class.forName`;Python zip/whl 格式;配置合并已把 `python-runtime.path` 钉回 bootstrap 值;不创建 interpreter、不 import 用户模块 | 日志 + 指标,该 subtask 保持旧 plan |
| 激活复核 | 激活 cp 的 `prepareSnapshotPreBarrier` | drain 后重查 Java/Python artifact 元数据/摘要,再清旧 Python artifact 模块图与缓存、创建新 runtime并预解析 plan 声明的 Python action 入口 | 使 task/checkpoint 失败,恢复旧 plan 后由 coordinator 重宣 staged 更新并重试 |

分层是必然的:JM 没有 TM 本地的 artifact,路径、摘要和可执行性只能在 TM 验;而 Python 用户模块不能在旧 plan drain 前导入,否则多个 interpreter 仍会通过同一 CPython 进程的 `sys.modules`/`sys.path` 相互影响。因此能脱离 CPython 完成的检查放在更新到达时,必须 import 才能完成的 action 入口解析放在激活 barrier,但仍早于新 plan 写入 subtask 快照。

## 已知限制与 future

*   **`agentName` 是部署身份**:同一作业可部署多个 agent,但名称必须唯一;更名会同时改变 operator uid,因此不是一次可保留状态的在线更名。
*   **Python reload 的部署前提**:coordinated AEO 使用专用 slot-sharing group 且禁止 chaining;生产部署要求每个 TM 只有 1 个 slot,并保证该 JVM/CPython 进程只有一个 AEO Python owner。详细限制见 Part.2。
*   **subtask 端拒绝无回执**:coordinator 不知道 prepare 在各 subtask 的结果。future:subtask 经 `handleEventFromOperator` 回传 ack/nack,coordinator 聚合出"更新状态"供查询。
*   **无 plan 级鉴权**:沿用 Flink REST endpoint 自身的安全体系(能访问 REST 即能推 plan),不做更细粒度权限。
*   **cp 完成前的 JM failover 丢更新**:如上表,靠客户端重发闭环;如需免重发,future 可让 coordinator 在响应前主动等待一次 cp(代价是提交延迟)。
