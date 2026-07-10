# Part.1 AgentPlan更新链路设计

想要支持 AgentPlan 的动态更新,首先要解决的问题是用户如何将更新的信息发送给运行中的 AEO 算子。本文档只讨论这条**外部链路**:从用户手里的一份新 AgentPlan JSON,到每个 AEO subtask 内存里出现一个待生效的 pendingPlan 为止。收到之后如何切换、如何与 checkpoint 配合、如何恢复,见 Part.2。

这条链路几乎全部由 Flink 自身组件构成(REST endpoint、OperatorCoordinator、SubtaskGateway),与 Agent 语义无关——这正是把它单独拆成一个文档的原因:边界清楚,可以独立演进。

## 链路要求

1. 不重启作业、不改 job graph 就能把完整的 AgentPlan JSON 送达每个 AEO subtask。
2. 提交是同步可反馈的:非法的 plan 要在提交时被拒绝并告知原因,而不是投出去之后作业悄悄不生效。
3. 更新有全局唯一的先后顺序(多次提交、并发提交不能产生歧义)。
4. 作业 failover、subtask 重启、扩缩容之后,已提交的最新更新不能丢。
5. Java 和 Python 用户都能用,不给 Python 用户强加 JVM 依赖。

## 通道选型:OperatorCoordinator 广播

候选方案对比:

| 通道 | 不重启 | 提交反馈 | 全局定序 | 失败恢复 | 额外依赖 | 结论 |
| --- | --- | --- | --- | --- | --- | --- |
| 重新提交作业 | ✗ | - | - | - | 无 | 这正是要消灭的现状 |
| Broadcast stream(控制流拓扑) | ✗(拓扑需提交时规划,已运行作业加不进去) | ✗(fire-and-forget) | 需自建 | 随 broadcast state | 需要一个更新源(Kafka 等) | 否决 |
| 外部存储轮询(Kafka/DB,AEO 定期拉) | ✓ | ✗(写入成功≠校验通过) | 需自建 seq | 需自建 | Kafka/DB 必选化 | 否决 |
| **OperatorCoordinator + REST CoordinationRequest** | ✓ | ✓(同步返回,校验失败即拒绝) | coordinator 单点铸造 seq | coordinator 状态随 checkpoint;事件与 cp 有 epoch 一致性 | 无(JM 内组件,Flink 原生) | **采用** |

OperatorCoordinator 是 Flink 为"JM 侧组件与算子 subtask 通信"提供的原生机制(Source 的 split 分配就用它)。它恰好逐条满足上面的要求,并且有两条别的通道给不了的保证(均经 Flink 2.2.1 源码核实,`OperatorCoordinatorHolder`/`SubtaskGatewayImpl`,2.3 待复核):

*   **事件与 checkpoint 的 epoch 一致性**:coordinator 快照前发出的事件必在该 cp 完成前确认送达(否则 cp 失败/触发 subtask failover),快照后的事件被缓冲到 subtask ack 该 cp 之后才投递。这让"更新"和"作业状态"之间没有脱节的中间态。
*   **恢复语义现成**:coordinator 状态随 checkpoint 快照(`checkpointCoordinator`/`resetToCheckpoint`);Flink 恢复后不重放事件,但 coordinator 会在每个 subtask `executionAttemptReady` 时收到通知——backfill 重发的挂载点就是它。

## 链路全景

```
 用户(Java client / 纯 Python 脚本 / curl)
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
      → 所有 subtask 在 checkpoint K+1 的 barrier 处切换
```

wire 类型只有三个(`PlanUpdateMessages.java`),全部已实现:

*   `PlanUpdateRequest { String agentPlanJson }` — 客户端 → coordinator。
*   `PlanUpdateEvent { long planVersion, String planId, String canonicalPlanJson }` — coordinator → subtask。全量 JSON 随事件下发,subtask 不需要回源取内容。
*   `PlanUpdateResponse { long planVersion, String planId, int subtasksNotified }` — coordinator → 客户端。

## 作业侧接线

不需要任何开关或额外 API:`CompileUtils.connectToAgent` **始终**走 `CoordinatedActionExecutionOperatorFactory`——它实现 `CoordinatedOperatorFactory`,向作业挂一个 `PlanUpdateCoordinator`,并把 AEO 注册为 operator event handler;算子 uid 固定为 `flink-agents-coordinated-operator`,供客户端寻址(`CompileUtils.java`)。

不设 feature flag 的依据:运行时成本可忽略(JM 内空闲 coordinator、从未切换时 union state 为空)。存量作业停留在旧版本代码上不受影响;升级到含本特性的版本会改变算子 uid/OperatorID,旧 checkpoint 不兼容,需全新启动——"老作业用老版本"是发布层面的约定,写入发布说明。

e2e 已验证接线的唯一性:开启后 job graph 中恰好一个 vertex 挂 coordinator,且就是 agent 算子(`CoordinatedPlanUpdateMiniClusterE2ETest`)。

## 客户端

### Java

```java
try (RestClusterClient<String> client = new RestClusterClient<>(conf, "cluster")) {
    PlanUpdateResponse resp = (PlanUpdateResponse) client.sendCoordinationRequest(
            jobId, CompileUtils.COORDINATED_OPERATOR_UID,
            new PlanUpdateRequest(agentPlanJson)).get();
}
```

已验证:`CoordinatedPlanUpdateRestE2ETest#javaClientRestSwitchesPlan`(真实 MiniCluster,提交后下一个 checkpoint 生效)。

### 纯 Python(无 JVM)

`PlanUpdateRequest` 只有一个 String 字段且 `serialVersionUID` 固定,Java 序列化字节布局因此是常量前缀 + 长度前缀 UTF-8 字符串,可以在 Python 里手工构造,不需要任何 Java 依赖(`e2e-test/test-scripts/send_plan_via_coordinator.py`):

1.  `GET /jobs/{jobId}` 按算子名 `coordinated-action-execute-operator` 找到 vertex id(即 OperatorID hex;不同 Flink 版本的 `:operatorid` 路径段有的收 hex 有的收 uid,脚本先试 hex 再回退 uid)。
2.  手工构造 Java 序列化字节,base64 后包进 `{"serializedCoordinationRequest": ...}`。
3.  `POST /jobs/{jobId}/coordinators/{operatorId}`。

已验证:`pythonRequestBytesMatchJava`(Python 构造的字节与 Java `InstantiationUtil.serializeObject` 逐字节相等)和 `pythonRestSwitchesPlan`(纯 Python 提交在真实集群上完成切换)。

**兼容性契约**:`PlanUpdateRequest` 的全限定类名、字段名、字段类型、`serialVersionUID` 均不可改动——任何改动都会破坏纯 Python 客户端的字节兼容。改协议 = 新增消息类型,不是修改现有的。

### 提交物

每次提交是**一份完整的 AgentPlan JSON**(全量快照,不是增量 patch):actions、actionsByEvent、resourceProviders、config 一次全带。若更新包含新代码,先把 artifact(jar / Python 文件)放到 TM 可访问的路径,并在 plan config 里声明位置与摘要(`dynamic-plan.java-artifact.path`/`.sha256`、`dynamic-plan.python-artifact.path`/`.sha256`,校验在 subtask prepare 阶段执行,见 Part.2)。全量提交让 planId = sha256(canonicalJson) 成为内容身份:重复提交同一份 plan 幂等,回滚 = 把旧版 JSON 原样再提交一次(会得到新 planVersion、同 planId)。

## 投递与定序语义

*   **定序**:所有更新经 coordinator 单点串行化,planVersion 全局单调。并发提交谁后到 coordinator 谁版本大,最后者胜——这与 newest-wins 的 subtask 语义一致。

*   **单更新在途,两类拒绝**:cp 进行中提交被拒;已有 staged 未生效时提交也被拒(两阶段协议一次只处理一个更新)。两者都同步返回、可重试,切换决策收敛为 JM 单点(单测:`PlanUpdateCoordinatorTest`)。客户端把"被拒后短暂等待重试"作为标准用法。

*   **投递时机由两阶段协议决定**:接受的更新先 stage、随下一个 checkpoint 持久化,该 cp 完成后才 announce 给 subtask;再下一个 cp 的 barrier 处全体切换。协议细节与"为什么全体必然同一个 cp 切换"的因果链论证见 Part.2 的"两阶段切换协议"一章。
*   **投递**:对每个 subtask 是 at-least-once(提交时立即 send + 重启后 backfill 重发),subtask 端 planVersion 检查把重复变 no-op,整体呈现 exactly-once 效果(单测:`DynamicPlanSwitchOperatorTest#staleOrDuplicateBackfillIsIgnored`)。
*   **backfill 是恢复的唯一机制**:Flink 恢复后不重放 operator event;coordinator 在每个 subtask `executionAttemptReady` 时重宣未完成的 staged(生效 plan 无需重发——subtask 从自身状态恢复,见 Part.2 启动流程)。subtask 的 planVersion 检查使重发幂等。恢复路径与首次投递共用同一段代码,无特殊分支(`PlanUpdateCoordinator#executionAttemptReady`)。
*   **事件与 barrier 无严格到达顺序**:operator event 走 RPC 直达,barrier 沿数据流传播,两者到达某 subtask 的先后不保证。这不影响本链路的正确性,只影响生效时点落在哪个 cp epoch——由 Part.2 的 checkpoint 对齐切换兜住。

## 响应语义:接受 ≠ 生效

客户端收到成功响应表示:**校验通过、planVersion 已铸造、已 staged**。它不表示已生效——生效发生在提交后第二个 checkpoint 的 barrier(第一个 cp 持久化决定,第二个 cp 全体切换,见 Part.2 两阶段协议)。`subtasksNotified` 只是提交时刻已 ready 的 gateway 数,信息性字段。

诚实的边界,按失败点走查:

| 场景 | 结果 |
| --- | --- |
| 校验失败 | 同步收到异常(HTTP 层为错误响应),集群无任何变化 |
| checkpoint 进行中 / 已有 staged 未生效时提交 | 同步拒绝,提示重试;未铸 planVersion,集群无任何变化 |
| 提交成功,staging cp 完成前 JM failover | staged 从未持久,更新丢失;**客户端重发即可**(全量+幂等,重发无副作用) |
| staging cp 完成后任意 failover | coordinator 恢复出 {active, staged},自动重新 announce、在恢复后的下一个 cp 完成切换,不需要客户端动作 |
| subtask 端 prepare 失败(artifact 缺失/sha256 不符/类加载失败) | 该 subtask 拒绝并打 WARN 日志 + `dynamicPlanUpdateRejected` 指标,继续跑旧 plan;**不回传客户端**(见下方 future) |

建议客户端把"提交后轮询确认"作为标准用法:提交 → 记住返回的 planVersion/planId → 通过日志/指标(或 future 的状态查询接口)确认生效。

## 校验分层

| 层 | 位置 | 内容 | 失败表现 |
| --- | --- | --- | --- |
| 静态校验 | coordinator(JM) | JSON canonical 化、反序列化为 AgentPlan | 同步拒绝,客户端立即可见 |
| 执行校验 | 每个 subtask 的 prepare(TM) | artifact 存在性与 sha256、Java action `Class.forName`、Python runtime 预建 | 日志 + 指标,该 subtask 保持旧 plan |

分两层是必然的:JM 没有 TM 本地的 artifact,可执行性只能在 TM 验;而 JSON 层面的坏 plan 没必要广播出去浪费所有 subtask 的校验。

## 已知限制与 future

*   **单作业单 agent 算子**:算子 uid 是固定常量,同一作业接两个 agent 会 uid 冲突。MVP 限定单 agent;future:uid 带 agent 名后缀,客户端按名寻址。
*   **subtask 端拒绝无回执**:coordinator 不知道 prepare 在各 subtask 的结果。future:subtask 经 `handleEventFromOperator` 回传 ack/nack,coordinator 聚合出"更新状态"供查询。
*   **无 plan 级鉴权**:沿用 Flink REST endpoint 自身的安全体系(能访问 REST 即能推 plan),不做更细粒度权限。
*   **cp 完成前的 JM failover 丢更新**:如上表,靠客户端重发闭环;如需免重发,future 可让 coordinator 在响应前主动等待一次 cp(代价是提交延迟)。
