# distributed-consistency-coordinator-go

可嵌入的分布式一致性协调模块（Raft 风格），以本地多节点方式模拟集群，
在并发提交流程下保证领导选举、日志复制与故障恢复的一致性。

## 结构

```
raft/
  types.go         核心类型：角色、拒绝原因、日志条目（含成员变更条目）、消息、配置、StateMachine 接口
  storage.go       Storage 接口与 MemoryStorage（任期/投票/日志/快照/应用位点持久化）
  transport.go     Network：本地内存网络，支持断连、隔离、恢复（模拟分区与崩溃）
  node.go          节点核心：选举、日志复制、提交推进、状态机应用、提案幂等
  membership.go    在线成员变更：learner 加入/提升/移除、变更互斥与幂等
  snapshot.go      快照生成、安装与追赶（快照信封携带成员配置）
  statemachine.go  KVMachine 示例状态机
  *_test.go        选举/复制/重启/快照/成员变更测试与集群 harness
```

## 快速开始

```go
net := raft.NewNetwork()
store := raft.NewMemoryStorage()          // 重启时复用同一 store 即模拟磁盘
sm := raft.NewKVMachine()                 // 或实现 raft.StateMachine
n := raft.NewNode(raft.Config{
    ID: "N1", Peers: []raft.NodeID{"N1", "N2", "N3"},
    SnapshotThreshold: 1000,               // 日志超过该长度对已提交前缀做快照
    EnableLog: true,                       // 打印任期/投票/提交位点/快照边界
}, store, net, sm)
n.Start()
defer n.Stop()

res, err := n.Propose(ctx, raft.Command{
    ClientID: "client-1", Seq: 1,         // 幂等键：重复提案安全
    Op: raft.EncodeKVOp(raft.KVOp{Op: "set", Key: "x", Value: "1"}),
})
```

## 一致性不变量

1. **选举安全**：同一任期至多一个 leader。投票需满足：任期不落后、本任期未投给他人、
   候选人日志不落后（否则分别以 `StaleTerm`/`AlreadyVoted`/`LogBehind` 拒绝）。
2. **单主性**：多数派可达时必选出唯一 leader；收到更高任期消息的节点立即降级为
   follower，分区恢复后旧 leader 必然退位，不出现双主。
3. **日志匹配**：`AppendEntries` 携带 `(prevLogIndex, prevLogTerm)` 前置校验；
   冲突条目被截断并覆盖（日志中打印冲突索引与双方任期），相同条目幂等跳过。
4. **提交安全**：只有被多数派复制、且属于 leader 当前任期的条目才推进 `commitIndex`；
   已提交条目永不丢失、永不改变。
5. **状态机安全**：条目严格按索引顺序应用，已提交前缀只应用一次；
   重启后状态机 = 快照 + 快照后已提交日志回放，不偏离已提交前缀。
6. **提案幂等**：`(ClientID, Seq)` 去重。已应用过的序列号直接返回缓存结果；
   并发重复提案只应用一次，去重状态随应用位点持久化、随快照传播。
7. **快照安全**：快照只覆盖 `lastApplied`（必已提交）之内的状态；
   快照边界永远 ≤ `commitIndex`。
8. **成员变更安全**：任意两个相邻生效配置只差一个节点，二者多数派必然相交，
   变更期间不会出现两个都能推进提交的多数派；learner 在被提升前不计入多数派；
   被移除节点立即退出多数派且不能凭旧身份参选（见「在线成员变更」）。

## 在线成员变更

运行过程中可以不停集群增删节点。成员配置不是启动参数，而是**已提交的日志状态**：
初始集群由 `Config.Peers` 引导，之后的每次变更都是一条成员变更日志条目，
提交（apply）时生效，并随快照信封与日志回放在重启/追赶后重建。

```go
// 扩容：新节点进程以 learner 身份启动（不参与选举、不计入多数派）
n4 := raft.NewNode(raft.Config{ID: "N4", Learner: true /* ... */}, store4, net, sm4)
n4.Start()
// 在 leader 上调用：自动完成 加入learner → 等待追平（必要时走快照）→ 提升为投票成员
err := leader.AddNode(ctx, "N4")

// 缩容：提交后 N2 立即退出多数派；移除 leader 自身时其提交后自动退位
err = leader.RemoveNode(ctx, "N2")

// 查询当前已提交生效的成员配置
voters, learners := leader.Membership()
```

### 规则

1. **扩容两阶段**（`AddNode(ctx, id)`）：
   - 先提交 `AddLearner`：新节点以 `Config{Learner: true}` 启动后加入，
     作为**非投票 learner** 接收复制（落后太多时走 `InstallSnapshot`），
     **不计入多数派、不参与选举**；
   - leader 等它追平当前提交位点后，再提交 `Promote` 将其提升为投票成员，
     此后才参与多数派计算与选主。
2. **缩容单阶段**（`RemoveNode(ctx, id)`）：提交 `RemoveNode` 即生效，
   被移除节点立即退出多数派计算。leader 会继续向它复制到它**确认已应用移除条目**
   为止（回复中回显提交位点），使其亲知退出、停止参选；移除 leader 自身时，
   它在提交后自动退位，剩余多数派在一个选举超时内选出新 leader。
3. **单节点变更约束**：同一时刻只允许一条未提交的成员变更。本任期已有未提交
   变更时，新变更以 `ConfigBusy` 拒绝（调用方可重试）；换主后遗留的前任期未提交
   变更，会先由新 leader 提交一条空条目（noop）使其随提交位点推进一并生效，
   再继续新变更，不会卡死。
4. **幂等**：`AddNode`/`RemoveNode` 对已达成目标的调用直接返回成功；
   变更条目携带变更后的**完整成员配置**（而非增量），应用即整体替换，
   重复提交、并发提交、重启回放都只生效一次。
5. **防扰乱**：非投票成员（learner、已移除节点）不发起选举、不投票；
   投票方拒绝不在其已提交成员配置中的候选人（`NotMember`），且不随其任期
   更新本地状态——被移除节点即使因分区没收到移除条目、持续以旧身份抬任期
   参选，也无法赢得选票或扰乱现有 leader。

### 一致性边界

- 任意两个相邻生效配置只差一个节点，二者多数派必然相交，因此变更期间
  **不会同时存在两个能推进提交的多数派**，已提交日志不回退、业务结果
  不重复执行（提案幂等机制与成员变更正交，照常生效）。
- 变更期间业务提案可正常提交；`commitIndex` 始终按**当前已提交生效的**
  投票成员配置计算多数派。
- 成员配置经快照信封传播：`InstallSnapshot` 与重启恢复的成员配置与
  生成快照时完全一致，重启后不会回到旧成员列表。

### 与现有能力的兼容范围（变更期间仍不支持）

- **一次只变一个节点**：扩/缩多节点需逐个进行（3→5 即两次 `AddNode`），
  未实现联合共识（joint consensus）的多节点原子变更。
- **无 leader 转移**：移除当前 leader 依赖其提交后自动退位 + 重新选举，
  有约一个选举超时的短暂不可用窗口。
- **被移除节点需由运维最终下线**：若它在移除提交期间被分区且之后再无
  leader 向其复制（如日志已压缩），它会持续以旧身份参选——但按第 5 条
  规则无法造成扰乱，建议尽快停止其进程。
- PreVote、ReadIndex、磁盘持久化等限制与上一节相同，不受成员变更影响。

## 故障恢复规则

| 故障 | 行为 |
|---|---|
| 节点崩溃/重启 | 从 `Storage` 恢复任期、投票、日志、快照与应用位点；状态机由快照+日志回放重建后以 follower 身份重入集群 |
| 网络分区（少数派） | 无法获得多数票，选不出 leader；旧 leader 的提案无法提交，客户端超时（`Timeout`） |
| 网络分区（多数派） | 正常选主、提交；分区恢复后少数派通过日志复制或快照追平 |
| follower 落后较少 | leader 按 reply 提示回退 `nextIndex` 增量重试 |
| follower 落后超过快照边界 | leader 发送 `InstallSnapshot`，follower 安装后从快照边界继续复制，不丢条目、不重复应用 |
| leader 降级 | 未完成的提案立即以 `StaleTerm` 失败，客户端可重试新 leader |

### 失败原因区分

`Propose` / 消息回复均携带 `RejectReason`：`NotLeader`（附 leader 提示）、
`Timeout`、`StaleTerm`、`AlreadyVoted`、`LogBehind`、`LogMismatch`、`Shutdown` 等，
调用方可据此决定重试、转向 leader 或放弃。

## 当前不支持的范围

- 多节点原子成员变更（联合共识）：在线增删已支持，但一次只能变一个节点。
- 预投票（PreVote）：分区节点重新入网会短暂抬高任期，随后自行收敛
  （已移除节点的参选扰乱由 `NotMember` 投票规则阻断）。
- 读一致性优化（ReadIndex / leader lease）：线性一致读请走 `Propose`。
- 磁盘持久化实现：仅提供 `MemoryStorage`，生产使用需自行实现 `Storage` 接口。
- 消息丢包/乱序/延迟的主动注入：`Network` 只支持确定性断连与隔离
  （异步投递天然乱序，协议本身已容忍）。
- 快照分块传输：快照一次性整体发送。

## 验证方法

全部验证仅依赖本地运行，无外部服务与真实凭据：

```bash
go build ./...
go test ./... -count=1            # 功能测试
go test ./... -race -count=1      # 竞态检测
go test ./... -count=3            # 稳定性重复跑
go test ./raft/ -run TestSnapshotCatchUp -v     # 查看任期/投票/提交位点/快照边界日志
go test ./raft/ -run TestMembership3To5To3 -v   # 查看成员变更全程日志（含成员配置变化）
```

测试覆盖：

- `election_test.go`：唯一 leader 选举、分区无双主、高任期降级、投票拒绝原因区分
- `replication_test.go`：复制与提交、重复/并发提案幂等、follower 追赶、
  冲突日志覆盖、`NotLeader`/`Timeout` 原因区分、提交前缀一致性断言
- `restart_test.go`：重启后已提交前缀保持、跨重启幂等、重启节点重入选举
- `snapshot_test.go`：快照追赶（不丢不重）、快照边界 ≤ 提交位点、
  快照期间并发写入一致性、快照随重启恢复
- `membership_test.go`：在线扩容（learner 快照追平后提升并参与选主与确认）、
  缩容（被移除节点退出多数派且不能自选主）、移除 leader 自身、
  变更遇分区（多数派继续、无双主、恢复后收敛）、变更遇重启（换主后幂等续做、
  全集群重启后成员配置不回退）、重复/并发变更（`ConfigBusy` 互斥 + 幂等）、
  3→5→3 全旅程（全程业务提交不中断）
