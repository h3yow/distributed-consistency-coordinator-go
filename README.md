# distributed-consistency-coordinator-go

可嵌入的分布式一致性协调模块（Raft 风格），以本地多节点方式模拟集群，
在并发提交流程下保证领导选举、日志复制与故障恢复的一致性。

## 结构

```
raft/
  types.go         核心类型：角色、拒绝原因、日志条目、消息、配置、StateMachine 接口
  storage.go       Storage 接口与 MemoryStorage（任期/投票/日志/快照/应用位点持久化）
  transport.go     Network：本地内存网络，支持断连、隔离、恢复（模拟分区与崩溃）
  node.go          节点核心：选举、日志复制、提交推进、状态机应用、提案幂等
  snapshot.go      快照生成、安装与追赶（快照信封携带成员配置）
  membership.go    在线成员变更：扩容（learner 追平提升）、缩容、配置派生与回滚
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

在线调整成员（在 leader 上调用，阻塞到变更提交）：

```go
// 扩容：新节点先以空 Peers 启动，再由 leader 拉入（learner 追平后提升 voter）
newNode := raft.NewNode(raft.Config{ID: "N4" /* Peers 留空 */}, store4, net, sm4)
newNode.Start()
err := leader.AddNode(ctx, "N4")

// 缩容：提交后 N3 退出多数派，无法再以旧身份干扰集群
err = leader.RemoveNode(ctx, "N3")

// 查看当前配置
voters, learners := leader.Membership()
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
8. **成员变更安全**：任何时刻集群至多一个未提交的成员变更；变更以日志条目
   复制并多数派提交后生效，已提交配置永不回退（重启、快照、追赶后一致）。

## 在线成员变更

不停集群、不重开进程即可增删节点。规则与边界如下。

### 模型

- 成员分两类：**voter**（参与多数派计算与选举）和 **learner**（只接收复制，
  不计入多数派、不发起选举、不投票）。
- 每次变更只动一个节点（单节点变更），旧配置与新配置的多数派必然相交，
  因此变更期间不可能出现两个 leader。
- 变更表示为特殊日志条目，**携带变更后的完整成员配置**，追加到日志即生效
  （用于多数派计算与复制目标），多数派提交即定局；未提交的变更若被新 leader
  的日志覆盖截断，会从快照基线重算回滚，不会残留。
- 同一时刻只允许一个未提交的成员变更；并发变更以 `ConfInProgress` 拒绝，
  调用方串行重试即可。同一变更重复提交幂等，只真正生效一次。

### 扩容：`AddNode(ctx, id)`

1. 新节点进程先以**空 `Peers`** 启动（未配置节点不选举、不投票，静默等待）。
2. leader 提交 `AddLearner`：新节点成为 learner，开始接收复制；
   落后较多时自动走 `InstallSnapshot` 追平，不丢条目、不重复应用。
3. leader 等待其 `matchIndex` 覆盖加入位点（追平），随后提交 `Promote`
   提升为 voter。`AddNode` 在提升提交后返回。
4. 新节点未上线时，`AddNode` 阻塞在追平阶段直至 `ctx` 超时；
   此时它是 learner，可 `RemoveNode` 清理或待其上线后重试（重试幂等）。

### 缩容：`RemoveNode(ctx, id)`

1. leader 提交 `Remove` 条目；提交后目标不再是成员，不再计入多数派，
   leader 停止向其复制（提交前仍会复制，让目标亲眼看到自己被移除）。
2. 其余节点拒绝被移除节点的拉票（`NotMember`），且**不随其任期升降**：
   被移除节点即使带着旧配置和更高任期重新入网，也无法打断现有集群。
3. 被移除的 leader 在提交点自动卸任，剩余节点在一个选举间隔内选出新 leader。
4. 对非成员重复调用幂等返回成功。

### 一致性边界

- 变更期间业务提案照常提交；成员变更条目与业务条目在同一日志流中定序，
  已提交的业务结果不会因变更丢失或重复执行。
- 分区少数派一侧无法提交变更（`Timeout`）；分区恢复后旧 leader 退位，
  其未提交的变更条目被截断回滚，全集群收敛到多数派一侧的配置。
- 成员配置随快照信封传播、随日志持久化：快照追赶、进程重启后配置不回退；
  空存储上线的新节点仅从日志+快照即可重建完整成员视图。
- 被移除节点应择机退役（`Stop`）：它不再接收复制，状态会停留在移除点。

### 与现有能力的兼容范围（变更期间暂不支持）

- 一次只进行一个成员变更（单节点变更规则）；批量增减请串行调用。
- 不支持 learner 长期挂载作为只读副本：learner 仅是扩容过渡态。
- 不支持 leader 转让（leader transfer）：移除 leader 靠其卸任后自然重选，
  可用性间隔约为一次选举超时。
- 新节点地址发现：`Network` 内以 `NodeID` 寻址，新节点需先 `Start` 注册。

## 故障恢复规则

| 故障 | 行为 |
|---|---|
| 节点崩溃/重启 | 从 `Storage` 恢复任期、投票、日志、快照与应用位点；状态机由快照+日志回放重建后以 follower 身份重入集群；成员配置由快照+日志中的变更条目重建，不回退 |
| 网络分区（少数派） | 无法获得多数票，选不出 leader；旧 leader 的提案与成员变更都无法提交，客户端超时（`Timeout`） |
| 网络分区（多数派） | 正常选主、提交、执行成员变更；分区恢复后少数派通过日志复制或快照追平（含最新成员配置） |
| 被移除节点重新入网 | 其余节点以 `NotMember` 拒绝其拉票且不随其任期升降，集群选主与提交不受干扰 |
| follower 落后较少 | leader 按 reply 提示回退 `nextIndex` 增量重试 |
| follower 落后超过快照边界 | leader 发送 `InstallSnapshot`，follower 安装后从快照边界继续复制，不丢条目、不重复应用 |
| leader 降级 | 未完成的提案立即以 `StaleTerm` 失败，客户端可重试新 leader |

### 失败原因区分

`Propose` / `AddNode` / `RemoveNode` / 消息回复均携带 `RejectReason`：
`NotLeader`（附 leader 提示）、`Timeout`、`StaleTerm`、`AlreadyVoted`、`LogBehind`、
`LogMismatch`、`Shutdown`、`NotMember`、`ConfInProgress` 等，
调用方可据此决定重试、转向 leader 或放弃。

## 当前不支持的范围

- 预投票（PreVote）：普通分区节点重新入网会短暂抬高任期，随后自行收敛
  （被移除节点已由 `NotMember` 拒绝规则隔离，无此问题）。
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
go test ./raft/ -run TestSnapshotCatchUp -v   # 查看任期/投票/提交位点/快照边界日志
go test ./raft/ -run TestGrow3To5ShrinkTo3 -v # 查看成员变更全过程日志
```

测试覆盖：

- `election_test.go`：唯一 leader 选举、分区无双主、高任期降级、投票拒绝原因区分
- `replication_test.go`：复制与提交、重复/并发提案幂等、follower 追赶、
  冲突日志覆盖、`NotLeader`/`Timeout` 原因区分、提交前缀一致性断言
- `restart_test.go`：重启后已提交前缀保持、跨重启幂等、重启节点重入选举
- `snapshot_test.go`：快照追赶（不丢不重）、快照边界 ≤ 提交位点、
  快照期间并发写入一致性、快照随重启恢复
- `membership_test.go`：在线成员变更——
  扩容（learner 经快照追平后提升为 voter 并参与多数派）、
  learner 阶段不计入多数派、缩容后集群继续提交、
  被移除节点（含分区后重新入网、含被移除的 leader）无法干扰集群、
  变更重复提交与并发提交只生效一次（`ConfInProgress` 互斥）、
  变更期间网络分区（少数派变更失败回滚、多数派完成后全集群收敛）、
  变更过程中节点重启后追赶上新配置、快照+重启后成员配置不回退、
  3 节点扩到 5 节点再缩回 3 节点全流程

测试日志（`EnableLog: true`）中包含任期、投票授予/拒绝及原因、当前成员配置
（voters/learners）、提交位点与快照边界，可直接用于排查。
