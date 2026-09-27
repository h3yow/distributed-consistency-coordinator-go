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
  snapshot.go      快照生成、安装与追赶
  statemachine.go  KVMachine 示例状态机
  *_test.go        选举/复制/重启/快照测试与集群 harness
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

- 集群成员变更（动态增删节点）：`Peers` 启动后固定。
- 预投票（PreVote）：分区节点重新入网会短暂抬高任期，随后自行收敛。
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
```

测试覆盖：

- `election_test.go`：唯一 leader 选举、分区无双主、高任期降级、投票拒绝原因区分
- `replication_test.go`：复制与提交、重复/并发提案幂等、follower 追赶、
  冲突日志覆盖、`NotLeader`/`Timeout` 原因区分、提交前缀一致性断言
- `restart_test.go`：重启后已提交前缀保持、跨重启幂等、重启节点重入选举
- `snapshot_test.go`：快照追赶（不丢不重）、快照边界 ≤ 提交位点、
  快照期间并发写入一致性、快照随重启恢复
