# distributed-consistency-coordinator-go

可嵌入的分布式一致性协调模块（Go）。以本地多节点方式模拟集群，
在并发提交流程下保证领导选举、日志复制与故障恢复的一致性。
协议为 Raft 风格：任期指派的单一 leader、多数派提交、冲突日志覆盖、
快照压缩与落后节点追赶。

```
coordinator/          模块包（全部实现与测试）
├── types.go          角色、日志条目、配置、错误定义
├── node.go           节点生命周期与单 goroutine 事件循环
├── election.go       选举：超时、投票、任期回退、角色切换
├── replication.go    日志复制、冲突覆盖、多数提交、状态机应用
├── snapshot.go       快照压缩与 InstallSnapshot 追赶
├── storage.go        持久化（原子 JSON 文件 / 纯内存）
├── transport.go      本地传输层（可注入分区、断链、上下线）
└── fsm.go            FSM 接口与内置幂等 KV 状态机
```

## 快速开始

```go
transport := coordinator.NewLocalTransport()
n, _ := coordinator.NewNode(coordinator.Config{
    ID: "n1", Peers: []string{"n1", "n2", "n3"},
    DataDir: "/var/lib/mynode/n1",   // 持久化目录；空串 = 纯内存
}, transport, coordinator.NewKVFSM(), nil)
n.Start()
defer n.Stop()

res, err := n.Propose(ctx, coordinator.MarshalKVCommand(coordinator.KVCommand{
    Op: coordinator.KVOpSet, Key: "k", Val: "v", ProposalID: "req-123",
}))
```

完整可运行示例见 `coordinator/example_test.go`。

## 一致性不变量

1. **选举安全（不双主）**：同一任期内一个节点至多投出一票（投票随任期
   持久化，重启后不重复投票）；候选人必须获得多数票才能上任；任何节点
   发现更高任期立即降级为 follower。分区、重启、任期回退场景下任意时刻
   至多一个被多数派承认的 leader。
2. **日志匹配**：两节点日志在同一 `(index, term)` 上有条目，则该下标之前
   的所有条目完全相同。冲突条目被可解释地覆盖：follower 返回冲突任期与
   冲突下标提示，leader 据此回退 `nextIndex` 后用正确前缀覆盖。
3. **提交安全**：只有被多数派复制的条目才推进提交位点；leader 只能直接
   提交**当前任期**的条目（前任期条目随当前任期条目间接提交），杜绝
   已提交条目被覆盖。leader 上任即追加当前任期 no-op 以锚定提交规则。
4. **状态机安全**：已应用条目永不回滚；重启后状态机不偏离已提交前缀。
   应用位点逐条持久化，崩溃恢复至多重放最后一条，配合 FSM 幂等约定
   实现"不重复应用"。
5. **快照安全**：快照只基于已提交且已应用的状态生成；先持久化快照再
   丢弃日志前缀（崩溃时宁可冗余不丢失）；InstallSnapshot 接收方整体
   替换到快照边界，边界之后的已提交后缀保留并继续应用。
6. **幂等**：AppendEntries 按 `(prevIndex, prevTerm)` 校验，重复/乱序
   投递不会产生重复条目；带 `ProposalID` 的提案在状态机层去重，客户端
   重试/超时重发只生效一次（去重记录随快照持久化，跨重启、跨快照有效）。

## 故障恢复规则

| 故障 | 行为 |
|---|---|
| follower 崩溃/重启 | 从磁盘恢复任期、投票、日志、快照与应用位点；重放至已应用位点；由 leader 心跳补齐后续条目 |
| leader 崩溃/重启 | 其余节点选举超时后选出新 leader（任期单调递增）；重启的旧 leader 以 follower 身份重新加入，未提交的旧条目被新 leader 覆盖 |
| 网络分区（少数派） | 少数派无法获得多数确认：候选人永远凑不够票，旧 leader 的提案以 `ErrTimeout` 失败，已提交前缀不受影响 |
| 网络分区恢复 | 高任期心跳使旧 leader 降级；冲突日志按冲突提示回退覆盖；落后节点先补日志，落后超过快照边界则走 InstallSnapshot |
| 快照期间崩溃 | 快照先于日志截断持久化；重启后从最近快照 + 残留日志恢复，至多重复应用最后一条（FSM 幂等兜底） |

## 失败原因区分

`Propose` 的错误可用 `errors.Is` 区分：

- `ErrNotLeader`（含 `NotLeaderError.LeaderHint` 提示当前 leader）
- `ErrLeadershipLost`：等待提交期间本节点被降级
- `ErrTimeout`：等待提交超时（常见于分区导致无法达成多数）
- `ErrStopped`：节点已停止

## 可观测性

节点通过 `slog` 输出关键事件（测试中以 `go test -v` 可见）：
任期与角色切换（`role change -> ...`，含降级原因）、投票授予/拒绝
（`vote granted/denied`，含票数与多数门槛）、提交位点推进
（`entry committed and applied`，含 `commitIndex`/`snapIndex`）、
快照边界（`snapshot taken/installed`，含 `snapIndex`/`snapTerm`）。
`Node.Status()` 与 `Node.CommittedEntries()` 提供运行时可观测状态。

## 当前不支持的范围

- **成员变更**：集群成员在启动时静态指定，不支持运行时增删节点。
- **真实网络**：仅提供进程内 `LocalTransport`；跨进程部署需自行实现
  `Transport` 接口（三个 RPC 方法）。
- **线性一致读**：读请求不经过共识（无 ReadIndex/租约），`KVFSM.Get`
  仅供测试，可能读到旧值。
- **快照分块传输**：快照一次性整体发送，不适合超大状态机。
- **磁盘 fsync 策略**：持久化采用原子 rename，但未做目录 fsync，
  不保证整机断电场景下的持久性（进程崩溃场景是安全的）。
- **PreVote**：未实现 PreVote 扩展，分区节点重新加入时会短暂抬高任期
  （不影响正确性，仅可能造成一次额外的选举轮换）。

## 验证方法

全部验证仅依赖本地运行，无外部服务：

```bash
go test ./coordinator/ -v          # 全部测试（日志含任期/投票/提交位点/快照边界）
go test ./coordinator/ -race       # 竞态检测
go test ./coordinator/ -count=3    # 稳定性重复验证
```

测试覆盖：

| 场景 | 测试 |
|---|---|
| 多数可达选出唯一 leader、采样窗口无双主 | `TestElectSingleLeader` |
| leader 停止后重新选举、任期递增 | `TestReelectAfterLeaderStop` |
| 多数被分区时选主与恢复收敛 | `TestNoElectionWithoutQuorum` |
| follower 提案按 `ErrNotLeader` 拒绝 | `TestProposeRejectsNonLeader` |
| 提交流程、提交前缀与 FSM 一致 | `TestReplicationCommitAndApply` |
| 重复提案幂等去重 | `TestDuplicateProposalIdempotent` |
| 并发提交无缺口、无重复应用 | `TestConcurrentProposals` |
| follower 落后补齐不丢不重 | `TestFollowerCatchUp` |
| 被隔离旧 leader 提案超时失败、恢复后冲突被覆盖 | `TestPartitionedLeaderCannotCommit` |
| 快照只基于已提交状态、压缩后继续写入 | `TestSnapshotCompaction` |
| 落后节点经 InstallSnapshot 追赶 | `TestSnapshotCatchUp` |
| 快照期间并发写入一致性 | `TestSnapshotWithConcurrentWrites` |
| 全集群重启后状态机不偏离已提交前缀 | `TestRestartPreservesCommittedState` |
| 带快照重启恢复 | `TestRestartWithSnapshot` |
| 单节点重启后重新加入并补齐 | `TestRestartedNodeRejoins` |
| leader 重启不出现双主 | `TestRestartedLeaderDoesNotSplitBrain` |
| 存储原子持久化往返 | `TestFileStorageRoundTrip` |
