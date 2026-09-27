package coordinator

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"
)

// Node 是集群中的一个共识节点。通过 NewNode 创建、Start 启动、Stop 停止。
//
// 所有协议状态变更都在内部事件循环 goroutine 中串行执行，
// RPC 处理器与定时器只向事件循环投递消息，避免锁竞争。
type Node struct {
	id    string
	peers []string
	cfg   Config
	log   *slog.Logger

	store     Storage
	transport Transport
	fsm       FSM

	// 事件循环
	// events 必须是无缓冲的：停止后向它发送会立即让位于 done 分支，
	// 避免把消息排进已死循环的缓冲区而永久阻塞调用方。
	events chan func()
	stopCh chan struct{}
	done   chan struct{}

	// ---- 协议状态（仅事件循环内访问）----
	role        Role
	currentTerm uint64
	votedFor    string
	leaderID    string

	logs        []LogEntry // logs[0] 为哨兵（快照边界占位）
	commitIndex uint64
	lastApplied uint64

	// leader 复制状态
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// 选举计票（候选人任期内有效）
	votesReceived int

	// 选举计时
	lastHeartbeat time.Time // 最近一次收到有效 leader 心跳/授予投票的时间
	electionDue   time.Duration

	// 等待提交的提案：index -> waiters
	waiters map[uint64][]chan proposeResult

	// 快照边界（logs[0] 的 Term/Index）
	snapIndex uint64
	snapTerm  uint64
	snapData  []byte // 缓存最近一次快照数据，供 InstallSnapshot 转发
}

type proposeResult struct {
	res any
	err error
}

// NewNode 创建节点（尚未启动）。storage 为 nil 时根据 cfg.DataDir 创建默认存储。
func NewNode(cfg Config, transport Transport, fsm FSM, storage Storage) (*Node, error) {
	cfg.setDefaults()
	if cfg.ID == "" {
		cfg.ID = "node"
	}
	if len(cfg.Peers) == 0 {
		cfg.Peers = []string{cfg.ID}
	}
	if storage == nil {
		var err error
		storage, err = NewFileStorage(cfg.DataDir)
		if err != nil {
			return nil, err
		}
	}
	n := &Node{
		id:          cfg.ID,
		peers:       append([]string(nil), cfg.Peers...),
		cfg:         cfg,
		log:         cfg.Logger.With("node", cfg.ID),
		store:       storage,
		transport:   transport,
		fsm:         fsm,
		events:      make(chan func()),
		stopCh:      make(chan struct{}),
		done:        make(chan struct{}),
		role:        RoleFollower,
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		waiters:     make(map[uint64][]chan proposeResult),
		logs:        []LogEntry{{Term: 0, Index: 0}}, // 哨兵
		electionDue: cfg.ElectionTimeoutMin,
	}
	return n, nil
}

// ID 返回节点 ID。
func (n *Node) ID() string { return n.id }

// Start 启动节点：恢复持久化状态并开启事件循环。
//
// 恢复顺序（重启一致性）：
//  1. 恢复任期/投票（防止重启后重复投票导致双主）；
//  2. 恢复快照：FSM 回放到快照边界；
//  3. 加载快照之后的日志；
//  4. 按已应用下标重放日志到 FSM（FSM.Apply 必须幂等）；
//  5. 已应用的条目必然已提交，提交位点至少恢复到 applied。
func (n *Node) Start() error {
	stable, err := n.store.LoadStable()
	if err != nil {
		return err
	}
	n.currentTerm, n.votedFor = stable.CurrentTerm, stable.VotedFor

	meta, data, hasSnap, err := n.store.LoadSnapshot()
	if err != nil {
		return err
	}
	if hasSnap {
		n.snapIndex = meta.LastIncludedIndex
		n.snapTerm = meta.LastIncludedTerm
		n.snapData = append([]byte(nil), data...)
		n.logs[0] = LogEntry{Index: n.snapIndex, Term: n.snapTerm}
		if err := n.fsm.Restore(data); err != nil {
			return err
		}
		n.commitIndex = n.snapIndex
		n.lastApplied = n.snapIndex
		n.log.Info("restored snapshot", "snapIndex", n.snapIndex, "snapTerm", n.snapTerm)
	}

	entries, err := n.store.LoadLog()
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.Index <= n.snapIndex {
			continue // 防御性跳过快照已覆盖的残留条目
		}
		n.logs = append(n.logs, e)
	}

	applied, err := n.store.LoadApplied()
	if err != nil {
		return err
	}
	if applied > n.snapIndex {
		for idx := n.snapIndex + 1; idx <= applied && idx <= n.lastIndex(); idx++ {
			n.applyEntry(n.entryAt(idx))
		}
		n.lastApplied = applied
	}
	if applied > n.commitIndex {
		n.commitIndex = applied
	}

	n.log.Info("node started", "term", n.currentTerm, "lastIndex", n.lastIndex(),
		"commitIndex", n.commitIndex, "lastApplied", n.lastApplied)

	n.resetElectionTimer()
	n.transport.Register(n.id, n)
	go n.loop()
	return nil
}

// Stop 停止节点（模拟进程退出；持久化状态保留，可再次 Start 模拟重启）。
func (n *Node) Stop() {
	select {
	case <-n.done:
		return
	default:
	}
	close(n.stopCh)
	<-n.done
	n.transport.Unregister(n.id)
}

// ctxErr 把 context 截止/取消映射为模块可区分的错误：
// 截止统一为 ErrTimeout，取消保留原错误。
func ctxErr(ctx context.Context) error {
	if ctx.Err() == context.DeadlineExceeded {
		return ErrTimeout
	}
	return ctx.Err()
}

// Propose 提交一条命令，阻塞直到被提交并应用、或失败。
// 失败原因可通过 errors.Is 区分：ErrNotLeader / ErrLeadershipLost / ErrTimeout / ErrStopped。
func (n *Node) Propose(ctx context.Context, command []byte) (any, error) {
	resCh := make(chan proposeResult, 1)
	submit := func() { n.submitProposal(command, resCh) }
	select {
	case n.events <- submit:
	case <-n.done:
		return nil, ErrStopped
	case <-ctx.Done():
		return nil, ctxErr(ctx)
	}
	timeout := time.NewTimer(n.cfg.ProposeTimeout)
	defer timeout.Stop()
	select {
	case r := <-resCh:
		return r.res, r.err
	case <-n.done:
		return nil, ErrStopped
	case <-timeout.C:
		return nil, ErrTimeout
	case <-ctx.Done():
		return nil, ctxErr(ctx)
	}
}

// Status 返回节点当前的可观测状态（线程安全）。
func (n *Node) Status() NodeStatus {
	resCh := make(chan NodeStatus, 1)
	select {
	case n.events <- func() {
		resCh <- NodeStatus{
			ID:          n.id,
			Role:        n.role,
			Term:        n.currentTerm,
			Leader:      n.leaderID,
			CommitIndex: n.commitIndex,
			LastApplied: n.lastApplied,
			LastIndex:   n.lastIndex(),
			SnapIndex:   n.snapIndex,
		}
	}:
	case <-n.done:
		return NodeStatus{ID: n.id, Role: RoleFollower}
	}
	return <-resCh
}

// CommittedEntries 返回已提交日志条目前缀（诊断与测试用途）。
func (n *Node) CommittedEntries() []LogEntry {
	resCh := make(chan []LogEntry, 1)
	select {
	case n.events <- func() {
		var out []LogEntry
		for idx := n.snapIndex + 1; idx <= n.commitIndex; idx++ {
			out = append(out, n.entryAt(idx))
		}
		resCh <- out
	}:
	case <-n.done:
		return nil
	}
	return <-resCh
}

// NodeStatus 是节点的可观测状态快照。
type NodeStatus struct {
	ID          string
	Role        Role
	Term        uint64
	Leader      string
	CommitIndex uint64
	LastApplied uint64
	LastIndex   uint64
	SnapIndex   uint64
}

// ---- 事件循环 ----

func (n *Node) loop() {
	defer close(n.done)
	tick := time.NewTicker(n.cfg.HeartbeatInterval)
	defer tick.Stop()
	for {
		select {
		case <-n.stopCh:
			n.failAllWaiters(ErrStopped)
			return
		case f := <-n.events:
			f()
		case <-tick.C:
			n.onTick()
		}
	}
}

// onTick 处理定时事件：leader 发心跳，其他角色检查选举超时。
func (n *Node) onTick() {
	if n.role == RoleLeader {
		n.broadcastAppend()
		return
	}
	if time.Since(n.lastHeartbeat) >= n.electionDue {
		n.becomeCandidate()
	}
}

// deliver 将一段处理逻辑投递回事件循环串行执行。
// 异步 RPC 的响应必须通过它回投，避免在节点事件循环外读写协议状态。
func (n *Node) deliver(f func()) {
	select {
	case n.events <- f:
	case <-n.done:
	}
}

// applyEntry 把单条已提交条目应用到状态机。
// Command 为空表示 leader 上任时追加的 no-op，不进入状态机。
func (n *Node) applyEntry(e LogEntry) any {
	if len(e.Command) == 0 {
		return nil
	}
	return n.fsm.Apply(e.Index, e.Command)
}

// failAllWaiters 让所有等待中的提案以 err 失败。
func (n *Node) failAllWaiters(err error) {
	for idx, chs := range n.waiters {
		for _, ch := range chs {
			ch <- proposeResult{err: err}
		}
		delete(n.waiters, idx)
	}
}

// ---- 日志辅助（仅事件循环内调用）----

func (n *Node) lastIndex() uint64 { return n.logs[len(n.logs)-1].Index }
func (n *Node) lastTerm() uint64  { return n.logs[len(n.logs)-1].Term }

// appendLocal 在本节点追加日志并持久化（仅事件循环内调用）。
func (n *Node) appendLocal(entries ...LogEntry) {
	n.logs = append(n.logs, entries...)
	if err := n.store.AppendLog(entries); err != nil {
		n.log.Error("persist log entries failed", "err", err)
	}
}

// entryAt 返回下标 idx 处的条目；idx 落在快照边界时返回哨兵。
func (n *Node) entryAt(idx uint64) LogEntry {
	if idx <= n.snapIndex {
		return LogEntry{Index: n.snapIndex, Term: n.snapTerm}
	}
	off := idx - n.snapIndex
	if off >= uint64(len(n.logs)) {
		return LogEntry{}
	}
	return n.logs[off]
}

// quorum 返回多数派数量。
func (n *Node) quorum() int { return len(n.peers)/2 + 1 }

// isMember 判断 id 是否为集群成员。
func (n *Node) isMember(id string) bool {
	for _, p := range n.peers {
		if p == id {
			return true
		}
	}
	return false
}

// resetElectionTimer 在 [ElectionTimeoutMin, ElectionTimeoutMax] 内随机化超时，
// 避免多个 follower 同时超时造成的选票分裂。
func (n *Node) resetElectionTimer() {
	n.lastHeartbeat = time.Now()
	span := int64(n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin)
	n.electionDue = n.cfg.ElectionTimeoutMin + time.Duration(rand.Int64N(span+1))
}
