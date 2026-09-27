package coordinator

import (
	"context"
	"log/slog"
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
	events chan func()
	stopCh chan struct{}
	done   chan struct{}

	// ---- 协议状态（仅事件循环内访问）----
	role        Role
	currentTerm uint64
	votedFor    string
	leaderID    string

	logs        []LogEntry  // logs[0] 为哨兵（快照边界占位）
	commitIndex uint64
	lastApplied uint64

	// leader 复制状态
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// 选举计时
	lastHeartbeat time.Time // 最近一次收到有效 leader 心跳/授予投票的时间
	electionDue   time.Duration

	// 等待提交的提案：index -> waiters
	waiters map[uint64][]chan proposeResult

	// 快照边界（logs[0] 的 Term/Index）
	snapIndex uint64
	snapTerm  uint64
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
		events:      make(chan func(), 256),
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
func (n *Node) Start() error {
	// TODO: 从 storage 恢复 stable/log/snapshot/applied，然后启动事件循环
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
		return nil, ctx.Err()
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
		return nil, ctx.Err()
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
	// TODO: 实现于 election.go / replication.go
}

// submitProposal 在事件循环内处理新提案（仅 leader 接受）。
func (n *Node) submitProposal(command []byte, resCh chan proposeResult) {
	// TODO: 实现于 replication.go
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

// resetElectionTimer 随机化选举超时（在 election.go 中补全随机逻辑）。
func (n *Node) resetElectionTimer() {
	n.lastHeartbeat = time.Now()
	n.electionDue = n.cfg.ElectionTimeoutMin // TODO: 随机化
}
