package coordinator

import "sync"

// ---- RPC 消息 ----

// RequestVoteRequest 是候选人发出的拉票请求。
type RequestVoteRequest struct {
	Term         uint64 // 候选人任期
	CandidateID  string
	LastLogIndex uint64 // 候选人最后一条日志下标
	LastLogTerm  uint64 // 候选人最后一条日志任期
}

// RequestVoteResponse 是投票响应。
type RequestVoteResponse struct {
	Term        uint64 // 响应者当前任期（供候选人回退）
	VoteGranted bool
}

// AppendEntriesRequest 由 leader 发出，兼作心跳（Entries 为空）。
type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64 // 紧邻新条目之前的下标
	PrevLogTerm  uint64 // 该下标处条目的任期
	Entries      []LogEntry
	LeaderCommit uint64 // leader 的提交位点
}

// AppendEntriesResponse 是复制响应，携带冲突提示以加速日志对齐。
type AppendEntriesResponse struct {
	Term    uint64
	Success bool

	// 冲突提示（仅 Success=false 时有意义）：
	// ConflictTerm 为 follower 在 PrevLogIndex 处冲突条目的任期（0 表示日志太短）；
	// ConflictIndex 为 leader 下次应尝试的 nextIndex 上界。
	ConflictTerm  uint64
	ConflictIndex uint64

	// MatchIndex 仅 Success=true 时有意义：follower 已匹配的最大下标。
	MatchIndex uint64
}

// InstallSnapshotRequest 由 leader 向严重落后的 follower 发送快照。
type InstallSnapshotRequest struct {
	Term     uint64
	LeaderID string
	Meta     SnapshotMeta
	Data     []byte
}

// InstallSnapshotResponse 是快照响应。
type InstallSnapshotResponse struct {
	Term uint64
}

// RPCHandler 由 Node 实现，Transport 将收到的 RPC 分发给它。
type RPCHandler interface {
	HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse
	HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse
	HandleInstallSnapshot(req *InstallSnapshotRequest) *InstallSnapshotResponse
}

// Transport 抽象节点间通信。本地实现可注入分区与断链。
type Transport interface {
	// Register 将节点 ID 绑定到其 RPC 处理器。
	Register(id string, h RPCHandler)
	// Unregister 解除绑定（节点停止时调用）。
	Unregister(id string)
	// SendRequestVote 向目标节点投递 RequestVote；不可达时返回 ErrUnreachable。
	SendRequestVote(to string, req *RequestVoteRequest) (*RequestVoteResponse, error)
	// SendAppendEntries 向目标节点投递 AppendEntries。
	SendAppendEntries(to string, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	// SendInstallSnapshot 向目标节点投递 InstallSnapshot。
	SendInstallSnapshot(to string, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error)
}

// LocalTransport 是进程内的 Transport 实现，用于本地多节点模拟集群。
// 支持按节点对注入断链（分区），并可整体启停某个节点的连通性。
type LocalTransport struct {
	mu       sync.RWMutex
	peers    map[string]RPCHandler
	blocked  map[[2]string]bool // 单向断链：blocked[{from,to}]
	online   map[string]bool    // 节点整体在线状态（默认在线）
	isolated map[string]bool    // 被整体隔离的节点（与注册顺序无关）
}

// NewLocalTransport 创建本地传输层。
func NewLocalTransport() *LocalTransport {
	return &LocalTransport{
		peers:    make(map[string]RPCHandler),
		blocked:  make(map[[2]string]bool),
		online:   make(map[string]bool),
		isolated: make(map[string]bool),
	}
}

// Register 实现 Transport。
func (t *LocalTransport) Register(id string, h RPCHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[id] = h
	t.online[id] = true
}

// Unregister 实现 Transport。
func (t *LocalTransport) Unregister(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.peers, id)
	delete(t.online, id)
}

// Disconnect 切断 from->to 与 to->from 的双向链路（模拟分区）。
func (t *LocalTransport) Disconnect(a, b string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.blocked[[2]string{a, b}] = true
	t.blocked[[2]string{b, a}] = true
}

// Connect 恢复 a、b 之间的链路。
func (t *LocalTransport) Connect(a, b string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.blocked, [2]string{a, b})
	delete(t.blocked, [2]string{b, a})
}

// Isolate 将 id 与所有其他节点断开（模拟该节点被分区）。
// 与注册顺序无关：对尚未注册的节点同样生效。
func (t *LocalTransport) Isolate(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.isolated[id] = true
}

// Heal 恢复 id 与所有节点的链路。
func (t *LocalTransport) Heal(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.isolated, id)
	for k := range t.blocked {
		if k[0] == id || k[1] == id {
			delete(t.blocked, k)
		}
	}
}

// SetOnline 设置节点整体在线状态（模拟进程崩溃/恢复，区别于分区）。
func (t *LocalTransport) SetOnline(id string, online bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.online[id] = online
}

func (t *LocalTransport) handler(from, to string) (RPCHandler, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	h, ok := t.peers[to]
	if !ok {
		return nil, ErrUnreachable
	}
	if !t.online[from] || !t.online[to] {
		return nil, ErrUnreachable
	}
	if t.isolated[from] || t.isolated[to] {
		return nil, ErrUnreachable
	}
	if t.blocked[[2]string{from, to}] {
		return nil, ErrUnreachable
	}
	return h, nil
}

// SendRequestVote 实现 Transport。from 由请求中的 CandidateID 推断。
func (t *LocalTransport) SendRequestVote(to string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	h, err := t.handler(req.CandidateID, to)
	if err != nil {
		return nil, err
	}
	return h.HandleRequestVote(req), nil
}

// SendAppendEntries 实现 Transport。
func (t *LocalTransport) SendAppendEntries(to string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	h, err := t.handler(req.LeaderID, to)
	if err != nil {
		return nil, err
	}
	return h.HandleAppendEntries(req), nil
}

// SendInstallSnapshot 实现 Transport。
func (t *LocalTransport) SendInstallSnapshot(to string, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	h, err := t.handler(req.LeaderID, to)
	if err != nil {
		return nil, err
	}
	return h.HandleInstallSnapshot(req), nil
}
