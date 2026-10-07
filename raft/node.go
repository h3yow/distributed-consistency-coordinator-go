package raft

import (
	"context"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

// Node 一致性协调节点。所有状态变更在 n.mu 保护下完成。
type Node struct {
	mu sync.Mutex

	cfg   Config
	store Storage
	net   *Network
	sm    StateMachine
	lg    *log.Logger

	// 持久化状态（写穿到 Storage）
	currentTerm uint64
	votedFor    NodeID
	log         []LogEntry // log[0] 为快照哨兵条目

	// 易失状态
	role        Role
	commitIndex uint64
	lastApplied uint64
	leaderID    NodeID

	// 已提交生效的成员配置（重建来源：初始 Config / 快照 / 已提交日志回放）
	members  map[NodeID]bool // 投票成员：参与选举与多数派计算
	learners map[NodeID]bool // 非投票成员：只接收复制，不计入多数派

	// 选举计时
	lastContact     time.Time
	electionTimeout time.Duration
	votes           map[NodeID]bool

	// leader 易失状态
	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64
	// 已被移除但尚未确认收到移除条目的节点：继续复制直到其确认，
	// 让被移除节点亲知自己已退出，从而停止参选。
	removedAck map[NodeID]uint64

	// 提案去重与等待
	lastSeq    map[string]uint64 // clientID -> 已应用的最大 Seq
	lastResult map[string]any    // clientID -> 最近一次应用结果
	pending    map[uint64]chan proposalResult

	stopCh  chan struct{}
	stopped bool
}

type proposalResult struct {
	result any
	err    error
}

// NewNode 创建节点（未启动）。
func NewNode(cfg Config, store Storage, net *Network, sm StateMachine) *Node {
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 50 * time.Millisecond
	}
	if cfg.ElectionTimeoutMin <= 0 {
		cfg.ElectionTimeoutMin = 150 * time.Millisecond
	}
	if cfg.ElectionTimeoutMax <= cfg.ElectionTimeoutMin {
		cfg.ElectionTimeoutMax = 2 * cfg.ElectionTimeoutMin
	}
	n := &Node{
		cfg:        cfg,
		store:      store,
		net:        net,
		sm:         sm,
		role:       RoleFollower,
		votes:      make(map[NodeID]bool),
		nextIndex:  make(map[NodeID]uint64),
		matchIndex: make(map[NodeID]uint64),
		removedAck: make(map[NodeID]uint64),
		lastSeq:    make(map[string]uint64),
		lastResult: make(map[string]any),
		pending:    make(map[uint64]chan proposalResult),
		members:    make(map[NodeID]bool),
		learners:   make(map[NodeID]bool),
	}
	// 初始成员配置：全新集群由 Config.Peers 引导；learner 节点初始无投票权，
	// 真实配置随后由快照与已提交日志回放重建（见 Start）。
	if cfg.Learner {
		n.learners[cfg.ID] = true
	} else {
		for _, p := range cfg.Peers {
			n.members[p] = true
		}
	}
	if cfg.EnableLog {
		n.lg = log.New(os.Stderr, "[node "+string(cfg.ID)+"] ", log.LstdFlags|log.Lmicroseconds)
	}
	return n
}

// Start 启动节点：从存储恢复状态（含快照与已提交前缀重放）并注册到网络。
func (n *Node) Start() {
	n.mu.Lock()
	defer n.mu.Unlock()

	term, votedFor, _ := n.store.LoadHardState()
	n.currentTerm, n.votedFor = term, votedFor
	n.log = n.store.Entries()

	snapIndex, _, snapData, _ := n.store.LoadSnapshot()
	if len(snapData) > 0 {
		n.restoreSnapshotLocked(snapIndex, snapData)
	}

	applied, lastSeq, lastResult, _ := n.store.LoadApplyState()
	n.lastSeq, n.lastResult = lastSeq, lastResult
	// 重放快照之后、已应用位点之前的已提交日志，重建状态机与成员配置。
	base := n.log[0].Index
	for i := base + 1; i <= applied; i++ {
		e := n.log[i-base]
		switch {
		case e.Type.IsConfig():
			n.applyConfigLocked(e)
		case e.Type == EntryNormal:
			n.sm.Apply(e.Op)
		}
	}
	n.commitIndex, n.lastApplied = applied, applied

	n.role = RoleFollower
	n.stopCh = make(chan struct{})
	n.stopped = false
	n.lastContact = time.Now()
	n.resetElectionTimeoutLocked()

	n.net.Register(n.cfg.ID, n.HandleMessage)
	go n.run()
	n.logf("started term=%d commitIndex=%d snapshotBoundary=%d members=%v learners=%v",
		n.currentTerm, n.commitIndex, snapIndex, sortedKeysLocked(n.members), sortedKeysLocked(n.learners))
}

// Stop 停止节点（模拟进程退出，存储保留，可重新 Start 模拟重启）。
func (n *Node) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return
	}
	n.stopped = true
	close(n.stopCh)
	n.failPendingLocked(&RejectError{Reason: ReasonShutdown, Detail: "node stopped"})
	n.net.Unregister(n.cfg.ID)
	n.logf("stopped term=%d commitIndex=%d", n.currentTerm, n.commitIndex)
}

// Propose 提交一条提案，等待其被多数确认并应用后返回结果。
// 相同 ClientID+Seq 的重复提案幂等返回首次结果，不会重复应用。
func (n *Node) Propose(ctx context.Context, cmd Command) (any, error) {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return nil, &RejectError{Reason: ReasonShutdown, Detail: "node stopped"}
	}
	if n.role != RoleLeader {
		hint := n.leaderID
		n.mu.Unlock()
		return nil, &RejectError{Reason: ReasonNotLeader, Leader: hint, Detail: "not leader"}
	}
	// 幂等：该客户端此序列号已应用过，直接返回缓存结果。
	if cmd.ClientID != "" && n.lastSeq[cmd.ClientID] >= cmd.Seq {
		res := n.lastResult[cmd.ClientID]
		n.mu.Unlock()
		return res, nil
	}
	lastIdx, _ := n.lastIndexTermLocked()
	entry := LogEntry{Term: n.currentTerm, Index: lastIdx + 1, ClientID: cmd.ClientID, Seq: cmd.Seq, Op: cmd.Op}
	n.log = append(n.log, entry)
	_ = n.store.Append([]LogEntry{entry})
	n.matchIndex[n.cfg.ID] = entry.Index

	ch := make(chan proposalResult, 1)
	n.pending[entry.Index] = ch
	n.logf("propose index=%d term=%d client=%s seq=%d", entry.Index, entry.Term, cmd.ClientID, cmd.Seq)
	n.broadcastAppendLocked()
	n.mu.Unlock()

	select {
	case r := <-ch:
		return r.result, r.err
	case <-ctx.Done():
		return nil, &RejectError{Reason: ReasonTimeout, Detail: "propose not committed: " + ctx.Err().Error()}
	}
}

// HandleMessage 处理收到的消息（由 Network 异步调用）。
func (n *Node) HandleMessage(m *Message) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return
	}
	switch m.Type {
	case MsgRequestVote:
		n.handleRequestVoteLocked(m)
	case MsgRequestVoteReply:
		n.handleRequestVoteReplyLocked(m)
	case MsgAppendEntries:
		n.handleAppendEntriesLocked(m)
	case MsgAppendEntriesReply:
		n.handleAppendEntriesReplyLocked(m)
	case MsgInstallSnapshot:
		n.handleInstallSnapshotLocked(m)
	case MsgInstallSnapshotReply:
		n.handleInstallSnapshotReplyLocked(m)
	}
}

// Role 返回当前角色。
func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// Term 返回当前任期。
func (n *Node) Term() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

// LeaderID 返回当前已知 leader。
func (n *Node) LeaderID() NodeID {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// CommitIndex 返回当前提交位点。
func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// LastApplied 返回状态机应用位点。
func (n *Node) LastApplied() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastApplied
}

// ---------------- 内部：计时主循环 ----------------

func (n *Node) run() {
	tick := time.NewTicker(n.cfg.HeartbeatInterval)
	defer tick.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-tick.C:
			n.mu.Lock()
			if n.stopped {
				n.mu.Unlock()
				return
			}
			if n.role == RoleLeader {
				n.broadcastAppendLocked()
			} else if n.isVoterLocked() && time.Since(n.lastContact) >= n.electionTimeout {
				// 仅投票成员可发起选举；learner 与已移除节点永不参选。
				n.startElectionLocked()
			}
			n.mu.Unlock()
		}
	}
}

func (n *Node) resetElectionTimeoutLocked() {
	span := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	n.electionTimeout = n.cfg.ElectionTimeoutMin + time.Duration(rand.Int63n(int64(span)))
}

// ---------------- 内部：选举 ----------------

func (n *Node) startElectionLocked() {
	n.role = RoleCandidate
	n.currentTerm++
	n.votedFor = n.cfg.ID
	_ = n.store.SaveHardState(n.currentTerm, n.votedFor)
	n.leaderID = ""
	n.votes = map[NodeID]bool{n.cfg.ID: true}
	n.lastContact = time.Now()
	n.resetElectionTimeoutLocked()
	lastIdx, lastTerm := n.lastIndexTermLocked()
	n.logf("election start term=%d lastLog=%d/%d members=%v", n.currentTerm, lastIdx, lastTerm, sortedKeysLocked(n.members))
	if n.quorumReachedLocked(len(n.votes)) {
		n.becomeLeaderLocked()
		return
	}
	for p := range n.members {
		if p == n.cfg.ID {
			continue
		}
		n.sendLocked(&Message{
			Type: MsgRequestVote, From: n.cfg.ID, To: p, Term: n.currentTerm,
			LastLogIndex: lastIdx, LastLogTerm: lastTerm,
		})
	}
}

func (n *Node) handleRequestVoteLocked(m *Message) {
	reply := &Message{Type: MsgRequestVoteReply, From: n.cfg.ID, To: m.From, Term: n.currentTerm}
	if m.Term < n.currentTerm {
		reply.Reason = ReasonStaleTerm
		n.logf("vote reject candidate=%s reason=%s myTerm=%d msgTerm=%d", m.From, reply.Reason, n.currentTerm, m.Term)
		n.sendLocked(reply)
		return
	}
	// 非投票成员的候选人一律拒绝，且不随其任期更新本地状态：
	// 已被移出集群的节点不能凭旧身份抬升任期、扰乱现有 leader。
	if !n.members[m.From] {
		reply.Reason = ReasonNotMember
		n.logf("vote reject candidate=%s reason=%s term=%d members=%v", m.From, reply.Reason, n.currentTerm, sortedKeysLocked(n.members))
		n.sendLocked(reply)
		return
	}
	// learner 与已移除节点自身不投票。
	if !n.isVoterLocked() {
		reply.Reason = ReasonNotMember
		n.logf("vote reject candidate=%s reason=%s(self non-voter) term=%d", m.From, reply.Reason, n.currentTerm)
		n.sendLocked(reply)
		return
	}
	if m.Term > n.currentTerm {
		n.becomeFollowerLocked(m.Term, "")
	}
	reply.Term = n.currentTerm
	myLastIdx, myLastTerm := n.lastIndexTermLocked()
	upToDate := m.LastLogTerm > myLastTerm || (m.LastLogTerm == myLastTerm && m.LastLogIndex >= myLastIdx)
	switch {
	case n.votedFor != "" && n.votedFor != m.From:
		reply.Reason = ReasonAlreadyVoted
	case !upToDate:
		reply.Reason = ReasonLogBehind
	default:
		n.votedFor = m.From
		_ = n.store.SaveHardState(n.currentTerm, n.votedFor)
		n.lastContact = time.Now()
		n.resetElectionTimeoutLocked()
		reply.Success = true
	}
	n.logf("vote candidate=%s granted=%v reason=%s term=%d votedFor=%s",
		m.From, reply.Success, reply.Reason, n.currentTerm, n.votedFor)
	n.sendLocked(reply)
}

func (n *Node) handleRequestVoteReplyLocked(m *Message) {
	if m.Term > n.currentTerm {
		n.becomeFollowerLocked(m.Term, "")
		return
	}
	if n.role != RoleCandidate || m.Term != n.currentTerm || !m.Success {
		return
	}
	if !n.members[m.From] {
		return // 只统计投票成员的选票
	}
	n.votes[m.From] = true
	n.logf("vote received from=%s term=%d votes=%d/%d", m.From, n.currentTerm, len(n.votes), len(n.members))
	if n.quorumReachedLocked(len(n.votes)) {
		n.becomeLeaderLocked()
	}
}

func (n *Node) becomeFollowerLocked(term uint64, leader NodeID) {
	oldRole := n.role
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
		_ = n.store.SaveHardState(n.currentTerm, n.votedFor)
	}
	n.role = RoleFollower
	n.leaderID = leader
	if oldRole == RoleLeader {
		n.logf("step down term=%d (higher/equal term seen from=%s)", n.currentTerm, leader)
		n.failPendingLocked(&RejectError{Reason: ReasonStaleTerm, Detail: "leader stepped down"})
	}
}

func (n *Node) becomeLeaderLocked() {
	n.role = RoleLeader
	n.leaderID = n.cfg.ID
	lastIdx, _ := n.lastIndexTermLocked()
	n.nextIndex = make(map[NodeID]uint64)
	n.matchIndex = make(map[NodeID]uint64)
	// 投票成员假设已同步（按需回退）；learner 从头（或快照）追赶。
	for p := range n.members {
		n.nextIndex[p] = lastIdx + 1
		n.matchIndex[p] = 0
	}
	for p := range n.learners {
		n.nextIndex[p] = 1
		n.matchIndex[p] = 0
	}
	n.matchIndex[n.cfg.ID] = lastIdx
	// 接力未完成的移除确认：扫描已提交日志中的移除条目，
	// 对仍不在成员配置中的目标继续复制，直到其确认收到移除。
	n.removedAck = make(map[NodeID]uint64)
	base := n.log[0].Index
	for i := base + 1; i <= n.commitIndex; i++ {
		e := n.log[i-base]
		if e.Type == EntryRemoveNode && e.NodeID != n.cfg.ID && !n.members[e.NodeID] && !n.learners[e.NodeID] {
			n.removedAck[e.NodeID] = e.Index
			if _, ok := n.nextIndex[e.NodeID]; !ok {
				n.nextIndex[e.NodeID] = 1
				n.matchIndex[e.NodeID] = 0
			}
		}
	}
	n.logf("became leader term=%d lastIndex=%d members=%v learners=%v",
		n.currentTerm, lastIdx, sortedKeysLocked(n.members), sortedKeysLocked(n.learners))
	n.broadcastAppendLocked()
}

func (n *Node) quorumReachedLocked(count int) bool {
	return count > len(n.members)/2
}

// ---------------- 内部：日志复制 ----------------

func (n *Node) broadcastAppendLocked() {
	for _, p := range n.replicationTargetsLocked() {
		n.sendAppendToLocked(p)
	}
}

func (n *Node) sendAppendToLocked(peer NodeID) {
	base := n.log[0].Index
	next := n.nextIndex[peer]
	if next <= base {
		n.sendSnapshotToLocked(peer)
		return
	}
	prevIdx := next - 1
	prevTerm := n.log[prevIdx-base].Term
	entries := make([]LogEntry, len(n.log[next-base:]))
	copy(entries, n.log[next-base:])
	n.sendLocked(&Message{
		Type: MsgAppendEntries, From: n.cfg.ID, To: peer, Term: n.currentTerm,
		PrevLogIndex: prevIdx, PrevLogTerm: prevTerm,
		Entries: entries, LeaderCommit: n.commitIndex,
	})
}

func (n *Node) handleAppendEntriesLocked(m *Message) {
	reply := &Message{Type: MsgAppendEntriesReply, From: n.cfg.ID, To: m.From, Term: n.currentTerm}
	if m.Term < n.currentTerm {
		reply.Reason = ReasonStaleTerm
		n.sendLocked(reply)
		return
	}
	n.becomeFollowerLocked(m.Term, m.From)
	n.lastContact = time.Now()
	n.resetElectionTimeoutLocked()
	reply.Term = n.currentTerm

	base := n.log[0].Index
	lastIdx, _ := n.lastIndexTermLocked()
	entries := m.Entries

	if m.PrevLogIndex < base {
		// 前置部分已被本地快照覆盖：跳过覆盖区，快照保证前缀一致。
		covered := base - m.PrevLogIndex
		if int(covered) >= len(entries) {
			reply.Success = true
			reply.MatchIndex = base
			n.advanceCommitFromLeaderLocked(m.LeaderCommit)
			reply.CommitIndex = n.commitIndex
			n.sendLocked(reply)
			return
		}
		entries = entries[covered:]
	} else {
		if m.PrevLogIndex > lastIdx {
			reply.Reason = ReasonLogMismatch
			reply.MatchIndex = lastIdx
			n.logf("append reject leader=%s reason=%s prev=%d lastIdx=%d", m.From, reply.Reason, m.PrevLogIndex, lastIdx)
			n.sendLocked(reply)
			return
		}
		if n.log[m.PrevLogIndex-base].Term != m.PrevLogTerm {
			reply.Reason = ReasonLogMismatch
			reply.MatchIndex = m.PrevLogIndex - 1
			n.logf("append reject leader=%s reason=%s prev=%d prevTerm=%d localTerm=%d",
				m.From, reply.Reason, m.PrevLogIndex, m.PrevLogTerm, n.log[m.PrevLogIndex-base].Term)
			n.sendLocked(reply)
			return
		}
	}

	// 合并：冲突条目被可解释地截断覆盖，相同条目幂等跳过。
	for _, e := range entries {
		if e.Index <= base {
			continue
		}
		off := e.Index - base
		switch {
		case int(off) < len(n.log) && n.log[off].Term != e.Term:
			n.logf("log conflict at index=%d localTerm=%d newTerm=%d: truncate and overwrite",
				e.Index, n.log[off].Term, e.Term)
			n.log = n.log[:off]
			_ = n.store.TruncateSuffix(e.Index)
			n.log = append(n.log, e)
			_ = n.store.Append([]LogEntry{e})
		case int(off) >= len(n.log):
			n.log = append(n.log, e)
			_ = n.store.Append([]LogEntry{e})
		}
	}

	reply.Success = true
	reply.MatchIndex = m.PrevLogIndex + uint64(len(m.Entries))
	if reply.MatchIndex < base {
		reply.MatchIndex = base
	}
	n.advanceCommitFromLeaderLocked(m.LeaderCommit)
	reply.CommitIndex = n.commitIndex
	n.sendLocked(reply)
}

func (n *Node) advanceCommitFromLeaderLocked(leaderCommit uint64) {
	if leaderCommit > n.commitIndex {
		lastIdx, _ := n.lastIndexTermLocked()
		if leaderCommit > lastIdx {
			leaderCommit = lastIdx
		}
		n.commitIndex = leaderCommit
		n.applyLocked()
	}
}

func (n *Node) handleAppendEntriesReplyLocked(m *Message) {
	if m.Term > n.currentTerm {
		n.becomeFollowerLocked(m.Term, "")
		return
	}
	if n.role != RoleLeader || m.Term != n.currentTerm {
		return
	}
	if m.Success {
		if m.MatchIndex > n.matchIndex[m.From] {
			n.matchIndex[m.From] = m.MatchIndex
		}
		n.nextIndex[m.From] = n.matchIndex[m.From] + 1
		n.maybeDropRemovedLocked(m.From, m.CommitIndex)
		n.advanceCommitLocked()
		return
	}
	// 日志不匹配：按 follower 提示回退 nextIndex 并重试（落后太多时改发快照）。
	next := m.MatchIndex + 1
	if next < 1 {
		next = 1
	}
	if next < n.nextIndex[m.From] {
		n.nextIndex[m.From] = next
	}
	n.sendAppendToLocked(m.From)
}

// advanceCommitLocked 仅提交当前任期的条目（Raft 安全规则），多数确认后推进。
// 多数派按当前已提交生效的投票成员配置计算；learner 不计入。
func (n *Node) advanceCommitLocked() {
	base := n.log[0].Index
	lastIdx, _ := n.lastIndexTermLocked()
	for i := n.commitIndex + 1; i <= lastIdx; i++ {
		if n.log[i-base].Term != n.currentTerm {
			continue
		}
		count := 0
		if n.isVoterLocked() {
			count = 1
		}
		for p := range n.members {
			if p != n.cfg.ID && n.matchIndex[p] >= i {
				count++
			}
		}
		if n.quorumReachedLocked(count) {
			n.commitIndex = i
		}
	}
	if n.commitIndex > n.lastApplied {
		n.applyLocked()
	}
}

// applyLocked 将已提交条目按序应用到状态机，带提案去重与等待者唤醒。
// 成员变更条目不进入业务状态机，而是直接更新成员配置（幂等集合操作）。
func (n *Node) applyLocked() {
	base := n.log[0].Index
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		e := n.log[n.lastApplied-base]
		var res any
		switch {
		case e.Type.IsConfig():
			n.applyConfigLocked(e)
		case e.Type == EntryNoop:
			// 空条目：仅用于推进提交位点，不进入状态机
		case e.ClientID != "" && n.lastSeq[e.ClientID] >= e.Seq:
			// 重复条目：幂等返回缓存结果，不重复应用。
			res = n.lastResult[e.ClientID]
			n.logf("apply skip duplicate index=%d client=%s seq=%d", e.Index, e.ClientID, e.Seq)
		default:
			res = n.sm.Apply(e.Op)
			if e.ClientID != "" {
				n.lastSeq[e.ClientID] = e.Seq
				n.lastResult[e.ClientID] = res
			}
		}
		if ch, ok := n.pending[e.Index]; ok {
			ch <- proposalResult{result: res}
			delete(n.pending, e.Index)
		}
	}
	_ = n.store.SaveApplyState(n.lastApplied, n.lastSeq, n.lastResult)
	n.logf("applied up to %d commitIndex=%d", n.lastApplied, n.commitIndex)
	// 自身已被移出集群：立即退位并终止在途提案，此后不再参选、不再计票。
	if !n.isVoterLocked() && !n.learners[n.cfg.ID] {
		if n.role != RoleFollower || n.leaderID == n.cfg.ID {
			n.logf("removed from cluster, step down term=%d commitIndex=%d", n.currentTerm, n.commitIndex)
		}
		if n.role == RoleLeader {
			n.failPendingLocked(&RejectError{Reason: ReasonStaleTerm, Detail: "leader removed from cluster"})
		}
		n.role = RoleFollower
		n.leaderID = ""
	}
	n.maybeSnapshotLocked()
}

// ---------------- 内部：工具 ----------------

func (n *Node) lastIndexTermLocked() (uint64, uint64) {
	e := n.log[len(n.log)-1]
	return e.Index, e.Term
}

func (n *Node) sendLocked(m *Message) {
	n.net.Send(m)
}

func (n *Node) failPendingLocked(err error) {
	for idx, ch := range n.pending {
		ch <- proposalResult{err: err}
		delete(n.pending, idx)
	}
}

func (n *Node) logf(format string, args ...any) {
	if n.lg != nil {
		n.lg.Printf("role=%s term=%d "+format, append([]any{n.role, n.currentTerm}, args...)...)
	}
}
