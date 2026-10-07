package raft

import (
	"encoding/json"
	"time"
)

// snapshotEnvelope 快照信封：状态机数据 + 提案去重状态 + 成员配置，
// 保证安装快照的节点与生成方在去重视图和成员列表上一致。
type snapshotEnvelope struct {
	SMData   []byte            `json:"sm"`
	LastSeq  map[string]uint64 `json:"seq"`
	Voters   []NodeID          `json:"voters,omitempty"`
	Learners []NodeID          `json:"learners,omitempty"`
}

// snapshotMembership 从快照数据中解出成员配置；快照不含成员信息（旧格式）时返回 false。
func snapshotMembership(data []byte) (membership, bool) {
	var env snapshotEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return membership{}, false
	}
	if len(env.Voters) == 0 && len(env.Learners) == 0 {
		return membership{}, false
	}
	m := newMembership(env.Voters)
	for _, id := range env.Learners {
		if !m.Voters[id] {
			m.Learners[id] = true
		}
	}
	return m, true
}

// maybeSnapshotLocked 在日志超过阈值时，基于已应用（必已提交）状态生成快照。
// 调用方需持有 n.mu；快照与并发写入的交互通过同一把互斥锁串行化。
func (n *Node) maybeSnapshotLocked() {
	th := n.cfg.SnapshotThreshold
	if th <= 0 {
		return
	}
	base := n.log[0].Index
	// 仅对 lastApplied（已提交前缀）之内的部分做快照。
	if int(n.lastApplied-base) <= th {
		return
	}
	smData, err := n.sm.Snapshot()
	if err != nil {
		n.logf("snapshot failed: %v", err)
		return
	}
	// 快照携带的是已提交配置（lastApplied 必已提交），
	// 不含日志尾部尚未提交的变更，保证快照边界语义精确。
	env, err := json.Marshal(snapshotEnvelope{
		SMData:   smData,
		LastSeq:  n.lastSeq,
		Voters:   n.committedMem.voterList(),
		Learners: n.committedMem.learnerList(),
	})
	if err != nil {
		n.logf("snapshot encode failed: %v", err)
		return
	}
	snapIndex := n.lastApplied
	snapTerm := n.termAtLocked(snapIndex)
	if err := n.store.SaveSnapshot(snapIndex, snapTerm, env); err != nil {
		n.logf("snapshot persist failed: %v", err)
		return
	}
	n.log = n.store.Entries()
	// 快照边界推进：快照中的已提交配置成为新的重算基线。
	n.snapMem = n.committedMem.clone()
	n.logf("snapshot created boundary=%d/%d commitIndex=%d membership=%s", snapIndex, snapTerm, n.commitIndex, n.committedMem)
}

// restoreSnapshotLocked 用快照数据恢复状态机与去重状态（不触碰日志，由调用方维护）。
func (n *Node) restoreSnapshotLocked(index uint64, data []byte) {
	var env snapshotEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		n.logf("snapshot decode failed: %v", err)
		return
	}
	if err := n.sm.Restore(env.SMData); err != nil {
		n.logf("snapshot restore failed: %v", err)
		return
	}
	for k, v := range env.LastSeq {
		if n.lastSeq[k] < v {
			n.lastSeq[k] = v
		}
	}
	if index > n.commitIndex {
		n.commitIndex = index
	}
	if index > n.lastApplied {
		n.lastApplied = index
	}
}

// sendSnapshotToLocked 向落后过多的 follower 发送快照（leader 侧）。
func (n *Node) sendSnapshotToLocked(peer NodeID) {
	index, term, data, err := n.store.LoadSnapshot()
	if err != nil || len(data) == 0 {
		return
	}
	n.logf("send snapshot to=%s boundary=%d/%d", peer, index, term)
	n.sendLocked(&Message{
		Type: MsgInstallSnapshot, From: n.cfg.ID, To: peer, Term: n.currentTerm,
		SnapshotIndex: index, SnapshotTerm: term, SnapshotData: data,
	})
}

// handleInstallSnapshotLocked 处理 leader 发来的快照（follower 侧）。
func (n *Node) handleInstallSnapshotLocked(m *Message) {
	reply := &Message{Type: MsgInstallSnapshotReply, From: n.cfg.ID, To: m.From, Term: n.currentTerm}
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
	if m.SnapshotIndex <= base {
		// 已覆盖，幂等成功。
		reply.Success = true
		reply.MatchIndex = base
		n.sendLocked(reply)
		return
	}

	// 持久化快照并截断日志前缀（存储层保留快照点之后的后缀）。
	if err := n.store.SaveSnapshot(m.SnapshotIndex, m.SnapshotTerm, m.SnapshotData); err != nil {
		reply.Reason = ReasonLogMismatch
		n.sendLocked(reply)
		return
	}
	n.log = n.store.Entries()
	n.restoreSnapshotLocked(m.SnapshotIndex, m.SnapshotData)
	// 快照中的成员配置成为新基线，叠加日志后缀中的变更。
	if sm, ok := snapshotMembership(m.SnapshotData); ok {
		n.snapMem = sm
		n.committedMem = sm
	}
	n.recomputeMembershipLocked()
	_ = n.store.SaveApplyState(n.lastApplied, n.lastSeq, n.lastResult)
	n.logf("snapshot installed boundary=%d/%d commitIndex=%d membership=%s",
		m.SnapshotIndex, m.SnapshotTerm, n.commitIndex, n.mem)

	reply.Success = true
	reply.MatchIndex = m.SnapshotIndex
	n.sendLocked(reply)
}

// handleInstallSnapshotReplyLocked 处理快照安装回复（leader 侧）。
func (n *Node) handleInstallSnapshotReplyLocked(m *Message) {
	if m.Term > n.currentTerm {
		n.becomeFollowerLocked(m.Term, "")
		return
	}
	if n.role != RoleLeader || m.Term != n.currentTerm || !m.Success {
		return
	}
	if m.MatchIndex > n.matchIndex[m.From] {
		n.matchIndex[m.From] = m.MatchIndex
	}
	n.nextIndex[m.From] = n.matchIndex[m.From] + 1
	n.advanceCommitLocked()
}

// termAtLocked 返回指定索引日志的任期（需在快照边界之后）。
func (n *Node) termAtLocked(index uint64) uint64 {
	base := n.log[0].Index
	if index < base || int(index-base) >= len(n.log) {
		return 0
	}
	return n.log[index-base].Term
}
