package raft

import (
	"encoding/json"
	"time"
)

// snapshotEnvelope 快照信封：状态机数据 + 提案去重状态 + 成员配置，
// 保证安装快照的节点与生成方在去重视图和成员配置上完全一致。
type snapshotEnvelope struct {
	SMData   []byte            `json:"sm"`
	LastSeq  map[string]uint64 `json:"seq"`
	Members  []NodeID          `json:"members,omitempty"`
	Learners []NodeID          `json:"learners,omitempty"`
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
	env, err := json.Marshal(snapshotEnvelope{
		SMData:   smData,
		LastSeq:  n.lastSeq,
		Members:  sortedKeysLocked(n.members),
		Learners: sortedKeysLocked(n.learners),
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
	n.logf("snapshot created boundary=%d/%d commitIndex=%d", snapIndex, snapTerm, n.commitIndex)
}

// restoreSnapshotLocked 用快照数据恢复状态机、去重状态与成员配置（不触碰日志，由调用方维护）。
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
	// 成员配置随快照整体替换（快照点处的已提交配置）；旧版本快照无此字段时保持现状。
	if env.Members != nil || env.Learners != nil {
		n.members = make(map[NodeID]bool, len(env.Members))
		for _, id := range env.Members {
			n.members[id] = true
		}
		n.learners = make(map[NodeID]bool, len(env.Learners))
		for _, id := range env.Learners {
			n.learners[id] = true
		}
		n.logf("membership restored from snapshot: members=%v learners=%v", env.Members, env.Learners)
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
		reply.CommitIndex = n.commitIndex
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
	_ = n.store.SaveApplyState(n.lastApplied, n.lastSeq, n.lastResult)
	n.logf("snapshot installed boundary=%d/%d commitIndex=%d", m.SnapshotIndex, m.SnapshotTerm, n.commitIndex)

	reply.Success = true
	reply.MatchIndex = m.SnapshotIndex
	reply.CommitIndex = n.commitIndex
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
	n.maybeDropRemovedLocked(m.From, m.CommitIndex)
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
