package raft

import (
	"context"
	"sort"
	"time"
)

// 本文件实现在线成员变更：单节点变更（一次只增/删一个节点），
// 变更以日志条目形式提交，在提交（apply）时生效。
//
// 安全论证：任意两个相邻生效配置之间只差一个节点，二者的多数派必然相交，
// 因此不会出现两个同时能推进提交的多数派，已提交日志不会回退。
//
// 扩容两阶段：AddLearner（成为非投票 learner，追平数据，不计入多数派）
// → 追平后 Promote（成为投票成员）。缩容单阶段：RemoveNode 提交即生效。

// Membership 返回当前已提交生效的成员配置：投票成员与 learner。
func (n *Node) Membership() (voters, learners []NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for id := range n.members {
		voters = append(voters, id)
	}
	for id := range n.learners {
		learners = append(learners, id)
	}
	sort.Slice(voters, func(i, j int) bool { return voters[i] < voters[j] })
	sort.Slice(learners, func(i, j int) bool { return learners[i] < learners[j] })
	return voters, learners
}

// IsVoter 报告本节点当前是否为投票成员。
func (n *Node) IsVoter() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.members[n.cfg.ID]
}

// AddNode 在线扩容：先把 id 作为 learner 拉入集群并等待其追平已提交状态，
// 再将其提升为投票成员。重复调用幂等：已是投票成员时直接返回成功。
// 只有在追平并完成 Promote 提交之后，新节点才被计入多数派。
func (n *Node) AddNode(ctx context.Context, id NodeID) error {
	n.mu.Lock()
	if n.members[id] {
		n.mu.Unlock()
		return nil // 已是投票成员：幂等成功
	}
	alreadyLearner := n.learners[id]
	n.mu.Unlock()

	if !alreadyLearner {
		if err := n.proposeConfig(ctx, EntryAddLearner, id); err != nil {
			return err
		}
	}
	if err := n.waitCaughtUp(ctx, id); err != nil {
		return err
	}
	return n.proposeConfig(ctx, EntryPromote, id)
}

// RemoveNode 在线缩容：提交一条移除条目，提交后 id 立即退出多数派计算。
// 对不存在的节点调用为幂等成功。移除 leader 自身时，本节点在提交后自动退位。
func (n *Node) RemoveNode(ctx context.Context, id NodeID) error {
	n.mu.Lock()
	present := n.members[id] || n.learners[id]
	n.mu.Unlock()
	if !present {
		return nil // 已不在集群中：幂等成功
	}
	return n.proposeConfig(ctx, EntryRemoveNode, id)
}

// proposeConfig 向日志追加一条成员变更条目并等待其提交生效。
// 同一时刻只允许一条未提交的变更（Raft 单节点变更约束）：
//   - 本任期已有未提交变更 → 返回 ReasonConfigBusy（并发变更互斥）；
//   - 仅有前任期遗留的未提交变更（换主后）→ 先提交一条本任期空条目
//     使其随提交位点推进一并生效，再继续本次变更。
func (n *Node) proposeConfig(ctx context.Context, typ EntryType, id NodeID) error {
	if err := n.drainPendingConfigLocked(ctx); err != nil {
		return err
	}
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return &RejectError{Reason: ReasonShutdown, Detail: "node stopped"}
	}
	if n.role != RoleLeader {
		hint := n.leaderID
		n.mu.Unlock()
		return &RejectError{Reason: ReasonNotLeader, Leader: hint, Detail: "not leader"}
	}
	if n.hasPendingConfigLocked() {
		n.mu.Unlock()
		return &RejectError{Reason: ReasonConfigBusy, Detail: "another membership change is not yet committed"}
	}
	lastIdx, _ := n.lastIndexTermLocked()
	voters, learners := n.configAfterLocked(typ, id)
	entry := LogEntry{
		Term: n.currentTerm, Index: lastIdx + 1, Type: typ, NodeID: id,
		Voters: voters, Learners: learners,
	}
	n.appendLeaderEntryLocked(&entry)
	n.logf("config propose %s target=%s index=%d term=%d members=%v learners=%v",
		typ, id, entry.Index, entry.Term, sortedKeysLocked(n.members), sortedKeysLocked(n.learners))
	ch := n.pending[entry.Index]
	n.mu.Unlock()

	select {
	case r := <-ch:
		return r.err
	case <-ctx.Done():
		return &RejectError{Reason: ReasonTimeout, Detail: "config change not committed: " + ctx.Err().Error()}
	}
}

// drainPendingConfigLocked 处理换主后遗留的未提交成员变更：
// 若遗留变更属于前任期，追加并等待一条本任期空条目提交，使其一并生效。
func (n *Node) drainPendingConfigLocked(ctx context.Context) error {
	for {
		n.mu.Lock()
		if n.stopped {
			n.mu.Unlock()
			return &RejectError{Reason: ReasonShutdown, Detail: "node stopped"}
		}
		if n.role != RoleLeader {
			hint := n.leaderID
			n.mu.Unlock()
			return &RejectError{Reason: ReasonNotLeader, Leader: hint, Detail: "not leader"}
		}
		pending, fromCurrentTerm := n.pendingConfigTermLocked()
		if !pending {
			n.mu.Unlock()
			return nil
		}
		if fromCurrentTerm {
			n.mu.Unlock()
			return nil // 本任期的未提交变更由调用方按 ConfigBusy 处理
		}
		noop := LogEntry{Term: n.currentTerm, Type: EntryNoop}
		n.appendLeaderEntryLocked(&noop)
		n.logf("commit noop index=%d to drain previous-term config change", noop.Index)
		ch := n.pending[noop.Index]
		n.mu.Unlock()

		select {
		case r := <-ch:
			if r.err != nil {
				return r.err
			}
		case <-ctx.Done():
			return &RejectError{Reason: ReasonTimeout, Detail: "drain pending config: " + ctx.Err().Error()}
		}
	}
}

// appendLeaderEntryLocked 为条目分配索引、追加到日志并注册等待通道（需持有锁且为 leader）。
func (n *Node) appendLeaderEntryLocked(e *LogEntry) {
	lastIdx, _ := n.lastIndexTermLocked()
	e.Index = lastIdx + 1
	e.Term = n.currentTerm
	n.log = append(n.log, *e)
	_ = n.store.Append([]LogEntry{*e})
	n.matchIndex[n.cfg.ID] = e.Index
	n.pending[e.Index] = make(chan proposalResult, 1)
	n.broadcastAppendLocked()
}

// pendingConfigTermLocked 报告是否存在未提交成员变更，以及是否包含本任期条目。
func (n *Node) pendingConfigTermLocked() (pending, fromCurrentTerm bool) {
	base := n.log[0].Index
	lastIdx, _ := n.lastIndexTermLocked()
	for i := n.commitIndex + 1; i <= lastIdx; i++ {
		if n.log[i-base].Type.IsConfig() {
			pending = true
			if n.log[i-base].Term == n.currentTerm {
				fromCurrentTerm = true
			}
		}
	}
	return pending, fromCurrentTerm
}

// hasPendingConfigLocked 报告是否存在已追加但未提交的成员变更条目。
func (n *Node) hasPendingConfigLocked() bool {
	base := n.log[0].Index
	lastIdx, _ := n.lastIndexTermLocked()
	for i := n.commitIndex + 1; i <= lastIdx; i++ {
		if n.log[i-base].Type.IsConfig() {
			return true
		}
	}
	return false
}

// waitCaughtUp 等待 learner 的复制进度追平当前提交位点（仅 leader 上有意义）。
func (n *Node) waitCaughtUp(ctx context.Context, id NodeID) error {
	tick := time.NewTicker(n.cfg.HeartbeatInterval)
	defer tick.Stop()
	for {
		n.mu.Lock()
		if n.stopped {
			n.mu.Unlock()
			return &RejectError{Reason: ReasonShutdown, Detail: "node stopped"}
		}
		if n.role != RoleLeader {
			hint := n.leaderID
			n.mu.Unlock()
			return &RejectError{Reason: ReasonNotLeader, Leader: hint, Detail: "lost leadership during catch-up"}
		}
		match, commit := n.matchIndex[id], n.commitIndex
		if match >= commit {
			n.logf("learner %s caught up: matchIndex=%d commitIndex=%d", id, match, commit)
			n.mu.Unlock()
			return nil
		}
		n.mu.Unlock()
		select {
		case <-ctx.Done():
			return &RejectError{Reason: ReasonTimeout, Detail: "learner catch-up timeout: " + ctx.Err().Error()}
		case <-tick.C:
		}
	}
}

// configAfterLocked 计算应用 typ(id) 变更后的完整成员配置。
func (n *Node) configAfterLocked(typ EntryType, id NodeID) (voters, learners []NodeID) {
	m := make(map[NodeID]bool, len(n.members))
	for k, v := range n.members {
		m[k] = v
	}
	l := make(map[NodeID]bool, len(n.learners))
	for k, v := range n.learners {
		l[k] = v
	}
	switch typ {
	case EntryAddLearner:
		if !m[id] {
			l[id] = true
		}
	case EntryPromote:
		delete(l, id)
		m[id] = true
	case EntryRemoveNode:
		delete(m, id)
		delete(l, id)
	}
	return sortedKeysLocked(m), sortedKeysLocked(l)
}

// applyConfigLocked 在成员变更条目提交时生效（applyLocked 与重启回放共用）。
// 条目携带变更后的完整成员配置，应用即整体替换，天然幂等。
func (n *Node) applyConfigLocked(e LogEntry) {
	newMembers := make(map[NodeID]bool, len(e.Voters))
	for _, id := range e.Voters {
		newMembers[id] = true
	}
	newLearners := make(map[NodeID]bool, len(e.Learners))
	for _, id := range e.Learners {
		newLearners[id] = true
	}
	// leader 侧同步复制进度表：新加入节点从头（或快照）追；
	// 退出节点保留复制通道直到其确认收到移除条目。
	if n.role == RoleLeader {
		for id := range newMembers {
			n.trackNewPeerLocked(id)
		}
		for id := range newLearners {
			n.trackNewPeerLocked(id)
		}
		for id := range n.members {
			if !newMembers[id] && !newLearners[id] {
				n.trackRemovedPeerLocked(id, e.Index)
			}
		}
		for id := range n.learners {
			if !newMembers[id] && !newLearners[id] {
				n.trackRemovedPeerLocked(id, e.Index)
			}
		}
	}
	n.members = newMembers
	n.learners = newLearners
	n.logf("config committed %s target=%s index=%d members=%v learners=%v commitIndex=%d",
		e.Type, e.NodeID, e.Index, sortedKeysLocked(n.members), sortedKeysLocked(n.learners), n.commitIndex)
}

// trackNewPeerLocked 为新加入的节点初始化复制进度（leader 侧）。
func (n *Node) trackNewPeerLocked(id NodeID) {
	if id == n.cfg.ID {
		return
	}
	delete(n.removedAck, id)
	if _, ok := n.nextIndex[id]; !ok {
		n.nextIndex[id] = 1
		n.matchIndex[id] = 0
	}
}

// trackRemovedPeerLocked 登记退出节点，继续复制直到其确认移除条目（leader 侧）。
func (n *Node) trackRemovedPeerLocked(id NodeID, index uint64) {
	if id == n.cfg.ID {
		delete(n.nextIndex, id)
		delete(n.matchIndex, id)
		return
	}
	n.removedAck[id] = index
}

// isVoterLocked 报告自身是否为投票成员（决定是否可发起选举/计票）。
func (n *Node) isVoterLocked() bool { return n.members[n.cfg.ID] }

// replicationTargetsLocked 返回需要复制日志的对端：投票成员 + learner
// + 尚未确认移除条目的已移除节点（不含自身）。
func (n *Node) replicationTargetsLocked() []NodeID {
	out := make([]NodeID, 0, len(n.members)+len(n.learners)+len(n.removedAck))
	for id := range n.members {
		if id != n.cfg.ID {
			out = append(out, id)
		}
	}
	for id := range n.learners {
		if id != n.cfg.ID {
			out = append(out, id)
		}
	}
	for id := range n.removedAck {
		if id != n.cfg.ID {
			out = append(out, id)
		}
	}
	return out
}

// maybeDropRemovedLocked 被移除节点确认已应用移除条目（回显的提交位点
// 覆盖移除条目）后，彻底停止对其复制。
func (n *Node) maybeDropRemovedLocked(id NodeID, knownCommit uint64) {
	idx, ok := n.removedAck[id]
	if !ok {
		return
	}
	if knownCommit >= idx {
		delete(n.removedAck, id)
		delete(n.nextIndex, id)
		delete(n.matchIndex, id)
		n.logf("removed node %s applied removal at index=%d, stop replicating", id, idx)
	}
}

func sortedKeysLocked(m map[NodeID]bool) []NodeID {
	out := make([]NodeID, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
