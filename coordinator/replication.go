package coordinator

// 本文件实现日志复制与提交：leader 追加与广播、follower 一致性检查、
// 冲突覆盖（带冲突提示）、多数确认后推进提交位点并应用状态机。
//
// 幂等性保证：
//   - AppendEntries 以 (PrevLogIndex, PrevLogTerm) 做一致性检查，重复/乱序
//     的心跳或重试不会让 follower 追加重复条目；
//   - 相同 index/term 的条目重复到达时直接成功返回；
//   - follower 落后时 leader 通过 nextIndex 回退补齐，不会跳过任何条目。

// submitProposal 在事件循环内处理新提案（仅 leader 接受）。
func (n *Node) submitProposal(command []byte, resCh chan proposeResult) {
	if n.role != RoleLeader {
		resCh <- proposeResult{err: &NotLeaderError{LeaderHint: n.leaderID}}
		return
	}
	e := LogEntry{Term: n.currentTerm, Index: n.lastIndex() + 1, Command: append([]byte(nil), command...)}
	n.appendLocal(e)
	n.matchIndex[n.id] = e.Index
	n.waiters[e.Index] = append(n.waiters[e.Index], resCh)
	n.log.Debug("proposal appended", "term", n.currentTerm, "index", e.Index)
	n.broadcastAppend()
}

// HandleAppendEntries 实现 RPCHandler。
func (n *Node) HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	respCh := make(chan *AppendEntriesResponse, 1)
	select {
	case n.events <- func() { respCh <- n.handleAppendEntries(req) }:
	case <-n.done:
		return &AppendEntriesResponse{}
	}
	return <-respCh
}

func (n *Node) handleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	resp := &AppendEntriesResponse{Term: n.currentTerm}

	if req.Term < n.currentTerm {
		// 旧 leader 的消息：拒绝，让它收到新任期后降级。
		return resp
	}
	if req.Term > n.currentTerm || n.role != RoleFollower {
		n.becomeFollower(req.Term, "AppendEntries from "+req.LeaderID)
	}
	// 当前任期的合法 leader：承认其权威并刷新选举超时。
	n.leaderID = req.LeaderID
	n.resetElectionTimer()

	// ---- 一致性检查 ----
	prev := req.PrevLogIndex
	if prev < n.snapIndex {
		// leader 的 prev 已落在我们的快照之内：正常情况下 leader 应改发
		// InstallSnapshot；告知冲突上界为快照之后。
		resp.ConflictIndex = n.snapIndex + 1
		resp.Term = n.currentTerm
		return resp
	}
	if prev > n.lastIndex() {
		// follower 日志太短：返回冲突提示（无 term，让 leader 直接退到自己的 lastIndex+1）。
		resp.ConflictIndex = n.lastIndex() + 1
		n.log.Info("append rejected: missing prev entry (log too short)",
			"term", n.currentTerm, "prevIndex", prev, "hint", resp.ConflictIndex)
		return resp
	}
	localPrev := n.entryAt(prev)
	if localPrev.Term != req.PrevLogTerm {
		// 任期冲突：返回该位置条目的任期及该任期首条下标，供 leader 快速回退。
		resp.ConflictTerm = localPrev.Term
		ci := prev
		for ci > n.snapIndex && n.entryAt(ci-1).Term == localPrev.Term {
			ci--
		}
		resp.ConflictIndex = ci
		n.log.Info("append rejected: term conflict at prev",
			"term", n.currentTerm, "prevIndex", prev,
			"localTerm", localPrev.Term, "wantTerm", req.PrevLogTerm,
			"conflictTerm", resp.ConflictTerm, "conflictIndex", resp.ConflictIndex)
		return resp
	}

	// ---- 合并新条目：相同 (index,term) 幂等保留；不同 term 则截断覆盖 ----
	for _, e := range req.Entries {
		existing := n.entryAt(e.Index)
		if e.Index <= n.lastIndex() && existing.Term == e.Term {
			continue // 重复投递，幂等跳过
		}
		// 冲突：删除本地从 e.Index 起的尾部（包括已有冲突条目与缺口之后）。
		if e.Index <= n.lastIndex() {
			n.log.Info("overwriting conflicting suffix",
				"term", n.currentTerm, "fromIndex", e.Index,
				"localTerm", existing.Term, "incomingTerm", e.Term)
			n.truncateSuffix(e.Index)
		}
		n.appendLocal(e)
	}

	resp.Success = true
	resp.MatchIndex = n.lastIndex()
	resp.Term = n.currentTerm

	// ---- 推进提交位点 ----
	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = min(req.LeaderCommit, n.lastIndex())
		n.log.Debug("follower commit advanced", "commitIndex", n.commitIndex)
	}
	n.applyCommitted()
	return resp
}

// truncateSuffix 删除下标 >= from 的内存与持久化日志。
func (n *Node) truncateSuffix(from uint64) {
	off := int(from - n.snapIndex)
	if off < 0 || off >= len(n.logs) {
		return
	}
	n.logs = n.logs[:off]
	if err := n.store.TruncateLog(from - 1); err != nil {
		n.log.Error("truncate persisted log failed", "err", err)
	}
}

// broadcastAppend 向所有 follower 并行发送 AppendEntries（心跳或复制）。
func (n *Node) broadcastAppend() {
	for _, peer := range n.peers {
		if peer == n.id {
			continue
		}
		n.sendAppend(peer)
	}
}

// sendAppend 向单个 follower 发送 AppendEntries；follower 已落后到快照范围则发快照。
func (n *Node) sendAppend(to string) {
	ni := n.nextIndex[to]
	if ni <= n.snapIndex {
		n.sendSnapshot(to)
		return
	}
	prevIdx := ni - 1
	prevEntry := n.entryAt(prevIdx)
	var entries []LogEntry
	for idx := ni; idx <= n.lastIndex(); idx++ {
		entries = append(entries, n.entryAt(idx))
	}
	req := &AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevEntry.Term,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	go func(peer string) {
		resp, err := n.transport.SendAppendEntries(peer, req)
		n.deliver(func() { n.onAppendResponse(peer, req, resp, err) })
	}(to)
}

// onAppendResponse 处理 follower 的复制响应。
func (n *Node) onAppendResponse(to string, req *AppendEntriesRequest, resp *AppendEntriesResponse, err error) {
	if err != nil {
		n.log.Debug("append rpc failed", "to", to, "err", err)
		return
	}
	if n.role != RoleLeader || req.Term != n.currentTerm {
		return // 过期响应
	}
	if resp.Term > n.currentTerm {
		n.becomeFollower(resp.Term, "higher term in append response from "+to)
		return
	}
	if resp.Term < n.currentTerm {
		return
	}
	if resp.Success {
		n.matchIndex[to] = max(resp.MatchIndex, req.PrevLogIndex+uint64(len(req.Entries)))
		n.nextIndex[to] = n.matchIndex[to] + 1
		n.advanceCommit()
		return
	}
	// 失败：按冲突提示回退 nextIndex，可解释地对齐日志。
	switch {
	case resp.ConflictTerm > 0:
		// 找到 leader 本地最后一条任期为 ConflictTerm 的下标；没有则退到 ConflictIndex。
		newNext := resp.ConflictIndex
		for idx := n.lastIndex(); idx > n.snapIndex; idx-- {
			if n.entryAt(idx).Term == resp.ConflictTerm {
				newNext = idx + 1
				break
			}
		}
		if newNext < n.nextIndex[to] {
			n.nextIndex[to] = newNext
		}
	default:
		n.nextIndex[to] = resp.ConflictIndex
	}
	n.log.Info("backing off nextIndex", "to", to,
		"conflictTerm", resp.ConflictTerm, "conflictIndex", resp.ConflictIndex,
		"newNextIndex", n.nextIndex[to])
	n.sendAppend(to)
}

// advanceCommit 在 leader 上按多数派 matchIndex 推进提交位点。
// 只有当前任期的条目可以直接通过计数提交（防止提交已被覆盖的前任期日志）。
func (n *Node) advanceCommit() {
	for idx := n.commitIndex + 1; idx <= n.lastIndex(); idx++ {
		if n.entryAt(idx).Term != n.currentTerm {
			continue
		}
		acks := 1 // leader 自己
		for _, peer := range n.peers {
			if peer != n.id && n.matchIndex[peer] >= idx {
				acks++
			}
		}
		if acks >= n.quorum() {
			n.commitIndex = idx
		}
	}
	n.applyCommitted()
}

// applyCommitted 将 (lastApplied, commitIndex] 的条目应用到状态机并唤醒提案等待者。
// 应用下标在每条应用后立即持久化，崩溃重启时最多重放最后一批，
// 配合 FSM.Apply 的幂等约定保证"不重复应用、不偏离已提交前缀"。
func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		e := n.entryAt(n.lastApplied)
		res := n.applyEntry(e)
		if err := n.store.SaveApplied(n.lastApplied); err != nil {
			n.log.Error("persist applied index failed", "err", err)
		}
		if len(e.Command) > 0 {
			n.log.Info("entry committed and applied",
				"term", e.Term, "index", e.Index,
				"commitIndex", n.commitIndex, "snapIndex", n.snapIndex)
		}
		for _, ch := range n.waiters[e.Index] {
			ch <- proposeResult{res: res}
		}
		delete(n.waiters, e.Index)
	}
	n.maybeSnapshot()
}
