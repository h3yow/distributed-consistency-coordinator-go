package coordinator

// 本文件实现快照压缩与落后节点追赶。
//
// 不变量：
//   - 快照只基于已提交且已应用的状态生成（lastApplied <= commitIndex）；
//   - 快照与日志压缩按序持久化，崩溃后重启至多重复应用最后一条（FSM 幂等兜底）；
//   - InstallSnapshot 接收方先校验任期与快照边界，再整体替换状态机与日志前缀，
//     不会丢失快照边界之后的已提交条目，也不会重复应用快照覆盖的条目；
//   - 快照期间并发写入的条目位于快照边界之后，不受影响。

// maybeSnapshot 在已应用未快照条目数超过阈值时生成快照（仅事件循环内调用）。
func (n *Node) maybeSnapshot() {
	if n.cfg.SnapshotThreshold == 0 {
		return
	}
	if n.lastApplied-n.snapIndex < n.cfg.SnapshotThreshold {
		return
	}
	n.takeSnapshot()
}

// takeSnapshot 基于 lastApplied 生成快照并压缩日志。
func (n *Node) takeSnapshot() {
	if n.lastApplied <= n.snapIndex {
		return
	}
	data, err := n.fsm.Snapshot()
	if err != nil {
		n.log.Error("fsm snapshot failed", "err", err)
		return
	}
	meta := SnapshotMeta{
		LastIncludedIndex: n.lastApplied,
		LastIncludedTerm:  n.entryAt(n.lastApplied).Term,
	}
	// 先持久化快照，再丢弃日志前缀：崩溃时宁可保留冗余日志，也不丢已提交数据。
	if err := n.store.SaveSnapshot(meta, data); err != nil {
		n.log.Error("persist snapshot failed", "err", err)
		return
	}
	if err := n.store.DiscardPrefix(meta.LastIncludedIndex); err != nil {
		n.log.Error("discard log prefix failed", "err", err)
	}
	n.snapIndex = meta.LastIncludedIndex
	n.snapTerm = meta.LastIncludedTerm
	n.snapData = data
	// 压缩内存日志：logs[0] 为快照边界哨兵。
	kept := n.logs[:1]
	n.logs[0] = LogEntry{Index: n.snapIndex, Term: n.snapTerm}
	for _, e := range n.logs[1:] {
		if e.Index > n.snapIndex {
			kept = append(kept, e)
		}
	}
	n.logs = kept
	n.log.Info("snapshot taken",
		"snapIndex", n.snapIndex, "snapTerm", n.snapTerm,
		"commitIndex", n.commitIndex, "lastApplied", n.lastApplied,
		"logLen", len(n.logs)-1)
}

// sendSnapshot 向落后到已快照范围的 follower 发送 InstallSnapshot。
func (n *Node) sendSnapshot(to string) {
	if n.snapIndex == 0 || len(n.snapData) == 0 {
		return
	}
	req := &InstallSnapshotRequest{
		Term:     n.currentTerm,
		LeaderID: n.id,
		Meta:     SnapshotMeta{LastIncludedIndex: n.snapIndex, LastIncludedTerm: n.snapTerm},
		Data:     n.snapData,
	}
	n.log.Info("sending snapshot to follower", "to", to,
		"snapIndex", req.Meta.LastIncludedIndex, "snapTerm", req.Meta.LastIncludedTerm)
	go func(peer string) {
		resp, err := n.transport.SendInstallSnapshot(peer, req)
		n.deliver(func() { n.onSnapshotResponse(peer, req, resp, err) })
	}(to)
}

// onSnapshotResponse 处理快照响应：成功后 follower 的 nextIndex 推进到快照边界之后。
func (n *Node) onSnapshotResponse(to string, req *InstallSnapshotRequest, resp *InstallSnapshotResponse, err error) {
	if err != nil {
		n.log.Debug("install snapshot rpc failed", "to", to, "err", err)
		return
	}
	if n.role != RoleLeader || req.Term != n.currentTerm {
		return
	}
	if resp.Term > n.currentTerm {
		n.becomeFollower(resp.Term, "higher term in snapshot response from "+to)
		return
	}
	n.matchIndex[to] = req.Meta.LastIncludedIndex
	n.nextIndex[to] = req.Meta.LastIncludedIndex + 1
	n.log.Info("follower installed snapshot", "to", to,
		"snapIndex", req.Meta.LastIncludedIndex, "nextIndex", n.nextIndex[to])
	// 继续用 AppendEntries 补齐快照之后的条目。
	n.sendAppend(to)
}

// HandleInstallSnapshot 实现 RPCHandler。
func (n *Node) HandleInstallSnapshot(req *InstallSnapshotRequest) *InstallSnapshotResponse {
	respCh := make(chan *InstallSnapshotResponse, 1)
	select {
	case n.events <- func() { respCh <- n.handleInstallSnapshot(req) }:
	case <-n.done:
		return &InstallSnapshotResponse{}
	}
	return <-respCh
}

func (n *Node) handleInstallSnapshot(req *InstallSnapshotRequest) *InstallSnapshotResponse {
	resp := &InstallSnapshotResponse{Term: n.currentTerm}
	if req.Term < n.currentTerm {
		return resp
	}
	if req.Term > n.currentTerm || n.role != RoleFollower {
		n.becomeFollower(req.Term, "InstallSnapshot from "+req.LeaderID)
	}
	n.leaderID = req.LeaderID
	n.resetElectionTimer()

	meta := req.Meta
	if meta.LastIncludedIndex <= n.snapIndex {
		// 过期快照：幂等忽略。
		n.log.Info("ignoring stale snapshot",
			"reqSnapIndex", meta.LastIncludedIndex, "snapIndex", n.snapIndex)
		resp.Term = n.currentTerm
		return resp
	}

	// 恢复快照状态机。
	if err := n.fsm.Restore(req.Data); err != nil {
		n.log.Error("restore snapshot failed", "err", err)
		resp.Term = n.currentTerm
		return resp
	}

	// 持久化快照与日志前缀（先快照后丢前缀，崩溃安全）。
	if err := n.store.SaveSnapshot(meta, req.Data); err != nil {
		n.log.Error("persist snapshot failed", "err", err)
	}
	if err := n.store.DiscardPrefix(meta.LastIncludedIndex); err != nil {
		n.log.Error("discard log prefix failed", "err", err)
	}

	// 调整内存日志：若边界之后存在 term 匹配的条目则保留其后缀，否则清空。
	var suffix []LogEntry
	if meta.LastIncludedIndex <= n.lastIndex() &&
		n.entryAt(meta.LastIncludedIndex).Term == meta.LastIncludedTerm {
		for _, e := range n.logs {
			if e.Index > meta.LastIncludedIndex {
				suffix = append(suffix, e)
			}
		}
	}
	n.logs = append([]LogEntry{{Index: meta.LastIncludedIndex, Term: meta.LastIncludedTerm}}, suffix...)
	n.snapIndex = meta.LastIncludedIndex
	n.snapTerm = meta.LastIncludedTerm
	n.snapData = append([]byte(nil), req.Data...)

	// 快照内容全部已提交：提交位点至少推进到快照边界；应用位点直接到位（FSM 已恢复）。
	if n.commitIndex < meta.LastIncludedIndex {
		n.commitIndex = meta.LastIncludedIndex
	}
	n.lastApplied = meta.LastIncludedIndex
	if err := n.store.SaveApplied(n.lastApplied); err != nil {
		n.log.Error("persist applied failed", "err", err)
	}
	// 快照边界之后的已提交条目照常应用。
	n.applyCommitted()

	n.log.Info("snapshot installed",
		"snapIndex", n.snapIndex, "snapTerm", n.snapTerm,
		"commitIndex", n.commitIndex, "lastIndex", n.lastIndex())
	resp.Term = n.currentTerm
	return resp
}
