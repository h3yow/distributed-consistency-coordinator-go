package coordinator

// 本文件实现日志复制与提交：leader 追加与广播、follower 一致性检查、
// 冲突覆盖、多数确认后推进提交位点并应用状态机。

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
	// TODO
	return &AppendEntriesResponse{Term: n.currentTerm}
}

// broadcastAppend 向所有 follower 发送 AppendEntries（心跳或复制）。
func (n *Node) broadcastAppend() {
	// TODO
}

// sendAppend 向单个 follower 发送 AppendEntries，从 nextIndex 推导条目。
func (n *Node) sendAppend(to string) {
	// TODO
}

// advanceCommit 根据 matchIndex 尝试推进 leader 提交位点。
func (n *Node) advanceCommit() {
	// TODO
}

// applyCommitted 将 (lastApplied, commitIndex] 的条目应用到状态机。
func (n *Node) applyCommitted() {
	// TODO
}
