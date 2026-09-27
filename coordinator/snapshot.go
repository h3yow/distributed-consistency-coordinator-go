package coordinator

// 本文件实现快照压缩与落后节点追赶：
// 快照只基于已提交状态生成；InstallSnapshot 用于 follower 严重落后时恢复。

// maybeSnapshot 在已提交未快照条目数超过阈值时生成快照。
func (n *Node) maybeSnapshot() {
	// TODO
}

// takeSnapshot 基于 lastApplied 生成快照并压缩日志。
func (n *Node) takeSnapshot() {
	// TODO
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
	// TODO
	return &InstallSnapshotResponse{Term: n.currentTerm}
}
