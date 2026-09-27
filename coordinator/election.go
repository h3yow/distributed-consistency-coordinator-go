package coordinator

// 本文件实现领导选举：超时发起选举、投票处理、任期回退。
// 不变量：同一任期内一个节点至多投出一票；获得多数票才成为 leader；
// 发现更高任期立即降级为 follower。

// becomeFollower 降级为 follower（term 不小于当前任期时调用）。
// reason 用于日志区分降级原因。
func (n *Node) becomeFollower(term uint64, reason string) {
	// TODO
}

// becomeCandidate 发起新一轮选举。
func (n *Node) becomeCandidate() {
	// TODO
}

// becomeLeader 在获得多数票后成为 leader。
func (n *Node) becomeLeader() {
	// TODO
}

// HandleRequestVote 实现 RPCHandler。
func (n *Node) HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	respCh := make(chan *RequestVoteResponse, 1)
	select {
	case n.events <- func() { respCh <- n.handleRequestVote(req) }:
	case <-n.done:
		return &RequestVoteResponse{}
	}
	return <-respCh
}

func (n *Node) handleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	// TODO
	return &RequestVoteResponse{Term: n.currentTerm}
}
