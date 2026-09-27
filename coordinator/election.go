package coordinator

// 本文件实现领导选举：超时发起选举、投票处理、任期回退。
// 不变量：同一任期内一个节点至多投出一票；获得多数票才成为 leader；
// 发现更高任期立即降级为 follower；旧任期的响应一律忽略。

// persistStable 保存任期与投票。
func (n *Node) persistStable() {
	if err := n.store.SaveStable(StableState{CurrentTerm: n.currentTerm, VotedFor: n.votedFor}); err != nil {
		n.log.Error("persist stable state failed", "term", n.currentTerm, "err", err)
	}
}

// becomeFollower 降级为 follower。term 不得小于当前任期。
// reason 用于日志区分降级原因（更高任期的 RPC / 投票响应 / 发现新 leader 等）。
func (n *Node) becomeFollower(term uint64, reason string) {
	if term < n.currentTerm {
		// 不应发生：调用方需先比较任期。保守忽略。
		return
	}
	wasLeader := n.role == RoleLeader
	advanced := term > n.currentTerm
	if advanced {
		n.currentTerm = term
		n.votedFor = ""
		n.persistStable()
	}
	n.role = RoleFollower
	n.leaderID = ""
	n.resetElectionTimer()
	if wasLeader {
		// leader 失去领导权：等待本节点提交的提案必须以可区分原因失败。
		n.failAllWaiters(ErrLeadershipLost)
	}
	n.log.Info("role change -> follower", "term", n.currentTerm,
		"reason", reason, "advanced", advanced)
}

// becomeCandidate 超时后发起新一轮选举。
func (n *Node) becomeCandidate() {
	n.role = RoleCandidate
	n.currentTerm++
	n.votedFor = n.id // 先投自己
	n.leaderID = ""
	n.votesReceived = 1
	n.persistStable()
	n.resetElectionTimer()

	term := n.currentTerm
	n.log.Info("role change -> candidate; election started",
		"term", term, "votes", n.votesReceived, "quorum", n.quorum())

	// 单节点集群：自己一票即多数。
	if n.votesReceived >= n.quorum() {
		n.becomeLeader()
		return
	}

	req := &RequestVoteRequest{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: n.lastIndex(),
		LastLogTerm:  n.lastTerm(),
	}
	for _, peer := range n.peers {
		if peer == n.id {
			continue
		}
		go func(to string) {
			resp, err := n.transport.SendRequestVote(to, req)
			n.deliver(func() { n.onVoteResponse(term, to, resp, err) })
		}(peer)
	}
}

// onVoteResponse 处理异步返回的投票响应。
func (n *Node) onVoteResponse(term uint64, from string, resp *RequestVoteResponse, err error) {
	if err != nil {
		// 分区/节点停止：计不上票即可，无需其他动作；超时后会重选举。
		n.log.Debug("request vote failed", "term", term, "from", from, "err", err)
		return
	}
	// 旧任期或身份已变：响应过期，忽略（防止重复/迟到响应干扰）。
	if n.role != RoleCandidate || n.currentTerm != term {
		return
	}
	if resp.Term > n.currentTerm {
		n.becomeFollower(resp.Term, "higher term in vote response from "+from)
		return
	}
	if resp.Term < n.currentTerm {
		return
	}
	if !resp.VoteGranted {
		n.log.Info("vote denied", "term", term, "from", from)
		return
	}
	n.votesReceived++
	n.log.Info("vote granted", "term", term, "from", from,
		"votes", n.votesReceived, "quorum", n.quorum())
	if n.votesReceived >= n.quorum() {
		n.becomeLeader()
	}
}

// becomeLeader 在获得多数票后成为 leader，并广播一条当前任期的 no-op：
// 这保证 leader 能随心跳安全提交前任期遗留条目（Raft 提交规则）。
func (n *Node) becomeLeader() {
	n.role = RoleLeader
	n.leaderID = n.id
	last := n.lastIndex()
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
	}
	n.matchIndex[n.id] = last

	n.log.Info("role change -> leader elected", "term", n.currentTerm,
		"lastIndex", last, "votes", n.votesReceived, "commitIndex", n.commitIndex)

	noop := LogEntry{Term: n.currentTerm, Index: last + 1} // Command==nil：no-op
	n.appendLocal(noop)
	n.broadcastAppend()
}

// HandleRequestVote 实现 RPCHandler（从 transport goroutine 转发到事件循环）。
func (n *Node) HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	respCh := make(chan *RequestVoteResponse, 1)
	select {
	case n.events <- func() { respCh <- n.handleRequestVote(req) }:
	case <-n.done:
		return &RequestVoteResponse{}
	}
	return <-respCh
}

// handleRequestVote 在事件循环内执行投票判断。
func (n *Node) handleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	resp := &RequestVoteResponse{Term: n.currentTerm}

	if req.Term < n.currentTerm {
		// 任期回退的候选人：拒绝（不降级自己）。
		n.log.Info("deny vote: stale term",
			"reqTerm", req.Term, "term", n.currentTerm, "from", req.CandidateID)
		return resp
	}

	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term, "higher term in RequestVote from "+req.CandidateID)
	}

	if !n.isMember(req.CandidateID) {
		n.log.Info("deny vote: candidate not member", "from", req.CandidateID)
		resp.Term = n.currentTerm
		return resp
	}

	// 日志足够新：最后任期更大，或任期相同且下标不落后。
	candidateUpToDate := req.LastLogTerm > n.lastTerm() ||
		(req.LastLogTerm == n.lastTerm() && req.LastLogIndex >= n.lastIndex())

	if (n.votedFor == "" || n.votedFor == req.CandidateID) && candidateUpToDate {
		n.votedFor = req.CandidateID
		n.persistStable()
		n.resetElectionTimer()
		resp.VoteGranted = true
		n.log.Info("vote granted", "term", n.currentTerm, "to", req.CandidateID,
			"reqLastIndex", req.LastLogIndex, "reqLastTerm", req.LastLogTerm)
	} else {
		n.log.Info("deny vote", "term", n.currentTerm, "to", req.CandidateID,
			"alreadyVoted", n.votedFor, "upToDate", candidateUpToDate,
			"localLastIndex", n.lastIndex(), "localLastTerm", n.lastTerm())
	}
	resp.Term = n.currentTerm
	return resp
}
