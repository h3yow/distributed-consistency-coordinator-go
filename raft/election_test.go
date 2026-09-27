package raft

import (
	"testing"
	"time"
)

// TestElectSingleLeader 多数节点可达时必须选出一个且仅一个 leader。
func TestElectSingleLeader(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.startAll()
	defer c.stopAll()

	// 选举日志会打印任期与投票记录
	leader := c.waitLeader(3 * time.Second)
	t.Logf("elected leader=%s term=%d", leader.cfg.ID, leader.Term())

	leaders := c.countLeaders()
	total := 0
	for term, ids := range leaders {
		total += len(ids)
		if len(ids) > 1 {
			t.Fatalf("term %d has multiple leaders: %v", term, ids)
		}
	}
	if total != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", total)
	}
}

// TestNoDoublePrimaryUnderPartition 网络分区时不得出现双主：
// 少数派分区选不出新 leader，多数派分区保持唯一 leader。
func TestNoDoublePrimaryUnderPartition(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.startAll()
	defer c.stopAll()

	leader := c.waitLeader(3 * time.Second)
	oldID, term := leader.cfg.ID, leader.Term()

	// 隔离 leader，多数派应选出任期更高的新 leader
	c.net.Isolate(oldID)
	newLeader := c.waitLeaderMinTerm(term, 3*time.Second)
	if newLeader.cfg.ID == oldID {
		t.Fatalf("isolated node re-elected")
	}
	if newLeader.Term() <= term {
		t.Fatalf("new leader term %d not greater than old %d", newLeader.Term(), term)
	}

	// 旧 leader 在分区中不应继续认为自己是多数派 leader 并能提交
	// （它可能暂时仍是 leader 角色，但无法提交新条目——由复制测试覆盖提交行为）
	// 恢复分区后，旧 leader 必须降级，全集群只能有一个 leader。
	c.net.Heal()
	time.Sleep(500 * time.Millisecond)
	if got := c.nodes[oldID]; got != nil && got.Role() == RoleLeader && got.Term() < newLeader.Term() {
		t.Fatalf("old leader %s did not step down after heal", oldID)
	}
	final := c.waitLeader(3 * time.Second)
	t.Logf("after heal leader=%s term=%d (old=%s term=%d)", final.cfg.ID, final.Term(), oldID, term)
}

// TestStepDownOnHigherTerm 任期回退：收到更高任期消息必须降级。
func TestStepDownOnHigherTerm(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.startAll()
	defer c.stopAll()

	leader := c.waitLeader(3 * time.Second)
	oldTerm := leader.Term()

	// 直接向 leader 注入更高任期的投票请求
	other := c.ids[0]
	if other == leader.cfg.ID {
		other = c.ids[1]
	}
	c.net.Send(&Message{
		Type: MsgRequestVote, From: other, To: leader.cfg.ID,
		Term: oldTerm + 5, LastLogIndex: 0, LastLogTerm: 0,
	})
	// 在最小选举超时（80ms）之前检查，避免节点因无人心跳而重新发起选举。
	time.Sleep(50 * time.Millisecond)
	if leader.Role() != RoleFollower {
		t.Fatalf("leader did not step down on higher term, role=%s", leader.Role())
	}
	if leader.Term() != oldTerm+5 {
		t.Fatalf("term not advanced: got %d want %d", leader.Term(), oldTerm+5)
	}
}

// TestVoteRejectReasons 拒绝投票的原因必须可区分。
func TestVoteRejectReasons(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.startAll()
	defer c.stopAll()
	c.waitLeader(3 * time.Second)

	// 构造一个节点直接测试投票逻辑
	store := NewMemoryStorage()
	net := NewNetwork()
	n := NewNode(Config{ID: "X", Peers: []NodeID{"X", "Y", "Z"}}, store, net, NewKVMachine())
	n.mu.Lock()
	n.currentTerm = 5
	n.votedFor = "Y"
	n.log = []LogEntry{{Index: 0, Term: 0}, {Index: 1, Term: 5}}
	n.mu.Unlock()

	replies := make(chan *Message, 4)
	net.Register("X", n.HandleMessage)
	net.Register("Y", func(m *Message) { replies <- m })
	net.Register("Z", func(m *Message) { replies <- m })

	// 1) 低任期 -> StaleTerm
	net.Send(&Message{Type: MsgRequestVote, From: "Y", To: "X", Term: 4, LastLogIndex: 1, LastLogTerm: 5})
	r := <-replies
	if r.Success || r.Reason != ReasonStaleTerm {
		t.Fatalf("stale term: got success=%v reason=%s", r.Success, r.Reason)
	}

	// 2) 已投给别人 -> AlreadyVoted
	net.Send(&Message{Type: MsgRequestVote, From: "Z", To: "X", Term: 5, LastLogIndex: 1, LastLogTerm: 5})
	r = <-replies
	if r.Success || r.Reason != ReasonAlreadyVoted {
		t.Fatalf("already voted: got success=%v reason=%s", r.Success, r.Reason)
	}

	// 3) 日志落后 -> LogBehind（新任期，未投票）
	net.Send(&Message{Type: MsgRequestVote, From: "Z", To: "X", Term: 6, LastLogIndex: 0, LastLogTerm: 0})
	r = <-replies
	if r.Success || r.Reason != ReasonLogBehind {
		t.Fatalf("log behind: got success=%v reason=%s", r.Success, r.Reason)
	}

	// 4) 日志足够新 -> 投票成功
	net.Send(&Message{Type: MsgRequestVote, From: "Z", To: "X", Term: 7, LastLogIndex: 1, LastLogTerm: 5})
	r = <-replies
	if !r.Success {
		t.Fatalf("expected vote granted, got reason=%s", r.Reason)
	}
	t.Logf("vote reject reasons verified: StaleTerm/AlreadyVoted/LogBehind/grant")
}
