package coordinator

import (
	"fmt"
	"testing"
	"time"
)

// TestRestartPreservesCommittedState：全集群重启后，
// 状态机不得偏离已提交前缀，不得重复应用，任期不得回退。
func TestRestartPreservesCommittedState(t *testing.T) {
	c := newTestCluster(t, "n1", "n2", "n3")
	c.startAll()
	leader, term := c.waitLeader(2 * time.Second)
	if leader == "" {
		t.Fatal("no leader")
	}

	const N = 15
	for i := 0; i < N; i++ {
		if _, err := c.proposeKV(t, fmt.Sprintf("r%02d", i), fmt.Sprintf("v%02d", i), fmt.Sprintf("r-%d", i)); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	c.waitApplied(t, N+1)
	wantCommit := c.nodes[leader].Status().CommitIndex
	t.Logf("pre-restart: leader=%s term=%d commitIndex=%d", leader, term, wantCommit)

	// 全停（模拟集群整体宕机），再全部重启。
	c.stopAll()
	for _, id := range c.peerList {
		c.restartNode(t, id)
	}
	defer c.cleanup()

	// 重启后必须能重新选出 leader，且任期不回退。
	leader2, term2 := c.waitLeader(3 * time.Second)
	if leader2 == "" {
		t.Fatal("no leader after restart")
	}
	if term2 < term {
		t.Fatalf("term regressed across restart: %d < %d", term2, term)
	}
	t.Logf("post-restart: leader=%s term=%d", leader2, term2)

	// 已提交前缀完整恢复：提交位点不回退、FSM 不重不漏。
	for _, id := range c.peerList {
		s := c.nodes[id].Status()
		t.Logf("node %s: term=%d commitIndex=%d lastApplied=%d lastIndex=%d",
			id, s.Term, s.CommitIndex, s.LastApplied, s.LastIndex)
		if s.CommitIndex < wantCommit {
			t.Fatalf("node %s commitIndex regressed: %d < %d", id, s.CommitIndex, wantCommit)
		}
		if c.fsms[id].Len() != N {
			t.Fatalf("node %s fsm len=%d want %d (lost or duplicated apply)", id, c.fsms[id].Len(), N)
		}
		for i := 0; i < N; i++ {
			c.waitValue(t, id, fmt.Sprintf("r%02d", i), fmt.Sprintf("v%02d", i))
		}
	}

	// 重启后继续提交正常。
	if _, err := c.proposeKV(t, "after", "restart", "after-1"); err != nil {
		t.Fatalf("propose after restart: %v", err)
	}
	for _, id := range c.peerList {
		c.waitValue(t, id, "after", "restart")
	}
}

// TestRestartWithSnapshot：带快照的重启——状态机从快照+日志后缀恢复。
func TestRestartWithSnapshot(t *testing.T) {
	c := newTestClusterSnap(t, 4, "n1", "n2", "n3")
	c.startAll()
	leader, _ := c.waitLeader(2 * time.Second)
	if leader == "" {
		t.Fatal("no leader")
	}

	const N = 20
	for i := 0; i < N; i++ {
		if _, err := c.proposeKV(t, fmt.Sprintf("rs%02d", i), fmt.Sprintf("v%02d", i), fmt.Sprintf("rs-%d", i)); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	c.waitApplied(t, N+1)
	preSnap := c.nodes[leader].Status().SnapIndex
	if preSnap == 0 {
		t.Fatal("expected snapshot before restart")
	}
	t.Logf("pre-restart snapIndex=%d", preSnap)

	c.stopAll()
	for _, id := range c.peerList {
		c.restartNode(t, id)
	}
	defer c.cleanup()

	if _, term := c.waitLeader(3 * time.Second); term == 0 {
		t.Fatal("no leader after restart")
	}
	for _, id := range c.peerList {
		s := c.nodes[id].Status()
		t.Logf("node %s: snapIndex=%d commitIndex=%d lastApplied=%d",
			id, s.SnapIndex, s.CommitIndex, s.LastApplied)
		if s.SnapIndex < preSnap {
			t.Fatalf("node %s snapshot boundary regressed: %d < %d", id, s.SnapIndex, preSnap)
		}
		if c.fsms[id].Len() != N {
			t.Fatalf("node %s fsm len=%d want %d", id, c.fsms[id].Len(), N)
		}
	}
	requireSameFSM(t, c, c.peerList...)
}

// TestRestartedNodeRejoins：单个 follower 重启后重新加入并补齐日志。
func TestRestartedNodeRejoins(t *testing.T) {
	c := newTestCluster(t, "n1", "n2", "n3")
	defer c.cleanup()
	c.startAll()
	leader, _ := c.waitLeader(2 * time.Second)
	if leader == "" {
		t.Fatal("no leader")
	}
	var victim string
	for _, id := range c.peerList {
		if id != leader {
			victim = id
			break
		}
	}

	// 基线提交。
	if _, err := c.proposeKV(t, "base", "0", "base-0"); err != nil {
		t.Fatal(err)
	}
	c.waitApplied(t, 2)

	// 停掉 victim，期间继续提交，然后重启 victim。
	c.stop(victim)
	const N = 8
	for i := 0; i < N; i++ {
		if _, err := c.proposeKV(t, fmt.Sprintf("j%d", i), fmt.Sprintf("v%d", i), fmt.Sprintf("j-%d", i)); err != nil {
			t.Fatalf("propose while %s down: %v", victim, err)
		}
	}
	head := c.nodes[leader].Status().CommitIndex
	t.Logf("victim %s stopped; cluster advanced to commitIndex=%d", victim, head)

	c.restartNode(t, victim)
	c.waitApplied(t, head, victim)
	for i := 0; i < N; i++ {
		c.waitValue(t, victim, fmt.Sprintf("j%d", i), fmt.Sprintf("v%d", i))
	}
	c.waitValue(t, victim, "base", "0")
	if c.fsms[victim].Len() != N+1 {
		t.Fatalf("victim fsm len=%d want %d", c.fsms[victim].Len(), N+1)
	}
	t.Logf("victim %s rejoined and caught up to %d", victim, head)
}

// TestRestartedLeaderDoesNotSplitBrain：leader 重启后不得出现双主，
// 且其重启前已提交的条目保持提交。
func TestRestartedLeaderDoesNotSplitBrain(t *testing.T) {
	c := newTestCluster(t, "n1", "n2", "n3")
	defer c.cleanup()
	c.startAll()
	leader, term := c.waitLeader(2 * time.Second)
	if leader == "" {
		t.Fatal("no leader")
	}
	if _, err := c.proposeKV(t, "keep", "me", "keep-1"); err != nil {
		t.Fatal(err)
	}
	c.waitApplied(t, 2)

	// 重启 leader。
	c.restartNode(t, leader)
	t.Logf("restarted former leader %s (term %d)", leader, term)

	// 集群应（可能由其他节点）维持唯一 leader。
	l2, t2 := c.requireSingleLeader(t, 500*time.Millisecond)
	if t2 < term {
		t.Fatalf("term regressed: %d < %d", t2, term)
	}
	t.Logf("stable leader=%s term=%d", l2, t2)

	// 重启节点最终与其他节点一致。
	for _, id := range c.peerList {
		c.waitValue(t, id, "keep", "me")
	}
	requireSameFSM(t, c, c.peerList...)
}
