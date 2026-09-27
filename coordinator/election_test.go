package coordinator

import (
	"errors"
	"testing"
	"time"
)

// TestElectSingleLeader：多数节点可达时必须选出唯一 leader，且各节点任期一致。
func TestElectSingleLeader(t *testing.T) {
	c := newTestCluster(t, "n1", "n2", "n3")
	defer c.cleanup()
	c.startAll()

	leader, term := c.waitLeader(2 * time.Second)
	if leader == "" {
		t.Fatal("no leader elected")
	}
	t.Logf("leader=%s term=%d", leader, term)

	// 所有节点应承认同一 leader 与任期。
	for id, n := range c.nodes {
		s := n.Status()
		if s.Term != term {
			t.Fatalf("node %s term=%d, want %d", id, s.Term, term)
		}
		if id != leader && s.Leader != leader {
			t.Fatalf("node %s leader=%q, want %q", id, s.Leader, leader)
		}
	}

	// 采样窗口内不得出现双主。
	l2, term2 := c.requireSingleLeader(t, 300*time.Millisecond)
	if l2 != leader || term2 != term {
		t.Fatalf("leadership changed without fault: %s@%d -> %s@%d", leader, term, l2, term2)
	}
}

// TestReelectAfterLeaderStop：leader 停掉后，多数派仍可重新选举。
func TestReelectAfterLeaderStop(t *testing.T) {
	c := newTestCluster(t, "n1", "n2", "n3")
	defer c.cleanup()
	c.startAll()

	leader, term := c.waitLeader(2 * time.Second)
	if leader == "" {
		t.Fatal("no leader")
	}
	t.Logf("initial leader=%s term=%d, stopping it", leader, term)
	c.stop(leader)

	remaining := []string{}
	for _, id := range c.peerList {
		if id != leader {
			remaining = append(remaining, id)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	var newLeader string
	var newTerm uint64
	for time.Now().Before(deadline) {
		for _, id := range remaining {
			s := c.nodes[id].Status()
			if s.Role == RoleLeader {
				newLeader, newTerm = id, s.Term
			}
		}
		if newLeader != "" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if newLeader == "" {
		t.Fatal("no new leader after stopping old leader")
	}
	if newTerm <= term {
		t.Fatalf("new term=%d not greater than old term=%d", newTerm, term)
	}
	t.Logf("new leader=%s term=%d", newLeader, newTerm)
}

// TestNoElectionWithoutQuorum：多数被分区时不能产生 leader；恢复后必须恢复。
func TestNoElectionWithoutQuorum(t *testing.T) {
	c := newTestCluster(t, "n1", "n2", "n3")
	defer c.cleanup()

	// 先隔离 n3，再启动 n1/n2：2 节点可达（多数 2），可以选出 leader。
	c.tr.Isolate("n3")
	c.start("n1")
	c.start("n2")
	c.start("n3")
	leader, term := c.waitLeader(2 * time.Second)
	if leader == "" || leader == "n3" {
		t.Fatalf("unexpected leader %q among majority side", leader)
	}
	t.Logf("majority side leader=%s term=%d", leader, term)

	// 再隔离当前 leader：多数被打破。Raft 允许旧 leader 在自己的分区内保留
	// 候选身份，但它无法获得多数确认；稍后复制测试会验证其提案会超时而失败。
	c.tr.Isolate(leader)
	time.Sleep(500 * time.Millisecond)

	// 恢复网络后必须重新收敛到唯一 leader（任期不得回退）。
	c.tr.Heal("n3")
	c.tr.Heal(leader)
	l2, t2 := c.waitLeader(3 * time.Second)
	if l2 == "" {
		t.Fatal("no leader after healing partition")
	}
	if t2 < term {
		t.Fatalf("term regressed after heal: %d < %d", t2, term)
	}
	t.Logf("post-heal leader=%s term=%d", l2, t2)
}

// TestProposeRejectsNonLeader：follower 提案必须以 ErrNotLeader 拒绝，原因可区分。
func TestProposeRejectsNonLeader(t *testing.T) {
	c := newTestCluster(t, "n1", "n2", "n3")
	defer c.cleanup()
	c.startAll()
	leader, _ := c.waitLeader(2 * time.Second)
	if leader == "" {
		t.Fatal("no leader")
	}
	var follower string
	for _, id := range c.peerList {
		if id != leader {
			follower = id
			break
		}
	}
	_, err := c.nodes[follower].Propose(t.Context(), []byte("x"))
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower propose err=%v, want ErrNotLeader", err)
	}
	t.Logf("follower %s rejected with: %v", follower, err)
}
