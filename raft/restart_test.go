package raft

import (
	"context"
	"testing"
	"time"
)

// TestRestartPreservesCommittedPrefix 节点重启后状态机不得偏离已提交前缀。
func TestRestartPreservesCommittedPrefix(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(3 * time.Second)

	for i := 0; i < 5; i++ {
		c.propose(t, leader, "c1", uint64(i+1), "k"+string(rune('a'+i)), "v")
	}
	c.waitAllApplied(5, 3*time.Second)

	// 重启一个 follower：状态机必须精确恢复到已提交前缀
	var follower NodeID
	for _, id := range c.ids {
		if id != leader.cfg.ID {
			follower = id
			break
		}
	}
	c.restart(follower)
	c.waitCommit(follower, 5, 3*time.Second)
	c.waitAllApplied(5, 3*time.Second)
	if got := c.machines[follower].Applied(); got != 5 {
		t.Fatalf("restarted follower applied=%d, want 5 (no re-apply, no loss)", got)
	}
	c.assertKVConsistent("ka", "kb", "kc", "kd", "ke")

	// 重启 leader：集群重新选举后已提交数据不丢
	c.restart(leader.cfg.ID)
	newLeader := c.waitLeaderMinTerm(leader.Term(), 3*time.Second)
	c.propose(t, newLeader, "c1", 6, "kf", "v")
	c.waitAllApplied(6, 3*time.Second)
	c.assertKVConsistent("ka", "kb", "kc", "kd", "ke", "kf")
	c.assertLogPrefixConsistent()
	t.Logf("restart verified: newLeader=%s commitIndex=%d", newLeader.cfg.ID, newLeader.CommitIndex())
}

// TestRestartKeepsIdempotency 重启后重复提案仍幂等（去重状态持久化）。
func TestRestartKeepsIdempotency(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(3 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := leader.Propose(ctx, Command{ClientID: "c1", Seq: 1, Op: EncodeKVOp(KVOp{Op: "set", Key: "x", Value: "1"})}); err != nil {
		t.Fatal(err)
	}
	c.waitAllApplied(1, 3*time.Second)
	appliedBefore := c.machines[leader.cfg.ID].Applied()

	// 重启 leader，等待重新当选（可能换主，找到新 leader 再重试）
	c.restart(leader.cfg.ID)
	time.Sleep(300 * time.Millisecond)
	newLeader := c.waitLeader(3 * time.Second)

	// 相同 ClientID+Seq 重试：无论谁当选，都不得重复应用
	if _, err := newLeader.Propose(ctx, Command{ClientID: "c1", Seq: 1, Op: EncodeKVOp(KVOp{Op: "set", Key: "x", Value: "1"})}); err != nil {
		t.Fatal(err)
	}
	c.waitAllApplied(newLeader.CommitIndex(), 3*time.Second)
	for _, id := range c.ids {
		if got := c.machines[id].Applied(); got != appliedBefore {
			t.Fatalf("node %s re-applied after restart: applied=%d, want %d", id, got, appliedBefore)
		}
	}
	t.Logf("idempotency across restart verified: applied=%d", appliedBefore)
}

// TestRestartedNodeRejoinsElection 节点重启后重新参与选举，集群保持单主。
func TestRestartedNodeRejoinsElection(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(3 * time.Second)

	// 停掉 leader，等剩余两节点选出新 leader
	c.stop(leader.cfg.ID)
	newLeader := c.waitLeaderMinTerm(leader.Term(), 3*time.Second)

	// 重启旧 leader：它应以 follower 身份加入，不得出现双主
	c.start(leader.cfg.ID)
	time.Sleep(500 * time.Millisecond)
	leaders := c.countLeaders()
	total := 0
	for term, ids := range leaders {
		total += len(ids)
		if len(ids) > 1 {
			t.Fatalf("term %d has multiple leaders: %v", term, ids)
		}
	}
	if total != 1 {
		t.Fatalf("expected exactly 1 leader after rejoin, got %d", total)
	}
	if got := c.nodes[leader.cfg.ID]; got.Role() == RoleLeader && got.Term() < newLeader.Term() {
		t.Fatalf("restarted node claims stale leadership")
	}
	t.Logf("rejoin verified: leader=%s term=%d", newLeader.cfg.ID, newLeader.Term())
}
