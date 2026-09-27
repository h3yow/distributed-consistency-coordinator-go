package raft

import (
	"context"
	"sync"
	"testing"
	"time"
)

// assertLogPrefixConsistent 断言所有运行中节点在公共提交前缀内日志完全一致。
// 兼容快照：比较区间取 [max(各节点快照边界)+1, min(各节点commitIndex)]。
func (c *testCluster) assertLogPrefixConsistent() {
	c.t.Helper()
	minCommit := ^uint64(0)
	var start uint64 = 1
	for _, n := range c.nodes {
		n.mu.Lock()
		base := n.log[0].Index
		n.mu.Unlock()
		if base+1 > start {
			start = base + 1
		}
		if ci := n.CommitIndex(); ci < minCommit {
			minCommit = ci
		}
	}
	var ref []LogEntry
	first := true
	for _, n := range c.nodes {
		n.mu.Lock()
		base := n.log[0].Index
		end := minCommit
		if last := n.log[len(n.log)-1].Index; end > last {
			end = last
		}
		var prefix []LogEntry
		for i := start; i <= end; i++ {
			if i > base {
				prefix = append(prefix, n.log[i-base])
			}
		}
		n.mu.Unlock()
		if first {
			ref = prefix
			first = false
			continue
		}
		if len(prefix) != len(ref) {
			c.t.Fatalf("log prefix length mismatch: %d vs %d", len(prefix), len(ref))
		}
		for i := range ref {
			if prefix[i].Term != ref[i].Term || string(prefix[i].Op) != string(ref[i].Op) {
				c.t.Fatalf("log diverged at index %d: (%d,%s) vs (%d,%s)",
					start+uint64(i), prefix[i].Term, prefix[i].Op, ref[i].Term, ref[i].Op)
			}
		}
	}
	c.t.Logf("committed prefix consistent in [%d,%d] across %d nodes", start, minCommit, len(c.nodes))
}

// TestReplicateAndCommit 提案被多数确认后提交并应用到全部节点。
func TestReplicateAndCommit(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(3 * time.Second)

	for i := 0; i < 5; i++ {
		c.propose(t, leader, "c1", uint64(i+1), "k"+string(rune('a'+i)), "v")
	}
	c.waitAllApplied(5, 3*time.Second)
	c.assertKVConsistent("ka", "kb", "kc", "kd", "ke")
	c.assertLogPrefixConsistent()
	t.Logf("leader=%s commitIndex=%d", leader.cfg.ID, leader.CommitIndex())
}

// TestDuplicateProposalIdempotent 重复提案与重试必须幂等：
// 状态机只应用一次，客户端拿到相同结果。
func TestDuplicateProposalIdempotent(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(3 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 顺序重试：相同 ClientID+Seq 直接返回缓存结果
	if _, err := leader.Propose(ctx, Command{ClientID: "c1", Seq: 1, Op: EncodeKVOp(KVOp{Op: "set", Key: "x", Value: "1"})}); err != nil {
		t.Fatal(err)
	}
	c.waitAllApplied(1, 3*time.Second)
	appliedBefore := c.machines[leader.cfg.ID].Applied()
	if _, err := leader.Propose(ctx, Command{ClientID: "c1", Seq: 1, Op: EncodeKVOp(KVOp{Op: "set", Key: "x", Value: "1"})}); err != nil {
		t.Fatal(err)
	}
	if got := c.machines[leader.cfg.ID].Applied(); got != appliedBefore {
		t.Fatalf("duplicate proposal re-applied: applied %d -> %d", appliedBefore, got)
	}

	// 并发重试：两个相同提案并发提交，状态机仍只应用一次
	var wg sync.WaitGroup
	errs := make([]error, 2)
	results := make([]any, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel2()
			results[i], errs[i] = leader.Propose(ctx2, Command{ClientID: "c2", Seq: 1, Op: EncodeKVOp(KVOp{Op: "set", Key: "y", Value: "2"})})
		}(i)
	}
	wg.Wait()
	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("concurrent propose %d failed: %v", i, errs[i])
		}
	}
	if results[0] != results[1] {
		t.Fatalf("duplicate proposals returned different results: %v vs %v", results[0], results[1])
	}
	c.waitAllApplied(leader.CommitIndex(), 3*time.Second)
	if got := c.machines[leader.cfg.ID].Applied(); got != appliedBefore+1 {
		t.Fatalf("concurrent duplicate applied %d times, want 1", got-appliedBefore)
	}
	t.Logf("idempotency verified: applied=%d (duplicates suppressed)", c.machines[leader.cfg.ID].Applied())
}

// TestFollowerCatchUp 落后 follower 恢复连通后必须追平日志且不重复应用。
func TestFollowerCatchUp(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(3 * time.Second)

	// 找出一个 follower 并隔离
	var lagging NodeID
	for _, id := range c.ids {
		if id != leader.cfg.ID {
			lagging = id
			break
		}
	}
	c.net.Isolate(lagging)

	for i := 0; i < 5; i++ {
		c.propose(t, leader, "c1", uint64(i+1), "k"+string(rune('a'+i)), "v")
	}
	if n := c.nodes[lagging]; n.LastApplied() != 0 {
		t.Fatalf("isolated follower applied entries: lastApplied=%d", n.LastApplied())
	}

	// 恢复后应追平，且状态机结果一致
	c.net.Heal()
	c.waitCommit(lagging, 5, 3*time.Second)
	c.waitAllApplied(5, 3*time.Second)
	c.assertKVConsistent("ka", "kb", "kc", "kd", "ke")
	c.assertLogPrefixConsistent()
	if got := c.machines[lagging].Applied(); got != 5 {
		t.Fatalf("lagging follower applied %d entries, want exactly 5 (no loss, no dup)", got)
	}
	t.Logf("follower %s caught up: applied=%d", lagging, c.machines[lagging].Applied())
}

// TestConflictOverwrite 分区期间旧 leader 的未提交条目必须被新 leader 可解释地覆盖，
// 已提交前缀不受影响。
func TestConflictOverwrite(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.startAll()
	defer c.stopAll()
	oldLeader := c.waitLeader(3 * time.Second)

	// 先提交一条公共前缀
	c.propose(t, oldLeader, "c1", 1, "base", "v0")
	c.waitAllApplied(1, 3*time.Second)

	// 隔离旧 leader（少数派），它在分区里接受的提案无法提交
	c.net.Isolate(oldLeader.cfg.ID)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		// 该提案无法获得多数确认，最终超时——不会提交
		_, _ = oldLeader.Propose(ctx, Command{ClientID: "c1", Seq: 2, Op: EncodeKVOp(KVOp{Op: "set", Key: "conflict", Value: "old"})})
	}()

	// 多数派选出新 leader 并提交冲突条目
	newLeader := c.waitLeaderMinTerm(oldLeader.Term(), 3*time.Second)
	c.propose(t, newLeader, "c2", 1, "conflict", "new")
	c.propose(t, newLeader, "c2", 2, "after", "v2")

	// 等待旧 leader 在分区中追加了未提交条目
	time.Sleep(300 * time.Millisecond)

	// 恢复分区：旧 leader 降级，冲突条目被覆盖
	c.net.Heal()
	finalLeader := c.waitLeaderMinTerm(oldLeader.Term(), 3*time.Second)
	c.waitAllApplied(3, 3*time.Second)

	// 所有节点状态机一致，且是 "new" 而非 "old"
	c.assertKVConsistent("base", "conflict", "after")
	for _, id := range c.ids {
		if v, _ := c.machines[id].Get("conflict"); v != "new" {
			t.Fatalf("node %s has overwritten value %q, want %q", id, v, "new")
		}
	}
	c.assertLogPrefixConsistent()
	t.Logf("conflict resolved: finalLeader=%s term=%d commitIndex=%d",
		finalLeader.cfg.ID, finalLeader.Term(), finalLeader.CommitIndex())
}

// TestRejectReasonsDistinguishable 失败原因可区分：
// 非 leader 提案 -> NotLeader（带 leader 提示）；分区 leader -> 超时。
func TestRejectReasonsDistinguishable(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(3 * time.Second)

	// 向 follower 提案 -> NotLeader 且带 leader 提示
	var follower *Node
	for _, id := range c.ids {
		if id != leader.cfg.ID {
			follower = c.nodes[id]
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := follower.Propose(ctx, Command{ClientID: "c1", Seq: 1, Op: EncodeKVOp(KVOp{Op: "set", Key: "k", Value: "v"})})
	re, ok := err.(*RejectError)
	if !ok || re.Reason != ReasonNotLeader {
		t.Fatalf("follower propose: got %v, want NotLeader", err)
	}
	if re.Leader != leader.cfg.ID {
		t.Fatalf("NotLeader hint=%s, want %s", re.Leader, leader.cfg.ID)
	}

	// 隔离 leader 后提案 -> 无法提交，超时
	c.net.Isolate(leader.cfg.ID)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel2()
	_, err = leader.Propose(ctx2, Command{ClientID: "c1", Seq: 1, Op: EncodeKVOp(KVOp{Op: "set", Key: "k", Value: "v"})})
	re, ok = err.(*RejectError)
	if !ok || re.Reason != ReasonTimeout {
		t.Fatalf("partitioned leader propose: got %v, want Timeout", err)
	}
	t.Logf("reject reasons verified: NotLeader(leader=%s) / Timeout", re.Leader)
}
