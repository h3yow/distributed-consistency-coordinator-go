package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestReplicationCommitAndApply：基本提交流程；所有节点提交前缀与 FSM 一致。
func TestReplicationCommitAndApply(t *testing.T) {
	c := newTestCluster(t, "n1", "n2", "n3")
	defer c.cleanup()
	c.startAll()
	leader, term := c.waitLeader(2 * time.Second)
	if leader == "" {
		t.Fatal("no leader")
	}
	t.Logf("leader=%s term=%d", leader, term)

	const N = 20
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("k%02d", i)
		val := fmt.Sprintf("v%02d", i)
		res, err := c.proposeKV(t, key, val, fmt.Sprintf("pid-%d", i))
		if err != nil {
			t.Fatalf("propose %d failed: %v", i, err)
		}
		kv, ok := res.(KVResult)
		if !ok || kv.Duplicate {
			t.Fatalf("unexpected result: %#v", res)
		}
	}

	// 提交位点应一致（全部节点）。
	c.waitApplied(t, N+1) // 含 leader 上任时的 1 条 no-op：set 条目下标 2..21 中 N 个？
	// no-op 下标为 1，第一条 set 下标为 2 => LastApplied 应为 N+1
	var leaderCommit uint64
	for _, id := range c.peerList {
		s := c.nodes[id].Status()
		if id == leader {
			leaderCommit = s.CommitIndex
		}
		if s.CommitIndex < uint64(N)+1 {
			t.Fatalf("node %s commitIndex=%d, want >= %d", id, s.CommitIndex, N+1)
		}
	}
	for id := range c.nodes {
		if id == leader {
			continue
		}
		if c.nodes[id].Status().CommitIndex != leaderCommit {
			t.Fatalf("commit index divergence on %s: %d vs leader %d",
				id, c.nodes[id].Status().CommitIndex, leaderCommit)
		}
	}

	// FSM 结果一致。
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("k%02d", i)
		want := fmt.Sprintf("v%02d", i)
		for _, id := range c.peerList {
			c.waitValue(t, id, key, want)
		}
	}
	t.Logf("all nodes converged at commitIndex=%d", leaderCommit)
}

// TestDuplicateProposalIdempotent：相同 ProposalID 的重试只生效一次。
func TestDuplicateProposalIdempotent(t *testing.T) {
	c := newTestCluster(t, "n1", "n2", "n3")
	defer c.cleanup()
	c.startAll()
	leader, _ := c.waitLeader(2 * time.Second)
	if leader == "" {
		t.Fatal("no leader")
	}

	cmd := MarshalKVCommand(KVCommand{Op: KVOpSet, Key: "dup", Val: "first", ProposalID: "same-id"})
	ctx := t.Context()
	r1, err := c.nodes[leader].Propose(ctx, cmd)
	if err != nil {
		t.Fatalf("first propose: %v", err)
	}
	firstIdx := r1.(KVResult).Applied

	// 不同值但相同 ProposalID：必须被识别为重复，不覆盖状态。
	cmd2 := MarshalKVCommand(KVCommand{Op: KVOpSet, Key: "dup", Val: "second", ProposalID: "same-id"})
	r2, err := c.nodes[leader].Propose(ctx, cmd2)
	if err != nil {
		t.Fatalf("duplicate propose: %v", err)
	}
	kv2 := r2.(KVResult)
	if !kv2.Duplicate || kv2.Applied != firstIdx {
		t.Fatalf("expected duplicate of index %d, got %#v", firstIdx, kv2)
	}
	for _, id := range c.peerList {
		c.waitValue(t, id, "dup", "first")
	}
	t.Logf("duplicate proposal de-duplicated at index=%d", firstIdx)
}

// TestConcurrentProposals：并发提交下提交位点连续、无缺口、无重复应用。
func TestConcurrentProposals(t *testing.T) {
	c := newTestCluster(t, "n1", "n2", "n3")
	defer c.cleanup()
	c.startAll()
	leader, _ := c.waitLeader(2 * time.Second)
	if leader == "" {
		t.Fatal("no leader")
	}

	const workers = 8
	const perWorker = 15
	var wg sync.WaitGroup
	errs := make(chan error, workers*perWorker)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				cmd := MarshalKVCommand(KVCommand{
					Op:         KVOpSet,
					Key:        fmt.Sprintf("w%d-k%d", w, i),
					Val:        fmt.Sprintf("w%d-v%d", w, i),
					ProposalID: fmt.Sprintf("w%d-i%d", w, i),
				})
				if _, err := c.nodes[leader].Propose(t.Context(), cmd); err != nil {
					errs <- fmt.Errorf("worker %d i %d: %w", w, i, err)
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}

	total := uint64(workers*perWorker) + 1 // +no-op
	c.waitApplied(t, total)
	for _, id := range c.peerList {
		if got := c.nodes[id].Status().CommitIndex; got != total {
			t.Fatalf("node %s commitIndex=%d want %d", id, got, total)
		}
		if c.fsms[id].Len() != workers*perWorker {
			t.Fatalf("node %s fsm len=%d want %d", id, c.fsms[id].Len(), workers*perWorker)
		}
	}
	t.Logf("concurrent proposals converged, commitIndex=%d", total)
}

// TestFollowerCatchUp：follower 离线期间产生新条目，恢复后必须补齐且不重复/丢失。
func TestFollowerCatchUp(t *testing.T) {
	c := newTestCluster(t, "n1", "n2", "n3")
	defer c.cleanup()
	c.startAll()
	leader, _ := c.waitLeader(2 * time.Second)
	if leader == "" {
		t.Fatal("no leader")
	}

	// 选一个 follower 隔离。
	var laggard string
	for _, id := range c.peerList {
		if id != leader {
			laggard = id
			break
		}
	}
	c.tr.Isolate(laggard)

	const N = 10
	for i := 0; i < N; i++ {
		if _, err := c.proposeKV(t, fmt.Sprintf("g%d", i), fmt.Sprintf("x%d", i), fmt.Sprintf("g-%d", i)); err != nil {
			t.Fatalf("propose during partition: %v", err)
		}
	}
	// 剩余 2 节点（含 leader）仍构成多数，提交应成功。
	others := []string{}
	for _, id := range c.peerList {
		if id != laggard {
			others = append(others, id)
		}
	}
	base := c.nodes[leader].Status().CommitIndex
	c.waitApplied(t, base, others...)

	// 落后节点状态机不应有隔离期间的数据。
	for i := 0; i < N; i++ {
		if _, ok := c.fsms[laggard].Get(fmt.Sprintf("g%d", i)); ok {
			t.Fatalf("laggard unexpectedly applied partitioned entry %d", i)
		}
	}

	// 恢复：必须完整补齐。
	c.tr.Heal(laggard)
	c.waitApplied(t, base)
	for i := 0; i < N; i++ {
		c.waitValue(t, laggard, fmt.Sprintf("g%d", i), fmt.Sprintf("x%d", i))
	}
	if c.fsms[laggard].Len() != N {
		t.Fatalf("laggard fsm len=%d want %d", c.fsms[laggard].Len(), N)
	}
	t.Logf("laggard %s caught up to commitIndex=%d", laggard, base)
}

// TestPartitionedLeaderCannotCommit：被隔离的旧 leader 提案必须失败，
// 恢复后其任期回退、未提交提案不得出现在已提交前缀中。
func TestPartitionedLeaderCannotCommit(t *testing.T) {
	c := newTestCluster(t, "n1", "n2", "n3")
	defer c.cleanup()
	c.startAll()
	leader, oldTerm := c.waitLeader(2 * time.Second)
	if leader == "" {
		t.Fatal("no leader")
	}

	// 先提交一条基线。
	if _, err := c.proposeKV(t, "base", "1", "base-1"); err != nil {
		t.Fatal(err)
	}

	c.tr.Isolate(leader)
	// 等待多数派选出新 leader。
	var newLeader string
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range c.peerList {
			if id == leader {
				continue
			}
			if s := c.nodes[id].Status(); s.Role == RoleLeader && s.Term > oldTerm {
				newLeader = id
			}
		}
		if newLeader != "" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if newLeader == "" {
		t.Fatal("majority did not elect a new leader")
	}
	t.Logf("old leader=%s (term %d) isolated; new leader=%s", leader, oldTerm, newLeader)

	// 旧 leader 上的提案无法获多数确认，应超时失败（ErrTimeout），且其状态不变。
	oldNode := c.nodes[leader]
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := oldNode.Propose(ctx, MarshalKVCommand(KVCommand{
		Op: KVOpSet, Key: "stale", Val: "x", ProposalID: "stale-1",
	}))
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("isolated leader propose err=%v, want ErrTimeout", err)
	}
	if v, ok := c.fsms[leader].Get("stale"); ok {
		t.Fatalf("uncommitted entry applied on old leader: %q", v)
	}

	// 新 leader 正常提交。
	res, err := c.nodes[newLeader].Propose(t.Context(), MarshalKVCommand(KVCommand{
		Op: KVOpSet, Key: "new", Val: "2", ProposalID: "new-1",
	}))
	if err != nil {
		t.Fatalf("new leader propose: %v", err)
	}
	t.Logf("new leader committed at index %d", res.(KVResult).Applied)

	// 恢复旧 leader：必须降级、日志冲突被覆盖，最终与多数派一致。
	c.tr.Heal(leader)
	c.waitApplied(t, c.nodes[newLeader].Status().CommitIndex)
	c.waitValue(t, leader, "new", "2")
	if _, ok := c.fsms[leader].Get("stale"); ok {
		t.Fatal("stale uncommitted entry survived on rejoined leader")
	}
	if s := c.nodes[leader].Status(); s.Role == RoleLeader && s.Term <= oldTerm {
		t.Fatalf("old leader did not step down: %+v", s)
	}
	t.Logf("old leader rejoined; status=%+v", c.nodes[leader].Status())
}
