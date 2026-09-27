package coordinator

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

// requireSameCommittedPrefix 断言所有给定节点的已提交前缀逐条一致。
func requireSameCommittedPrefix(t *testing.T, c *testCluster, ids ...string) {
	t.Helper()
	var ref []LogEntry
	var refID string
	for _, id := range ids {
		entries := c.nodes[id].CommittedEntries()
		if ref == nil {
			ref, refID = entries, id
			continue
		}
		if len(entries) != len(ref) {
			t.Fatalf("committed prefix length diverged: %s=%d vs %s=%d",
				id, len(entries), refID, len(ref))
		}
		for i := range ref {
			if entries[i].Term != ref[i].Term || entries[i].Index != ref[i].Index ||
				!bytes.Equal(entries[i].Command, ref[i].Command) {
				t.Fatalf("committed entry %d diverged: %s=%+v vs %s=%+v",
					i, id, entries[i], refID, ref[i])
			}
		}
	}
}

// requireSameFSM 断言所有给定节点的 KV 状态机内容完全一致。
func requireSameFSM(t *testing.T, c *testCluster, ids ...string) {
	t.Helper()
	var refID string
	var ref map[string]string
	for _, id := range ids {
		items := c.fsms[id].Items()
		if refID == "" {
			refID, ref = id, items
			continue
		}
		if len(items) != len(ref) {
			t.Fatalf("fsm size diverged: %s=%d vs %s=%d", id, len(items), refID, len(ref))
		}
		for k, v := range ref {
			if items[k] != v {
				t.Fatalf("fsm diverged at key %q: %s=%q vs %s=%q", k, id, items[k], refID, v)
			}
		}
	}
}

// requireSameCommitIndex 断言所有给定节点提交位点一致。
func requireSameCommitIndex(t *testing.T, c *testCluster, ids ...string) {
	t.Helper()
	var ref uint64
	var refID string
	for _, id := range ids {
		ci := c.nodes[id].Status().CommitIndex
		if refID == "" {
			refID, ref = id, ci
			continue
		}
		if ci != ref {
			t.Fatalf("commitIndex diverged: %s=%d vs %s=%d", id, ci, refID, ref)
		}
	}
}

// TestSnapshotCompaction：快照只基于已提交状态生成，生成后日志被压缩、状态机不丢数据。
func TestSnapshotCompaction(t *testing.T) {
	c := newTestClusterSnap(t, 5, "n1", "n2", "n3")
	defer c.cleanup()
	c.startAll()
	leader, _ := c.waitLeader(2 * time.Second)
	if leader == "" {
		t.Fatal("no leader")
	}

	const N = 30
	for i := 0; i < N; i++ {
		if _, err := c.proposeKV(t, fmt.Sprintf("s%02d", i), fmt.Sprintf("v%02d", i), fmt.Sprintf("s-%d", i)); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	c.waitApplied(t, N+1)

	// 等待快照在所有节点上发生（心跳传播 commitIndex 后 follower 也会快照）。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for _, id := range c.peerList {
			if c.nodes[id].Status().SnapIndex == 0 {
				all = false
			}
		}
		if all {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, id := range c.peerList {
		s := c.nodes[id].Status()
		t.Logf("node %s: snapIndex=%d commitIndex=%d lastApplied=%d lastIndex=%d",
			id, s.SnapIndex, s.CommitIndex, s.LastApplied, s.LastIndex)
		if s.SnapIndex == 0 {
			t.Fatalf("node %s did not snapshot", id)
		}
		if s.SnapIndex > s.CommitIndex {
			t.Fatalf("node %s snapshot beyond commit: snap=%d commit=%d", id, s.SnapIndex, s.CommitIndex)
		}
	}

	// 快照后状态机数据完整、提交前缀一致。
	for i := 0; i < N; i++ {
		for _, id := range c.peerList {
			c.waitValue(t, id, fmt.Sprintf("s%02d", i), fmt.Sprintf("v%02d", i))
		}
	}
	requireSameCommittedPrefix(t, c, c.peerList...)
	requireSameFSM(t, c, c.peerList...)

	// 快照之后继续写入仍正常。
	if _, err := c.proposeKV(t, "post", "snap", "post-1"); err != nil {
		t.Fatalf("propose after snapshot: %v", err)
	}
	for _, id := range c.peerList {
		c.waitValue(t, id, "post", "snap")
	}
}

// TestSnapshotCatchUp：落后节点在 leader 已压缩日志后恢复，
// 必须通过 InstallSnapshot 追赶，不丢条目、不重复应用。
func TestSnapshotCatchUp(t *testing.T) {
	c := newTestClusterSnap(t, 5, "n1", "n2", "n3")
	defer c.cleanup()
	c.startAll()
	leader, _ := c.waitLeader(2 * time.Second)
	if leader == "" {
		t.Fatal("no leader")
	}

	var laggard string
	for _, id := range c.peerList {
		if id != leader {
			laggard = id
			break
		}
	}
	c.tr.Isolate(laggard)

	// 提交足够多的条目触发 leader 快照并压缩日志。
	const N = 25
	for i := 0; i < N; i++ {
		if _, err := c.proposeKV(t, fmt.Sprintf("c%02d", i), fmt.Sprintf("v%02d", i), fmt.Sprintf("c-%d", i)); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	// 等 leader 完成快照压缩。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if c.nodes[leader].Status().SnapIndex >= 10 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	ls := c.nodes[leader].Status()
	if ls.SnapIndex == 0 {
		t.Fatalf("leader did not snapshot; status=%+v", ls)
	}
	t.Logf("leader snapshotted: snapIndex=%d commitIndex=%d", ls.SnapIndex, ls.CommitIndex)

	// 恢复落后节点：它应通过快照 + 后缀日志追平。
	c.tr.Heal(laggard)
	c.waitApplied(t, ls.CommitIndex, laggard)

	got := c.nodes[laggard].Status()
	t.Logf("laggard caught up: snapIndex=%d commitIndex=%d lastApplied=%d",
		got.SnapIndex, got.CommitIndex, got.LastApplied)
	if got.SnapIndex == 0 {
		t.Fatal("laggard did not install snapshot")
	}
	for i := 0; i < N; i++ {
		c.waitValue(t, laggard, fmt.Sprintf("c%02d", i), fmt.Sprintf("v%02d", i))
	}
	if c.fsms[laggard].Len() != N {
		t.Fatalf("laggard fsm len=%d want %d (lost or duplicated entries)", c.fsms[laggard].Len(), N)
	}
	requireSameCommittedPrefix(t, c, c.peerList...)
	requireSameFSM(t, c, c.peerList...)
}

// TestSnapshotWithConcurrentWrites：快照期间并发写入不破坏一致性。
func TestSnapshotWithConcurrentWrites(t *testing.T) {
	c := newTestClusterSnap(t, 3, "n1", "n2", "n3")
	defer c.cleanup()
	c.startAll()
	leader, _ := c.waitLeader(2 * time.Second)
	if leader == "" {
		t.Fatal("no leader")
	}

	const N = 60
	done := make(chan error, N)
	for i := 0; i < N; i++ {
		go func(i int) {
			_, err := c.nodes[leader].Propose(t.Context(), MarshalKVCommand(KVCommand{
				Op:         KVOpSet,
				Key:        fmt.Sprintf("cw%03d", i),
				Val:        fmt.Sprintf("v%d", i),
				ProposalID: fmt.Sprintf("cw-%d", i),
			}))
			done <- err
		}(i)
	}
	for i := 0; i < N; i++ {
		if err := <-done; err != nil {
			t.Fatalf("concurrent propose: %v", err)
		}
	}

	c.waitApplied(t, N+1)
	// 快照应已多次发生；各节点快照边界可以不同（压缩时机独立），
	// 但提交位点与状态机内容必须一致。
	for _, id := range c.peerList {
		s := c.nodes[id].Status()
		t.Logf("node %s: snapIndex=%d commitIndex=%d", id, s.SnapIndex, s.CommitIndex)
	}
	requireSameCommitIndex(t, c, c.peerList...)
	requireSameFSM(t, c, c.peerList...)
	for i := 0; i < N; i++ {
		for _, id := range c.peerList {
			c.waitValue(t, id, fmt.Sprintf("cw%03d", i), fmt.Sprintf("v%d", i))
		}
	}
}
