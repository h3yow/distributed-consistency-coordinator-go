package raft

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// snapshotBoundary 读取节点当前快照边界。
func (c *testCluster) snapshotBoundary(id NodeID) (uint64, uint64) {
	idx, term, _, _ := c.stores[id].LoadSnapshot()
	return idx, term
}

// TestSnapshotCatchUp 落后 follower 通过快照追平：不丢条目、不重复应用。
func TestSnapshotCatchUp(t *testing.T) {
	c := newCluster(t, 3, 3) // 快照阈值 3 条
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(3 * time.Second)

	// 隔离一个 follower
	var lagging NodeID
	for _, id := range c.ids {
		if id != leader.cfg.ID {
			lagging = id
			break
		}
	}
	c.net.Isolate(lagging)

	// 提交 8 条，leader 与另一 follower 会生成快照（日志打印快照边界）
	for i := 0; i < 8; i++ {
		c.propose(t, leader, "c1", uint64(i+1), fmt.Sprintf("k%d", i), "v")
	}
	snapIdx, snapTerm := c.snapshotBoundary(leader.cfg.ID)
	if snapIdx == 0 {
		t.Fatalf("leader did not take snapshot")
	}
	if snapIdx > leader.CommitIndex() {
		t.Fatalf("snapshot boundary %d beyond commitIndex %d: snapshot must cover committed only", snapIdx, leader.CommitIndex())
	}
	t.Logf("leader snapshot boundary=%d/%d commitIndex=%d", snapIdx, snapTerm, leader.CommitIndex())

	// 恢复隔离节点：leader 的 nextIndex 已小于其快照边界，应发送 InstallSnapshot
	c.net.Heal()
	c.waitCommit(lagging, 8, 5*time.Second)
	c.waitAllApplied(8, 3*time.Second)

	// 不丢条目、不重复应用
	if got := c.machines[lagging].Applied(); got != 8 {
		t.Fatalf("lagging follower applied=%d, want exactly 8", got)
	}
	keys := make([]string, 8)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
	}
	c.assertKVConsistent(keys...)
	c.assertLogPrefixConsistent()
	lagSnap, _ := c.snapshotBoundary(lagging)
	t.Logf("lagging follower caught up via snapshot boundary=%d applied=%d", lagSnap, c.machines[lagging].Applied())
}

// TestSnapshotConcurrentWrites 快照期间并发写入不得破坏一致性。
func TestSnapshotConcurrentWrites(t *testing.T) {
	c := newCluster(t, 3, 2) // 极小阈值，频繁触发快照
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(3 * time.Second)

	const writers = 4
	const perWriter = 6
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			clientID := fmt.Sprintf("w%d", w)
			for i := 0; i < perWriter; i++ {
				c.propose(t, leader, clientID, uint64(i+1), fmt.Sprintf("w%d-k%d", w, i), "v")
			}
		}(w)
	}
	wg.Wait()

	total := uint64(writers * perWriter)
	c.waitAllApplied(total, 5*time.Second)
	// 所有节点应用条目数一致且等于提交总数（无重复、无丢失）
	for _, id := range c.ids {
		if got := c.machines[id].Applied(); got != total {
			t.Fatalf("node %s applied=%d, want %d", id, got, total)
		}
	}
	keys := make([]string, 0, total)
	for w := 0; w < writers; w++ {
		for i := 0; i < perWriter; i++ {
			keys = append(keys, fmt.Sprintf("w%d-k%d", w, i))
		}
	}
	c.assertKVConsistent(keys...)
	c.assertLogPrefixConsistent()
	snapIdx, _ := c.snapshotBoundary(leader.cfg.ID)
	t.Logf("concurrent writes consistent: total=%d leaderSnapshot=%d", total, snapIdx)
}

// TestSnapshotSurvivesRestart 快照后重启：状态机从快照+日志后缀精确恢复。
func TestSnapshotSurvivesRestart(t *testing.T) {
	c := newCluster(t, 3, 3)
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(3 * time.Second)

	for i := 0; i < 7; i++ {
		c.propose(t, leader, "c1", uint64(i+1), fmt.Sprintf("k%d", i), "v")
	}
	c.waitAllApplied(7, 3*time.Second)
	snapIdx, _ := c.snapshotBoundary(leader.cfg.ID)
	if snapIdx == 0 {
		t.Fatalf("expected snapshot before restart")
	}

	// 重启 leader：从快照恢复 + 回放快照后日志
	c.restart(leader.cfg.ID)
	c.waitCommit(leader.cfg.ID, 7, 3*time.Second)
	time.Sleep(300 * time.Millisecond)
	if got := c.machines[leader.cfg.ID].Applied(); got != 7 {
		t.Fatalf("restarted node applied=%d, want 7", got)
	}
	keys := make([]string, 7)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
	}
	c.assertKVConsistent(keys...)

	// 重启后集群继续可用
	newLeader := c.waitLeader(3 * time.Second)
	c.propose(t, newLeader, "c1", 8, "k7", "v")
	c.waitAllApplied(8, 3*time.Second)
	c.assertLogPrefixConsistent()
	t.Logf("snapshot restart verified: snapshot=%d commitIndex=%d", snapIdx, newLeader.CommitIndex())
}
