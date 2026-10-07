package raft

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// assertKVConsistentAmong 断言指定节点集合的状态机一致（用于排除已移除节点）。
func (c *testCluster) assertKVConsistentAmong(ids []NodeID, keys ...string) {
	c.t.Helper()
	var ref *KVMachine
	for _, id := range ids {
		if m, ok := c.machines[id]; ok && c.nodes[id] != nil {
			ref = m
			break
		}
	}
	for _, id := range ids {
		m, ok := c.machines[id]
		if !ok || c.nodes[id] == nil {
			continue
		}
		for _, k := range keys {
			rv, rok := ref.Get(k)
			v, ok2 := m.Get(k)
			if rv != v || rok != ok2 {
				c.t.Fatalf("machine %s diverged at key %s: got (%q,%v), want (%q,%v)", id, k, v, ok2, rv, rok)
			}
		}
	}
}

// waitAppliedOn 等待指定节点集合全部应用到 idx。
func (c *testCluster) waitAppliedOn(ids []NodeID, idx uint64, timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range ids {
			n, ok2 := c.nodes[id]
			if !ok2 || n.LastApplied() < idx {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("nodes %v not all applied up to %d within %v", ids, idx, timeout)
}

// waitAppliedExcept 等待除 exclude 外的所有运行中节点应用到 idx。
func (c *testCluster) waitAppliedExcept(exclude NodeID, idx uint64, timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for id, n := range c.nodes {
			if id == exclude {
				continue
			}
			if n.LastApplied() < idx {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("not all nodes (except %s) applied up to %d within %v", exclude, idx, timeout)
}

// waitMembership 等待 id 节点的已提交成员配置达到期望。
func (c *testCluster) waitMembership(id NodeID, wantVoters, wantLearners int, timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n, ok := c.nodes[id]; ok {
			v, l := n.Membership()
			if len(v) == wantVoters && len(l) == wantLearners {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	v, l := c.nodes[id].Membership()
	c.t.Fatalf("node %s membership not converged: voters=%v learners=%v, want %d/%d", id, v, l, wantVoters, wantLearners)
}

// membershipOf 收集所有运行中节点的成员视图（用于日志与断言）。
func (c *testCluster) membershipOf(id NodeID) (voters, learners []NodeID) {
	if n, ok := c.nodes[id]; ok {
		return n.Membership()
	}
	return nil, nil
}

// TestAddNodeCatchUpAndVote 扩容：新节点先以 learner 追平（含快照），
// 提升为投票成员后参与选主与日志确认。
func TestAddNodeCatchUpAndVote(t *testing.T) {
	c := newCluster(t, 3, 5) // 小快照阈值，迫使追赶走快照路径
	c.startAll()
	defer c.stopAll()

	leader := c.waitLeader(3 * time.Second)
	oldLeader := leader.cfg.ID

	// 写入足够数据触发快照
	for i := 0; i < 12; i++ {
		c.propose(t, leader, "cli", uint64(i+1), fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	c.waitAllApplied(12, 3*time.Second)

	// 新节点以 learner 身份启动并加入
	c.addLearnerNode("N4")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := leader.AddNode(ctx, "N4"); err != nil {
		t.Fatalf("AddNode N4 failed: %v", err)
	}

	// 全员（含 N4）应看到 4 个投票成员
	for _, id := range c.ids {
		c.waitMembership(id, 4, 0, 3*time.Second)
	}
	voters, _ := c.membershipOf("N4")
	t.Logf("after add: N4 voters=%v", voters)

	// N4 应通过快照+日志追平全部数据
	snapIdx, _, _, _ := c.stores["N4"].LoadSnapshot()
	t.Logf("N4 snapshot boundary=%d", snapIdx)
	if snapIdx == 0 {
		t.Fatalf("N4 expected to catch up via snapshot")
	}
	c.assertKVConsistent("k0", "k5", "k11")

	// 提升后继续提交：N4 作为投票成员参与确认
	leader = c.waitLeader(3 * time.Second)
	base := leader.CommitIndex()
	c.propose(t, leader, "cli", 100, "after-add", "1")
	c.waitCommit("N4", base+1, 3*time.Second)
	c.waitAllApplied(base+1, 3*time.Second)
	c.assertKVConsistent("after-add")

	// 旧 leader 停掉后，包含 N4 的多数派仍能选主提交
	c.stop(oldLeader)
	leader = c.waitLeader(3 * time.Second)
	c.propose(t, leader, "cli", 101, "after-reElect", "2")
	c.waitAllApplied(leader.CommitIndex(), 3*time.Second)
	c.assertKVConsistent("after-reElect")
	t.Logf("add-node flow ok, final leader=%s term=%d", leader.cfg.ID, leader.Term())
}

// TestRemoveNodeStopsParticipation 缩容：节点被移除后不再计入多数派、
// 不能自行选主，剩余节点继续正常选主与提交。
func TestRemoveNodeStopsParticipation(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.startAll()
	defer c.stopAll()

	leader := c.waitLeader(3 * time.Second)
	c.propose(t, leader, "cli", 1, "a", "1")

	// 选一个不当前是 leader 的节点移除，便于观察集群连续性
	victim := NodeID("")
	for _, id := range c.ids {
		if id != leader.cfg.ID {
			victim = id
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := leader.RemoveNode(ctx, victim); err != nil {
		t.Fatalf("RemoveNode %s failed: %v", victim, err)
	}

	// 剩余节点成员视图收敛为 2 个投票成员
	for _, id := range c.ids {
		if id == victim {
			continue
		}
		c.waitMembership(id, 2, 0, 3*time.Second)
	}
	t.Logf("removed %s, survivors see 2 voters", victim)

	// 被移除节点自身也学到新配置，不再参选
	c.waitMembership(victim, 2, 0, 3*time.Second)
	termAfterRemove := c.nodes[victim].Term()
	time.Sleep(500 * time.Millisecond) // 远超选举超时
	if r := c.nodes[victim].Role(); r != RoleFollower {
		t.Fatalf("removed node %s should stay follower, got %s", victim, r)
	}
	if got := c.nodes[victim].Term(); got != termAfterRemove {
		t.Fatalf("removed node %s kept campaigning: term %d -> %d", victim, termAfterRemove, got)
	}

	// 剩余 2 节点（多数派=2）继续提交；被移除节点的日志不再前进
	victimCommit := c.nodes[victim].CommitIndex()
	leader = c.waitLeader(3 * time.Second)
	c.propose(t, leader, "cli", 2, "b", "2")
	c.propose(t, leader, "cli", 3, "c", "3")
	time.Sleep(200 * time.Millisecond)
	if got := c.nodes[victim].CommitIndex(); got != victimCommit {
		t.Fatalf("removed node %s still replicating: commitIndex %d -> %d", victim, victimCommit, got)
	}
	t.Logf("cluster keeps committing without %s; victim commitIndex frozen at %d", victim, victimCommit)
}

// TestRemoveLeaderItself 移除当前 leader：提交后自动退位，
// 剩余多数派快速选出新 leader，可用性不长时间中断。
func TestRemoveLeaderItself(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.startAll()
	defer c.stopAll()

	leader := c.waitLeader(3 * time.Second)
	oldLeader := leader.cfg.ID
	oldTerm := leader.Term()
	c.propose(t, leader, "cli", 1, "x", "1")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := leader.RemoveNode(ctx, oldLeader); err != nil {
		t.Fatalf("RemoveNode(self) failed: %v", err)
	}
	// 老 leader 退位且不再参选
	time.Sleep(300 * time.Millisecond)
	if r := c.nodes[oldLeader].Role(); r != RoleFollower {
		t.Fatalf("removed leader should step down, got %s", r)
	}

	// 剩余 2 节点选出新 leader 并继续提交
	newLeader := c.waitLeaderMinTerm(oldTerm, 3*time.Second)
	if newLeader.cfg.ID == oldLeader {
		t.Fatalf("removed leader re-elected")
	}
	c.propose(t, newLeader, "cli", 2, "y", "2")
	c.waitAppliedExcept(oldLeader, newLeader.CommitIndex(), 3*time.Second)
	// 已移除的旧 leader 不再参与复制，只对存活成员断言一致性
	survivors := make([]NodeID, 0, len(c.ids))
	for _, id := range c.ids {
		if id != oldLeader {
			survivors = append(survivors, id)
		}
	}
	c.assertKVConsistentAmong(survivors, "x", "y")
	t.Logf("leader self-removal ok: old=%s new=%s term=%d", oldLeader, newLeader.cfg.ID, newLeader.Term())
}

// TestConfigChangeDuplicateAndConcurrent 成员变更的幂等与互斥：
// 并发变更在日志层面被串行化（同一时刻只放行一条未提交变更，其余得到
// ConfigBusy 或被合法串行）；同一变更重复提交只生效一次。
func TestConfigChangeDuplicateAndConcurrent(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.startAll()
	defer c.stopAll()

	leader := c.waitLeader(3 * time.Second)
	c.addLearnerNode("N4")
	c.addLearnerNode("N5")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	// 1) 互斥的确定性验证：隔离 leader 使其无法提交，第一条变更悬置期间，
	//    第二条变更必须立即得到 ConfigBusy。
	for _, id := range c.bootstrap {
		if id != leader.cfg.ID {
			c.net.Disconnect(leader.cfg.ID, id)
		}
	}
	blockedCtx, blockedCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer blockedCancel()
	firstDone := make(chan error, 1)
	go func() { firstDone <- leader.proposeConfig(blockedCtx, EntryAddLearner, "N4") }()
	// 等第一条变更条目落盘但未提交
	time.Sleep(150 * time.Millisecond)
	err := leader.RemoveNode(ctx, "N3")
	var re *RejectError
	if err == nil || !errorAs(err, &re) || re.Reason != ReasonConfigBusy {
		t.Fatalf("expected ConfigBusy while another change pending, got %v", err)
	}
	t.Logf("second change rejected with ConfigBusy while first is uncommitted")
	c.net.Heal()
	// 隔离期间多数派可能已另选 leader，第一条变更可能因此失败（StaleTerm）：
	// 两种结果都合法，失败则在新 leader 上幂等重试。
	if err := <-firstDone; err != nil {
		t.Logf("first change aborted as leader stepped down during isolation: %v", err)
		leader = c.waitLeader(3 * time.Second)
		if err := leader.proposeConfig(ctx, EntryAddLearner, "N4"); err != nil {
			t.Fatalf("retry add learner N4: %v", err)
		}
	}
	leader = c.waitLeader(3 * time.Second)
	c.waitMembership(leader.cfg.ID, 3, 1, 3*time.Second)
	// N4 已是 learner，AddNode 幂等完成追平与提升
	if err := leader.AddNode(ctx, "N4"); err != nil {
		t.Fatalf("promote learner N4: %v", err)
	}
	c.waitMembership(leader.cfg.ID, 4, 0, 3*time.Second)

	// 2) 并发不同变更：可能串行成功或一方 ConfigBusy，但最终都生效且只生效一次
	leader = c.waitLeader(3 * time.Second)
	errCh := make(chan error, 2)
	go func() { errCh <- leader.AddNode(ctx, "N5") }()
	go func() { errCh <- leader.RemoveNode(ctx, "N3") }()
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			if !errorAs(err, &re) || (re.Reason != ReasonConfigBusy && re.Reason != ReasonStaleTerm && re.Reason != ReasonNotLeader) {
				t.Fatalf("unexpected concurrent change error: %v", err)
			}
			t.Logf("one concurrent change got %s, will retry", re.Reason)
		}
	}
	// 重试直至全部生效（幂等：已生效的调用直接成功）
	leader = c.waitLeader(3 * time.Second)
	for {
		if err := leader.AddNode(ctx, "N5"); err == nil {
			break
		}
		leader = c.waitLeader(3 * time.Second)
	}
	for {
		if err := leader.RemoveNode(ctx, "N3"); err == nil {
			break
		}
		leader = c.waitLeader(3 * time.Second)
	}
	c.waitMembership(leader.cfg.ID, 4, 0, 3*time.Second)
	voters, _ := leader.Membership()
	t.Logf("concurrent changes converged: voters=%v", voters)

	// 3) 重复提交同一变更：幂等成功，成员配置不变
	if err := leader.AddNode(ctx, "N5"); err != nil {
		t.Fatalf("duplicate AddNode should be idempotent: %v", err)
	}
	if err := leader.RemoveNode(ctx, "N3"); err != nil {
		t.Fatalf("duplicate RemoveNode should be idempotent: %v", err)
	}
	voters, learners := leader.Membership()
	if len(voters) != 4 || len(learners) != 0 {
		t.Fatalf("unexpected membership after duplicate ops: voters=%v learners=%v", voters, learners)
	}
	t.Logf("duplicate add/remove idempotent, final voters=%v", voters)
}

// errorAs 包装 errors.As 便于在表驱动断言中使用。
func errorAs(err error, target any) bool {
	for err != nil {
		if re, ok := err.(*RejectError); ok {
			if p, ok2 := target.(**RejectError); ok2 {
				*p = re
				return true
			}
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// TestMembershipChangeUnderPartition 变更过程中发生网络分区：
// 多数派一侧继续完成扩容与提交，不出现双主；分区恢复后成员视图收敛。
func TestMembershipChangeUnderPartition(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.startAll()
	defer c.stopAll()

	leader := c.waitLeader(3 * time.Second)
	c.propose(t, leader, "cli", 1, "p0", "0")

	// 隔离一个 follower，多数派 {leader, 另一节点} 仍可用
	isolated := NodeID("")
	for _, id := range c.ids {
		if id != leader.cfg.ID {
			isolated = id
			break
		}
	}
	c.net.Isolate(isolated)
	isolatedTerm := c.nodes[isolated].Term()

	// 分区期间完成扩容：N4 加入为第 4 个投票成员（多数派 2/3 -> 3/4）
	c.addLearnerNode("N4")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := leader.AddNode(ctx, "N4"); err != nil {
		t.Fatalf("AddNode under partition failed: %v", err)
	}
	c.waitMembership(leader.cfg.ID, 4, 0, 3*time.Second)

	// 新配置多数派为 3/4：{leader, 另一可达节点, N4} 足够继续提交
	c.propose(t, leader, "cli", 2, "p1", "1")

	// 被隔离节点在分区期间无法抬升集群任期（它仍是成员，但拿不到多数票）
	time.Sleep(300 * time.Millisecond)

	// 恢复分区：被隔离节点追平新配置与数据，集群收敛到 4 个投票成员
	c.net.Heal()
	for _, id := range c.ids {
		c.waitMembership(id, 4, 0, 3*time.Second)
	}
	leader = c.waitLeader(3 * time.Second)
	c.waitAllApplied(leader.CommitIndex(), 3*time.Second)
	c.assertKVConsistent("p0", "p1")

	// 分区恢复后最终只有一个稳定 leader
	leaders := c.countLeaders()
	total := 0
	for term, ids := range leaders {
		total += len(ids)
		if len(ids) > 1 {
			t.Fatalf("term %d has multiple leaders: %v", term, ids)
		}
	}
	if total != 1 {
		t.Fatalf("expected exactly 1 leader after heal, got %d", total)
	}
	t.Logf("partition during add-node: isolated=%s (term %d->%d), final leader=%s term=%d",
		isolated, isolatedTerm, c.nodes[isolated].Term(), leader.cfg.ID, leader.Term())
}

// TestMembershipChangeWithRestart 变更过程中发生节点重启：
// leader 在 learner 已加入、尚未提升时重启，新 leader 能幂等完成剩余流程；
// 扩容完成后全集群重启，成员配置不丢、不回退。
func TestMembershipChangeWithRestart(t *testing.T) {
	c := newCluster(t, 3, 3) // 小阈值触发快照，验证快照也携带成员配置
	c.startAll()
	defer c.stopAll()

	leader := c.waitLeader(3 * time.Second)
	for i := 0; i < 8; i++ {
		c.propose(t, leader, "cli", uint64(i+1), fmt.Sprintf("r%d", i), "v")
	}

	// 第一阶段：N4 作为 learner 加入（不经 AddNode 的提升阶段）
	c.addLearnerNode("N4")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := leader.proposeConfig(ctx, EntryAddLearner, "N4"); err != nil {
		t.Fatalf("add learner N4: %v", err)
	}
	c.waitMembership(leader.cfg.ID, 3, 1, 3*time.Second)

	// 重启 leader：变更进行到一半时发生故障转移
	oldLeaderID := leader.cfg.ID
	c.restart(oldLeaderID)
	newLeader := c.waitLeaderMinTerm(1, 5*time.Second)
	t.Logf("leader restarted: old=%s new=%s term=%d", oldLeaderID, newLeader.cfg.ID, newLeader.Term())

	// 新 leader 幂等完成 AddNode：N4 已是 learner，只需追平并提升
	if err := newLeader.AddNode(ctx, "N4"); err != nil {
		t.Fatalf("AddNode after leader restart: %v", err)
	}
	for _, id := range c.ids {
		c.waitMembership(id, 4, 0, 3*time.Second)
	}

	// 扩容完成后全集群滚动重启：成员配置必须保持 4 个投票成员
	for _, id := range c.ids {
		c.restart(id)
	}
	leader = c.waitLeader(5 * time.Second)
	for _, id := range c.ids {
		c.waitMembership(id, 4, 0, 3*time.Second)
	}
	voters, _ := leader.Membership()
	t.Logf("after full restart voters=%v", voters)

	// 重启后数据不丢、可继续提交
	c.propose(t, leader, "cli", 100, "post-restart", "1")
	c.waitAllApplied(leader.CommitIndex(), 3*time.Second)
	c.assertKVConsistent("r0", "r7", "post-restart")
}

// TestMembership3To5To3 完整旅程：3 节点集群在线扩到 5 节点，再缩回 3 节点。
// 全程业务持续提交不中断；每步校验成员配置、数据一致性与提交进度。
func TestMembership3To5To3(t *testing.T) {
	c := newCluster(t, 3, 4) // 小快照阈值：追赶走快照路径
	c.startAll()
	defer c.stopAll()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := c.waitLeader(3 * time.Second)
	seq := uint64(0)
	proposeN := func(n int, prefix string) {
		for i := 0; i < n; i++ {
			seq++
			c.propose(t, leader, "journey", seq, fmt.Sprintf("%s-%d", prefix, seq), "v")
		}
	}

	// 阶段 0：3 节点，写入初始数据
	proposeN(6, "base")
	c.waitAllApplied(leader.CommitIndex(), 3*time.Second)

	// 阶段 1：逐个加入 N4、N5，扩到 5 个投票成员
	for _, id := range []NodeID{"N4", "N5"} {
		c.addLearnerNode(id)
		if err := leader.AddNode(ctx, id); err != nil {
			t.Fatalf("AddNode %s: %v", id, err)
		}
		c.waitMembership(leader.cfg.ID, len(c.ids), 0, 3*time.Second)
		proposeN(3, "grow-"+string(id[1]))
		t.Logf("grown to %d voters, leader=%s term=%d commitIndex=%d",
			len(c.ids), leader.cfg.ID, leader.Term(), leader.CommitIndex())
	}
	for _, id := range c.ids {
		c.waitMembership(id, 5, 0, 3*time.Second)
	}
	c.waitAllApplied(leader.CommitIndex(), 3*time.Second)

	// 5 节点下继续提交，新成员参与多数派确认
	proposeN(4, "five")
	c.waitAllApplied(leader.CommitIndex(), 3*time.Second)
	c.assertKVConsistent("base-1", "base-6", "five-16")

	// 阶段 2：逐个移除 N5、N4，缩回 3 个投票成员
	for _, id := range []NodeID{"N5", "N4"} {
		// 若 leader 自身在移除列表中则先移除别的；这里 N4/N5 不会是 leader（后加入）
		if err := leader.RemoveNode(ctx, id); err != nil {
			t.Fatalf("RemoveNode %s: %v", id, err)
		}
		proposeN(2, "shrink-"+string(id[1]))
		t.Logf("shrunk by %s, leader=%s term=%d commitIndex=%d", id, leader.cfg.ID, leader.Term(), leader.CommitIndex())
	}
	for _, id := range c.bootstrap {
		c.waitMembership(id, 3, 0, 3*time.Second)
	}

	// 被移除节点不再参选、不再推进
	for _, id := range []NodeID{"N4", "N5"} {
		term := c.nodes[id].Term()
		time.Sleep(300 * time.Millisecond)
		if r := c.nodes[id].Role(); r != RoleFollower {
			t.Fatalf("removed node %s role=%s, want Follower", id, r)
		}
		if got := c.nodes[id].Term(); got != term {
			t.Fatalf("removed node %s still campaigning: term %d -> %d", id, term, got)
		}
	}

	// 阶段 3：3 节点集群继续正常服务
	proposeN(3, "final")
	survivors := c.bootstrap
	c.waitAppliedOn(survivors, leader.CommitIndex(), 3*time.Second)
	c.assertKVConsistentAmong(survivors, "base-1", "five-16", "final-25")

	// 阶段 4：幸存节点重启后成员配置与数据保持
	for _, id := range survivors {
		c.restart(id)
	}
	leader = c.waitLeader(5 * time.Second)
	for _, id := range survivors {
		c.waitMembership(id, 3, 0, 3*time.Second)
	}
	voters, learners := leader.Membership()
	if len(voters) != 3 || len(learners) != 0 {
		t.Fatalf("membership after restart: voters=%v learners=%v", voters, learners)
	}
	proposeN(1, "post-restart")
	c.waitAppliedOn(survivors, leader.CommitIndex(), 3*time.Second)
	c.assertKVConsistentAmong(survivors, "base-1", "five-16", "final-25", "post-restart-26")
	t.Logf("3->5->3 journey ok, final leader=%s term=%d voters=%v", leader.cfg.ID, leader.Term(), voters)
}
