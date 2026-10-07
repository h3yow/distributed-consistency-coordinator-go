package raft

import (
	"context"
	"fmt"

	"testing"
	"time"
)

// addNode 向 leader 发起扩容并校验成功。
func addNode(t *testing.T, leader *Node, id NodeID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := leader.AddNode(ctx, id); err != nil {
		t.Fatalf("AddNode(%s) failed: %v", id, err)
	}
}

// removeNode 向 leader 发起缩容并校验成功。
func removeNode(t *testing.T, leader *Node, id NodeID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := leader.RemoveNode(ctx, id); err != nil {
		t.Fatalf("RemoveNode(%s) failed: %v", id, err)
	}
}

// waitMembership 等待某节点的成员视图满足条件。
func waitMembership(t *testing.T, n *Node, wantVoters int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		voters, _ := n.Membership()
		if len(voters) == wantVoters {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	voters, learners := n.Membership()
	t.Fatalf("node membership not converged: voters=%v learners=%v want %d voters", voters, learners, wantVoters)
}

// waitVoters 等待某节点的 voter 集合精确等于期望值（顺序无关）。
func waitVoters(t *testing.T, n *Node, want []NodeID, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		voters, _ := n.Membership()
		if sameSet(voters, want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	voters, learners := n.Membership()
	t.Fatalf("voters not converged: voters=%v learners=%v want=%v", voters, learners, want)
}

func sameSet(a, b []NodeID) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[NodeID]bool, len(a))
	for _, x := range a {
		set[x] = true
	}
	for _, x := range b {
		if !set[x] {
			return false
		}
	}
	return true
}

// TestAddNodeCatchUpAndVote 扩容：新节点先以 learner 追平（含快照路径），
// 提升为 voter 后参与多数派与复制。
func TestAddNodeCatchUpAndVote(t *testing.T) {
	c := newCluster(t, 3, 5) // 小快照阈值，强迫追平走快照
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(3 * time.Second)

	// 写入足够多数据触发快照。
	for i := 0; i < 12; i++ {
		c.propose(t, leader, "cli", uint64(i+1), fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	c.waitAllApplied(12, 3*time.Second)

	// 新节点以未配置身份上线：不应发起选举、不应成为 leader。
	c.addJoiningNode("N4")
	time.Sleep(300 * time.Millisecond)
	if n4 := c.nodes["N4"]; n4.Role() != RoleFollower {
		t.Fatalf("unconfigured node should stay follower, got %s", n4.Role())
	}

	// 执行扩容：learner 追平（走快照）-> 提升 voter。
	addNode(t, leader, "N4")

	// 全部节点成员视图应收敛为 4 个 voter。
	for _, id := range []NodeID{"N1", "N2", "N3", "N4"} {
		waitMembership(t, c.nodes[id], 4, 3*time.Second)
	}
	t.Logf("membership after add: %v", c.membershipOf())

	// N4 通过快照+日志追平了全部已提交数据。
	c.waitCommit("N4", 12, 3*time.Second)
	c.waitAllApplied(12, 3*time.Second)
	keys := make([]string, 12)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
	}
	c.assertKVConsistent(keys...)

	// N4 成为 voter 后参与多数派：停掉一个老节点，集群（含 N4 共 3/4）仍能提交。
	victim := NodeID("N3")
	if leader.cfg.ID == "N3" {
		victim = "N2"
	}
	c.stop(victim)
	for i := 12; i < 15; i++ {
		c.propose(t, leader, "cli", uint64(i+1), fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	c.waitCommit("N4", 15, 3*time.Second)
	t.Logf("cluster still commits with N4 in quorum after stopping %s", victim)
}

// TestAddNodeLearnerPhaseNotInQuorum 追平前的新节点只是 learner，
// 不计入多数派；未上线时扩容会等在追平阶段，集群照常提交。
func TestAddNodeLearnerPhaseNotInQuorum(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(3 * time.Second)
	c.propose(t, leader, "cli", 1, "a", "1")

	// 新节点的存储/状态机先建好，但进程先不启动。
	c.stores["N4"] = NewMemoryStorage()
	c.machines["N4"] = NewKVMachine()

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done <- leader.AddNode(ctx, "N4")
	}()

	// 等 AddLearner 条目提交、N4 以 learner 身份出现在成员配置里。
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, learners := leader.Membership()
		if len(learners) == 1 && learners[0] == "N4" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("N4 did not appear as learner")
		}
		time.Sleep(10 * time.Millisecond)
	}
	voters, learners := leader.Membership()
	t.Logf("during catch-up: voters=%v learners=%v", voters, learners)

	// learner 不计入多数派：3 个 voter 的集群照常提交新提案。
	c.propose(t, leader, "cli", 2, "b", "2")

	// AddNode 应仍阻塞在追平阶段（N4 未上线）。
	select {
	case err := <-done:
		t.Fatalf("AddNode returned before N4 online: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	// N4 上线，追平后 AddNode 完成，N4 成为 voter。
	c.addJoiningNode("N4")
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("AddNode failed after N4 online: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("AddNode did not finish after N4 online")
	}
	waitMembership(t, c.nodes["N4"], 4, 3*time.Second)
	c.waitCommit("N4", 2, 3*time.Second)
	if v, ok := c.machines["N4"].Get("b"); !ok || v != "2" {
		t.Fatalf("N4 missing committed data: b=(%q,%v)", v, ok)
	}
	t.Logf("N4 promoted and caught up: %v", c.membershipOf())
}

// TestRemoveNode 缩容：被移除节点退出多数派并安静下来，剩余节点照常提交。
func TestRemoveNode(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(3 * time.Second)
	for i := 0; i < 3; i++ {
		c.propose(t, leader, "cli", uint64(i+1), fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}

	// 摘掉一个非 leader 节点。
	victim := NodeID("N3")
	if leader.cfg.ID == "N3" {
		victim = "N2"
	}
	termBefore := leader.Term()
	removeNode(t, leader, victim)

	// 剩余节点视图收敛为 2 voter；被移除节点也看到自己已不在配置中。
	for _, id := range c.ids {
		if id == victim {
			continue
		}
		waitMembership(t, c.nodes[id], 2, 3*time.Second)
	}
	waitMembership(t, c.nodes[victim], 2, 3*time.Second) // 提交前最后一批复制会送达
	t.Logf("membership after remove: %v", c.membershipOf())

	// 被移除节点不再发起选举（它已看到自己不在配置中），集群只有一个 leader 且任期稳定。
	time.Sleep(500 * time.Millisecond)
	leaders := c.countLeaders()
	if len(leaders) != 1 {
		t.Fatalf("expected exactly 1 leader after removal, got %v", leaders)
	}
	for term := range leaders {
		if term != termBefore {
			t.Fatalf("term moved after removal: before=%d now=%d", termBefore, term)
		}
	}

	// 下线被移除节点（退役），剩余 2 节点集群（多数派=2）继续提交。
	c.stop(victim)
	c.propose(t, leader, "cli", 4, "after", "remove")
	c.waitAllApplied(leader.CommitIndex(), 3*time.Second)
	c.assertKVConsistent("k0", "k1", "k2", "after")
}

// TestRemovedNodeCannotDisrupt 被移除节点在分区期间没收到移除条目，
// 恢复后凭旧身份拉票：其余节点拒绝（NotMember）且不随其任期升降，
// 集群 leader 与提交不受影响。
func TestRemovedNodeCannotDisrupt(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(3 * time.Second)
	c.propose(t, leader, "cli", 1, "x", "1")

	// 选一个非 leader 节点，先把它分区隔离，再把它移出集群。
	victim := NodeID("N3")
	if leader.cfg.ID == "N3" {
		victim = "N2"
	}
	c.net.Isolate(victim)
	removeNode(t, leader, victim) // 多数派 {leader, 另一节点} 提交移除
	waitMembership(t, leader, 2, 3*time.Second)
	termAfterRemove := leader.Term()

	// 恢复网络：victim 没看到自己被移除，会用旧配置+更高任期反复竞选。
	c.net.Heal()
	time.Sleep(800 * time.Millisecond)

	// 集群仍只有一个 leader，且任期没有被 victim 的竞选推高。
	leaders := c.countLeaders()
	if len(leaders) != 1 {
		t.Fatalf("removed node disrupted cluster: leaders=%v", leaders)
	}
	for term := range leaders {
		if term != termAfterRemove {
			t.Fatalf("term bumped by removed node: before=%d now=%d", termAfterRemove, term)
		}
	}
	if c.nodes[victim].Role() == RoleLeader {
		t.Fatalf("removed node became leader")
	}

	// 集群照常提交。
	c.propose(t, leader, "cli", 2, "y", "2")
	c.waitCommit(leader.cfg.ID, leader.CommitIndex(), 3*time.Second)
	t.Logf("removed node kept out: leaderTerm=%d membership=%v", leader.Term(), c.membershipOf())
}

// TestRemoveLeaderItself 移除当前 leader：提交后 leader 自动卸任，
// 剩余节点选出新 leader 并继续提交，可用性仅经历一次选举间隔。
func TestRemoveLeaderItself(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(3 * time.Second)
	oldLeaderID := leader.cfg.ID
	c.propose(t, leader, "cli", 1, "x", "1")

	removeNode(t, leader, oldLeaderID)

	// 老 leader 卸任且不再是 leader。
	if role := c.nodes[oldLeaderID].Role(); role == RoleLeader {
		t.Fatalf("removed leader still thinks it is leader")
	}

	// 退役老 leader（已移除节点不再接收复制），剩余 2 节点选出新 leader 并继续提交。
	c.stop(oldLeaderID)
	newLeader := c.waitLeaderMinTerm(leader.Term(), 5*time.Second)
	if newLeader.cfg.ID == oldLeaderID {
		t.Fatalf("removed leader re-elected")
	}
	c.propose(t, newLeader, "cli", 2, "y", "2")
	c.waitAllApplied(newLeader.CommitIndex(), 3*time.Second)
	t.Logf("leadership transferred to %s, membership=%v", newLeader.cfg.ID, c.membershipOf())
}

// TestDuplicateAndConcurrentConfChange 成员变更抗重复与并发互斥：
// 同一变更重复提交幂等生效一次；并发变更只有一个能先进入日志。
func TestDuplicateAndConcurrentConfChange(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(3 * time.Second)
	c.addJoiningNode("N4")
	c.addJoiningNode("N5")

	// 并发提交两个不同变更：至少一个成功，失败方必须是 ConfInProgress。
	type addResult struct {
		id  NodeID
		err error
	}
	results := make(chan addResult, 2)
	for _, id := range []NodeID{"N4", "N5"} {
		go func(id NodeID) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			results <- addResult{id, leader.AddNode(ctx, id)}
		}(id)
	}
	r1, r2 := <-results, <-results
	t.Logf("concurrent AddNode results: %s=%v %s=%v", r1.id, r1.err, r2.id, r2.err)
	if r1.err != nil && r2.err != nil {
		t.Fatalf("both concurrent changes failed: %v / %v", r1.err, r2.err)
	}
	var retry NodeID
	for _, r := range []addResult{r1, r2} {
		if r.err == nil {
			continue
		}
		re, ok := r.err.(*RejectError)
		if !ok || (re.Reason != ReasonConfInProgress && re.Reason != ReasonNotLeader && re.Reason != ReasonTimeout) {
			t.Fatalf("unexpected concurrent change error for %s: %v", r.id, r.err)
		}
		retry = r.id
	}

	// 重试失败方（串行化后应成功）。
	if retry != "" {
		addNode(t, leader, retry)
	}
	waitMembership(t, leader, 5, 3*time.Second)

	// 重复提交同一变更：幂等成功，成员不重复。
	addNode(t, leader, "N4")
	addNode(t, leader, "N4")
	voters, _ := leader.Membership()
	count := 0
	for _, v := range voters {
		if v == "N4" {
			count++
		}
	}
	if count != 1 || len(voters) != 5 {
		t.Fatalf("duplicate add took effect twice: voters=%v", voters)
	}

	// 重复移除：第一次生效，之后幂等成功。
	removeNode(t, leader, "N5")
	removeNode(t, leader, "N5")
	waitMembership(t, leader, 4, 3*time.Second)
	voters, _ = leader.Membership()
	if len(voters) != 4 {
		t.Fatalf("duplicate remove broke membership: voters=%v", voters)
	}
	t.Logf("final membership=%v", c.membershipOf())
}

// TestMembershipChangeUnderPartition 变更期间发生分区：
// 少数派侧的旧 leader 无法推进变更（超时失败且本地未提交变更随后被回滚），
// 多数派侧完成扩容；分区恢复后全集群成员视图一致、已提交数据不丢。
func TestMembershipChangeUnderPartition(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(3 * time.Second)
	oldLeaderID := leader.cfg.ID
	c.propose(t, leader, "cli", 1, "before", "partition")

	// 把旧 leader 隔到少数派侧。
	c.net.Isolate(oldLeaderID)

	// 多数派侧选出新 leader。
	newLeader := c.waitLeaderMinTerm(leader.Term(), 5*time.Second)
	t.Logf("partition: old leader %s isolated, new leader %s term=%d", oldLeaderID, newLeader.cfg.ID, newLeader.Term())

	// 旧 leader 在少数派侧发起的变更必须失败（拿不到多数派）。
	c.addJoiningNode("N4")
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	err := c.nodes[oldLeaderID].AddNode(ctx, "N4")
	cancel()
	if err == nil {
		t.Fatalf("minority-side leader should not commit membership change")
	}
	t.Logf("minority-side change rejected as expected: %v", err)

	// 多数派侧完成扩容与业务提交。
	addNode(t, newLeader, "N4")
	c.propose(t, newLeader, "cli", 2, "after", "partition")

	// 分区恢复：旧 leader 卸任、回滚未提交变更、追赶新日志。
	c.net.Heal()
	waitMembership(t, c.nodes[oldLeaderID], 4, 5*time.Second)
	waitMembership(t, c.nodes["N4"], 4, 5*time.Second)

	// 全集群只有一个 leader。
	time.Sleep(300 * time.Millisecond)
	leaders := c.countLeaders()
	if len(leaders) != 1 {
		t.Fatalf("more than one leader after heal: %v", leaders)
	}

	// 已提交数据不丢，新节点数据完整。
	c.waitCommit(oldLeaderID, newLeader.CommitIndex(), 3*time.Second)
	c.waitAllApplied(newLeader.CommitIndex(), 3*time.Second)
	c.assertKVConsistent("before", "after")
	t.Logf("after heal: membership=%v", c.membershipOf())
}

// TestMembershipPersistRestart 成员配置在快照与进程重启后保持一致：
// 扩容+缩容+快照后全集群重启，成员视图不回退、集群继续工作。
func TestMembershipPersistRestart(t *testing.T) {
	c := newCluster(t, 3, 3) // 小阈值强迫快照
	c.startAll()
	leader := c.waitLeader(3 * time.Second)
	for i := 0; i < 6; i++ {
		c.propose(t, leader, "cli", uint64(i+1), fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}

	// 扩容 N4，再缩容掉一个老节点。
	c.addJoiningNode("N4")
	addNode(t, leader, "N4")
	victim := NodeID("N2")
	if leader.cfg.ID == "N2" {
		victim = "N3"
	}
	removeNode(t, leader, victim)
	waitMembership(t, leader, 3, 3*time.Second)
	c.stop(victim) // 退役被移除节点（已移除成员不再接收复制）

	// 再写一批，让成员变更也落入快照。
	for i := 6; i < 12; i++ {
		c.propose(t, leader, "cli", uint64(i+1), fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	c.waitAllApplied(leader.CommitIndex(), 3*time.Second)

	// 全集群重启（复用存储，状态机重建）。
	members := []NodeID{}
	for id := range c.nodes {
		members = append(members, id)
	}
	for _, id := range members {
		c.restart(id)
	}
	t.Logf("cluster restarted, members=%v", members)

	// 重启后成员视图必须是变更后的 {3 个 voter}，不能回到旧列表。
	leader2 := c.waitLeader(5 * time.Second)
	for _, id := range members {
		waitMembership(t, c.nodes[id], 3, 3*time.Second)
		voters, _ := c.nodes[id].Membership()
		for _, v := range voters {
			if v == victim {
				t.Fatalf("node %s fell back to old membership after restart: voters=%v", id, voters)
			}
		}
	}

	// 数据不丢，集群继续提交。
	if v, ok := c.machines[leader2.cfg.ID].Get("k11"); !ok || v != "v11" {
		t.Fatalf("committed data lost after restart: k11=(%q,%v)", v, ok)
	}
	c.propose(t, leader2, "cli", 13, "post", "restart")
	c.waitAllApplied(leader2.CommitIndex(), 3*time.Second)
	keys := []string{"k0", "k5", "k11", "post"}
	c.assertKVConsistent(keys...)
	t.Logf("membership stable across restart: %v", c.membershipOf())
}

// TestGrow3To5ShrinkTo3 完整演练：3 节点扩到 5 节点再缩回 3 节点，
// 每个阶段都验证成员视图、提交能力与数据一致性。
func TestGrow3To5ShrinkTo3(t *testing.T) {
	c := newCluster(t, 3, 7)
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(3 * time.Second)

	seq := uint64(0)
	proposeN := func(n int, prefix string) {
		for i := 0; i < n; i++ {
			seq++
			c.propose(t, leader, "cli", seq, fmt.Sprintf("%s-%d", prefix, seq), fmt.Sprintf("v%d", seq))
		}
	}

	// 阶段 0：3 节点基线。
	proposeN(5, "base")

	// 阶段 1：扩到 5 节点。
	c.addJoiningNode("N4")
	c.addJoiningNode("N5")
	addNode(t, leader, "N4")
	addNode(t, leader, "N5")
	for _, id := range []NodeID{"N1", "N2", "N3", "N4", "N5"} {
		waitMembership(t, c.nodes[id], 5, 3*time.Second)
	}
	t.Logf("grown to 5: %v", c.membershipOf())

	// 5 节点下继续提交，新成员完整追平。
	proposeN(5, "grown")
	c.waitAllApplied(leader.CommitIndex(), 3*time.Second)

	// 阶段 2：缩回 3 节点。
	removeNode(t, leader, "N4")
	removeNode(t, leader, "N5")
	for _, id := range []NodeID{"N1", "N2", "N3"} {
		waitMembership(t, c.nodes[id], 3, 3*time.Second)
	}
	c.stop("N4")
	c.stop("N5")
	t.Logf("shrunk to 3: %v", c.membershipOf())

	// 3 节点下继续提交。
	proposeN(5, "shrunk")
	c.waitAllApplied(leader.CommitIndex(), 3*time.Second)

	// 全程数据一致：抽查各阶段写入的 key。
	keys := []string{"base-1", "base-5", "grown-6", "grown-10", "shrunk-11", "shrunk-15"}
	c.assertKVConsistent(keys...)
	for _, k := range keys {
		want := fmt.Sprintf("v%s", k[len(k)-1:])
		if k == "grown-10" || k == "shrunk-15" || k == "shrunk-11" {
			want = fmt.Sprintf("v%s", k[len(k)-2:])
		}
		if v, ok := c.machines[leader.cfg.ID].Get(k); !ok || v != want {
			t.Fatalf("leader missing %s: got (%q,%v) want %q", k, v, ok, want)
		}
	}
	t.Logf("3->5->3 done, final membership=%v", c.membershipOf())
}

// TestMembershipChangeWithNodeRestart 变更过程中有节点处于重启/停机状态：
// 停机期间变更由其余多数派提交，节点恢复后通过复制追赶上新成员配置，
// 不出现视图分裂，集群持续可提交。
func TestMembershipChangeWithNodeRestart(t *testing.T) {
	c := newCluster(t, 3, 4)
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(3 * time.Second)
	for i := 0; i < 4; i++ {
		c.propose(t, leader, "cli", uint64(i+1), fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}

	// 先扩到 4 节点。
	c.addJoiningNode("N4")
	addNode(t, leader, "N4")

	// 停掉一个非 leader 节点，在它缺席时完成一次缩容。
	down := NodeID("N2")
	if leader.cfg.ID == "N2" {
		down = "N3"
	}
	victim := NodeID("N3")
	if leader.cfg.ID == "N3" || down == "N3" {
		victim = "N1"
	}
	c.stop(down)
	removeNode(t, leader, victim) // 4 voter 多数派=3：leader + victim + N4 提交
	waitMembership(t, leader, 3, 3*time.Second)
	c.stop(victim)

	// 停机节点重启：追赶日志（含成员变更条目与快照），视图与集群一致。
	c.restart(down)
	remain := []NodeID{}
	for _, id := range []NodeID{"N1", "N2", "N3", "N4"} {
		if id != victim {
			remain = append(remain, id)
		}
	}
	waitVoters(t, c.nodes[down], remain, 5*time.Second)

	// 恢复后集群继续提交，全部成员数据一致。
	for i := 4; i < 8; i++ {
		c.propose(t, leader, "cli", uint64(i+1), fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	c.waitAllApplied(leader.CommitIndex(), 3*time.Second)
	c.assertKVConsistent("k0", "k3", "k7")
	t.Logf("node %s restarted into new membership: %v", down, c.membershipOf())
}
