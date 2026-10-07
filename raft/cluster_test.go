package raft

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// testCluster 本地多节点测试集群。
type testCluster struct {
	t        *testing.T
	net      *Network
	ids      []NodeID
	stores   map[NodeID]*MemoryStorage
	machines map[NodeID]*KVMachine
	nodes    map[NodeID]*Node
	snapTh   int
}

func newCluster(t *testing.T, size int, snapshotThreshold int) *testCluster {
	t.Helper()
	c := &testCluster{
		t:        t,
		net:      NewNetwork(),
		stores:   make(map[NodeID]*MemoryStorage),
		machines: make(map[NodeID]*KVMachine),
		nodes:    make(map[NodeID]*Node),
		snapTh:   snapshotThreshold,
	}
	for i := 0; i < size; i++ {
		id := NodeID(fmt.Sprintf("N%d", i+1))
		c.ids = append(c.ids, id)
		c.stores[id] = NewMemoryStorage()
		c.machines[id] = NewKVMachine()
	}
	return c
}

func (c *testCluster) start(id NodeID) {
	// 初始成员用静态 Peers 启动；后加入的节点以未配置身份启动，
	// 成员信息从存储（快照+日志）恢复。
	peers := []NodeID(nil)
	for _, pid := range c.ids {
		if pid == id {
			peers = c.ids
			break
		}
	}
	cfg := Config{
		ID:                 id,
		Peers:              peers,
		HeartbeatInterval:  20 * time.Millisecond,
		ElectionTimeoutMin: 80 * time.Millisecond,
		ElectionTimeoutMax: 160 * time.Millisecond,
		SnapshotThreshold:  c.snapTh,
		EnableLog:          true,
	}
	n := NewNode(cfg, c.stores[id], c.net, c.machines[id])
	c.nodes[id] = n
	n.Start()
}

func (c *testCluster) startAll() {
	for _, id := range c.ids {
		c.start(id)
	}
}

func (c *testCluster) stop(id NodeID) {
	if n, ok := c.nodes[id]; ok {
		n.Stop()
		delete(c.nodes, id)
	}
}

// restart 模拟节点重启：复用同一存储（模拟磁盘），
// 状态机换新实例（嵌入方约定：状态机由快照+日志回放重建）。
func (c *testCluster) restart(id NodeID) {
	c.stop(id)
	c.machines[id] = NewKVMachine()
	c.start(id)
}

func (c *testCluster) stopAll() {
	for id := range c.nodes {
		c.stop(id)
	}
}

// addJoiningNode 以未配置身份启动一个新节点（Peers 为空）：
// 模拟新机器上线，不发起选举、不投票，等待被 AddNode 拉入集群。
func (c *testCluster) addJoiningNode(id NodeID) {
	c.t.Helper()
	c.stores[id] = NewMemoryStorage()
	c.machines[id] = NewKVMachine()
	cfg := Config{
		ID:                 id,
		Peers:              nil, // 未配置：等待集群成员变更纳入
		HeartbeatInterval:  20 * time.Millisecond,
		ElectionTimeoutMin: 80 * time.Millisecond,
		ElectionTimeoutMax: 160 * time.Millisecond,
		SnapshotThreshold:  c.snapTh,
		EnableLog:          true,
	}
	n := NewNode(cfg, c.stores[id], c.net, c.machines[id])
	c.nodes[id] = n
	n.Start()
}

// membershipOf 收集所有运行中节点的成员配置视图。
func (c *testCluster) membershipOf() map[NodeID]string {
	out := make(map[NodeID]string)
	for id, n := range c.nodes {
		voters, learners := n.Membership()
		out[id] = fmt.Sprintf("voters=%v learners=%v", voters, learners)
	}
	return out
}

// waitLeader 等待选出一个 leader 且多数运行中节点认可，返回 leader。
func (c *testCluster) waitLeader(timeout time.Duration) *Node {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for id, n := range c.nodes {
			if n.Role() != RoleLeader {
				continue
			}
			// 确认多数运行中节点认可该 leader 且任期一致
			agree := 0
			for _, on := range c.nodes {
				if on.LeaderID() == id && on.Term() == n.Term() {
					agree++
				}
			}
			if agree > len(c.nodes)/2 {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no stable leader within %v", timeout)
	return nil
}

// waitLeaderMinTerm 等待选出一个任期 > minTerm 的稳定 leader。
func (c *testCluster) waitLeaderMinTerm(minTerm uint64, timeout time.Duration) *Node {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for id, n := range c.nodes {
			if n.Role() != RoleLeader || n.Term() <= minTerm {
				continue
			}
			agree := 0
			for _, on := range c.nodes {
				if on.LeaderID() == id && on.Term() == n.Term() {
					agree++
				}
			}
			if agree > len(c.nodes)/2 {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no stable leader with term > %d within %v", minTerm, timeout)
	return nil
}

// countLeaders 统计当前自认为是 leader 的节点数。
func (c *testCluster) countLeaders() map[uint64][]NodeID {
	out := make(map[uint64][]NodeID)
	for _, n := range c.nodes {
		if n.Role() == RoleLeader {
			term := n.Term()
			out[term] = append(out[term], n.cfg.ID)
		}
	}
	return out
}

// waitCommit 等待节点提交位点到达 idx。
func (c *testCluster) waitCommit(id NodeID, idx uint64, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n, ok := c.nodes[id]; ok && n.CommitIndex() >= idx {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("node %s commitIndex did not reach %d within %v", id, idx, timeout)
}

// waitAllApplied 等待所有运行中节点应用到 idx。
func (c *testCluster) waitAllApplied(idx uint64, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for _, n := range c.nodes {
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
	c.t.Fatalf("not all nodes applied up to %d within %v", idx, timeout)
}

// propose 向 leader 提交 KV 写提案。
func (c *testCluster) propose(t *testing.T, leader *Node, clientID string, seq uint64, key, value string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := leader.Propose(ctx, Command{
		ClientID: clientID, Seq: seq,
		Op: EncodeKVOp(KVOp{Op: "set", Key: key, Value: value}),
	})
	if err != nil {
		t.Fatalf("propose %s=%s failed: %v", key, value, err)
	}
}

// assertKVConsistent 断言所有运行中节点状态机一致。
func (c *testCluster) assertKVConsistent(keys ...string) {
	c.t.Helper()
	var ref *KVMachine
	for _, id := range c.ids {
		if m, ok := c.machines[id]; ok && c.nodes[id] != nil {
			ref = m
			break
		}
	}
	for _, id := range c.ids {
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
