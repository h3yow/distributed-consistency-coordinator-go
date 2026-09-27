package coordinator

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// testCluster 封装本地多节点测试环境。
type testCluster struct {
	t        *testing.T
	tr       *LocalTransport
	nodes    map[string]*Node
	fsms     map[string]*KVFSM
	dirs     map[string]string
	peerList []string
}

func testLogger(t *testing.T) *slog.Logger {
	// 单测日志中打印任期、投票、提交位点、快照边界等关键事件。
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

func newTestCluster(t *testing.T, ids ...string) *testCluster {
	t.Helper()
	tr := NewLocalTransport()
	c := &testCluster{
		t:        t,
		tr:       tr,
		nodes:    map[string]*Node{},
		fsms:     map[string]*KVFSM{},
		dirs:     map[string]string{},
		peerList: ids,
	}
	for _, id := range ids {
		c.addNode(t, id)
	}
	return c
}

// addNode 构建（但不启动）一个节点。
func (c *testCluster) addNode(t *testing.T, id string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), id)
	c.dirs[id] = dir
	fsm := NewKVFSM()
	c.fsms[id] = fsm
	cfg := Config{
		ID:                 id,
		Peers:              c.peerList,
		DataDir:            dir,
		HeartbeatInterval:  15 * time.Millisecond,
		ElectionTimeoutMin: 60 * time.Millisecond,
		ElectionTimeoutMax: 120 * time.Millisecond,
		ProposeTimeout:     2 * time.Second,
		Logger:             testLogger(t),
	}
	n, err := NewNode(cfg, c.tr, fsm, nil)
	if err != nil {
		t.Fatalf("new node %s: %v", id, err)
	}
	c.nodes[id] = n
}

func (c *testCluster) start(id string) {
	c.t.Helper()
	if err := c.nodes[id].Start(); err != nil {
		c.t.Fatalf("start node %s: %v", id, err)
	}
}

func (c *testCluster) startAll() {
	for _, id := range c.peerList {
		c.start(id)
	}
}

func (c *testCluster) stop(id string) { c.nodes[id].Stop() }
func (c *testCluster) stopAll()       {
	for _, id := range c.peerList {
		c.stop(id)
	}
}

func (c *testCluster) cleanup() {
	for _, id := range c.peerList {
		c.nodes[id].Stop()
	}
}

// restartNode 模拟进程重启：停止后用同一 DataDir 与新 FSM 重建节点。
func (c *testCluster) restartNode(t *testing.T, id string) *Node {
	t.Helper()
	c.nodes[id].Stop()
	fsm := NewKVFSM()
	c.fsms[id] = fsm
	cfg := Config{
		ID:                 id,
		Peers:              c.peerList,
		DataDir:            c.dirs[id],
		HeartbeatInterval:  15 * time.Millisecond,
		ElectionTimeoutMin: 60 * time.Millisecond,
		ElectionTimeoutMax: 120 * time.Millisecond,
		ProposeTimeout:     2 * time.Second,
		Logger:             testLogger(t),
	}
	n, err := NewNode(cfg, c.tr, fsm, nil)
	if err != nil {
		t.Fatalf("rebuild node %s: %v", id, err)
	}
	c.nodes[id] = n
	if err := n.Start(); err != nil {
		t.Fatalf("restart node %s: %v", id, err)
	}
	return n
}

// leaderID 轮询等待集群出现唯一 leader，返回其 ID 与任期。
func (c *testCluster) waitLeader(wait time.Duration) (string, uint64) {
	deadline := time.Now().Add(wait)
	var (
		leader  string
		term    uint64
		leaders = map[string]int{}
	)
	for time.Now().Before(deadline) {
		leaders = map[string]int{}
		leader, term = "", 0
		for id, n := range c.nodes {
			s := n.Status()
			if s.Role == RoleLeader {
				leaders[id]++
				leader, term = id, s.Term
			}
		}
		if len(leaders) == 1 {
			return leader, term
		}
		time.Sleep(5 * time.Millisecond)
	}
	return "", 0
}

// requireSingleLeader 断言在采样窗口内不出现双主。
func (c *testCluster) requireSingleLeader(t *testing.T, window time.Duration) (string, uint64) {
	t.Helper()
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		var ls []string
		for id, n := range c.nodes {
			if n.Status().Role == RoleLeader {
				ls = append(ls, id)
			}
		}
		if len(ls) > 1 {
			t.Fatalf("split brain: simultaneous leaders %v", ls)
		}
		time.Sleep(2 * time.Millisecond)
	}
	leader, term := c.waitLeader(2 * time.Second)
	if leader == "" {
		t.Fatal("no leader elected in time")
	}
	return leader, term
}

func (c *testCluster) leaderNode() *Node {
	for _, n := range c.nodes {
		if n.Status().Role == RoleLeader {
			return n
		}
	}
	return nil
}

// waitApplied 等待所有在线节点 FSM 至少应用到 index。
func (c *testCluster) waitApplied(t *testing.T, index uint64, ids ...string) {
	t.Helper()
	if len(ids) == 0 {
		ids = c.peerList
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range ids {
			if c.nodes[id].Status().LastApplied < index {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	st := map[string]NodeStatus{}
	for _, id := range ids {
		st[id] = c.nodes[id].Status()
	}
	t.Fatalf("timed out waiting applied=%d: %+v", index, st)
}

var proposeSeq int64

// proposeKV 向 leader 提交一个 set 操作，返回其日志下标（通过 KVResult.Applied）。
func (c *testCluster) proposeKV(t *testing.T, key, val, proposalID string) (any, error) {
	t.Helper()
	if proposalID == "" {
		proposalID = fmt.Sprintf("p-%d-%d", time.Now().UnixNano(), proposeSeq)
		proposeSeq++
	}
	cmd := MarshalKVCommand(KVCommand{Op: KVOpSet, Key: key, Val: val, ProposalID: proposalID})
	leader := c.leaderNode()
	if leader == nil {
		return nil, ErrNotLeader
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return leader.Propose(ctx, cmd)
}

// waitValue 等待某节点 FSM 中 key 等于 want（容忍复制延迟）。
func (c *testCluster) waitValue(t *testing.T, id, key, want string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok := c.fsms[id].Get(key); ok && v == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	v, ok := c.fsms[id].Get(key)
	t.Fatalf("node %s key=%q want=%q got=%q(ok=%v)", id, key, want, v, ok)
}

var _ = sync.Mutex{}
