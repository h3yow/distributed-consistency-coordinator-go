package coordinator_test

import (
	"context"
	"fmt"
	"time"

	coordinator "github.com/h3yow/distributed-consistency-coordinator-go/coordinator"
)

// Example 演示如何把协调模块嵌入应用：本地起 3 个节点组成集群，
// 通过 leader 提交一条 KV 命令并等待其被多数派提交、应用到状态机。
func Example() {
	transport := coordinator.NewLocalTransport()
	peers := []string{"n1", "n2", "n3"}

	nodes := map[string]*coordinator.Node{}
	fsms := map[string]*coordinator.KVFSM{}
	for _, id := range peers {
		fsm := coordinator.NewKVFSM()
		fsms[id] = fsm
		n, err := coordinator.NewNode(coordinator.Config{
			ID:                 id,
			Peers:              peers,
			DataDir:            "", // 空目录 = 纯内存；生产环境应提供持久化目录
			HeartbeatInterval:  20 * time.Millisecond,
			ElectionTimeoutMin: 80 * time.Millisecond,
			ElectionTimeoutMax: 160 * time.Millisecond,
		}, transport, fsm, nil)
		if err != nil {
			panic(err)
		}
		nodes[id] = n
		if err := n.Start(); err != nil {
			panic(err)
		}
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	// 等待选主。
	var leader *coordinator.Node
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.Status().Role == coordinator.RoleLeader {
				leader = n
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if leader == nil {
		panic("no leader elected")
	}

	// 提交命令（带 ProposalID 以获得幂等去重）。
	cmd := coordinator.MarshalKVCommand(coordinator.KVCommand{
		Op: coordinator.KVOpSet, Key: "hello", Val: "world", ProposalID: "demo-1",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := leader.Propose(ctx, cmd); err != nil {
		panic(err)
	}

	// 等待值传播到所有节点。
	for _, id := range peers {
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if v, ok := fsms[id].Get("hello"); ok && v == "world" {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	for _, id := range peers {
		v, _ := fsms[id].Get("hello")
		fmt.Printf("%s: hello=%s\n", id, v)
	}
	// Output:
	// n1: hello=world
	// n2: hello=world
	// n3: hello=world
}
