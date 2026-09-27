package raft

import (
	"encoding/json"
	"fmt"
	"sync"
)

// KVOp KV 状态机操作。
type KVOp struct {
	Op    string `json:"op"` // "set" / "del" / "get"
	Key   string `json:"key"`
	Value string `json:"value"`
}

// EncodeKVOp 编码 KV 操作为提案负载。
func EncodeKVOp(op KVOp) []byte {
	b, _ := json.Marshal(op)
	return b
}

// KVMachine 示例状态机：内存 KV 存储，用于测试与演示。
// Apply 非幂等操作（如计数器）也可安全使用：raft 层保证已提交前缀只应用一次。
type KVMachine struct {
	mu      sync.Mutex
	data    map[string]string
	applied uint64
}

// NewKVMachine 创建空 KV 状态机。
func NewKVMachine() *KVMachine {
	return &KVMachine{data: make(map[string]string)}
}

// Apply 应用一条已提交操作，返回结果（get 返回当前值，set/del 返回旧值）。
func (m *KVMachine) Apply(op []byte) any {
	var kv KVOp
	if err := json.Unmarshal(op, &kv); err != nil {
		return fmt.Sprintf("error: bad op: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.applied++
	old := m.data[kv.Key]
	switch kv.Op {
	case "set":
		m.data[kv.Key] = kv.Value
	case "del":
		delete(m.data, kv.Key)
	case "get":
		// 只读
	default:
		return "error: unknown op " + kv.Op
	}
	return old
}

// Snapshot 序列化当前状态。
func (m *KVMachine) Snapshot() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return json.Marshal(map[string]any{
		"data":    m.data,
		"applied": m.applied,
	})
}

// Restore 从快照恢复。
func (m *KVMachine) Restore(data []byte) error {
	var s struct {
		Data    map[string]string `json:"data"`
		Applied uint64            `json:"applied"`
	}
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = s.Data
	if m.data == nil {
		m.data = make(map[string]string)
	}
	m.applied = s.Applied
	return nil
}

// Get 读取键值（测试辅助）。
func (m *KVMachine) Get(key string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[key]
	return v, ok
}

// Applied 返回已应用条目数。
func (m *KVMachine) Applied() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.applied
}
