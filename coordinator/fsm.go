package coordinator

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// FSM 是由复制日志驱动的确定性状态机。
//
// 约定：Apply 必须幂等——同一 (index, command) 被重复应用时，
// 结果与只应用一次相同。协调模块在崩溃恢复边界上可能重放
// 最后一条已提交条目，幂等性保证状态机不偏离已提交前缀。
type FSM interface {
	// Apply 应用一条已提交日志，返回提案结果（原样返回给 Propose 调用方）。
	Apply(index uint64, command []byte) any
	// Snapshot 序列化当前状态（只会基于已提交状态调用）。
	Snapshot() ([]byte, error)
	// Restore 从快照数据恢复状态。
	Restore(data []byte) error
}

// ---- 内置 KV 状态机 ----

// KVOp 是 KV 状态机的操作类型。
type KVOp string

const (
	KVOpSet    KVOp = "set"
	KVOpDelete KVOp = "delete"
)

// KVCommand 是一条 KV 命令，经 JSON 编码后作为 LogEntry.Command。
type KVCommand struct {
	Op  KVOp   `json:"op"`
	Key string `json:"key"`
	Val string `json:"val"`

	// ProposalID 用于幂等去重：同一 ProposalID 的重复提案
	// （客户端重试、超时重发）只应用一次并返回首次结果。
	ProposalID string `json:"proposalID,omitempty"`
}

// MarshalKVCommand 编码 KV 命令。
func MarshalKVCommand(c KVCommand) []byte {
	b, err := json.Marshal(c)
	if err != nil {
		panic(err) // KVCommand 必然可编码
	}
	return b
}

// KVResult 是 KV 命令的应用结果。
type KVResult struct {
	Duplicate bool   // 是否为被去重的重复提案
	Applied   uint64 // 实际生效的日志下标（重复提案时为首次应用的下标）
}

// KVFSM 是一个内置的键值状态机，支持提案级幂等去重。
type KVFSM struct {
	mu      sync.Mutex
	data    map[string]string
	results map[string]KVResult // ProposalID -> 首次应用结果
}

// NewKVFSM 创建空的 KV 状态机。
func NewKVFSM() *KVFSM {
	return &KVFSM{
		data:    make(map[string]string),
		results: make(map[string]KVResult),
	}
}

// ErrUnknownKVCommand 表示命令无法解析。
var ErrUnknownKVCommand = errors.New("coordinator: unknown kv command")

// Apply 实现 FSM。
func (f *KVFSM) Apply(index uint64, command []byte) any {
	var cmd KVCommand
	if err := json.Unmarshal(command, &cmd); err != nil {
		return ErrUnknownKVCommand
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if cmd.ProposalID != "" {
		if prev, dup := f.results[cmd.ProposalID]; dup {
			return KVResult{Duplicate: true, Applied: prev.Applied}
		}
	}
	switch cmd.Op {
	case KVOpSet:
		f.data[cmd.Key] = cmd.Val
	case KVOpDelete:
		delete(f.data, cmd.Key)
	default:
		return fmt.Errorf("%w: op=%q", ErrUnknownKVCommand, cmd.Op)
	}
	res := KVResult{Applied: index}
	if cmd.ProposalID != "" {
		f.results[cmd.ProposalID] = res
	}
	return res
}

// kvSnapshot 是 KVFSM 的快照序列化格式。
type kvSnapshot struct {
	Data    map[string]string   `json:"data"`
	Results map[string]KVResult `json:"results"`
}

// Snapshot 实现 FSM。
func (f *KVFSM) Snapshot() ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return json.Marshal(kvSnapshot{Data: f.data, Results: f.results})
}

// Restore 实现 FSM。
func (f *KVFSM) Restore(data []byte) error {
	var s kvSnapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data = s.Data
	f.results = s.Results
	if f.data == nil {
		f.data = make(map[string]string)
	}
	if f.results == nil {
		f.results = make(map[string]KVResult)
	}
	return nil
}

// Get 读取键值（仅供测试与只读场景使用，不走共识）。
func (f *KVFSM) Get(key string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.data[key]
	return v, ok
}

// Len 返回键数量（测试辅助）。
func (f *KVFSM) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.data)
}

// Items 返回全部键值对的副本（测试与诊断辅助）。
func (f *KVFSM) Items() map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]string, len(f.data))
	for k, v := range f.data {
		out[k] = v
	}
	return out
}
