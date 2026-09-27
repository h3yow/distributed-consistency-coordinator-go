// Package coordinator 提供可嵌入的分布式一致性协调模块。
//
// 该模块实现了一个 Raft 风格的共识协议：领导选举、日志复制、
// 提交与状态机应用、快照压缩与落后节点追赶。集群通过本地
// Transport 模拟网络，可注入分区、断链与节点重启。
package coordinator

import (
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Role 表示节点在集群中的角色。
type Role int

const (
	RoleFollower Role = iota
	RoleCandidate
	RoleLeader
)

func (r Role) String() string {
	switch r {
	case RoleFollower:
		return "follower"
	case RoleCandidate:
		return "candidate"
	case RoleLeader:
		return "leader"
	default:
		return "unknown"
	}
}

// LogEntry 是一条复制日志条目。
type LogEntry struct {
	Term    uint64 // 创建该条目的 leader 任期
	Index   uint64 // 日志下标，从 1 开始
	Command []byte // 状态机命令（由 Propose 传入）
}

// SnapshotMeta 描述一次快照的边界。
type SnapshotMeta struct {
	LastIncludedIndex uint64 // 快照覆盖的最后一条日志下标
	LastIncludedTerm  uint64 // 该条目的任期
}

// 拒绝与降级原因：调用方与测试可据此区分失败来源。
var (
	// ErrNotLeader 表示当前节点不是 leader，提案被拒绝。
	ErrNotLeader = errors.New("coordinator: not leader")
	// ErrStopped 表示节点已停止。
	ErrStopped = errors.New("coordinator: node stopped")
	// ErrTimeout 表示提案在等待提交期间超时（例如分区导致无法达成多数）。
	ErrTimeout = errors.New("coordinator: proposal timeout")
	// ErrLeadershipLost 表示提案等待期间本节点失去了领导权（降级）。
	ErrLeadershipLost = errors.New("coordinator: leadership lost")
	// ErrUnreachable 表示目标节点当前不可达（被分区或已停止）。
	ErrUnreachable = errors.New("coordinator: peer unreachable")
)

// NotLeaderError 在节点不是 leader 时返回，附带已知的 leader 提示。
type NotLeaderError struct {
	LeaderHint string
}

func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("%v (hint: leader=%q)", ErrNotLeader, e.LeaderHint)
}

func (e *NotLeaderError) Unwrap() error { return ErrNotLeader }

// Config 是节点配置。
type Config struct {
	ID      string   // 本节点 ID
	Peers   []string // 集群全部成员 ID（含自己）
	DataDir string   // 持久化目录；为空则仅内存（重启即丢失）

	HeartbeatInterval  time.Duration // leader 心跳间隔
	ElectionTimeoutMin time.Duration // 选举超时下界
	ElectionTimeoutMax time.Duration // 选举超时上界
	ProposeTimeout     time.Duration // 提案等待提交的超时

	// SnapshotThreshold：已提交但未快照的条目数达到该值时触发快照；0 表示关闭自动快照。
	SnapshotThreshold uint64

	Logger *slog.Logger // 可选；为空时使用默认 logger
}

func (c *Config) setDefaults() {
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	if c.ElectionTimeoutMin <= 0 {
		c.ElectionTimeoutMin = 150 * time.Millisecond
	}
	if c.ElectionTimeoutMax <= c.ElectionTimeoutMin {
		c.ElectionTimeoutMax = 2 * c.ElectionTimeoutMin
	}
	if c.ProposeTimeout <= 0 {
		c.ProposeTimeout = 5 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// applyMsg 是提交后待应用到状态机的条目。
type applyMsg struct {
	entry LogEntry
}
