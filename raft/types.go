// Package raft 提供可嵌入的分布式一致性协调模块（Raft 风格）。
package raft

import "time"

// NodeID 唯一标识集群中的一个节点。
type NodeID string

// Role 表示节点当前角色。
type Role int

const (
	RoleFollower Role = iota
	RoleCandidate
	RoleLeader
)

func (r Role) String() string {
	switch r {
	case RoleFollower:
		return "Follower"
	case RoleCandidate:
		return "Candidate"
	case RoleLeader:
		return "Leader"
	}
	return "Unknown"
}

// RejectReason 区分请求被拒绝/降级的具体原因。
type RejectReason int

const (
	ReasonNone         RejectReason = iota
	ReasonStaleTerm                 // 任期过期：收到更高任期后降级
	ReasonAlreadyVoted              // 本任期已投票给其他候选人
	ReasonLogBehind                 // 候选人日志落后，拒绝投票
	ReasonLogMismatch               // AppendEntries 前置日志不匹配
	ReasonNotLeader                 // 当前节点不是 leader
	ReasonNoQuorum                  // 无法获得多数派确认
	ReasonShutdown                  // 节点已停止
	ReasonTimeout                   // 等待提交超时
	ReasonDuplicate                 // 重复提案（幂等返回缓存结果）
)

func (r RejectReason) String() string {
	switch r {
	case ReasonNone:
		return "None"
	case ReasonStaleTerm:
		return "StaleTerm"
	case ReasonAlreadyVoted:
		return "AlreadyVoted"
	case ReasonLogBehind:
		return "LogBehind"
	case ReasonLogMismatch:
		return "LogMismatch"
	case ReasonNotLeader:
		return "NotLeader"
	case ReasonNoQuorum:
		return "NoQuorum"
	case ReasonShutdown:
		return "Shutdown"
	case ReasonTimeout:
		return "Timeout"
	case ReasonDuplicate:
		return "Duplicate"
	}
	return "Unknown"
}

// RejectError 携带可区分的失败原因。
type RejectError struct {
	Reason RejectReason
	Leader NodeID // 已知 leader 提示（可为空）
	Detail string
}

func (e *RejectError) Error() string {
	if e.Leader != "" {
		return "raft: rejected (" + e.Reason.String() + ", leader=" + string(e.Leader) + "): " + e.Detail
	}
	return "raft: rejected (" + e.Reason.String() + "): " + e.Detail
}

// LogEntry 日志条目。ClientID+Seq 用于提案幂等去重。
type LogEntry struct {
	Term     uint64
	Index    uint64
	ClientID string
	Seq      uint64
	Op       []byte
}

// Command 客户端提案。
type Command struct {
	ClientID string
	Seq      uint64
	Op       []byte
}

// MessageType 节点间消息类型。
type MessageType int

const (
	MsgRequestVote MessageType = iota
	MsgRequestVoteReply
	MsgAppendEntries
	MsgAppendEntriesReply
	MsgInstallSnapshot
	MsgInstallSnapshotReply
)

func (t MessageType) String() string {
	switch t {
	case MsgRequestVote:
		return "RequestVote"
	case MsgRequestVoteReply:
		return "RequestVoteReply"
	case MsgAppendEntries:
		return "AppendEntries"
	case MsgAppendEntriesReply:
		return "AppendEntriesReply"
	case MsgInstallSnapshot:
		return "InstallSnapshot"
	case MsgInstallSnapshotReply:
		return "InstallSnapshotReply"
	}
	return "Unknown"
}

// Message 节点间传递的统一消息结构。
type Message struct {
	Type MessageType
	From NodeID
	To   NodeID
	Term uint64

	// RequestVote
	LastLogIndex uint64
	LastLogTerm  uint64

	// AppendEntries
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64

	// 回复通用字段
	Success    bool
	Reason     RejectReason
	MatchIndex uint64

	// InstallSnapshot
	SnapshotIndex uint64
	SnapshotTerm  uint64
	SnapshotData  []byte
}

// Config 节点配置。
type Config struct {
	ID                 NodeID
	Peers              []NodeID // 含自身的全部节点
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	SnapshotThreshold  int // 日志条目数超过该值触发快照，<=0 表示不自动快照
	EnableLog          bool
}

// StateMachine 上层状态机接口。
type StateMachine interface {
	// Apply 应用一条已提交日志，返回结果（用于提案响应）。
	Apply(op []byte) any
	// Snapshot 序列化当前状态。
	Snapshot() ([]byte, error)
	// Restore 从快照恢复状态。
	Restore(data []byte) error
}
