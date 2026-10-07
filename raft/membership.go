package raft

import (
	"context"
	"encoding/json"
	"sort"
	"time"
)

// ConfChangeType 成员变更类型。
type ConfChangeType int

const (
	// ConfAddLearner 以 learner（非投票成员）身份加入，追平前不计入多数派。
	ConfAddLearner ConfChangeType = iota
	// ConfPromote 将已追平的 learner 提升为 voter（正式成员）。
	ConfPromote
	// ConfRemove 将节点移出集群（voter 或 learner 均可）。
	ConfRemove
)

func (t ConfChangeType) String() string {
	switch t {
	case ConfAddLearner:
		return "AddLearner"
	case ConfPromote:
		return "Promote"
	case ConfRemove:
		return "Remove"
	}
	return "Unknown"
}

// ConfChange 一次成员变更请求（API 入参）。
type ConfChange struct {
	Type   ConfChangeType
	NodeID NodeID
}

// confEntry 日志中记录的成员变更条目：携带变更后完整的成员配置。
// 跟随者/重启重放只需取最新一条即可还原配置，无需依赖日志之外的初始成员，
// 因此空存储上线的新节点也能仅从日志+快照重建完整成员视图。
type confEntry struct {
	Type     ConfChangeType `json:"type"`
	NodeID   NodeID         `json:"node"`
	Voters   []NodeID       `json:"voters"`
	Learners []NodeID       `json:"learners"`
}

// confOpPrefix 成员变更日志条目的 Op 前缀，与业务负载区分。
const confOpPrefix = "\x00raft-conf\x00"

// encodeConfOp 将成员变更条目编码为日志负载。
func encodeConfOp(e confEntry) []byte {
	b, _ := json.Marshal(e)
	return append([]byte(confOpPrefix), b...)
}

// decodeConfOp 尝试把日志负载解码为成员变更条目；非成员变更条目返回 false。
func decodeConfOp(op []byte) (confEntry, bool) {
	var e confEntry
	if len(op) <= len(confOpPrefix) || string(op[:len(confOpPrefix)]) != confOpPrefix {
		return e, false
	}
	if err := json.Unmarshal(op[len(confOpPrefix):], &e); err != nil {
		return e, false
	}
	return e, true
}

// membership 当前成员配置：voters 参与多数派与选举，learners 只接收复制。
type membership struct {
	Voters   map[NodeID]bool `json:"voters"`
	Learners map[NodeID]bool `json:"learners"`
}

func newMembership(voters []NodeID) membership {
	m := membership{Voters: make(map[NodeID]bool), Learners: make(map[NodeID]bool)}
	for _, id := range voters {
		m.Voters[id] = true
	}
	return m
}

func membershipFromLists(voters, learners []NodeID) membership {
	m := newMembership(voters)
	for _, id := range learners {
		if !m.Voters[id] {
			m.Learners[id] = true
		}
	}
	return m
}

func (m membership) clone() membership {
	out := membership{Voters: make(map[NodeID]bool, len(m.Voters)), Learners: make(map[NodeID]bool, len(m.Learners))}
	for id := range m.Voters {
		out.Voters[id] = true
	}
	for id := range m.Learners {
		out.Learners[id] = true
	}
	return out
}

// apply 由一次变更请求算出变更后的完整配置（幂等：重复/无效变更返回原配置）。
func (m membership) apply(cc ConfChange) membership {
	out := m.clone()
	switch cc.Type {
	case ConfAddLearner:
		if !out.Voters[cc.NodeID] && !out.Learners[cc.NodeID] {
			out.Learners[cc.NodeID] = true
		}
	case ConfPromote:
		if out.Learners[cc.NodeID] {
			delete(out.Learners, cc.NodeID)
			out.Voters[cc.NodeID] = true
		}
	case ConfRemove:
		delete(out.Voters, cc.NodeID)
		delete(out.Learners, cc.NodeID)
	}
	return out
}

// contains 判断节点是否为成员（voter 或 learner）。
func (m membership) contains(id NodeID) bool {
	return m.Voters[id] || m.Learners[id]
}

// replicationTargets 返回需要复制日志的全部节点（voters + learners）。
func (m membership) replicationTargets() []NodeID {
	out := make([]NodeID, 0, len(m.Voters)+len(m.Learners))
	for id := range m.Voters {
		out = append(out, id)
	}
	for id := range m.Learners {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (m membership) voterList() []NodeID {
	out := make([]NodeID, 0, len(m.Voters))
	for id := range m.Voters {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (m membership) learnerList() []NodeID {
	out := make([]NodeID, 0, len(m.Learners))
	for id := range m.Learners {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (m membership) String() string {
	return "voters=" + jsonList(m.voterList()) + " learners=" + jsonList(m.learnerList())
}

func jsonList(ids []NodeID) string {
	b, _ := json.Marshal(ids)
	return string(b)
}

// ---------------- 成员变更公共 API ----------------

// Membership 返回当前生效的成员配置（voters 参与多数派，learners 只跟随复制）。
func (n *Node) Membership() (voters, learners []NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.mem.voterList(), n.mem.learnerList()
}

// AddNode 在线扩容：先把新节点以 learner 身份加入（不计入多数派），
// 等它追平已提交日志（数据量大时自动走快照）后提升为 voter。
// 阻塞直到提升提交或 ctx 取消。重复调用幂等：已是 voter 直接返回成功。
//
// 注意：调用方需先以空 Peers 启动新节点进程（未配置节点不发起选举、
// 不投票，静静等待 leader 联系）。若新节点未启动，本调用会在等待
// 追平阶段阻塞直至 ctx 超时；此时它仍是 learner，可 RemoveNode 清理
// 或待其上线后重试 AddNode。
func (n *Node) AddNode(ctx context.Context, id NodeID) error {
	n.mu.Lock()
	if n.mem.Voters[id] {
		n.mu.Unlock()
		return nil // 已是正式成员，幂等成功
	}
	isLearner := n.mem.Learners[id]
	n.mu.Unlock()

	// 阶段一：以 learner 身份加入。
	var catchUpTarget uint64
	if !isLearner {
		idx, err := n.proposeConfChange(ctx, ConfChange{Type: ConfAddLearner, NodeID: id})
		if err != nil {
			return err
		}
		catchUpTarget = idx
	} else {
		n.mu.Lock()
		catchUpTarget = n.commitIndex
		n.mu.Unlock()
	}

	// 阶段二：等待新节点追平（matchIndex 覆盖加入位点）。
	if err := n.waitCaughtUp(ctx, id, catchUpTarget); err != nil {
		return err
	}

	// 阶段三：提升为 voter，此后参与多数派与选举。
	_, err := n.proposeConfChange(ctx, ConfChange{Type: ConfPromote, NodeID: id})
	return err
}

// RemoveNode 在线缩容：把节点移出集群。变更提交后该节点不再计入多数派，
// 集群其余节点拒绝它的拉票；被移除的 leader 在提交点自动卸任。
// 幂等：目标本就不是成员时直接返回成功。
func (n *Node) RemoveNode(ctx context.Context, id NodeID) error {
	n.mu.Lock()
	member := n.mem.contains(id)
	n.mu.Unlock()
	if !member {
		return nil
	}
	_, err := n.proposeConfChange(ctx, ConfChange{Type: ConfRemove, NodeID: id})
	return err
}

// proposeConfChange 以日志条目形式提交一次成员变更并等待提交。
// 同一时刻只允许一个未提交的成员变更（单节点变更安全规则）。
func (n *Node) proposeConfChange(ctx context.Context, cc ConfChange) (uint64, error) {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return 0, &RejectError{Reason: ReasonShutdown, Detail: "node stopped"}
	}
	if n.role != RoleLeader {
		hint := n.leaderID
		n.mu.Unlock()
		return 0, &RejectError{Reason: ReasonNotLeader, Leader: hint, Detail: "not leader"}
	}
	if idx, ok := n.uncommittedConfLocked(); ok {
		n.mu.Unlock()
		return 0, &RejectError{Reason: ReasonConfInProgress, Detail: "uncommitted membership change at index " + itoa(idx)}
	}
	// 变更后的完整配置随条目复制，跟随者直接采用。
	next := n.mem.apply(cc)
	entry := LogEntry{
		Term:  n.currentTerm,
		Index: 0, // 占位，下面赋值
		Op: encodeConfOp(confEntry{
			Type:     cc.Type,
			NodeID:   cc.NodeID,
			Voters:   next.voterList(),
			Learners: next.learnerList(),
		}),
	}
	lastIdx, _ := n.lastIndexTermLocked()
	entry.Index = lastIdx + 1
	n.log = append(n.log, entry)
	_ = n.store.Append([]LogEntry{entry})
	n.matchIndex[n.cfg.ID] = entry.Index
	ch := make(chan proposalResult, 1)
	n.pending[entry.Index] = ch
	// 追加即生效（单节点变更语义），随后向新配置复制。
	n.applyConfEntryLocked(entry)
	n.logf("membership change proposed index=%d change=%s node=%s", entry.Index, cc.Type, cc.NodeID)
	n.broadcastAppendLocked()
	n.mu.Unlock()

	select {
	case r := <-ch:
		if r.err != nil {
			return 0, r.err
		}
		return entry.Index, nil
	case <-ctx.Done():
		return 0, &RejectError{Reason: ReasonTimeout, Detail: "membership change not committed: " + ctx.Err().Error()}
	}
}

// uncommittedConfLocked 返回日志中第一个未提交的成员变更条目索引。
func (n *Node) uncommittedConfLocked() (uint64, bool) {
	base := n.log[0].Index
	lastIdx, _ := n.lastIndexTermLocked()
	for i := n.commitIndex + 1; i <= lastIdx; i++ {
		if _, ok := decodeConfOp(n.log[i-base].Op); ok {
			return i, true
		}
	}
	return 0, false
}

// waitCaughtUp 等待 follower 的复制进度覆盖 target 索引。
func (n *Node) waitCaughtUp(ctx context.Context, id NodeID, target uint64) error {
	tick := time.NewTicker(n.cfg.HeartbeatInterval)
	defer tick.Stop()
	for {
		n.mu.Lock()
		if n.stopped {
			n.mu.Unlock()
			return &RejectError{Reason: ReasonShutdown, Detail: "node stopped"}
		}
		if n.role != RoleLeader {
			hint := n.leaderID
			n.mu.Unlock()
			return &RejectError{Reason: ReasonNotLeader, Leader: hint, Detail: "leadership lost while waiting catch-up"}
		}
		if n.matchIndex[id] >= target {
			n.logf("node %s caught up to %d, promoting", id, target)
			n.mu.Unlock()
			return nil
		}
		n.mu.Unlock()
		select {
		case <-ctx.Done():
			return &RejectError{Reason: ReasonTimeout, Detail: "node " + string(id) + " not caught up: " + ctx.Err().Error()}
		case <-tick.C:
		}
	}
}

// itoa 避免为小工具引入 strconv 依赖之外的格式转换。
func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
