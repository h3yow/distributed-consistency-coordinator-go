package raft

import "sync"

// Storage 持久化接口：任期、投票、日志与快照。重启后从 Storage 恢复。
type Storage interface {
	// SaveHardState 原子保存当前任期与投票记录。
	SaveHardState(term uint64, votedFor NodeID) error
	// LoadHardState 读取任期与投票记录。
	LoadHardState() (term uint64, votedFor NodeID, err error)

	// Append 追加日志条目。
	Append(entries []LogEntry) error
	// TruncateSuffix 截断 fromIndex 及之后的日志。
	TruncateSuffix(fromIndex uint64) error
	// Entries 返回全部日志（含快照哨兵条目）。
	Entries() []LogEntry

	// SaveSnapshot 保存快照并丢弃 index 之前的日志。
	SaveSnapshot(index, term uint64, data []byte) error
	// LoadSnapshot 读取快照。
	LoadSnapshot() (index, term uint64, data []byte, err error)

	// SaveApplyState 持久化状态机应用位点与提案去重状态。
	SaveApplyState(applied uint64, lastSeq map[string]uint64, lastResult map[string]any) error
	// LoadApplyState 读取应用位点与去重状态。
	LoadApplyState() (applied uint64, lastSeq map[string]uint64, lastResult map[string]any, err error)
}

// MemoryStorage 基于内存的 Storage 实现。
// 节点重启时复用同一 MemoryStorage 即可模拟磁盘持久化。
type MemoryStorage struct {
	mu        sync.Mutex
	term      uint64
	votedFor  NodeID
	log       []LogEntry // log[0] 为快照哨兵条目（Index/Term 为快照边界）
	snapIndex uint64
	snapTerm  uint64
	snapData  []byte
	applied   uint64
	lastSeq   map[string]uint64
	lastRes   map[string]any
}

// NewMemoryStorage 创建空存储，日志中放置索引 0 的哨兵条目。
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		log:     []LogEntry{{Index: 0, Term: 0}},
		lastSeq: make(map[string]uint64),
		lastRes: make(map[string]any),
	}
}

func (s *MemoryStorage) SaveHardState(term uint64, votedFor NodeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.term = term
	s.votedFor = votedFor
	return nil
}

func (s *MemoryStorage) LoadHardState() (uint64, NodeID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.term, s.votedFor, nil
}

func (s *MemoryStorage) Append(entries []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = append(s.log, entries...)
	return nil
}

func (s *MemoryStorage) TruncateSuffix(fromIndex uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	base := s.log[0].Index
	if fromIndex <= base {
		return nil // 已落入快照范围，无需截断
	}
	cut := int(fromIndex - base)
	if cut < len(s.log) {
		s.log = s.log[:cut]
	}
	return nil
}

func (s *MemoryStorage) Entries() []LogEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]LogEntry, len(s.log))
	copy(out, s.log)
	return out
}

func (s *MemoryStorage) SaveSnapshot(index, term uint64, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapIndex = index
	s.snapTerm = term
	s.snapData = append([]byte(nil), data...)
	// 丢弃快照覆盖的日志前缀，保留哨兵条目。
	base := s.log[0].Index
	if index > base {
		cut := int(index - base) // 快照点条目所在位置，保留其后的条目
		if cut+1 >= len(s.log) {
			s.log = []LogEntry{{Index: index, Term: term}}
		} else {
			rest := make([]LogEntry, len(s.log)-cut-1)
			copy(rest, s.log[cut+1:])
			s.log = append([]LogEntry{{Index: index, Term: term}}, rest...)
		}
	}
	return nil
}

func (s *MemoryStorage) LoadSnapshot() (uint64, uint64, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapIndex, s.snapTerm, append([]byte(nil), s.snapData...), nil
}

func (s *MemoryStorage) SaveApplyState(applied uint64, lastSeq map[string]uint64, lastResult map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = applied
	s.lastSeq = make(map[string]uint64, len(lastSeq))
	for k, v := range lastSeq {
		s.lastSeq[k] = v
	}
	s.lastRes = make(map[string]any, len(lastResult))
	for k, v := range lastResult {
		s.lastRes[k] = v
	}
	return nil
}

func (s *MemoryStorage) LoadApplyState() (uint64, map[string]uint64, map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seq := make(map[string]uint64, len(s.lastSeq))
	for k, v := range s.lastSeq {
		seq[k] = v
	}
	res := make(map[string]any, len(s.lastRes))
	for k, v := range s.lastRes {
		res[k] = v
	}
	return s.applied, seq, res, nil
}
