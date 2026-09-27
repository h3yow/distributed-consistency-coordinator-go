package coordinator

import "sync"

// StableState 是需要在选举相关路径上持久化的状态。
type StableState struct {
	CurrentTerm uint64 `json:"currentTerm"`
	VotedFor    string `json:"votedFor"`
}

// Storage 抽象节点持久化。实现必须保证写后读一致，
// 以便节点重启后能恢复任期、投票与日志，维持"不双主"不变量。
type Storage interface {
	// SaveStable 原子地持久化任期与投票。
	SaveStable(s StableState) error
	// LoadStable 读取任期与投票；无存档时返回零值。
	LoadStable() (StableState, error)

	// AppendLog 追加日志条目（条目下标必须连续递增）。
	AppendLog(entries []LogEntry) error
	// TruncateLog 删除下标大于 from 的所有条目（用于覆盖冲突日志）。
	TruncateLog(from uint64) error
	// LoadLog 读取全部日志条目。
	LoadLog() ([]LogEntry, error)

	// SaveSnapshot 原子地保存快照（元数据 + 状态机数据）。
	SaveSnapshot(meta SnapshotMeta, data []byte) error
	// LoadSnapshot 读取快照；无快照时 ok=false。
	LoadSnapshot() (meta SnapshotMeta, data []byte, ok bool, err error)

	// SaveApplied 持久化已应用到状态机的最大下标，
	// 保证重启后不会重复应用同一批条目。
	SaveApplied(index uint64) error
	// LoadApplied 读取已应用下标。
	LoadApplied() (uint64, error)

	Close() error
}

// fileStorage 是基于目录文件的 Storage 实现。
// 目录不存在时自动创建；DataDir 为空时退化为纯内存实现。
type fileStorage struct {
	mu     sync.Mutex
	dir    string
	mem    bool // 纯内存模式
	stable StableState
	log    []LogEntry
	snap   SnapshotMeta
	snapD  []byte
	hasSnp bool
	applied uint64
}

// NewFileStorage 创建基于 dir 的持久化存储；dir 为空时使用纯内存存储。
func NewFileStorage(dir string) (Storage, error) {
	fs := &fileStorage{dir: dir, mem: dir == ""}
	if fs.mem {
		return fs, nil
	}
	if err := fs.load(); err != nil {
		return nil, err
	}
	return fs, nil
}

func (s *fileStorage) load() error {
	// TODO: 从目录加载 stable/log/snapshot/applied
	return nil
}

func (s *fileStorage) SaveStable(st StableState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stable = st
	return s.persistStableLocked()
}

func (s *fileStorage) LoadStable() (StableState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stable, nil
}

func (s *fileStorage) AppendLog(entries []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = append(s.log, entries...)
	return s.persistLogLocked()
}

func (s *fileStorage) TruncateLog(from uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// TODO: 截断并持久化
	return s.persistLogLocked()
}

func (s *fileStorage) LoadLog() ([]LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]LogEntry, len(s.log))
	copy(out, s.log)
	return out, nil
}

func (s *fileStorage) SaveSnapshot(meta SnapshotMeta, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snap, s.snapD, s.hasSnp = meta, append([]byte(nil), data...), true
	return s.persistSnapshotLocked()
}

func (s *fileStorage) LoadSnapshot() (SnapshotMeta, []byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasSnp {
		return SnapshotMeta{}, nil, false, nil
	}
	return s.snap, append([]byte(nil), s.snapD...), true, nil
}

func (s *fileStorage) SaveApplied(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = index
	return s.persistAppliedLocked()
}

func (s *fileStorage) LoadApplied() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applied, nil
}

func (s *fileStorage) Close() error { return nil }

func (s *fileStorage) persistStableLocked() error   { return nil } // TODO
func (s *fileStorage) persistLogLocked() error      { return nil } // TODO
func (s *fileStorage) persistSnapshotLocked() error { return nil } // TODO
func (s *fileStorage) persistAppliedLocked() error  { return nil } // TODO
