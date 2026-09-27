package coordinator

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

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
// 每次写入都通过 "临时文件 + rename" 保证原子性。
type fileStorage struct {
	mu      sync.Mutex
	dir     string
	mem     bool // 纯内存模式
	stable  StableState
	log     []LogEntry
	snap    SnapshotMeta
	snapD   []byte
	hasSnp  bool
	applied uint64
}

const (
	stableFile   = "stable.json"
	logFile      = "log.json"
	snapshotFile = "snapshot.json"
	appliedFile  = "applied.json"
)

// NewFileStorage 创建基于 dir 的持久化存储；dir 为空时使用纯内存存储。
func NewFileStorage(dir string) (Storage, error) {
	fs := &fileStorage{dir: dir, mem: dir == ""}
	if fs.mem {
		return fs, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("coordinator: create data dir: %w", err)
	}
	if err := fs.load(); err != nil {
		return nil, err
	}
	return fs, nil
}

func (s *fileStorage) load() error {
	var stable StableState
	if err := readJSON(filepath.Join(s.dir, stableFile), &stable); err != nil {
		return err
	}
	s.stable = stable

	var entries []LogEntry
	if err := readJSON(filepath.Join(s.dir, logFile), &entries); err != nil {
		return err
	}
	s.log = entries

	var snap struct {
		Meta SnapshotMeta `json:"meta"`
		Data []byte       `json:"data"`
	}
	exists, err := readJSONIfExists(filepath.Join(s.dir, snapshotFile), &snap)
	if err != nil {
		return err
	}
	if exists {
		s.snap, s.snapD, s.hasSnp = snap.Meta, snap.Data, true
	}

	var applied uint64
	if err := readJSON(filepath.Join(s.dir, appliedFile), &applied); err != nil {
		return err
	}
	s.applied = applied
	return nil
}

// readJSON 读取 JSON 文件；文件不存在视为零值。
func readJSON(path string, v any) error {
	_, err := readJSONIfExists(path, v)
	return err
}

func readJSONIfExists(path string, v any) (bool, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("coordinator: read %s: %w", path, err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		return false, fmt.Errorf("coordinator: decode %s: %w", path, err)
	}
	return true, nil
}

// writeJSONAtomic 先写临时文件再 rename，保证崩溃时不会留下半截文件。
func writeJSONAtomic(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return fmt.Errorf("coordinator: write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("coordinator: rename %s: %w", path, err)
	}
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
	if len(entries) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = append(s.log, entries...)
	return s.persistLogLocked()
}

func (s *fileStorage) TruncateLog(from uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	keep := s.log[:0]
	for _, e := range s.log {
		if e.Index <= from {
			keep = append(keep, e)
		}
	}
	s.log = keep
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

func (s *fileStorage) persistStableLocked() error {
	if s.mem {
		return nil
	}
	return writeJSONAtomic(filepath.Join(s.dir, stableFile), s.stable)
}

func (s *fileStorage) persistLogLocked() error {
	if s.mem {
		return nil
	}
	return writeJSONAtomic(filepath.Join(s.dir, logFile), s.log)
}

func (s *fileStorage) persistSnapshotLocked() error {
	if s.mem {
		return nil
	}
	return writeJSONAtomic(filepath.Join(s.dir, snapshotFile), struct {
		Meta SnapshotMeta `json:"meta"`
		Data []byte       `json:"data"`
	}{Meta: s.snap, Data: s.snapD})
}

func (s *fileStorage) persistAppliedLocked() error {
	if s.mem {
		return nil
	}
	return writeJSONAtomic(filepath.Join(s.dir, appliedFile), s.applied)
}
