package coordinator

import (
	"path/filepath"
	"testing"
)

func TestFileStorageRoundTrip(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "n1")

	s1, err := NewFileStorage(dir)
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}
	if err := s1.SaveStable(StableState{CurrentTerm: 7, VotedFor: "n2"}); err != nil {
		t.Fatalf("save stable: %v", err)
	}
	entries := []LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 2, Index: 2, Command: []byte("b")},
		{Term: 3, Index: 3, Command: []byte("c")},
	}
	if err := s1.AppendLog(entries); err != nil {
		t.Fatalf("append log: %v", err)
	}
	if err := s1.TruncateLog(2); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if err := s1.SaveSnapshot(SnapshotMeta{LastIncludedIndex: 2, LastIncludedTerm: 2}, []byte("snapdata")); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	if err := s1.SaveApplied(2); err != nil {
		t.Fatalf("save applied: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 重新打开（模拟重启）
	s2, err := NewFileStorage(dir)
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	st, err := s2.LoadStable()
	if err != nil || st.CurrentTerm != 7 || st.VotedFor != "n2" {
		t.Fatalf("stable mismatch: %+v err=%v", st, err)
	}
	logs, err := s2.LoadLog()
	if err != nil {
		t.Fatalf("load log: %v", err)
	}
	if len(logs) != 2 || logs[0].Index != 1 || logs[1].Index != 2 {
		t.Fatalf("log mismatch after truncate: %+v", logs)
	}
	meta, data, ok, err := s2.LoadSnapshot()
	if err != nil || !ok {
		t.Fatalf("load snapshot: ok=%v err=%v", ok, err)
	}
	if meta.LastIncludedIndex != 2 || meta.LastIncludedTerm != 2 || string(data) != "snapdata" {
		t.Fatalf("snapshot mismatch: meta=%+v data=%q", meta, data)
	}
	applied, err := s2.LoadApplied()
	if err != nil || applied != 2 {
		t.Fatalf("applied mismatch: %d err=%v", applied, err)
	}
}

func TestMemStorageDefaults(t *testing.T) {
	s, err := NewFileStorage("")
	if err != nil {
		t.Fatalf("create mem storage: %v", err)
	}
	st, _ := s.LoadStable()
	if st.CurrentTerm != 0 || st.VotedFor != "" {
		t.Fatalf("unexpected stable: %+v", st)
	}
	if _, _, ok, _ := s.LoadSnapshot(); ok {
		t.Fatal("unexpected snapshot in empty storage")
	}
}
