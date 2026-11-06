package kvdb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type memoryFile struct {
	data        []byte
	pos         int64
	maxWrite    int
	writeErr    error
	failAfter   int64
	truncateErr error
	syncErrAt   int
	syncCalls   int
	closed      bool
}

func (f *memoryFile) Read(p []byte) (int, error) {
	if f.pos >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[f.pos:])
	f.pos += int64(n)
	return n, nil
}

func (f *memoryFile) Write(p []byte) (int, error) {
	if f.writeErr != nil {
		if f.pos >= f.failAfter {
			return 0, f.writeErr
		}
		toFailure := int(f.failAfter - f.pos)
		if toFailure < len(p) {
			f.writeAt(p[:toFailure])
			return toFailure, f.writeErr
		}
	}
	n := len(p)
	if f.maxWrite > 0 && n > f.maxWrite {
		n = f.maxWrite
	}
	f.writeAt(p[:n])
	return n, nil
}

func (f *memoryFile) writeAt(p []byte) {
	end := f.pos + int64(len(p))
	if end > int64(len(f.data)) {
		f.data = append(f.data, make([]byte, int(end)-len(f.data))...)
	}
	copy(f.data[f.pos:end], p)
	f.pos = end
}

func (f *memoryFile) Seek(offset int64, whence int) (int64, error) {
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = f.pos + offset
	case io.SeekEnd:
		next = int64(len(f.data)) + offset
	default:
		return f.pos, errors.New("invalid whence")
	}
	if next < 0 {
		return f.pos, errors.New("negative position")
	}
	f.pos = next
	return next, nil
}

func (f *memoryFile) Stat() (os.FileInfo, error) { return memoryInfo{size: int64(len(f.data))}, nil }
func (f *memoryFile) Truncate(size int64) error {
	if f.truncateErr != nil {
		return f.truncateErr
	}
	if size < int64(len(f.data)) {
		f.data = f.data[:size]
	}
	if f.pos > size {
		f.pos = size
	}
	return nil
}
func (f *memoryFile) Sync() error {
	f.syncCalls++
	if f.syncErrAt == f.syncCalls {
		return errors.New("injected sync failure")
	}
	return nil
}
func (f *memoryFile) Close() error { f.closed = true; return nil }

type memoryInfo struct{ size int64 }

func (i memoryInfo) Name() string       { return "memory" }
func (i memoryInfo) Size() int64        { return i.size }
func (i memoryInfo) Mode() os.FileMode  { return 0600 }
func (i memoryInfo) ModTime() time.Time { return time.Time{} }
func (i memoryInfo) IsDir() bool        { return false }
func (i memoryInfo) Sys() any           { return nil }

func legacyFrame(t *testing.T, rec diskRecord) []byte {
	t.Helper()
	rec.Checksum = nil
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	frame := make([]byte, 4+len(b))
	binary.LittleEndian.PutUint32(frame, uint32(len(b)))
	copy(frame[4:], b)
	return frame
}

func newMemoryStorage(f *memoryFile, maxBytes int64) *storage {
	return &storage{f: f, maxBytes: maxBytes}
}

func TestLegacyAndChecksummedRecordsReplayTogether(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.kv")
	legacy := append(legacyFrame(t, diskRecord{TxID: 1, Type: Put, Key: []byte("old"), Value: []byte("legacy")}), legacyFrame(t, diskRecord{TxID: 1, Commit: true})...)
	if err := os.WriteFile(path, legacy, 0600); err != nil {
		t.Fatal(err)
	}
	s, recovered, err := openStorage(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(recovered["old"].Value) != "legacy" {
		t.Fatalf("legacy value not replayed: %#v", recovered)
	}
	if err := s.commit([]Operation{{Type: Put, Key: []byte("new"), Value: []byte("checked")}}); err != nil {
		t.Fatal(err)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	s, recovered, err = openStorage(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if string(recovered["old"].Value) != "legacy" || string(recovered["new"].Value) != "checked" {
		t.Fatalf("mixed records not replayed: %#v", recovered)
	}
}

func TestRecoveryRejectsChecksumAndStructuralCorruption(t *testing.T) {
	t.Run("checksum", func(t *testing.T) {
		frame, err := marshalRecord(diskRecord{TxID: 1, Type: Put, Key: []byte("key"), Value: []byte("value")})
		if err != nil {
			t.Fatal(err)
		}
		frame[len(frame)-3] ^= 1
		path := filepath.Join(t.TempDir(), "bad.kv")
		if err := os.WriteFile(path, frame, 0600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := openStorage(path, 0); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
			t.Fatalf("expected checksum error, got %v", err)
		}
		got, _ := os.ReadFile(path)
		if !bytes.Equal(got, frame) {
			t.Fatal("complete corrupt record was truncated")
		}
	})

	t.Run("structure", func(t *testing.T) {
		frame := legacyFrame(t, diskRecord{TxID: 1, Type: 99, Key: []byte("key")})
		path := filepath.Join(t.TempDir(), "bad.kv")
		if err := os.WriteFile(path, frame, 0600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := openStorage(path, 0); err == nil || !strings.Contains(err.Error(), "unknown operation type") {
			t.Fatalf("expected structural validation error, got %v", err)
		}
		got, _ := os.ReadFile(path)
		if !bytes.Equal(got, frame) {
			t.Fatal("complete invalid record was truncated")
		}
	})
}

func TestRecordValidationRejectsMalformedFrames(t *testing.T) {
	for name, frame := range map[string][]byte{
		"zero length":       {0, 0, 0, 0},
		"oversized length":  {1, 0, 0, 4}, // 0x04000001, over 64 MiB
		"malformed JSON":    {1, 0, 0, 0, '{'},
		"invalid operation": legacyFrame(t, diskRecord{TxID: 1, Type: 99}),
		"invalid commit":    legacyFrame(t, diskRecord{TxID: 1, Type: Put, Commit: true}),
		"zero transaction":  legacyFrame(t, diskRecord{Type: Put, Key: []byte("k")}),
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "bad.kv")
			if err := os.WriteFile(path, frame, 0600); err != nil {
				t.Fatal(err)
			}
			if _, _, err := openStorage(path, 0); err == nil {
				t.Fatal("expected invalid frame error")
			}
			got, _ := os.ReadFile(path)
			if !bytes.Equal(got, frame) {
				t.Fatal("invalid complete frame was truncated")
			}
		})
	}
}

func TestCRC32CUsesCastagnoliPolynomial(t *testing.T) {
	if got := crc32.Checksum([]byte("123456789"), castagnoliTable); got != 0xe3069283 {
		t.Fatalf("CRC32C = %#08x, want 0xe3069283", got)
	}
}

func TestRecoveryRepairsOnlyIncompleteTails(t *testing.T) {
	valid := append(legacyFrame(t, diskRecord{TxID: 1, Type: Put, Key: []byte("k"), Value: []byte("v")}), legacyFrame(t, diskRecord{TxID: 1, Commit: true})...)
	for name, tail := range map[string][]byte{"partial prefix": {1, 2}, "partial body": {8, 0, 0, 0, '{', '"'}} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "tail.kv")
			original := append(append([]byte(nil), valid...), tail...)
			if err := os.WriteFile(path, original, 0600); err != nil {
				t.Fatal(err)
			}
			s, index, err := openStorage(path, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer s.close()
			if string(index["k"].Value) != "v" {
				t.Fatalf("committed prefix lost: %#v", index)
			}
			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, valid) {
				t.Fatalf("tail not truncated: got %d bytes, want %d", len(got), len(valid))
			}
		})
	}
}

func TestTailRepairSyncFailurePreventsStorageUse(t *testing.T) {
	f := &memoryFile{data: []byte{1, 2}, syncErrAt: 1}
	s := newMemoryStorage(f, 0)
	if err := s.recover(map[string]diskRecord{}); err == nil || !s.poisoned {
		t.Fatalf("expected failed repair sync and poisoned storage, err=%v poisoned=%v", err, s.poisoned)
	}
	if err := s.commit(nil); err == nil {
		t.Fatal("storage accepted a write after repair sync failure")
	}
}

func TestUncommittedTransactionsAreIgnoredAndCommittedOrderReplayed(t *testing.T) {
	data := append(legacyFrame(t, diskRecord{TxID: 1, Type: Put, Key: []byte("k"), Value: []byte("old")}), legacyFrame(t, diskRecord{TxID: 1, Commit: true})...)
	data = append(data, legacyFrame(t, diskRecord{TxID: 2, Type: Put, Key: []byte("k"), Value: []byte("uncommitted")})...)
	data = append(data, legacyFrame(t, diskRecord{TxID: 3, Type: Put, Key: []byte("k"), Value: []byte("first")})...)
	data = append(data, legacyFrame(t, diskRecord{TxID: 3, Type: Put, Key: []byte("k"), Value: []byte("last")})...)
	data = append(data, legacyFrame(t, diskRecord{TxID: 3, Commit: true})...)
	f := &memoryFile{data: data}
	s := newMemoryStorage(f, 0)
	index := map[string]diskRecord{}
	if err := s.recover(index); err != nil {
		t.Fatal(err)
	}
	if string(index["k"].Value) != "last" || s.nextTx != 3 {
		t.Fatalf("unexpected replay: value=%q nextTx=%d", index["k"].Value, s.nextTx)
	}
}

func TestCommitHandlesShortWritesAndRollsBackFailures(t *testing.T) {
	t.Run("short writes", func(t *testing.T) {
		f := &memoryFile{maxWrite: 3}
		s := newMemoryStorage(f, 0)
		if err := s.commit([]Operation{{Type: Put, Key: []byte("k"), Value: []byte("v")}}); err != nil {
			t.Fatal(err)
		}
		if _, index, err := openStorageFromMemory(f); err != nil || string(index["k"].Value) != "v" {
			t.Fatalf("short write frame not readable: index=%#v err=%v", index, err)
		}
	})

	for name, failAfter := range map[string]int64{"length prefix": 2, "record body": 7} {
		t.Run(name+" failure rolls back", func(t *testing.T) {
			f := &memoryFile{writeErr: errors.New("injected write failure"), failAfter: failAfter}
			s := newMemoryStorage(f, 0)
			if err := s.commit([]Operation{{Type: Put, Key: []byte("k"), Value: []byte("v")}}); err == nil {
				t.Fatal("expected append error")
			}
			if len(f.data) != 0 || s.poisoned {
				t.Fatalf("successful rollback state: bytes=%d poisoned=%v", len(f.data), s.poisoned)
			}
			f.writeErr = nil
			if err := s.commit([]Operation{{Type: Put, Key: []byte("k"), Value: []byte("v")}}); err != nil {
				t.Fatalf("write after successful rollback failed: %v", err)
			}
		})
	}

	t.Run("commit marker failure rolls back", func(t *testing.T) {
		opFrame, err := marshalRecord(diskRecord{TxID: 1, Type: Put, Key: []byte("k"), Value: []byte("v")})
		if err != nil {
			t.Fatal(err)
		}
		f := &memoryFile{writeErr: errors.New("commit marker write failure"), failAfter: int64(len(opFrame) + 2)}
		s := newMemoryStorage(f, 0)
		if err := s.commit([]Operation{{Type: Put, Key: []byte("k"), Value: []byte("v")}}); err == nil {
			t.Fatal("expected commit marker error")
		}
		if len(f.data) != 0 || s.poisoned {
			t.Fatalf("commit marker rollback: bytes=%d poisoned=%v", len(f.data), s.poisoned)
		}
	})
}

func TestCommitPoisonsStorageWhenRollbackOrSyncFails(t *testing.T) {
	t.Run("rollback failure", func(t *testing.T) {
		f := &memoryFile{writeErr: errors.New("write failure"), truncateErr: errors.New("truncate failure")}
		s := newMemoryStorage(f, 0)
		if err := s.commit([]Operation{{Type: Put, Key: []byte("k"), Value: []byte("v")}}); err == nil {
			t.Fatal("expected write/rollback error")
		}
		if !s.poisoned {
			t.Fatal("storage was not poisoned after failed rollback")
		}
		if err := s.commit(nil); err == nil {
			t.Fatal("poisoned storage accepted a write")
		}
	})

	t.Run("rollback sync failure", func(t *testing.T) {
		f := &memoryFile{writeErr: errors.New("write failure"), syncErrAt: 1}
		s := newMemoryStorage(f, 0)
		if err := s.commit([]Operation{{Type: Put, Key: []byte("k"), Value: []byte("v")}}); err == nil {
			t.Fatal("expected write/rollback sync error")
		}
		if !s.poisoned {
			t.Fatal("storage was not poisoned after rollback sync failure")
		}
		if err := s.commit(nil); err == nil {
			t.Fatal("poisoned storage accepted a write")
		}
	})

	t.Run("transaction sync failure", func(t *testing.T) {
		f := &memoryFile{syncErrAt: 1}
		s := newMemoryStorage(f, 0)
		if err := s.commit([]Operation{{Type: Put, Key: []byte("k"), Value: []byte("v")}}); err == nil {
			t.Fatal("expected sync error")
		}
		if len(f.data) != 0 || !s.poisoned {
			t.Fatalf("sync failure handling: bytes=%d poisoned=%v", len(f.data), s.poisoned)
		}
		if err := s.commit(nil); err == nil {
			t.Fatal("storage accepted a write after sync failure")
		}
	})
}

func TestSizeLimitRejectsTransactionWithoutWriting(t *testing.T) {
	f := &memoryFile{}
	s := newMemoryStorage(f, 5)
	if err := s.commit([]Operation{{Type: Put, Key: []byte("k"), Value: []byte("v")}}); err == nil {
		t.Fatal("expected size limit error")
	}
	if len(f.data) != 0 {
		t.Fatalf("size rejection wrote %d bytes", len(f.data))
	}
}

func TestCheckValidatesChecksumsAndDoesNotRepairTail(t *testing.T) {
	frame, err := marshalRecord(diskRecord{TxID: 1, Type: Put, Key: []byte("k"), Value: []byte("v")})
	if err != nil {
		t.Fatal(err)
	}
	f := &memoryFile{data: append(append([]byte(nil), frame...), 1, 2)}
	s := newMemoryStorage(f, 0)
	original := append([]byte(nil), f.data...)
	if err := s.check(); err == nil || !strings.Contains(err.Error(), "incomplete trailing record") {
		t.Fatalf("expected incomplete tail diagnostic, got %v", err)
	}
	if !bytes.Equal(f.data, original) {
		t.Fatal("check modified the file")
	}
	if _, err := f.Seek(0, io.SeekCurrent); err != nil || f.pos != int64(len(f.data)) {
		t.Fatalf("check did not leave file at end: pos=%d err=%v", f.pos, err)
	}

	bad, err := marshalRecord(diskRecord{TxID: 2, Type: Put, Key: []byte("x"), Value: []byte("y")})
	if err != nil {
		t.Fatal(err)
	}
	bad[len(bad)-3] ^= 1
	checksumStorage := newMemoryStorage(&memoryFile{data: bad}, 0)
	if err := checksumStorage.check(); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("check did not detect checksum mismatch: %v", err)
	}
}

func openStorageFromMemory(f *memoryFile) (*storage, map[string]diskRecord, error) {
	s := newMemoryStorage(f, 0)
	index := map[string]diskRecord{}
	err := s.recover(index)
	return s, index, err
}

func TestCompactionCrashBoundariesRecover(t *testing.T) {
	if os.Getenv("KVDB_COMPACTION_CRASH_HELPER") == "1" {
		path := os.Getenv("KVDB_COMPACTION_PATH")
		point := os.Getenv("KVDB_COMPACTION_CRASH_POINT")
		compactionHook = func(name string) {
			if name == point {
				os.Exit(91)
			}
		}
		db, err := Open(path, 0)
		if err != nil {
			os.Exit(92)
		}
		_ = db.Compact()
		os.Exit(93)
	}

	points := []string{"before-temp", "after-temp-sync", "after-replacement", "before-backup-delete"}
	for _, point := range points {
		t.Run(point, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db.kv")
			db, err := Open(path, 0)
			if err != nil {
				t.Fatal(err)
			}
			if err = db.Put([]byte("keep"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			if err = db.Put([]byte("gone"), []byte("x")); err != nil {
				t.Fatal(err)
			}
			if err = db.Delete([]byte("gone")); err != nil {
				t.Fatal(err)
			}
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}

			cmd := exec.Command(os.Args[0], "-test.run=TestCompactionCrashBoundariesRecover")
			cmd.Env = append(os.Environ(), "KVDB_COMPACTION_CRASH_HELPER=1", "KVDB_COMPACTION_PATH="+path, "KVDB_COMPACTION_CRASH_POINT="+point)
			err = cmd.Run()
			if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 91 {
				t.Fatalf("helper did not terminate at crash point: %v", err)
			}

			reopened, err := Open(path, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			v, err := reopened.Get([]byte("keep"))
			if err != nil || string(v) != "value" {
				t.Fatalf("keep lost after %s: %q %v", point, v, err)
			}
			if _, err = reopened.Get([]byte("gone")); !errors.Is(err, ErrNotFound) {
				t.Fatalf("deleted key resurrected after %s: %v", point, err)
			}
			if err = reopened.Put([]byte("new"), []byte("ok")); err != nil {
				t.Fatalf("database not writable after %s: %v", point, err)
			}
		})
	}
}

func TestRecoverySelectsOnlyValidatedCandidates(t *testing.T) {
	makeDBFile := func(t *testing.T, path, key, value string) {
		t.Helper()
		s, _, err := openStorage(path, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err = s.commit([]Operation{{Type: Put, Key: []byte(key), Value: []byte(value)}}); err != nil {
			t.Fatal(err)
		}
		if err = s.close(); err != nil {
			t.Fatal(err)
		}
	}
	states := []string{"original", "tmp-only", "original-plus-backup", "backup-plus-tmp", "new-plus-backup", "new-only", "missing-with-backup", "corrupt-with-backup"}
	for _, state := range states {
		t.Run(state, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "db.kv")
			backup := path + ".compact-old"
			tmp := path + ".compact-tmp"
			makeDBFile(t, path, "keep", "old")
			newPath := filepath.Join(dir, "new.kv")
			makeDBFile(t, newPath, "keep", "new")
			newBytes, err := os.ReadFile(newPath)
			if err != nil {
				t.Fatal(err)
			}
			oldBytes, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			switch state {
			case "tmp-only":
				os.Remove(path)
				os.WriteFile(tmp, newBytes, 0600)
			case "original-plus-backup":
				os.WriteFile(backup, newBytes, 0600)
			case "backup-plus-tmp":
				os.Remove(path)
				os.WriteFile(backup, oldBytes, 0600)
				os.WriteFile(tmp, newBytes, 0600)
			case "new-plus-backup":
				os.WriteFile(path, newBytes, 0600)
				os.WriteFile(backup, oldBytes, 0600)
			case "new-only":
				os.WriteFile(path, newBytes, 0600)
			case "missing-with-backup":
				os.Remove(path)
				os.WriteFile(backup, oldBytes, 0600)
			case "corrupt-with-backup":
				os.WriteFile(path, []byte("corrupt"), 0600)
				os.WriteFile(backup, oldBytes, 0600)
			}
			db, err := Open(path, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			v, err := db.Get([]byte("keep"))
			if err != nil {
				t.Fatal(err)
			}
			want := "old"
			if state == "tmp-only" || state == "new-plus-backup" || state == "new-only" {
				want = "new"
			}
			if string(v) != want {
				t.Fatalf("state %s selected %q, want %q", state, v, want)
			}
			if _, err = os.Stat(backup); err == nil {
				t.Fatalf("backup remained authoritative after %s", state)
			}
		})
	}
}

func TestCheckpointValidationAndLegacyDetection(t *testing.T) {
	base, err := makeCheckpoint(map[string][]byte{"a": []byte("1")}, 7, 1)
	if err != nil {
		t.Fatal(err)
	}
	tests := map[string][]byte{
		"checksum corruption":  append([]byte(nil), base...),
		"truncated checkpoint": base[:len(base)-1],
		"unknown magic":        append([]byte("UNKNOWN!"), base[8:]...),
	}
	tests["checksum corruption"][20] ^= 1
	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db.kv")
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			if _, _, err := openStorage(path, 0); err == nil {
				t.Fatal("corrupt checkpoint was accepted")
			}
		})
	}
	t.Run("short checkpoint header", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "db.kv")
		if err := os.WriteFile(path, []byte("KVCPv003"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := openStorage(path, 0); err == nil {
			t.Fatal("short checkpoint was accepted")
		}
	})
	t.Run("oversized payload", func(t *testing.T) {
		data := make([]byte, checkpointHeaderSize+checkpointTrailerSize)
		copy(data[:8], checkpointMagic)
		binary.LittleEndian.PutUint16(data[8:10], 3)
		binary.LittleEndian.PutUint16(data[10:12], checkpointHeaderSize)
		binary.LittleEndian.PutUint64(data[12:20], 1)
		binary.LittleEndian.PutUint64(data[20:28], 1)
		binary.LittleEndian.PutUint64(data[28:36], 1)
		binary.LittleEndian.PutUint64(data[36:44], maxCheckpointSize+1)
		binary.LittleEndian.PutUint32(data[48:52], crc32.Checksum(data[:48], castagnoliTable))
		path := filepath.Join(t.TempDir(), "db.kv")
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := openStorage(path, 0); err == nil {
			t.Fatal("oversized checkpoint was accepted")
		}
	})
}

func TestCheckpointReplayBoundaryAndMalformedTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.kv")
	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Batch([]Operation{{Type: Put, Key: []byte("old"), Value: []byte("1")}, {Type: Put, Key: []byte("gone"), Value: []byte("x")}}); err != nil {
		t.Fatal(err)
	}
	if err = db.Delete([]byte("gone")); err != nil {
		t.Fatal(err)
	}
	if err = db.Compact(); err != nil {
		t.Fatal(err)
	}
	if err = db.Batch([]Operation{{Type: Put, Key: []byte("new"), Value: []byte("2")}, {Type: Delete, Key: []byte("old")}}); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = reopened.Get([]byte("old")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old key survived: %v", err)
	}
	if _, err = reopened.Get([]byte("gone")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted key resurrected: %v", err)
	}
	if v, e := reopened.Get([]byte("new")); e != nil || string(v) != "2" {
		t.Fatalf("new key missing: %q %v", v, e)
	}
	reopened.Close()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, 4, 0, 0, 0, '{', 'b', 'a', 'd')
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = Open(path, 0); err == nil {
		t.Fatal("complete malformed post-checkpoint record was ignored")
	}
}

func TestCheckpointMillionEntryCount(t *testing.T) {
	values := make(map[string][]byte, 1_000_000)
	for i := 0; i < 1_000_000; i++ {
		values[fmt.Sprintf("k%07d", i)] = []byte("v")
	}
	frame, err := makeCheckpoint(values, 9, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(frame) == 0 {
		t.Fatal("empty checkpoint")
	}
}
