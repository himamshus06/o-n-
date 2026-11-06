package kvdb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"os"
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
