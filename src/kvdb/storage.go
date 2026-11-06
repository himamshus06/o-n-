package kvdb

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var ErrNotFound = errors.New("key not found")
var ErrClosed = errors.New("database is closed")
var ErrMaxBytes = errors.New("database size limit exceeded")

type OperationType uint8

const (
	Put OperationType = iota + 1
	Delete
)

type Operation struct {
	Type       OperationType
	Key, Value []byte
}
type RecordLocation struct {
	Offset int64
	Length uint32
}

type diskRecord struct {
	TxID       uint64
	Type       OperationType
	Key, Value []byte
	Commit     bool
}

type storage struct {
	mu       sync.Mutex
	f        *os.File
	maxBytes int64
	nextTx   uint64
	path     string
	lastCompaction time.Time
	bytesReclaimed int64
}

func openStorage(path string, maxBytes int64) (*storage, map[string]diskRecord, error) {
	backup := path + ".compact-old"
	if _, statErr := os.Stat(path); errors.Is(statErr, os.ErrNotExist) {
		if _, backupErr := os.Stat(backup); backupErr == nil {
			if err := os.Rename(backup, path); err != nil { return nil, nil, err }
		}
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, nil, err
	}
	s := &storage{f: f, maxBytes: maxBytes, path: path}
	// A backup is retained until the replacement has been installed and the
	// directory entry synchronized. If the base is missing after a crash, it
	// is the only known-good copy.
	_ = os.Remove(path + ".compact-tmp")
	committed := map[string]diskRecord{}
	if err := s.recover(committed); err != nil {
		f.Close()
		return nil, nil, err
	}
	_ = os.Remove(backup)
	_ = syncDir(filepath.Dir(path))
	return s, committed, nil
}

func (s *storage) recover(index map[string]diskRecord) error {
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	pending := map[uint64][]diskRecord{}
	var pos int64
	r := bufio.NewReader(s.f)
	for {
		var n uint32
		if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if n == 0 || n > 64<<20 {
			return fmt.Errorf("invalid record length %d at %d", n, pos)
		}
		b := make([]byte, n)
		if _, err := io.ReadFull(r, b); err != nil {
			return err
		}
		pos += int64(4 + n)
		var rec diskRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			return err
		}
		if rec.TxID > s.nextTx {
			s.nextTx = rec.TxID
		}
		if rec.Commit {
			for _, op := range pending[rec.TxID] {
				index[string(op.Key)] = op
			}
			delete(pending, rec.TxID)
		} else {
			pending[rec.TxID] = append(pending[rec.TxID], rec)
		}
	}
	_, err := s.f.Seek(0, io.SeekEnd)
	return err
}

func (s *storage) appendLocked(rec diskRecord) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	n := uint32(len(b))
	if s.maxBytes > 0 {
		st, e := s.f.Stat()
		if e != nil { return e }
		if st.Size()+int64(4+n) > s.maxBytes {
			return ErrMaxBytes
		}
	}
	frame := make([]byte, 4+len(b)); binary.LittleEndian.PutUint32(frame, n); copy(frame[4:], b)
	_, err = writeFull(s.f, frame)
	return err
}

func writeFull(w io.Writer, b []byte) (int, error) {
	total := 0
	for total < len(b) { n, err := w.Write(b[total:]); total += n; if err != nil { return total, err }; if n == 0 { return total, io.ErrShortWrite } }
	return total, nil
}

func (s *storage) commit(ops []Operation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	start, err := s.f.Seek(0, io.SeekEnd); if err != nil { return err }
	s.nextTx++
	tx := s.nextTx
	for _, op := range ops {
		if err := s.appendLocked(diskRecord{TxID: tx, Type: op.Type, Key: append([]byte(nil), op.Key...), Value: append([]byte(nil), op.Value...)}); err != nil {
			return s.rollbackLocked(start, err)
		}
	}
	if err := s.appendLocked(diskRecord{TxID: tx, Commit: true}); err != nil {
		return s.rollbackLocked(start, err)
	}
	if err := s.f.Sync(); err != nil { return s.rollbackLocked(start, err) }
	return nil
}

func (s *storage) rollbackLocked(start int64, cause error) error {
	if err := s.f.Truncate(start); err != nil { return fmt.Errorf("%w (rollback failed: %v)", cause, err) }
	if _, err := s.f.Seek(0, io.SeekEnd); err != nil { return fmt.Errorf("%w (seek after rollback: %v)", cause, err) }
	if err := s.f.Sync(); err != nil { return fmt.Errorf("%w (sync after rollback: %v)", cause, err) }
	return cause
}

// compactLocked installs a complete replacement log while the caller holds
// DB.mu exclusively. storage.mu excludes other file operations.
func (s *storage) compactLocked(values map[string][]byte) error {
	s.mu.Lock(); defer s.mu.Unlock()
	tmp, backup := s.path+".compact-tmp", s.path+".compact-old"
	_ = os.Remove(tmp)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600); if err != nil { return err }
	ok := false
	defer func(){ f.Close(); if !ok { _ = os.Remove(tmp) } }()
	var size int64; var tx uint64 = 1
	for k, v := range values {
		for _, rec := range []diskRecord{{TxID: tx, Type: Put, Key: []byte(k), Value: v}, {TxID: tx, Commit: true}} {
			b, e := json.Marshal(rec); if e != nil { return e }; frame := make([]byte, 4+len(b)); binary.LittleEndian.PutUint32(frame, uint32(len(b))); copy(frame[4:], b)
			if s.maxBytes > 0 && size+int64(len(frame)) > s.maxBytes { return ErrMaxBytes }
			if _, e = writeFull(f, frame); e != nil { return e }; size += int64(len(frame))
		}
		tx++
	}
	if err = f.Sync(); err != nil { return err }
	if _, _, err = openStorageCheck(f); err != nil { return fmt.Errorf("validate compacted file: %w", err) }
	if err = f.Close(); err != nil { return err }
	// The backup protocol works on Windows (Rename cannot replace an existing
	// file) and POSIX. Open recovers the backup if a crash leaves no base file.
	_ = os.Remove(backup)
	if err = os.Rename(s.path, backup); err != nil { return err }
	if err = os.Rename(tmp, s.path); err != nil { _ = os.Rename(backup, s.path); return err }
	newFile, err := os.OpenFile(s.path, os.O_RDWR, 0600); if err != nil { return err }
	old := s.f; s.f = newFile
	oldInfo, _ := old.Stat(); oldSize := int64(0); if oldInfo != nil { oldSize = oldInfo.Size() }
	s.nextTx = tx-1
	s.lastCompaction = time.Now(); s.bytesReclaimed = oldSize-size
	if err = syncDir(filepath.Dir(s.path)); err != nil { _ = old.Close(); return err }
	if err = old.Close(); err != nil { return err }
	_ = os.Remove(backup)
	_ = syncDir(filepath.Dir(s.path))
	ok = true
	return nil
}

func syncDir(dir string) error {
	if os.PathSeparator == '\\' { return nil }
	f, err := os.Open(dir); if err != nil { return err }; defer f.Close()
	if err = f.Sync(); err != nil { return err }; return nil
}

func (s *storage) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Sync()
	e := s.f.Close()
	s.f = nil
	if err != nil {
		return err
	}
	return e
}
func (s *storage) check() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.f.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	_, _, err = openStorageCheck(s.f)
	return err
}
func openStorageCheck(f *os.File) (int64, int64, error) {
	var pos, records int64
	r := bufio.NewReader(f)
	for {
		var n uint32
		err := binary.Read(r, binary.LittleEndian, &n)
		if err == io.EOF {
			break
		}
		if err != nil {
			return pos, records, err
		}
		if n == 0 || n > 64<<20 {
			return pos, records, fmt.Errorf("invalid record length at %d", pos)
		}
		b := make([]byte, n)
		if _, err = io.ReadFull(r, b); err != nil {
			return pos, records, err
		}
		var rec diskRecord
		if err = json.Unmarshal(b, &rec); err != nil {
			return pos, records, err
		}
		pos += int64(n) + 4
		records++
	}
	_, err := f.Seek(0, io.SeekEnd)
	return pos, records, err
}
