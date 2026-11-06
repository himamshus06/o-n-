package kvdb

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var ErrNotFound = errors.New("key not found")
var ErrClosed = errors.New("database is closed")
var ErrMaxBytes = errors.New("database size limit exceeded")

const maxRecordSize = 64 << 20

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

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
	Checksum   *uint32 `json:",omitempty"`
}

type storageFile interface {
	io.Reader
	io.Writer
	io.Seeker
	Stat() (os.FileInfo, error)
	Truncate(size int64) error
	Sync() error
	Close() error
}

type storage struct {
	mu       sync.Mutex
	f        storageFile
	maxBytes int64
	nextTx   uint64
	poisoned bool
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
		_ = f.Close()
		return nil, nil, err
	}
	_ = os.Remove(backup)
	_ = syncDir(filepath.Dir(path))
	return s, committed, nil
}

func (s *storage) recover(index map[string]diskRecord) error {
	return s.scan(index, true)
}

// scan validates and replays the file. Repair is enabled only for a final
// frame whose length prefix or body is physically incomplete.
func (s *storage) scan(index map[string]diskRecord, repair bool) error {
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	st, err := s.f.Stat()
	if err != nil {
		return err
	}
	size := st.Size()
	pending := map[uint64][]diskRecord{}
	var pos int64
	for pos < size {
		start := pos
		remaining := size - pos
		if remaining < 4 {
			return s.incompleteTail(start, repair, io.ErrUnexpectedEOF)
		}
		var prefix [4]byte
		if _, err := io.ReadFull(s.f, prefix[:]); err != nil {
			return fmt.Errorf("read record length at %d: %w", pos, err)
		}
		n := binary.LittleEndian.Uint32(prefix[:])
		pos += 4
		if n == 0 || n > maxRecordSize {
			return fmt.Errorf("invalid record length %d at %d", n, start)
		}
		if int64(n) > size-pos {
			return s.incompleteTail(start, repair, io.ErrUnexpectedEOF)
		}
		b := make([]byte, n)
		if _, err := io.ReadFull(s.f, b); err != nil {
			return fmt.Errorf("read record body at %d: %w", start, err)
		}
		pos += int64(n)
		var rec diskRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			return fmt.Errorf("decode record at %d: %w", start, err)
		}
		if err := validateRecord(rec); err != nil {
			return fmt.Errorf("invalid record at %d: %w", start, err)
		}
		if rec.Checksum != nil {
			want, err := recordChecksum(rec)
			if err != nil {
				return fmt.Errorf("checksum record at %d: %w", start, err)
			}
			if *rec.Checksum != want {
				return fmt.Errorf("checksum mismatch at %d", start)
			}
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
	_, err = s.f.Seek(0, io.SeekEnd)
	return err
}

func (s *storage) incompleteTail(offset int64, repair bool, cause error) error {
	if !repair {
		return fmt.Errorf("incomplete trailing record at %d: %w", offset, cause)
	}
	if err := s.f.Truncate(offset); err != nil {
		s.poisoned = true
		return fmt.Errorf("truncate incomplete trailing record at %d: %w", offset, err)
	}
	if _, err := s.f.Seek(offset, io.SeekStart); err != nil {
		s.poisoned = true
		return fmt.Errorf("seek after tail repair: %w", err)
	}
	if err := s.f.Sync(); err != nil {
		s.poisoned = true
		return fmt.Errorf("sync tail repair: %w", err)
	}
	_, err := s.f.Seek(0, io.SeekEnd)
	return err
}

func validateRecord(rec diskRecord) error {
	if rec.TxID == 0 {
		return errors.New("transaction ID must be positive")
	}
	if rec.Commit {
		if rec.Type != 0 || len(rec.Key) != 0 || len(rec.Value) != 0 {
			return errors.New("commit record contains operation fields")
		}
		return nil
	}
	if rec.Type != Put && rec.Type != Delete {
		return fmt.Errorf("unknown operation type %d", rec.Type)
	}
	if len(rec.Key) > 1024 {
		return errors.New("key exceeds 1024 bytes")
	}
	if rec.Type == Put && len(rec.Value) > 1<<20 {
		return errors.New("value exceeds 1 MB")
	}
	if rec.Type == Delete && len(rec.Value) != 0 {
		return errors.New("delete record contains a value")
	}
	return nil
}

func recordChecksum(rec diskRecord) (uint32, error) {
	rec.Checksum = nil
	b, err := json.Marshal(rec)
	if err != nil {
		return 0, err
	}
	return crc32.Checksum(b, castagnoliTable), nil
}

func marshalRecord(rec diskRecord) ([]byte, error) {
	if err := validateRecord(rec); err != nil {
		return nil, err
	}
	checksum, err := recordChecksum(rec)
	if err != nil {
		return nil, err
	}
	rec.Checksum = &checksum
	b, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	if len(b) == 0 || len(b) > maxRecordSize {
		return nil, fmt.Errorf("record size %d is outside supported range", len(b))
	}
	frame := make([]byte, 4+len(b))
	binary.LittleEndian.PutUint32(frame[:4], uint32(len(b)))
	copy(frame[4:], b)
	return frame, nil
}

func writeFull(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if n < 0 || n > len(p) {
			return errors.New("invalid write count")
		}
		p = p[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func writeFull(w io.Writer, b []byte) (int, error) {
	total := 0
	for total < len(b) { n, err := w.Write(b[total:]); total += n; if err != nil { return total, err }; if n == 0 { return total, io.ErrShortWrite } }
	return total, nil
}

func (s *storage) commit(ops []Operation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return ErrClosed
	}
	if s.poisoned {
		return errors.New("storage is unusable after an earlier I/O failure")
	}
	if s.nextTx == ^uint64(0) {
		return errors.New("transaction ID space exhausted")
	}
	tx := s.nextTx + 1
	var transactionSize int64
	for _, op := range ops {
		rec := diskRecord{TxID: tx, Type: op.Type, Key: append([]byte(nil), op.Key...), Value: append([]byte(nil), op.Value...)}
		frame, err := marshalRecord(rec)
		if err != nil {
			return err
		}
		transactionSize += int64(len(frame))
	}
	commitFrame, err := marshalRecord(diskRecord{TxID: tx, Commit: true})
	if err != nil {
		return err
	}
	transactionSize += int64(len(commitFrame))

	st, err := s.f.Stat()
	if err != nil {
		return err
	}
	start := st.Size()
	if s.maxBytes > 0 && transactionSize > s.maxBytes-start {
		return fmt.Errorf("database size limit exceeded")
	}
	if _, err := s.f.Seek(start, io.SeekStart); err != nil {
		return err
	}
	var wrote bool
	for _, op := range ops {
		rec := diskRecord{TxID: tx, Type: op.Type, Key: append([]byte(nil), op.Key...), Value: append([]byte(nil), op.Value...)}
		frame, err := marshalRecord(rec)
		if err != nil {
			if wrote {
				return s.rollbackAfterFailure(start, err, false)
			}
			return err
		}
		if err := writeFull(s.f, frame); err != nil {
			return s.rollbackAfterFailure(start, err, false)
		}
		wrote = true
	}
	if err := writeFull(s.f, commitFrame); err != nil {
		return s.rollbackAfterFailure(start, err, false)
	}
	if err := s.f.Sync(); err != nil {
		return s.rollbackAfterFailure(start, err, true)
	}
	s.nextTx = tx
	return nil
}

func (s *storage) rollbackAfterFailure(start int64, cause error, poison bool) error {
	rollbackErr := s.f.Truncate(start)
	if rollbackErr == nil {
		_, rollbackErr = s.f.Seek(start, io.SeekStart)
	}
	if rollbackErr == nil {
		rollbackErr = s.f.Sync()
	}
	if poison || rollbackErr != nil {
		s.poisoned = true
	}
	if rollbackErr != nil {
		return fmt.Errorf("transaction failed: %v; rollback failed: %w", cause, rollbackErr)
	}
	return cause
}

func (s *storage) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	var syncErr error
	if !s.poisoned {
		syncErr = s.f.Sync()
		if syncErr != nil {
			s.poisoned = true
		}
	}
	closeErr := s.f.Close()
	s.f = nil
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func (s *storage) check() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return ErrClosed
	}
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	scanErr := s.scan(map[string]diskRecord{}, false)
	if _, err := s.f.Seek(0, io.SeekEnd); err != nil {
		if scanErr != nil {
			return fmt.Errorf("%v; restore file position: %w", scanErr, err)
		}
		return err
	}
	return scanErr
}
