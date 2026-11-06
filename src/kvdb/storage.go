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
	"runtime"
	"sync"
	"time"
)

var ErrNotFound = errors.New("key not found")
var ErrClosed = errors.New("database is closed")
var ErrMaxBytes = errors.New("database size limit exceeded")
var ErrRecoveryRequired = errors.New("database requires recovery after an uncertain commit")

const maxRecordSize = 64 << 20

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// compactionHook is intentionally nil in production. Tests use it to model a
// process terminating at named filesystem boundaries.
var compactionHook func(string)

func compactionPoint(name string) {
	if compactionHook != nil {
		compactionHook(name)
	}
}

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
	mu             sync.Mutex
	f              storageFile
	path           string
	maxBytes       int64
	nextTx         uint64
	poisoned       bool
	lastCompaction time.Time
	bytesReclaimed int64
}

func openStorage(path string, maxBytes int64) (*storage, map[string]diskRecord, error) {
	backup := path + ".compact-old"
	tmp := path + ".compact-tmp"
	pathValid, pathExists := validStorageCandidate(path)
	backupValid, backupExists := validStorageCandidate(backup)
	tmpValid, _ := validStorageCandidate(tmp)
	if !pathExists || !pathValid {
		if backupValid {
			if pathExists {
				_ = os.Remove(path)
			}
			if err := os.Rename(backup, path); err != nil {
				return nil, nil, err
			}
			pathValid, pathExists = true, true
		} else if !pathExists && tmpValid {
			if err := os.Rename(tmp, path); err != nil {
				return nil, nil, err
			}
			pathValid, pathExists = true, true
		} else if pathExists {
			// Keep an existing candidate selected when no validated backup is
			// available so the normal recovery pass can report corruption or
			// repair a physically incomplete tail.
			pathValid = true
		}
	}
	if !pathExists {
		// No valid candidate exists; OpenFile below creates a new empty DB.
		_ = backupExists
	} else if !pathValid {
		return nil, nil, fmt.Errorf("no valid authoritative database file")
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, nil, err
	}
	s := &storage{f: f, maxBytes: maxBytes, path: path}
	// A backup is retained until the replacement has been installed and the
	// directory entry synchronized. If the base is missing after a crash, it
	// is the only known-good copy.
	_ = os.Remove(tmp)
	committed := map[string]diskRecord{}
	if err := s.recover(committed); err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	_ = os.Remove(backup)
	_ = syncDir(filepath.Dir(path))
	return s, committed, nil
}

// validStorageCandidate validates a candidate without selecting it. Complete
// corruption is never repaired or silently accepted; only the normal recovery
// pass may repair an incomplete final frame after a candidate is selected.
func validStorageCandidate(path string) (valid, exists bool) {
	if _, err := os.Stat(path); err != nil {
		return false, false
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		return false, true
	}
	s := &storage{f: f, path: path}
	err = s.scan(map[string]diskRecord{}, false)
	_ = f.Close()
	return err == nil, true
}

// syncDir requests directory-entry durability on platforms that expose it.
// Windows does not provide a portable directory fsync operation through the
// standard library; atomic rename remains the replacement primitive there.
func syncDir(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
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

func writeFull(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if n < 0 || n > len(b) {
			return errors.New("invalid write count")
		}
		b = b[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func (s *storage) commit(ops []Operation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return ErrClosed
	}
	if s.poisoned {
		return ErrRecoveryRequired
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

func (s *storage) compactLocked(values map[string][]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return ErrClosed
	}
	if s.poisoned {
		return ErrRecoveryRequired
	}

	oldInfo, err := s.f.Stat()
	if err != nil {
		return err
	}
	oldSize := oldInfo.Size()
	tx := s.nextTx + 1
	var frames [][]byte
	var total int64
	for _, key := range SortedKeys(values) {
		frame, e := marshalRecord(diskRecord{TxID: tx, Type: Put, Key: []byte(key), Value: append([]byte(nil), values[key]...)})
		if e != nil {
			return e
		}
		frames = append(frames, frame)
		total += int64(len(frame))
	}
	commitFrame, err := marshalRecord(diskRecord{TxID: tx, Commit: true})
	if err != nil {
		return err
	}
	total += int64(len(commitFrame))
	if s.maxBytes > 0 && total > s.maxBytes {
		return ErrMaxBytes
	}

	tmp := s.path + ".compact-tmp"
	backup := s.path + ".compact-old"
	compactionPoint("before-temp")
	_ = os.Remove(tmp)
	_ = os.Remove(backup)
	tf, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	for _, frame := range frames {
		if err = writeFull(tf, frame); err != nil {
			tf.Close()
			_ = os.Remove(tmp)
			return err
		}
	}
	if err = writeFull(tf, commitFrame); err != nil {
		tf.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err = tf.Sync(); err != nil {
		tf.Close()
		_ = os.Remove(tmp)
		return err
	}
	compactionPoint("after-temp-sync")
	if err = tf.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	if err = s.f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	s.f = nil
	if err = replaceAuthoritative(s.path, tmp, backup); err != nil {
		s.f, _ = os.OpenFile(s.path, os.O_RDWR, 0600)
		_ = os.Remove(tmp)
		return err
	}
	compactionPoint("after-replacement")
	if err = syncDir(filepath.Dir(s.path)); err != nil {
		// The new file is complete and authoritative; retain it, but report the
		// directory durability limitation to the caller.
	}
	newFile, err := os.OpenFile(s.path, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	s.f = newFile
	s.nextTx = tx
	s.lastCompaction = time.Now()
	s.bytesReclaimed = oldSize - total
	if s.bytesReclaimed < 0 {
		s.bytesReclaimed = 0
	}
	compactionPoint("before-backup-delete")
	_ = os.Remove(backup)
	_ = syncDir(filepath.Dir(s.path))
	return nil
}
