package kvdb

import (
	"encoding/json"
	"sort"
	"sync"
	"time"
)

type DB struct {
	mu               sync.RWMutex
	storage          *storage
	values           map[string][]byte
	closed           bool
	recoveryRequired bool
}

type SpaceStats struct {
	CurrentBytes   int64
	MaxBytes       int64
	LiveBytes      int64
	ObsoleteBytes  int64
	AvailableBytes int64
	LastCompaction time.Time
	BytesReclaimed int64
}

type SpaceStats struct {
	CurrentBytes int64
	MaxBytes int64
	LiveBytes int64
	ObsoleteBytes int64
	AvailableBytes int64
	LastCompaction time.Time
	BytesReclaimed int64
}

func Open(path string, maxBytes int64) (*DB, error) {
	s, recovered, err := openStorage(path, maxBytes)
	if err != nil {
		return nil, err
	}
	d := &DB{storage: s, values: map[string][]byte{}}
	for k, r := range recovered {
		if r.Type == Put {
			d.values[k] = append([]byte(nil), r.Value...)
		}
	}
	return d, nil
}
func (db *DB) Put(k, v []byte) error { return db.Batch([]Operation{{Type: Put, Key: k, Value: v}}) }
func (db *DB) Delete(k []byte) error { return db.Batch([]Operation{{Type: Delete, Key: k}}) }
func (db *DB) Get(k []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	v, ok := db.values[string(k)]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), v...), nil
}
func (db *DB) Batch(ops []Operation) error {
	if len(ops) > 10000 {
		return &limitError{"batch exceeds 10000 operations"}
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.recoveryRequired {
		return ErrRecoveryRequired
	}
	for _, op := range ops {
		if len(op.Key) > 1024 {
			return &limitError{"key exceeds 1024 bytes"}
		}
		if op.Type == Put && len(op.Value) > 1<<20 {
			return &limitError{"value exceeds 1 MB"}
		}
		if op.Type != Put && op.Type != Delete {
			return &limitError{"unknown operation"}
		}
	}
	if err := db.storage.commit(ops); err != nil {
		if db.storage.poisoned {
			db.recoveryRequired = true
		}
		return err
	}
	for _, op := range ops {
		if op.Type == Delete {
			delete(db.values, string(op.Key))
		} else {
			db.values[string(op.Key)] = append([]byte(nil), op.Value...)
		}
	}
	return nil
}

// RecoveryRequired reports whether a failed storage operation left the commit
// outcome uncertain. Reopening the database performs recovery and clears this
// state.
func (db *DB) RecoveryRequired() bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.recoveryRequired
}
func (db *DB) Keys() map[string][]byte {
	db.mu.RLock()
	defer db.mu.RUnlock()
	out := map[string][]byte{}
	for k, v := range db.values {
		out[k] = append([]byte(nil), v...)
	}
	return out
}

// Compact rewrites the committed live values into a new log. It blocks DB
// readers and writers while taking the snapshot and installing the file.
func (db *DB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	db.storage.mu.Lock()
	boundary := db.storage.nextTx
	db.storage.mu.Unlock()
	return db.storage.compactLocked(db.values, boundary)
}

// SpaceStats reports file and estimated live-data usage.
func (db *DB) SpaceStats() (SpaceStats, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return SpaceStats{}, ErrClosed
	}
	db.storage.mu.Lock()
	defer db.storage.mu.Unlock()
	st, err := db.storage.f.Stat()
	if err != nil {
		return SpaceStats{}, err
	}
	var live int64
	for k, v := range db.values {
		for _, rec := range []diskRecord{{TxID: 1, Type: Put, Key: []byte(k), Value: v}, {TxID: 1, Commit: true}} {
			b, e := json.Marshal(rec)
			if e != nil {
				return SpaceStats{}, e
			}
			live += int64(4 + len(b))
		}
	}
	stats := SpaceStats{CurrentBytes: st.Size(), MaxBytes: db.storage.maxBytes, LiveBytes: live, ObsoleteBytes: st.Size() - live, LastCompaction: db.storage.lastCompaction, BytesReclaimed: db.storage.bytesReclaimed}
	if stats.ObsoleteBytes < 0 {
		stats.ObsoleteBytes = 0
	}
	if stats.MaxBytes > 0 {
		stats.AvailableBytes = stats.MaxBytes - st.Size()
		if stats.AvailableBytes < 0 {
			stats.AvailableBytes = 0
		}
	}
	return stats, nil
}
func (db *DB) Check() error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrClosed
	}
	return db.storage.check()
}
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	return db.storage.close()
}

type limitError struct{ s string }

func (e *limitError) Error() string { return e.s }
func SortedKeys(m map[string][]byte) []string {
	r := make([]string, 0, len(m))
	for k := range m {
		r = append(r, k)
	}
	sort.Strings(r)
	return r
}
