package tests

import (
	"errors"
	"path/filepath"
	"testing"

	"killitandproveit/src/kvdb"
)

func TestPutGetDeleteAndRecovery(t *testing.T) {
	p := filepath.Join(t.TempDir(), "db.kv")
	db, err := kvdb.Open(p, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Put([]byte("a"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	v, err := db.Get([]byte("a"))
	if err != nil || string(v) != "one" {
		t.Fatalf("got %q, %v", v, err)
	}
	if err = db.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Get([]byte("a")); !errors.Is(err, kvdb.ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = kvdb.Open(p, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Get([]byte("a")); !errors.Is(err, kvdb.ErrNotFound) {
		t.Fatalf("expected recovered delete, got %v", err)
	}
}

func TestBatchLastOperationWins(t *testing.T) {
	db, err := kvdb.Open(filepath.Join(t.TempDir(), "db.kv"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Batch([]kvdb.Operation{{Type: kvdb.Put, Key: []byte("k"), Value: []byte("1")}, {Type: kvdb.Put, Key: []byte("k"), Value: []byte("2")}, {Type: kvdb.Delete, Key: []byte("k")}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Get([]byte("k")); !errors.Is(err, kvdb.ErrNotFound) {
		t.Fatalf("expected deleted key, got %v", err)
	}
}

func TestCompactPreservesLiveValuesAndReopens(t *testing.T) {
	p := filepath.Join(t.TempDir(), "db.kv")
	db, err := kvdb.Open(p, 0)
	if err != nil { t.Fatal(err) }
	for i := 0; i < 10; i++ { if err = db.Put([]byte("keep"), []byte{byte('0'+i)}); err != nil { t.Fatal(err) } }
	if err = db.Put([]byte("empty"), []byte{}); err != nil { t.Fatal(err) }
	if err = db.Put([]byte("gone"), []byte("x")); err != nil { t.Fatal(err) }
	if err = db.Delete([]byte("gone")); err != nil { t.Fatal(err) }
	before := db.Keys()
	if err = db.Compact(); err != nil { t.Fatal(err) }
	if got := db.Keys(); len(got) != len(before) || string(got["keep"]) != string(before["keep"]) { t.Fatalf("values changed: %#v", got) }
	stats, err := db.SpaceStats(); if err != nil { t.Fatal(err) }; if stats.CurrentBytes <= 0 || stats.BytesReclaimed <= 0 { t.Fatalf("unexpected stats: %+v", stats) }
	if err = db.Close(); err != nil { t.Fatal(err) }
	db, err = kvdb.Open(p, 0); if err != nil { t.Fatal(err) }; defer db.Close()
	if got := db.Keys(); len(got) != len(before) || string(got["keep"]) != string(before["keep"]) { t.Fatalf("reopened values changed: %#v", got) }
}

func TestCompactRejectsIfLiveSetExceedsLimit(t *testing.T) {
	p := filepath.Join(t.TempDir(), "db.kv")
	db, err := kvdb.Open(p, 0); if err != nil { t.Fatal(err) }
	if err = db.Put([]byte("k"), []byte("large enough live record")); err != nil { t.Fatal(err) }
	if err = db.Close(); err != nil { t.Fatal(err) }
	db, err = kvdb.Open(p, 1); if err != nil { t.Fatal(err) }; defer db.Close()
	if err = db.Compact(); !errors.Is(err, kvdb.ErrMaxBytes) { t.Fatalf("expected max size error, got %v", err) }
	if v, e := db.Get([]byte("k")); e != nil || string(v) != "large enough live record" { t.Fatalf("old data unavailable after failed compaction: %q %v", v, e) }
}
