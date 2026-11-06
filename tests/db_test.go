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
