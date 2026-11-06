package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"killitandproveit/src/kvdb"
	"os"
	"runtime"
	"sort"
	"time"
)

func main() {
	path := flag.String("path", "million.kv", "database path")
	n := flag.Int("keys", 1000000, "keys to generate")
	repeats := flag.Int("repeats", 3, "recovery repetitions")
	generate := flag.Bool("generate", false, "generate the database before measuring")
	compact := flag.Bool("compact", false, "compact after generation to create a checkpoint")
	flag.Parse()
	if *generate {
		if err := generateDB(*path, *n); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if *compact {
			db, err := kvdb.Open(*path, 0)
			if err != nil {
				panic(err)
			}
			if err = db.Compact(); err != nil {
				panic(err)
			}
			if err = db.Close(); err != nil {
				panic(err)
			}
		}
	}
	for i := 1; i <= *repeats; i++ {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		start := time.Now()
		db, err := kvdb.Open(*path, 0)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		openDuration := time.Since(start)
		values := db.Keys()
		runtime.ReadMemStats(&after)
		hash := checksum(values)
		fmt.Printf("run=%d keys=%d recovery_ms=%d checksum=%x heap_delta_bytes=%d\n", i, len(values), openDuration.Milliseconds(), hash, int64(after.HeapAlloc)-int64(before.HeapAlloc))
		if err := db.Close(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}

func generateDB(path string, n int) error {
	_ = os.Remove(path)
	db, err := kvdb.Open(path, 0)
	if err != nil {
		return err
	}
	defer db.Close()
	for start := 0; start < n; start += 10000 {
		end := start + 10000
		if end > n {
			end = n
		}
		ops := make([]kvdb.Operation, 0, end-start)
		for i := start; i < end; i++ {
			ops = append(ops, kvdb.Operation{Type: kvdb.Put, Key: []byte(fmt.Sprintf("key-%08d", i)), Value: []byte(fmt.Sprintf("value-%08d", i))})
		}
		if err := db.Batch(ops); err != nil {
			return err
		}
	}
	return nil
}

func checksum(values map[string][]byte) [32]byte {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, key := range keys {
		h.Write([]byte(key))
		h.Write([]byte{0})
		h.Write(values[key])
		h.Write([]byte{0})
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}
