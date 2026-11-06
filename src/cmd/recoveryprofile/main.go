package main

import (
	"flag"
	"fmt"
	"killitandproveit/src/kvdb"
	"os"
	"runtime"
	"runtime/pprof"
	"time"
)

func main() {
	path := flag.String("path", "million-benchmark.kv", "database path")
	cpu := flag.String("cpu", "recovery.cpu", "CPU profile output")
	mem := flag.String("mem", "recovery.mem", "heap profile output")
	flag.Parse()
	cpuFile, err := os.Create(*cpu)
	if err != nil {
		panic(err)
	}
	if err = pprof.StartCPUProfile(cpuFile); err != nil {
		panic(err)
	}
	start := time.Now()
	db, err := kvdb.Open(*path, 0)
	elapsed := time.Since(start)
	pprof.StopCPUProfile()
	_ = cpuFile.Close()
	if err != nil {
		panic(err)
	}
	fmt.Printf("keys=%d recovery_ms=%d\n", len(db.Keys()), elapsed.Milliseconds())
	runtime.GC()
	memFile, err := os.Create(*mem)
	if err != nil {
		panic(err)
	}
	if err = pprof.WriteHeapProfile(memFile); err != nil {
		panic(err)
	}
	_ = memFile.Close()
	_ = db.Close()
}
