//go:build kvdb_crashprobe

package kvdb

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

var crashProbeState struct {
	sync.Mutex
	operationID   string
	operationType string
}

type crashProbeEventRecord struct {
	PID           int    `json:"pid"`
	OperationID   string `json:"operation_id"`
	OperationType string `json:"operation_type"`
	Event         string `json:"event"`
	TimestampNS   int64  `json:"timestamp_ns"`
	TxID          uint64 `json:"tx_id"`
}

func CrashProbeSetOperation(id, operationType string) {
	crashProbeState.Lock()
	crashProbeState.operationID = id
	crashProbeState.operationType = operationType
	crashProbeState.Unlock()
}

func CrashProbeClearOperation() {
	crashProbeState.Lock()
	crashProbeState.operationID = ""
	crashProbeState.operationType = ""
	crashProbeState.Unlock()
}

func CrashProbeCloseRequestEnabled() bool {
	return os.Getenv("KVDB_CRASHPROBE_ALLOW_CLOSE") == "1"
}

func crashProbeEvent(event string, txID uint64) {
	path := os.Getenv("KVDB_CRASHPROBE_FILE")
	if path == "" {
		return
	}
	crashProbeState.Lock()
	record := crashProbeEventRecord{
		PID:           os.Getpid(),
		OperationID:   crashProbeState.operationID,
		OperationType: crashProbeState.operationType,
		Event:         event,
		TimestampNS:   time.Now().UnixNano(),
		TxID:          txID,
	}
	crashProbeState.Unlock()
	b, err := json.Marshal(record)
	if err != nil {
		return
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return
	}
	_, _ = f.Write(append(b, '\n'))
	_ = f.Close()
	if gate := os.Getenv("KVDB_CRASHPROBE_GATE"); gate != "" &&
		(os.Getenv("KVDB_CRASHPROBE_GATE_EVENT") == "" || os.Getenv("KVDB_CRASHPROBE_GATE_EVENT") == event) {
		for {
			if _, err := os.Stat(gate); err == nil {
				break
			}
			time.Sleep(time.Millisecond)
		}
	}
}
