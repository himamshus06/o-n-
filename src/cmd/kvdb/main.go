package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"killitandproveit/src/kvdb"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		return
	}
	cmd := os.Args[1]
	switch cmd {
	case "open":
		openCmd(os.Args[2:])
	case "put":
		one(cmd, os.Args[2:])
	case "get":
		one(cmd, os.Args[2:])
	case "del":
		one(cmd, os.Args[2:])
	case "batch":
		batch(os.Args[2:])
	case "dump":
		dump(os.Args[2:])
	case "check":
		check(os.Args[2:])
	case "serve":
		serve(os.Args[2:])
	default:
		usage()
	}
}
func usage()                               { fmt.Fprintln(os.Stderr, "usage: kvdb open|put|get|del|batch|dump|check|serve ...") }
func openDB(path string) (*kvdb.DB, error) { return kvdb.Open(path, 0) }
func openCmd(a []string) {
	if len(a) < 1 {
		usage()
		return
	}
	max := int64(0)
	for i := 1; i+1 < len(a); i++ {
		if a[i] == "--max-bytes" {
			max, _ = strconv.ParseInt(a[i+1], 10, 64)
		}
	}
	t := time.Now()
	db, e := kvdb.Open(a[0], max)
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		return
	}
	defer db.Close()
	fmt.Printf("keys %d\nrecovery_ms %d\n", len(db.Keys()), time.Since(t).Milliseconds())
}
func one(c string, a []string) {
	if len(a) < 2 {
		usage()
		return
	}
	db, e := openDB(a[0])
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		return
	}
	defer db.Close()
	var v []byte
	if c == "put" {
		if len(a) < 3 {
			usage()
			return
		}
		e = db.Put([]byte(a[1]), []byte(a[2]))
	} else if c == "del" {
		e = db.Delete([]byte(a[1]))
	} else {
		v, e = db.Get([]byte(a[1]))
		if e == kvdb.ErrNotFound {
			fmt.Println("NOTFOUND")
			return
		}
	}
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		return
	}
	if c == "get" {
		fmt.Println(string(v))
	} else {
		fmt.Println("OK")
	}
}
func batch(a []string) {
	if len(a) < 2 {
		usage()
		return
	}
	db, e := openDB(a[0])
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		return
	}
	defer db.Close()
	f, e := os.Open(a[1])
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		return
	}
	defer f.Close()
	var ops []kvdb.Operation
	s := bufio.NewScanner(f)
	for s.Scan() {
		p := strings.SplitN(s.Text(), " ", 3)
		if len(p) < 2 {
			continue
		}
		if strings.EqualFold(p[0], "PUT") && len(p) == 3 {
			ops = append(ops, kvdb.Operation{Type: kvdb.Put, Key: []byte(p[1]), Value: []byte(p[2])})
		} else if strings.EqualFold(p[0], "DELETE") {
			ops = append(ops, kvdb.Operation{Type: kvdb.Delete, Key: []byte(p[1])})
		}
	}
	if e = s.Err(); e == nil {
		e = db.Batch(ops)
	}
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		return
	}
	fmt.Println("OK")
}
func dump(a []string) {
	if len(a) < 1 {
		return
	}
	db, e := openDB(a[0])
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		return
	}
	defer db.Close()
	m := db.Keys()
	for _, k := range kvdb.SortedKeys(m) {
		fmt.Printf("%s %s\n", k, m[k])
	}
}
func check(a []string) {
	if len(a) < 1 {
		return
	}
	db, e := openDB(a[0])
	if e == nil {
		e = db.Check()
		db.Close()
	}
	if e != nil {
		fmt.Printf("ERROR %v\n", e)
	} else {
		fmt.Println("OK")
	}
}
func serve(a []string) {
	if len(a) < 1 {
		return
	}
	db, e := openDB(a[0])
	if e != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", e)
		return
	}
	defer db.Close()
	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()
	fmt.Fprintln(os.Stderr, "READY")
	s := bufio.NewScanner(os.Stdin)
	s.Buffer(make([]byte, 64*1024), 16<<20)
	for s.Scan() {
		var req serveRequest
		if err := json.Unmarshal(s.Bytes(), &req); err != nil {
			writeServeResponse(out, serveResponse{Status: "ERROR", Error: "Invalid JSON"})
			continue
		}
		if err := validateID(req.ID); err != nil {
			writeServeResponse(out, serveResponse{ID: req.ID, Status: "ERROR", Error: err.Error()})
			continue
		}
		if req.Op == "" {
			writeServeResponse(out, serveResponse{ID: req.ID, Status: "ERROR", Error: "Missing operation"})
			continue
		}
		op := strings.ToUpper(req.Op)
		switch op {
		case "COMPACT":
			if err := db.Compact(); err != nil {
				if !writeServeError(out, req.ID, err, db.RecoveryRequired()) {
					return
				}
			} else {
				writeServeResponse(out, serveResponse{ID: req.ID, Status: "ACK"})
			}
		case "PUT", "GET", "DELETE":
			if err := validateEncodedKey(req.Key); err != nil {
				writeServeResponse(out, serveResponse{ID: req.ID, Status: "ERROR", Error: err.Error()})
				continue
			}
			key, _ := base64.StdEncoding.DecodeString(req.Key)
			if op == "GET" {
				value, err := db.Get(key)
				if err == kvdb.ErrNotFound {
					writeServeResponse(out, serveResponse{ID: req.ID, Status: "NOT_FOUND"})
				} else if err != nil {
					if !writeServeError(out, req.ID, err, db.RecoveryRequired()) {
						return
					}
				} else {
					encodedValue := base64.StdEncoding.EncodeToString(value)
					writeServeResponse(out, serveResponse{ID: req.ID, Status: "VALUE", Value: &encodedValue})
				}
				continue
			}
			if op == "PUT" {
				if req.Value == nil {
					writeServeResponse(out, serveResponse{ID: req.ID, Status: "ERROR", Error: "Missing value"})
					continue
				}
				if err := validateEncodedValue(*req.Value); err != nil {
					writeServeResponse(out, serveResponse{ID: req.ID, Status: "ERROR", Error: err.Error()})
					continue
				}
				value, _ := base64.StdEncoding.DecodeString(*req.Value)
				if err := db.Put(key, value); err != nil {
					if !writeServeError(out, req.ID, err, db.RecoveryRequired()) {
						return
					}
				} else {
					writeServeResponse(out, serveResponse{ID: req.ID, Status: "ACK"})
				}
			} else {
				if err := db.Delete(key); err != nil {
					if !writeServeError(out, req.ID, err, db.RecoveryRequired()) {
						return
					}
				} else {
					writeServeResponse(out, serveResponse{ID: req.ID, Status: "ACK"})
				}
			}
		case "BATCH":
			ops, err := decodeBatch(req.Operations)
			if err != nil {
				writeServeResponse(out, serveResponse{ID: req.ID, Status: "ERROR", Error: err.Error()})
				continue
			}
			if err = db.Batch(ops); err != nil {
				if !writeServeError(out, req.ID, err, db.RecoveryRequired()) {
					return
				}
			} else {
				writeServeResponse(out, serveResponse{ID: req.ID, Status: "ACK"})
			}
		case "DUMP":
			entries := make([]serveEntry, 0)
			values := db.Keys()
			for _, key := range kvdb.SortedKeys(values) {
				entries = append(entries, serveEntry{Key: base64.StdEncoding.EncodeToString([]byte(key)), Value: base64.StdEncoding.EncodeToString(values[key])})
			}
			writeServeResponse(out, serveResponse{ID: req.ID, Status: "DUMP", Entries: entries})
		default:
			writeServeResponse(out, serveResponse{ID: req.ID, Status: "ERROR", Error: "Invalid operation"})
		}
	}
	if err := s.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "stdin: %v\n", err)
	}
}

type serveRequest struct {
	ID         json.RawMessage       `json:"id"`
	Op         string                `json:"op"`
	Key        string                `json:"key"`
	Value      *string               `json:"value"`
	Operations []serveBatchOperation `json:"operations"`
}
type serveBatchOperation struct {
	Op    string  `json:"op"`
	Key   string  `json:"key"`
	Value *string `json:"value"`
}
type serveEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
type serveResponse struct {
	ID      json.RawMessage `json:"id,omitempty"`
	Status  string          `json:"status"`
	Value   *string         `json:"value,omitempty"`
	Error   string          `json:"error,omitempty"`
	Outcome string          `json:"outcome,omitempty"`
	Entries []serveEntry    `json:"entries"`
}

func writeServeResponse(w *bufio.Writer, response serveResponse) {
	b, _ := json.Marshal(response)
	_, _ = w.Write(append(b, '\n'))
	_ = w.Flush()
}
func writeServeError(w *bufio.Writer, id json.RawMessage, err error, recoveryRequired bool) bool {
	if recoveryRequired || err == kvdb.ErrRecoveryRequired || dbRecoveryError(err) {
		writeServeResponse(w, serveResponse{ID: id, Status: "ERROR", Outcome: "UNKNOWN", Error: "Commit outcome uncertain; database requires recovery"})
		return false
	}
	writeServeResponse(w, serveResponse{ID: id, Status: "ERROR", Error: err.Error()})
	return true
}
func dbRecoveryError(err error) bool { return strings.Contains(err.Error(), "requires recovery") }
func validateID(raw json.RawMessage) error {
	if len(raw) == 0 || string(raw) == "null" {
		return fmt.Errorf("Missing id")
	}
	var v any
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.UseNumber()
	if err := d.Decode(&v); err != nil {
		return fmt.Errorf("Invalid id")
	}
	switch x := v.(type) {
	case string:
		if x == "" {
			return fmt.Errorf("Invalid id")
		}
	case json.Number:
		if _, err := x.Int64(); err != nil {
			return fmt.Errorf("Invalid id")
		}
	default:
		return fmt.Errorf("Invalid id")
	}
	return nil
}
func validateEncodedKey(s string) error {
	if s == "" {
		return fmt.Errorf("Missing key")
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return fmt.Errorf("Invalid Base64 key")
	}
	if len(b) > 1024 {
		return fmt.Errorf("key exceeds 1024 bytes")
	}
	return nil
}
func validateEncodedValue(s string) error {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return fmt.Errorf("Invalid Base64 value")
	}
	if len(b) > 1<<20 {
		return fmt.Errorf("value exceeds 1 MB")
	}
	return nil
}
func decodeBatch(items []serveBatchOperation) ([]kvdb.Operation, error) {
	if len(items) > 10000 {
		return nil, fmt.Errorf("batch exceeds 10000 operations")
	}
	ops := make([]kvdb.Operation, 0, len(items))
	for _, item := range items {
		op := strings.ToUpper(item.Op)
		if op != "PUT" && op != "DELETE" {
			return nil, fmt.Errorf("invalid batch operation")
		}
		if err := validateEncodedKey(item.Key); err != nil {
			return nil, err
		}
		key, _ := base64.StdEncoding.DecodeString(item.Key)
		if op == "PUT" {
			if item.Value == nil {
				return nil, fmt.Errorf("Missing value")
			}
			if err := validateEncodedValue(*item.Value); err != nil {
				return nil, err
			}
			value, _ := base64.StdEncoding.DecodeString(*item.Value)
			ops = append(ops, kvdb.Operation{Type: kvdb.Put, Key: key, Value: value})
		} else {
			if item.Value != nil {
				return nil, fmt.Errorf("DELETE must not contain value")
			}
			ops = append(ops, kvdb.Operation{Type: kvdb.Delete, Key: key})
		}
	}
	return ops, nil
}
