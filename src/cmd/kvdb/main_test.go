package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestServeValidation(t *testing.T) {
	if err := validateID(json.RawMessage(`1`)); err != nil {
		t.Fatal(err)
	}
	if err := validateID(json.RawMessage(`"x"`)); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{`null`, `true`, `{}`, `1.5`} {
		if validateID(json.RawMessage(raw)) == nil {
			t.Fatalf("accepted invalid id %s", raw)
		}
	}
	if err := validateEncodedKey("not-base64"); err == nil {
		t.Fatal("accepted invalid key")
	}
	key := base64.StdEncoding.EncodeToString([]byte("a"))
	value := base64.StdEncoding.EncodeToString([]byte("1"))
	if ops, err := decodeBatch([]serveBatchOperation{{Op: "PUT", Key: key, Value: &value}, {Op: "DELETE", Key: key}}); err != nil || len(ops) != 2 {
		t.Fatalf("batch validation: %v %#v", err, ops)
	}
}

func TestServeSubprocessJSONL(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Parallel()
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db.kv")
	exe := filepath.Join(dir, "kvdb-test.exe")
	if out, err := exec.Command("go", "build", "-o", exe, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	cmd := exec.Command(exe, "serve", dbPath)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan string, 1)
	go func() {
		s := bufio.NewScanner(stderr)
		if s.Scan() {
			ready <- s.Text()
		}
	}()
	if got := <-ready; got != "READY" {
		t.Fatalf("ready=%q", got)
	}
	key := base64.StdEncoding.EncodeToString([]byte("apple"))
	value := base64.StdEncoding.EncodeToString([]byte("red"))
	requests := []string{
		`{"id":"1","op":"PUT","key":"` + key + `","value":"` + value + `"}`,
		`{"id":2,"op":"GET","key":"` + key + `"}`,
		`{"id":"3","op":"BATCH","operations":[{"op":"PUT","key":"Yg==","value":"Mg=="},{"op":"DELETE","key":"Y2M="}]}`,
		`{"id":4,"op":"DUMP"}`,
		`{"id":"5","op":"GET","key":"Y2M="}`,
	}
	for _, request := range requests {
		_, _ = stdin.Write([]byte(request + "\n"))
	}
	_ = stdin.Close()
	var output bytes.Buffer
	_, _ = output.ReadFrom(stdout)
	if err = cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != len(requests) {
		t.Fatalf("responses=%d output=%q", len(lines), output.String())
	}
	var response map[string]any
	if err = json.Unmarshal([]byte(lines[0]), &response); err != nil || response["status"] != "ACK" || response["id"] != "1" {
		t.Fatalf("put response=%s", lines[0])
	}
	if err = json.Unmarshal([]byte(lines[1]), &response); err != nil || response["status"] != "VALUE" || response["id"].(float64) != 2 {
		t.Fatalf("get response=%s", lines[1])
	}
	if err = json.Unmarshal([]byte(lines[2]), &response); err != nil || response["status"] != "ACK" {
		t.Fatalf("batch response=%s", lines[2])
	}
	if err = json.Unmarshal([]byte(lines[3]), &response); err != nil || response["status"] != "DUMP" {
		t.Fatalf("dump response=%s", lines[3])
	}
	if err = json.Unmarshal([]byte(lines[4]), &response); err != nil || response["status"] != "NOT_FOUND" {
		t.Fatalf("missing response=%s", lines[4])
	}
}
