package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"os"
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

func TestServeExecutableSyncFailureUnknownAndRecovery(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sync-failure.kv")
	exe := filepath.Join(dir, "kvdb-syncfail.exe")
	if out, err := exec.Command("go", "build", "-tags", "kvdb_syncfail", "-o", exe, ".").CombinedOutput(); err != nil {
		t.Fatalf("build injected executable: %v\n%s", err, out)
	}

	start := func(fail bool) (*exec.Cmd, io.WriteCloser, *bufio.Scanner, *bufio.Scanner) {
		cmd := exec.Command(exe, "serve", dbPath)
		if fail {
			cmd.Env = append(os.Environ(), "KVDB_FAIL_SYNC_ONCE=1")
		}
		in, err := cmd.StdinPipe()
		if err != nil {
			t.Fatal(err)
		}
		out, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		errout, err := cmd.StderrPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		stderr := bufio.NewScanner(errout)
		if !stderr.Scan() || stderr.Text() != "READY" {
			t.Fatalf("server did not become ready")
		}
		return cmd, in, bufio.NewScanner(out), stderr
	}

	cmd, in, out, _ := start(false)
	_, _ = io.WriteString(in, `{"id":"ok","op":"PUT","key":"YQ==","value":"MQ=="}`+"\n")
	if !out.Scan() || !strings.Contains(out.Text(), `"status":"ACK"`) {
		t.Fatalf("acknowledged write response: %q", out.Text())
	}
	_ = in.Close()
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}

	cmd, in, out, _ = start(true)
	_, _ = io.WriteString(in, `{"id":"uncertain","op":"PUT","key":"Yg==","value":"Mg=="}`+"\n")
	if !out.Scan() {
		t.Fatal("missing UNKNOWN response")
	}
	if got := out.Text(); !strings.Contains(got, `"status":"ERROR"`) || !strings.Contains(got, `"outcome":"UNKNOWN"`) || strings.Contains(got, `"status":"ACK"`) {
		t.Fatalf("unexpected sync failure response: %q", got)
	}
	_ = in.Close()
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if out.Scan() {
		t.Fatalf("server continued after UNKNOWN: %q", out.Text())
	}

	cmd, in, out, _ = start(false)
	_, _ = io.WriteString(in, `{"id":1,"op":"DUMP"}`+"\n")
	if !out.Scan() {
		t.Fatal("missing recovery dump")
	}
	var dump map[string]any
	if err := json.Unmarshal([]byte(out.Text()), &dump); err != nil {
		t.Fatal(err)
	}
	entries, ok := dump["entries"].([]any)
	if !ok || len(entries) != 1 {
		t.Fatalf("recovered entries=%v", dump["entries"])
	}
	_ = in.Close()
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}

	cmd, in, out, _ = start(false)
	_, _ = io.WriteString(in, `{"id":2,"op":"PUT","key":"Yw==","value":"Mw=="}`+"\n")
	if !out.Scan() || !strings.Contains(out.Text(), `"status":"ACK"`) {
		t.Fatalf("post-recovery write response: %q", out.Text())
	}
	_ = in.Close()
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestServeExecutableSyncFailureBatchIsNotPartiallyRecovered(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sync-batch.kv")
	exe := filepath.Join(dir, "kvdb-syncfail.exe")
	if out, err := exec.Command("go", "build", "-tags", "kvdb_syncfail", "-o", exe, ".").CombinedOutput(); err != nil {
		t.Fatalf("build injected executable: %v\n%s", err, out)
	}
	cmd := exec.Command(exe, "serve", dbPath)
	cmd.Env = append(os.Environ(), "KVDB_FAIL_SYNC_ONCE=1")
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	outPipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	errPipe, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	ready := bufio.NewScanner(errPipe)
	if !ready.Scan() || ready.Text() != "READY" {
		t.Fatal("server did not become ready")
	}
	_, _ = io.WriteString(in, `{"id":1,"op":"BATCH","operations":[{"op":"PUT","key":"eA==","value":"MQ=="},{"op":"PUT","key":"eQ==","value":"Mg=="}]}`+"\n")
	scan := bufio.NewScanner(outPipe)
	if !scan.Scan() || !strings.Contains(scan.Text(), `"outcome":"UNKNOWN"`) {
		t.Fatalf("batch response=%q", scan.Text())
	}
	_ = in.Close()
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}

	check := exec.Command(exe, "check", dbPath)
	if output, err := check.CombinedOutput(); err != nil {
		t.Fatalf("check after failed batch: %v\n%s", err, output)
	}
	get := exec.Command(exe, "get", dbPath, "x")
	output, err := get.CombinedOutput()
	if err != nil {
		t.Fatalf("get after failed batch: %v\n%s", err, output)
	}
	if strings.TrimSpace(string(output)) != "NOTFOUND" {
		t.Fatalf("failed batch partially visible: %q", output)
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
		`{"id":"6","op":"COMPACT"}`,
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
	if err = json.Unmarshal([]byte(lines[5]), &response); err != nil || response["status"] != "ACK" {
		t.Fatalf("compact response=%s", lines[5])
	}
}
