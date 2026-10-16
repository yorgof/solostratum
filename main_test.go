package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yorgof/solostratum/internal/config"
	"github.com/yorgof/solostratum/internal/rpc"
	"github.com/yorgof/solostratum/internal/work"
)

const testPayout = "bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080"

func settingsFile(t *testing.T, nodeURL string, overrides map[string]string) string {
	t.Helper()
	values := map[string]string{
		"node_url": nodeURL, "node_user": "u", "node_password": "p", "payout_address": testPayout,
		"stratum_listen": "127.0.0.1:0", "status_listen": "127.0.0.1:0",
	}
	for key, value := range overrides {
		values[key] = value
	}
	var text strings.Builder
	for key, value := range values {
		text.WriteString(key + " = " + value + "\n")
	}
	path := filepath.Join(t.TempDir(), config.FileName)
	if err := os.WriteFile(path, []byte(text.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// startupNode serves just the RPC contract used during startup. Its long
// poll stays in flight until shutdown, exercising cancellation and draining.
func startupNode(t *testing.T, chain string) (*httptest.Server, <-chan struct{}) {
	t.Helper()
	return startupNodeSubmitting(t, chain, nil)
}

// startupNodeSubmitting is startupNode for a node that also takes blocks:
// onSubmit is called with each block it is given, after a pause that stands
// for the time a real node needs to validate one.
func startupNodeSubmitting(t *testing.T, chain string, onSubmit func(blockHex string)) (*httptest.Server, <-chan struct{}) {
	t.Helper()
	polling := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string
			Params []json.RawMessage
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		var result any
		switch req.Method {
		case "getblockchaininfo":
			result = rpc.ChainInfo{Chain: chain, Blocks: 99, BestBlockHash: strings.Repeat("01", 32)}
		case "getblocktemplate":
			var params struct{ Mode, LongPollID string }
			if len(req.Params) != 1 || json.Unmarshal(req.Params[0], &params) != nil {
				t.Error("invalid template parameters")
				return
			}
			if params.LongPollID != "" {
				select {
				case polling <- struct{}{}:
				default:
				}
				<-r.Context().Done()
				return
			}
			if params.Mode != "proposal" {
				result = rpc.BlockTemplate{Version: 0x20000000, PreviousBlockHash: strings.Repeat("01", 32),
					Bits: "207fffff", Height: 100, CurTime: time.Now().Unix(), CoinbaseValue: 5000000000, LongPollID: "tip-1"}
			}
		case "submitblock":
			if onSubmit == nil || len(req.Params) != 1 {
				t.Error("unexpected submitblock")
				return
			}
			var blockHex string
			json.Unmarshal(req.Params[0], &blockHex)
			time.Sleep(200 * time.Millisecond)
			onSubmit(blockHex)
		default:
			t.Errorf("unexpected RPC method %q", req.Method)
		}
		json.NewEncoder(w).Encode(map[string]any{"result": result, "error": nil, "id": 1})
	}))
	t.Cleanup(srv.Close)
	return srv, polling
}

func TestRunCheckAndShutdown(t *testing.T) {
	for _, tc := range []struct {
		name      string
		checkOnly bool
		disabled  bool
	}{{"check", true, false}, {"services enabled", false, false}, {"optional services disabled", false, true}} {
		t.Run(tc.name, func(t *testing.T) {
			node, polling := startupNode(t, "regtest")
			overrides := map[string]string{}
			if tc.disabled {
				overrides["stats_dir"], overrides["status_listen"] = "", ""
			}
			path := settingsFile(t, node.URL, overrides)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- run(ctx, path, tc.checkOnly) }()
			if !tc.checkOnly {
				select {
				case <-polling:
					cancel()
				case err := <-done:
					t.Fatalf("stopped before starting services: %v", err)
				case <-time.After(5 * time.Second):
					t.Fatal("services did not start")
				}
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("shutdown did not finish")
			}
		})
	}
}

func TestStartupFailures(t *testing.T) {
	for _, tc := range []struct {
		name, chain, key, value, want string
	}{
		{"unknown chain", "future", "", "", "unknown network"},
		{"signet", "signet", "", "", "cannot be mined this way"},
		{"wrong payout network", "main", "", "", "not usable on the node's network"},
		{"invalid config", "regtest", "unexpected", "value", "problem in settings file"},
		{"unusable block storage", "regtest", "blocks_dir", "file", "cannot use blocks_dir"},
		{"invalid miner port", "regtest", "stratum_listen", "127.0.0.1:65536", "cannot listen for miners"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node, _ := startupNode(t, tc.chain)
			overrides := map[string]string{}
			if tc.key != "" {
				overrides[tc.key] = tc.value
			}
			path := settingsFile(t, node.URL, overrides)
			if err := os.WriteFile(filepath.Join(filepath.Dir(path), "file"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := run(ctx, path, false); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v, want %q", err, tc.want)
			}
		})
	}
	if err := run(context.Background(), filepath.Join(t.TempDir(), "missing.conf"), true); err == nil || !strings.Contains(err.Error(), "settings file not found") {
		t.Fatalf("missing explicit config: %v", err)
	}
}

func TestHealthcheck(t *testing.T) {
	for _, tc := range []struct{ name, body, want string }{
		{"healthy", `{"node":{"connected":true}}`, ""},
		{"unhealthy", `{"node":{"connected":false,"error":"node offline"}}`, "node offline"},
		{"malformed", `broken`, "unreadable status answer"},
		{"empty report", `{}`, "node problem"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/status" {
					t.Errorf("unexpected path %q", r.URL.Path)
				}
				w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			_, port, _ := net.SplitHostPort(srv.Listener.Addr().String())
			path := settingsFile(t, "http://localhost:8332", map[string]string{"status_listen": "0.0.0.0:" + port})
			err := runHealthcheck(path)
			if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("error %v, want %q", err, tc.want)
			}
			srv.Close()
			if err := runHealthcheck(path); err == nil || !strings.Contains(err.Error(), "not answering") {
				t.Fatalf("stopped server: %v", err)
			}
		})
	}
	path := settingsFile(t, "http://localhost:8332", map[string]string{"status_listen": ""})
	if err := runHealthcheck(path); err == nil || !strings.Contains(err.Error(), "needs the status page") {
		t.Fatalf("disabled status: %v", err)
	}
	if err := runHealthcheck(filepath.Join(t.TempDir(), "missing")); err == nil || !strings.Contains(err.Error(), "problem in settings") {
		t.Fatalf("missing config: %v", err)
	}
}

func TestWaitForNodeFailures(t *testing.T) {
	for _, tc := range []struct {
		name, body, want string
		status           int
	}{
		{"syncing", `{"result":{"initialblockdownload":true,"blocks":1,"headers":2}}`, "still downloading", 200},
		{"login", `denied`, "cannot use the node", 401},
		{"forbidden", `denied`, "cannot use the node", 403},
		{"busy", `busy`, "Cannot reach the node", 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			node := rpc.New(srv.URL, "u", "p", "")
			if _, err := waitForNode(context.Background(), node, srv.URL, true); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("check: %v, want %q", err, tc.want)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := waitForNode(ctx, node, srv.URL, false); err == nil || !strings.Contains(err.Error(), "stopped while waiting") {
				t.Fatalf("cancelled wait: %v", err)
			}
		})
	}
	node := rpc.New("http://localhost:8332", "", "", filepath.Join(t.TempDir(), "missing.cookie"))
	if _, err := waitForNode(context.Background(), node, "local", false); err == nil || !strings.Contains(err.Error(), "cookie file not found") {
		t.Fatalf("missing cookie: %v", err)
	}
}

func TestSelfTestFailureAndCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	m := work.NewManager(rpc.New(srv.URL, "u", "p", ""), "regtest", "", func(*work.Job) {})
	if err := selfTest(context.Background(), m, nil, true); err == nil || !strings.Contains(err.Error(), "could not get a block template") {
		t.Fatalf("check: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := selfTest(ctx, m, nil, false); err == nil || !strings.Contains(err.Error(), "stopped while waiting") {
		t.Fatalf("cancelled self-test: %v", err)
	}
}

func TestCreateConfigAndDisplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), config.FileName)
	if err := createConfig(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != config.Example {
		t.Fatalf("created config differs from the documented example: %v", err)
	}
	if err := createConfig(filepath.Join(path, "blocked")); err == nil {
		t.Fatal("could not report config write failure")
	}
	for listen, want := range map[string]string{"127.0.0.1:3333": "127.0.0.1", "[::1]:3333": "::1", "miner.local:3333": "miner.local"} {
		if got := displayHost(listen); got != want {
			t.Errorf("displayHost(%q) = %q, want %q", listen, got, want)
		}
	}
	if chainName("main") != "mainnet" || chainName("regtest") != "regtest" {
		t.Fatal("incorrect chain display names")
	}
}

func TestCommandFlags(t *testing.T) {
	node, _ := startupNode(t, "regtest")
	path := settingsFile(t, node.URL, nil)
	status := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"node":{"connected":true}}`))
	}))
	defer status.Close()
	healthPath := settingsFile(t, node.URL, map[string]string{"status_listen": status.Listener.Addr().String()})
	for _, tc := range []struct {
		name, want string
		args       []string
	}{
		{"version", "solostratum " + version, []string{"-version"}},
		{"check", "Everything looks good.", []string{"-config", path, "-check"}},
		{"healthcheck", "healthy", []string{"-config", healthPath, "-healthcheck"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args, flags, stdout, signals := os.Args, flag.CommandLine, os.Stdout, restoreSignals
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			defer w.Close()
			t.Cleanup(func() {
				os.Args, flag.CommandLine, os.Stdout, restoreSignals = args, flags, stdout, signals
			})
			os.Args = append([]string{"solostratum"}, tc.args...)
			flag.CommandLine = flag.NewFlagSet("solostratum", flag.ContinueOnError)
			os.Stdout = w
			main()
			w.Close()
			output, err := io.ReadAll(r)
			if err != nil || strings.TrimSpace(string(output)) != tc.want {
				t.Fatalf("output %q, error %v, want %q", output, err, tc.want)
			}
		})
	}
}

// syncBuffer collects log output from several goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureLog returns what the program logs until the test ends.
func captureLog(t *testing.T) *syncBuffer {
	t.Helper()
	out := &syncBuffer{}
	log.SetOutput(out)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	return out
}

// Statistics and the status page are not worth stopping the mining for: a
// problem with either is logged and the program starts without it.
func TestStatisticsAndStatusPageAreOptional(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	for _, tc := range []struct {
		name      string
		overrides map[string]string
		warning   string
	}{
		{"unusable stats_dir", map[string]string{"stats_dir": "file"}, "WARNING: cannot use stats_dir"},
		{"status port taken", map[string]string{"status_listen": taken.Addr().String()}, "WARNING: cannot serve the status page"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureLog(t)
			node, polling := startupNode(t, "regtest")
			path := settingsFile(t, node.URL, tc.overrides)
			if err := os.WriteFile(filepath.Join(filepath.Dir(path), "file"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- run(ctx, path, false) }()
			select {
			case <-polling: // the node connection is up: mining has started
				cancel()
			case err := <-done:
				t.Fatalf("stopped instead of mining: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("services did not start")
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("shutdown did not finish")
			}
			if out := logs.String(); !strings.Contains(out, tc.warning) || !strings.Contains(out, "Mining is not affected") {
				t.Fatalf("no warning in the log:\n%s", out)
			}
		})
	}

	// -check does not mine, so there a folder that cannot be used is reported.
	node, _ := startupNode(t, "regtest")
	path := settingsFile(t, node.URL, map[string]string{"stats_dir": "file"})
	if err := os.WriteFile(filepath.Join(filepath.Dir(path), "file"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), path, true); err == nil || !strings.Contains(err.Error(), "cannot use stats_dir") {
		t.Fatalf("-check with an unusable stats_dir: %v", err)
	}
}

// A block left over from the last run is resumed the moment the block store
// opens. If the start then fails, for whatever reason, the program must not
// quit before the node has answered.
func TestFoundBlockIsDeliveredEvenWhenStartupFails(t *testing.T) {
	var mu sync.Mutex
	var submitted []string
	node, _ := startupNodeSubmitting(t, "regtest", func(blockHex string) {
		mu.Lock()
		defer mu.Unlock()
		submitted = append(submitted, blockHex)
	})
	blocksDir := t.TempDir()
	name := "block-101-" + time.Now().UTC().Format("20060102T150405.000000000Z") + "-" + strings.Repeat("ab", 32)
	if err := os.WriteFile(filepath.Join(blocksDir, name+".hex"), []byte("00\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	path := settingsFile(t, node.URL, map[string]string{"blocks_dir": blocksDir, "stratum_listen": taken.Addr().String()})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := run(ctx, path, false); err == nil || !strings.Contains(err.Error(), "cannot listen for miners") {
		t.Fatalf("run: %v, want a failure to listen", err)
	}
	// run has returned: the node must already have answered.
	mu.Lock()
	got := append([]string(nil), submitted...)
	mu.Unlock()
	if len(got) != 1 || got[0] != "00" {
		t.Fatalf("the node was given %q, want the one leftover block", got)
	}
	result, err := os.ReadFile(filepath.Join(blocksDir, name+".result"))
	if err != nil || strings.TrimSpace(string(result)) != "accepted" {
		t.Fatalf("result file %q, %v: run quit before the delivery finished", result, err)
	}
}

func TestGuardedContainsAPanic(t *testing.T) {
	logs := captureLog(t)
	guarded("The thing", func() { panic("boom") })
	if out := logs.String(); !strings.Contains(out, "The thing failed (boom)") || !strings.Contains(out, "Mining is not affected") {
		t.Fatalf("log:\n%s", out)
	}
	ran := false
	guarded("The other thing", func() { ran = true })
	if !ran {
		t.Fatal("f was not run")
	}
}

func TestSaveStatistics(t *testing.T) {
	t.Run("writes after the writer has finished", func(t *testing.T) {
		writer := make(chan struct{})
		flushed := make(chan struct{})
		returned := make(chan struct{})
		go func() {
			saveStatistics(writer, func() { close(flushed) }, 5*time.Second)
			close(returned)
		}()
		select {
		case <-flushed:
			t.Fatal("written before the writer's own last write")
		case <-time.After(50 * time.Millisecond):
		}
		close(writer)
		for _, ch := range []chan struct{}{flushed, returned} {
			select {
			case <-ch:
			case <-time.After(5 * time.Second):
				t.Fatal("the last write did not happen")
			}
		}
	})
	t.Run("gives up on a stuck disk", func(t *testing.T) {
		logs := captureLog(t)
		writer := make(chan struct{})
		close(writer)
		stuck := make(chan struct{})
		t.Cleanup(func() { close(stuck) })
		returned := make(chan struct{})
		go func() {
			saveStatistics(writer, func() { <-stuck }, 100*time.Millisecond)
			close(returned)
		}()
		select {
		case <-returned:
		case <-time.After(5 * time.Second):
			t.Fatal("a stuck disk held up the end of the program")
		}
		if out := logs.String(); !strings.Contains(out, "could not be saved within") {
			t.Fatalf("log:\n%s", out)
		}
	})
	t.Run("survives a panic", func(t *testing.T) {
		captureLog(t)
		writer := make(chan struct{})
		close(writer)
		saveStatistics(writer, func() { panic("boom") }, 5*time.Second)
	})
}
