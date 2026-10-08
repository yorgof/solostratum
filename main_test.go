package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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
		{"unusable history storage", "regtest", "stats_dir", "file", "cannot use stats_dir"},
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
