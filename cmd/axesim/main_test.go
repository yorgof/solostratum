package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"log"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// commandPool uses the wire protocol, so the command is exercised through
// negotiation, work parsing, hashing and its actual submission counters.
func commandPool(t *testing.T, mode string, stop chan struct{}) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := l.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		cleanup := context.AfterFunc(ctx, func() { conn.Close() })
		defer cleanup()
		dec, enc := json.NewDecoder(conn), json.NewEncoder(conn)
		for {
			var req struct {
				ID     int
				Method string
			}
			if dec.Decode(&req) != nil {
				return
			}
			var result, failure any = true, nil
			switch req.Method {
			case "mining.configure":
				result = map[string]any{"version-rolling": true, "version-rolling.mask": "1fffe000"}
			case "mining.subscribe":
				result = []any{[]any{}, "00000001", 4}
			case "mining.submit":
				if mode == "submit disconnect" {
					return
				}
				close(stop)
				if mode == "rejected" {
					result, failure = nil, []any{23, "low difficulty", nil}
				}
			}
			enc.Encode(map[string]any{"id": req.ID, "result": result, "error": failure})
			if req.Method == "mining.extranonce.subscribe" && mode == "no job" {
				return
			}
			if req.Method == "mining.authorize" && mode != "no job" {
				difficulty := 1e-12
				if mode == "no share" {
					difficulty = 1e308
				}
				enc.Encode(map[string]any{"method": "mining.set_difficulty", "params": []any{difficulty}})
				enc.Encode(map[string]any{"method": "mining.notify", "params": []any{
					"one", strings.Repeat("00", 32), "0102", "0304", []string{}, "20000000", "207fffff", "6553f100", true,
				}})
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		l.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("command test pool did not stop")
		}
	})
	return l.Addr().String()
}

func TestMineOutcomes(t *testing.T) {
	for _, mode := range []string{"accepted", "rejected", "submit disconnect", "no job", "no share"} {
		t.Run(mode, func(t *testing.T) {
			stop := make(chan struct{})
			addr := commandPool(t, mode, stop)
			var accepted, rejected, hashes atomic.Uint64
			done := make(chan error, 1)
			go func() { done <- mine(addr, "rig", 0, stop, &accepted, &rejected, &hashes) }()
			if mode == "no share" {
				deadline := time.Now().Add(5 * time.Second)
				for hashes.Load() == 0 && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
				close(stop)
				if hashes.Load() == 0 {
					t.Error("mining never completed a batch")
				}
			}
			select {
			case err := <-done:
				wantError := mode == "submit disconnect" || mode == "no job"
				if wantError != (err != nil) {
					t.Fatalf("error %v, want error %v", err, wantError)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("miner did not stop")
			}
			var wantAccepted, wantRejected uint64
			if mode == "accepted" {
				wantAccepted = 1
			}
			if mode == "rejected" {
				wantRejected = 1
			}
			if accepted.Load() != wantAccepted || rejected.Load() != wantRejected {
				t.Fatalf("counts: %d accepted, %d rejected", accepted.Load(), rejected.Load())
			}
		})
	}
}

func TestCommandDurationStopsReconnectWait(t *testing.T) {
	args, flags, output := os.Args, flag.CommandLine, log.Writer()
	t.Cleanup(func() { os.Args, flag.CommandLine = args, flags; log.SetOutput(output) })
	var logs bytes.Buffer
	log.SetOutput(&logs)
	os.Args = []string{"axesim", "-addr", "127.0.0.1:65536", "-duration", "20ms"}
	flag.CommandLine = flag.NewFlagSet("axesim", flag.ContinueOnError)
	main()
	if !strings.Contains(logs.String(), "done: 0 accepted, 0 rejected") {
		t.Fatalf("missing completion report: %s", logs.String())
	}
}
