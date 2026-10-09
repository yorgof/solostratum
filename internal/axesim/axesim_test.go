package axesim

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

type poolRequest struct {
	ID     int
	Method string
	Params []json.RawMessage
}

func pool(t *testing.T, respond func(net.Conn, poolRequest) bool) string {
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
		stop := context.AfterFunc(ctx, func() { conn.Close() })
		defer stop()
		decoder := json.NewDecoder(conn)
		for {
			var req poolRequest
			if decoder.Decode(&req) != nil || !respond(conn, req) {
				return
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		l.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("test pool did not stop")
		}
	})
	return l.Addr().String()
}

func answer(conn net.Conn, id int, result, err any) {
	json.NewEncoder(conn).Encode(map[string]any{"id": id, "result": result, "error": err})
}

func handshakeResult(method string) any {
	switch method {
	case "mining.configure":
		return map[string]any{"version-rolling": true, "version-rolling.mask": "1fffe000"}
	case "mining.subscribe":
		return []any{[]any{}, "aabbccdd", 4}
	default:
		return true
	}
}

func notifyParams() []any {
	return []any{"job-1", strings.Repeat("00", 32), "0102", "0304", []string{}, "20000000", "207fffff", "6553f100", true}
}

func TestDialMineAndSubmit(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy=%v", legacy), func(t *testing.T) {
			var submits int
			addr := pool(t, func(conn net.Conn, req poolRequest) bool {
				if req.Method == "mining.submit" {
					want := 6
					if legacy {
						want = 5
					}
					if len(req.Params) != want || string(req.Params[0]) != `"rig\"one"` {
						t.Errorf("submit parameters: %s", req.Params)
					}
					submits++
					if submits == 1 {
						// Replies and notifications can arrive in either order.
						fmt.Fprintln(conn, `{"method":"mining.set_version_mask","params":["00002000"]}`)
						fmt.Fprintln(conn, `{"method":"mining.set_extranonce","params":["01020304",4]}`)
						answer(conn, req.ID, true, nil)
					} else {
						answer(conn, req.ID, nil, []any{22, "duplicate share", nil})
					}
					return true
				}
				answer(conn, req.ID, handshakeResult(req.Method), nil)
				if req.Method == "mining.authorize" {
					fmt.Fprintln(conn, `not JSON`)
					fmt.Fprintln(conn, `{"id":999,"result":true}`)
					fmt.Fprintln(conn, `{"method":"mining.notify","params":[]}`)
					fmt.Fprintln(conn, `{"method":"mining.set_difficulty","params":[0.0000000001]}`)
					json.NewEncoder(conn).Encode(map[string]any{"method": "mining.notify", "params": notifyParams()})
				}
				return true
			})
			m, err := Dial(addr, Options{User: "rig\"one", Password: "x", SuggestDifficulty: 1, ExtranonceSub: true, NoVersionRolling: legacy})
			if err != nil {
				t.Fatal(err)
			}
			defer m.Close()
			job, err := m.WaitJob(0, time.Second, func(j *Job) bool { return j.CleanJobs })
			if err != nil {
				t.Fatal(err)
			}
			if m.Extranonce1() != "aabbccdd" || m.Difficulty() != 1e-10 || len(m.Notifies()) != 1 {
				t.Fatal("handshake or job notifications were not applied")
			}
			share, found := m.Mine(job, 0, 1)
			if !found || share.JobID != job.ID || share.Extranonce2 != "01000000" || share.Hash != SHA256d(share.Header[:]) {
				t.Fatalf("invalid mined share %+v", share)
			}
			if res, err := m.Submit(share); err != nil || !res.Accepted {
				t.Fatalf("accepted submit: %+v, %v", res, err)
			}
			if m.Extranonce1() != "01020304" || m.VersionMask() != 0x2000 {
				t.Fatal("mid-session updates were not applied")
			}
			if res, err := m.Submit(share); err != nil || res.Accepted || res.Code != 22 || res.Message != "duplicate share" {
				t.Fatalf("rejected submit: %+v, %v", res, err)
			}
			m.Close()
			if _, err := m.WaitJob(job.Seq, time.Second, nil); err == nil || !strings.Contains(err.Error(), "closed") || !m.Closed() {
				t.Fatalf("closed connection: %v", err)
			}
			if _, err := m.Submit(share); err == nil {
				t.Fatal("submit on a closed connection succeeded")
			}
		})
	}
}

func TestHandshakeFailures(t *testing.T) {
	for _, tc := range []struct {
		name, method string
		result       any
	}{
		{"configure disabled", "mining.configure", map[string]any{"version-rolling": false}},
		{"configure malformed", "mining.configure", true},
		{"invalid mask", "mining.configure", map[string]any{"version-rolling": true, "version-rolling.mask": "xyz"}},
		{"short subscribe", "mining.subscribe", []any{}},
		{"bad extranonce type", "mining.subscribe", []any{[]any{}, 3, 4}},
		{"bad size type", "mining.subscribe", []any{[]any{}, "aa", "four"}},
		{"authorization rejected", "mining.authorize", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := pool(t, func(conn net.Conn, req poolRequest) bool {
				result := handshakeResult(req.Method)
				if req.Method == tc.method {
					result = tc.result
				}
				answer(conn, req.ID, result, nil)
				return true
			})
			if m, err := Dial(addr, Options{Timeout: time.Second}); err == nil || !strings.Contains(err.Error(), tc.method) {
				if m != nil {
					m.Close()
				}
				t.Fatalf("error %v, want failure at %s", err, tc.method)
			}
		})
	}
	for _, method := range []string{"mining.configure", "mining.subscribe", "mining.authorize", "mining.suggest_difficulty", "mining.extranonce.subscribe"} {
		t.Run("disconnected/"+method, func(t *testing.T) {
			addr := pool(t, func(conn net.Conn, req poolRequest) bool {
				if req.Method == method {
					return false
				}
				answer(conn, req.ID, handshakeResult(req.Method), nil)
				return true
			})
			if m, err := Dial(addr, Options{Timeout: time.Second, SuggestDifficulty: 1, ExtranonceSub: true}); err == nil || !strings.Contains(err.Error(), method) {
				if m != nil {
					m.Close()
				}
				t.Fatalf("error %v, want failure at %s", err, method)
			}
		})
	}
	t.Run("timeout", func(t *testing.T) {
		addr := pool(t, func(net.Conn, poolRequest) bool { return true })
		if m, err := Dial(addr, Options{Timeout: 100 * time.Millisecond}); err == nil || !strings.Contains(err.Error(), "timed out") {
			if m != nil {
				m.Close()
			}
			t.Fatalf("silent server: %v", err)
		}
	})
}

func TestParseNotifyAndMineFailures(t *testing.T) {
	params := notifyParams()
	decode := func(p []any) []json.RawMessage {
		raw, _ := json.Marshal(p)
		var out []json.RawMessage
		json.Unmarshal(raw, &out)
		return out
	}
	if _, err := parseNotify(decode(params[:7])); err == nil {
		t.Fatal("short notification accepted")
	}
	for _, index := range []int{0, 4, 5, 6, 7} {
		bad := append([]any(nil), params...)
		bad[index] = 42
		if _, err := parseNotify(decode(bad)); err == nil {
			t.Fatalf("wrong parameter type at %d accepted", index)
		}
		if index >= 5 {
			bad[index] = "not hex"
			if _, err := parseNotify(decode(bad)); err == nil {
				t.Fatalf("invalid hex at %d accepted", index)
			}
		}
	}
	job, err := parseNotify(decode(params))
	if err != nil {
		t.Fatal(err)
	}
	for _, mask := range []uint32{0, 0x2000} {
		m := &Miner{extranonce1: "01020304", en2Len: 4, versionMask: mask}
		for _, attempts := range []int{0, 1, 4097} {
			if _, found := m.Mine(job, math.Inf(1), attempts); found {
				t.Fatal("found an impossible-difficulty share")
			}
		}
		for _, change := range []func(*Job){
			func(j *Job) { j.Coinbase1 = "zz" },
			func(j *Job) { j.PrevHash = "01" },
			func(j *Job) { j.PrevHash = strings.Repeat("zz", 32) },
			func(j *Job) { j.Branches = []string{"01"} },
			func(j *Job) { j.Branches = []string{strings.Repeat("zz", 32)} },
		} {
			bad := *job
			change(&bad)
			if _, found := m.Mine(&bad, 0, 1); found {
				t.Fatal("malformed job produced a share")
			}
		}
	}
}

func TestWaitJobTimeoutAndSequence(t *testing.T) {
	m := &Miner{job: &Job{ID: "one", Seq: 1}}
	m.cond = sync.NewCond(&m.mu)
	for _, test := range []struct {
		after int
		ok    func(*Job) bool
	}{{1, nil}, {0, func(*Job) bool { return false }}} {
		if _, err := m.WaitJob(test.after, 10*time.Millisecond, test.ok); err == nil || !strings.Contains(err.Error(), "timed out") {
			t.Fatalf("old or filtered job was returned: %v", err)
		}
	}
}

func TestHashAndVersionBoundaries(t *testing.T) {
	var hash [32]byte
	if HashDifficulty(hash) != math.MaxUint32 {
		t.Fatal("zero hash difficulty must be finite")
	}
	hash[26], hash[27] = 0xff, 0xff
	if HashDifficulty(hash) != 1 || (&Job{NBits: 0x1d00ffff}).NetworkDifficulty() != 1 {
		t.Fatal("difficulty-one target decoded incorrectly")
	}
	if DisplayHash(hash) != "00000000ffff"+strings.Repeat("00", 26) {
		t.Fatal("display hash has incorrect byte order")
	}
	// A fixed digest catches shared mistakes in header/hash assertions.
	if got := SHA256d(nil); hex.EncodeToString(got[:]) != "5df6e0e2761359d30a8275058e299fcc0381534545f55cf43e41983f5d4c9456" {
		t.Fatalf("double SHA256 of empty input: %x", got)
	}
	for _, tc := range []struct{ value, mask, want uint32 }{
		{0x20000000, 0, 0x20000000}, {0x20000000, 0xa000, 0x20002000},
		{0x20002000, 0xa000, 0x20008000}, {0x20008000, 0xa000, 0x2000a000},
		{0x2000a000, 0xa000, 0x20000000},
		{0xffffffff, 0xffffffff, 0},
	} {
		if got := incrementBitmask(tc.value, tc.mask); got != tc.want {
			t.Errorf("increment(%x, %x) = %x, want %x", tc.value, tc.mask, got, tc.want)
		}
	}
}
