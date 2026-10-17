package work

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yorgof/solostratum/internal/rpc"
)

type loopNode struct {
	info     func(context.Context) (rpc.ChainInfo, error)
	template func(context.Context, string) (*rpc.BlockTemplate, error)
	proposal func(context.Context, []byte) (string, error)
}

func (n loopNode) GetBlockchainInfo(ctx context.Context) (rpc.ChainInfo, error) {
	return n.info(ctx)
}
func (n loopNode) GetBlockTemplate(ctx context.Context, id string, _ time.Duration) (*rpc.BlockTemplate, error) {
	return n.template(ctx, id)
}
func (n loopNode) ProposeBlock(ctx context.Context, block []byte) (string, error) {
	return n.proposal(ctx, block)
}

func waitForJob(t *testing.T, jobs <-chan *Job, height int64) {
	t.Helper()
	select {
	case job := <-jobs:
		if job.Height != height {
			t.Fatalf("height %d, want %d", job.Height, height)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("no job at height %d", height)
	}
}

func runManager(t *testing.T, m *Manager) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { m.Run(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("manager did not drain its long poll")
		}
	})
	return cancel
}

func TestManagerRunRefreshAndRecover(t *testing.T) {
	var templates, polls atomic.Int32
	failed := make(chan struct{}, 1)
	node := loopNode{
		template: func(ctx context.Context, id string) (*rpc.BlockTemplate, error) {
			if id != "" {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			n := templates.Add(1)
			return template(byte(n), int64(99+n), 1000, 0), nil
		},
		info: func(context.Context) (rpc.ChainInfo, error) {
			if polls.Add(1) == 1 {
				failed <- struct{}{}
				return rpc.ChainInfo{}, errors.New("node temporarily unavailable")
			}
			// The same tip still needs a fresh template after a failure.
			return rpc.ChainInfo{BestBlockHash: strings.Repeat("02", 32)}, nil
		},
	}
	jobs := make(chan *Job, 10)
	m := NewManager(node, "regtest", "", func(j *Job) { jobs <- j })
	if err := m.Fetch(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitForJob(t, jobs, 100)
	runManager(t, m)
	m.Refresh()
	waitForJob(t, jobs, 101)
	select {
	case <-failed:
	case <-time.After(5 * time.Second):
		t.Fatal("regular health poll did not run")
	}
	waitForJob(t, jobs, 102)
	if state := m.State(); !state.Connected || state.Error != "" || !m.Healthy() {
		t.Fatalf("node did not recover: %+v", state)
	}
}

func TestLongPollRetriesTimeoutAndPublishes(t *testing.T) {
	var calls atomic.Int32
	blocked := make(chan struct{})
	node := loopNode{
		template: func(ctx context.Context, id string) (*rpc.BlockTemplate, error) {
			if id == "" {
				return template(1, 100, 1000, 0), nil
			}
			switch calls.Add(1) {
			case 1:
				return nil, fmt.Errorf("long poll: %w", context.DeadlineExceeded)
			case 2:
				return template(2, 101, 1100, 0), nil
			default:
				close(blocked)
				<-ctx.Done()
				return nil, ctx.Err()
			}
		},
		info: func(context.Context) (rpc.ChainInfo, error) {
			return rpc.ChainInfo{BestBlockHash: strings.Repeat("02", 32)}, nil
		},
	}
	jobs := make(chan *Job, 10)
	m := NewManager(node, "regtest", "", func(j *Job) { jobs <- j })
	if err := m.Fetch(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitForJob(t, jobs, 100)
	cancel := runManager(t, m)
	waitForJob(t, jobs, 101)
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("long poll did not resume after publishing")
	}
	if !m.State().Connected {
		t.Fatal("normal long-poll timeout marked the node offline")
	}
	cancel()
}

func TestSelfTestRPCFailures(t *testing.T) {
	node := loopNode{
		template: func(context.Context, string) (*rpc.BlockTemplate, error) { return template(1, 100, 1000, 0), nil },
		proposal: func(context.Context, []byte) (string, error) { return "", errors.New("proposal disconnected") },
	}
	m := NewManager(node, "regtest", "", func(*Job) {})
	if err := m.SelfTest(context.Background(), payout); err == nil || !strings.Contains(err.Error(), "proposal disconnected") {
		t.Fatalf("proposal failure: %v", err)
	}
	node.template = func(context.Context, string) (*rpc.BlockTemplate, error) {
		return nil, errors.New("template disconnected")
	}
	m = NewManager(node, "regtest", "", func(*Job) {})
	if err := m.SelfTest(context.Background(), payout); err == nil || !strings.Contains(err.Error(), "template disconnected") {
		t.Fatalf("template failure: %v", err)
	}
	if got := (&SelfTestError{Reason: "bad-txnmrklroot"}).Error(); !strings.Contains(got, "bad-txnmrklroot") || !strings.Contains(got, "Mining was not started") {
		t.Fatalf("missing actionable validation failure: %q", got)
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

func captureLog(t *testing.T) *syncBuffer {
	t.Helper()
	out := &syncBuffer{}
	log.SetOutput(out)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	return out
}

// A bug hit while updating the work counts as a failed update: it is
// reported like a node problem, and the next update tries again.
func TestPanicUpdatingTheWorkIsAFailedUpdate(t *testing.T) {
	logs := captureLog(t)
	var broken atomic.Bool
	var templates atomic.Int32
	node := loopNode{
		template: func(ctx context.Context, id string) (*rpc.BlockTemplate, error) {
			if id != "" {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			if broken.Load() {
				panic("bug in reading a template")
			}
			n := templates.Add(1)
			return template(byte(n), int64(99+n), 1000, 0), nil
		},
		info: func(context.Context) (rpc.ChainInfo, error) {
			return rpc.ChainInfo{BestBlockHash: strings.Repeat(fmt.Sprintf("%02x", templates.Load()), 32)}, nil
		},
	}
	jobs := make(chan *Job, 10)
	m := NewManager(node, "regtest", "", func(j *Job) { jobs <- j })
	if err := m.Fetch(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitForJob(t, jobs, 100)
	runManager(t, m)

	broken.Store(true)
	m.Refresh()
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(m.State().Error, "internal error: bug in reading a template") {
		if time.Now().After(deadline) {
			t.Fatalf("state after the bug: %+v", m.State())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if m.State().Connected {
		t.Fatal("a failed update left the node shown as connected")
	}
	broken.Store(false)
	m.Refresh()
	waitForJob(t, jobs, 101)
	if state := m.State(); !state.Connected || state.Error != "" {
		t.Fatalf("the work did not recover: %+v", state)
	}
	if out := logs.String(); !strings.Contains(out, "ERROR: updating the work failed: bug in reading a template") {
		t.Fatalf("log:\n%s", out)
	}
}

// A bug hit in the long poll counts as a failed long poll. The regular
// updates go on, and the node is not shown as having a problem.
func TestPanicInTheLongPollIsContained(t *testing.T) {
	logs := captureLog(t)
	var templates atomic.Int32
	polled := make(chan struct{}, 1)
	node := loopNode{
		template: func(ctx context.Context, id string) (*rpc.BlockTemplate, error) {
			if id != "" {
				select {
				case polled <- struct{}{}:
				default:
				}
				panic("bug in the long poll")
			}
			n := templates.Add(1)
			return template(byte(n), int64(99+n), 1000, 0), nil
		},
		info: func(context.Context) (rpc.ChainInfo, error) {
			return rpc.ChainInfo{BestBlockHash: strings.Repeat(fmt.Sprintf("%02x", templates.Load()), 32)}, nil
		},
	}
	jobs := make(chan *Job, 10)
	m := NewManager(node, "regtest", "", func(j *Job) { jobs <- j })
	if err := m.Fetch(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitForJob(t, jobs, 100)
	runManager(t, m)
	select {
	case <-polled:
	case <-time.After(5 * time.Second):
		t.Fatal("the long poll did not start")
	}
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(logs.String(), "ERROR: waiting for a new block failed: bug in the long poll") {
		if time.Now().After(deadline) {
			t.Fatalf("log:\n%s", logs.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
	m.Refresh()
	waitForJob(t, jobs, 101)
	if state := m.State(); !state.Connected || state.Error != "" {
		t.Fatalf("a failed long poll was reported as a node problem: %+v", state)
	}
}

// within fails the test unless f returns. It is for calls that must never
// wait: a broken version of them blocks forever.
func within(t *testing.T, what string, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); f() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s blocked", what)
	}
}

// A bug hit while the manager's lock is held must not leave it held: every
// miner, the broadcast of new work and the health check need it.
func TestPanicUnderTheManagerLockReleasesIt(t *testing.T) {
	captureLog(t)
	var templates atomic.Int32
	node := loopNode{template: func(context.Context, string) (*rpc.BlockTemplate, error) {
		n := templates.Add(1)
		return template(byte(n), int64(99+n), 1000, 0), nil
	}}
	m := NewManager(node, "regtest", "", func(*Job) {})
	if err := m.Fetch(context.Background()); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	m.jobs["broken"] = nil // a bug: dropping old jobs now panics
	m.mu.Unlock()
	within(t, "a failed update", func() { m.update(func() { m.Fetch(context.Background()) }) })
	within(t, "reading the current job", func() { m.Current() })
	if state := m.State(); !strings.Contains(state.Error, "internal error") {
		t.Fatalf("state after the bug: %+v", state)
	}
}
