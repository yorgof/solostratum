package work

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
