package blocks

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yorgof/solostratum/internal/rpc"
)

type fakeNode struct {
	mu       sync.Mutex
	failures int    // transport failures before answering
	reason   string // the node's answer
	rpcErr   error
	calls    int
	onCall   func()
}

func (f *fakeNode) SubmitBlock(ctx context.Context, block []byte) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.onCall != nil {
		f.onCall()
	}
	if f.calls <= f.failures {
		return "", errors.New("dial tcp: connection refused")
	}
	return f.reason, f.rpcErr
}

func open(t *testing.T, node *fakeNode) (*Store, string, *atomic.Int64) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "blocks")
	var accepted atomic.Int64
	s, err := Open(dir, node, func() { accepted.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	s.retryEvery = time.Millisecond
	return s, dir, &accepted
}

func hexFiles(t *testing.T, dir string) []string {
	t.Helper()
	names, err := filepath.Glob(filepath.Join(dir, "*.hex"))
	if err != nil {
		t.Fatal(err)
	}
	return names
}

func TestBlockIsSavedBeforeSubmitting(t *testing.T) {
	node := &fakeNode{}
	s, dir, accepted := open(t, node)
	block := []byte{0xde, 0xad, 0xbe, 0xef}
	node.onCall = func() {
		// At the moment the node is contacted the file must already hold
		// the complete block.
		files := hexFiles(t, dir)
		if len(files) != 1 {
			t.Errorf("%d block files exist when submitting, want 1", len(files))
			return
		}
		raw, _ := os.ReadFile(files[0])
		if strings.TrimSpace(string(raw)) != hex.EncodeToString(block) {
			t.Errorf("file content %q is not the block", raw)
		}
	}
	s.Found(840000, "00000000aabb", block, "rig1")
	s.Wait()

	recs := s.Records()
	if len(recs) != 1 || recs[0].Status != "accepted" || recs[0].Height != 840000 || recs[0].Worker != "rig1" {
		t.Fatalf("records: %+v", recs)
	}
	if accepted.Load() != 1 {
		t.Errorf("accepted callback ran %d times", accepted.Load())
	}
	name := filepath.Base(hexFiles(t, dir)[0])
	if !strings.HasPrefix(name, "block-840000-") || !strings.HasSuffix(name, "-00000000aabb.hex") {
		t.Errorf("unexpected file name %s", name)
	}
	result, err := os.ReadFile(filepath.Join(dir, strings.TrimSuffix(name, ".hex")+".result"))
	if err != nil || strings.TrimSpace(string(result)) != "accepted" {
		t.Errorf("result file: %q, %v", result, err)
	}
}

// Blocks found in quick succession, even at the same height or with the same
// hash, must each get their own file.
func TestEveryBlockGetsItsOwnFile(t *testing.T) {
	s, dir, _ := open(t, &fakeNode{})
	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			hash := "00000000000000000000aaaa"
			if i%2 == 0 {
				hash = "00000000000000000000bbbb"
			}
			s.Found(840000, hash, []byte{byte(i)}, "rig")
		}(i)
	}
	wg.Wait()
	s.Wait()

	files := hexFiles(t, dir)
	if len(files) != n {
		t.Fatalf("%d files for %d blocks", len(files), n)
	}
	seen := map[string]bool{}
	for _, f := range files {
		raw, _ := os.ReadFile(f)
		seen[strings.TrimSpace(string(raw))] = true
	}
	if len(seen) != n {
		t.Fatalf("only %d distinct blocks on disk, want %d: a file was overwritten", len(seen), n)
	}
}

func TestSameNameIsNeverOverwritten(t *testing.T) {
	s, dir, _ := open(t, &fakeNode{})
	now := time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)
	first, f1, err := s.save(1, "abcd", []byte{1}, now)
	if err != nil {
		t.Fatal(err)
	}
	f1.Close()
	second, f2, err := s.save(1, "abcd", []byte{2}, now)
	if err != nil {
		t.Fatal(err)
	}
	f2.Close()
	if first == second {
		t.Fatal("two blocks were saved under the same name")
	}
	raw, _ := os.ReadFile(filepath.Join(dir, first+".hex"))
	if strings.TrimSpace(string(raw)) != "01" {
		t.Fatalf("first block was overwritten: %q", raw)
	}
	if first != "block-1-20260102T030405.000000006Z-abcd" {
		t.Errorf("unexpected name %q", first)
	}
}

func TestSubmitRetriesUntilNodeIsReachable(t *testing.T) {
	node := &fakeNode{failures: 5}
	s, _, accepted := open(t, node)
	s.Found(100, "aa", []byte{1}, "")
	s.Wait()
	if node.calls != 6 {
		t.Errorf("node contacted %d times, want 6", node.calls)
	}
	if recs := s.Records(); recs[0].Status != "accepted" || accepted.Load() != 1 {
		t.Errorf("status %q after retries", recs[0].Status)
	}
}

func TestSubmitGivesUpEventually(t *testing.T) {
	node := &fakeNode{failures: 1 << 30}
	s, dir, accepted := open(t, node)
	s.retryFor = 20 * time.Millisecond
	s.Found(100, "aa", []byte{1}, "")
	s.Wait()
	if recs := s.Records(); !strings.HasPrefix(recs[0].Status, "not submitted") || accepted.Load() != 0 {
		t.Errorf("status %q", recs[0].Status)
	}
	if len(hexFiles(t, dir)) != 1 {
		t.Error("the block must stay on disk for manual submission")
	}
}

func TestRejectedAndDuplicate(t *testing.T) {
	node := &fakeNode{reason: "high-hash"}
	s, _, accepted := open(t, node)
	s.Found(100, "aa", []byte{1}, "")
	s.Wait()
	if recs := s.Records(); recs[0].Status != "rejected: high-hash" || accepted.Load() != 0 || node.calls != 1 {
		t.Errorf("status %q, %d calls", recs[0].Status, node.calls)
	}

	// The node already having the block counts as success.
	node = &fakeNode{reason: "duplicate"}
	s, _, accepted = open(t, node)
	s.Found(100, "aa", []byte{1}, "")
	s.Wait()
	if recs := s.Records(); recs[0].Status != "accepted" || accepted.Load() != 1 {
		t.Errorf("status %q", recs[0].Status)
	}

	// An error from the node itself (not the network) is final.
	node = &fakeNode{rpcErr: &rpc.Error{Code: -22, Message: "Block decode failed"}}
	s, _, _ = open(t, node)
	s.Found(100, "aa", []byte{1}, "")
	s.Wait()
	if recs := s.Records(); recs[0].Status != "rejected: Block decode failed" || node.calls != 1 {
		t.Errorf("status %q, %d calls", recs[0].Status, node.calls)
	}
}

func TestRecordsSurviveRestart(t *testing.T) {
	node := &fakeNode{}
	s, dir, _ := open(t, node)
	s.Found(100, "aa", []byte{1}, "rig")
	s.Wait()
	node.reason = "bad-prevblk"
	s.Found(101, "bb", []byte{2}, "rig")
	s.Wait()
	os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("unrelated"), 0o644)

	reopened, err := Open(dir, node, nil)
	if err != nil {
		t.Fatal(err)
	}
	recs := reopened.Records()
	if len(recs) != 2 {
		t.Fatalf("%d records after restart, want 2", len(recs))
	}
	if recs[0].Height != 100 || recs[0].Hash != "aa" || recs[0].Status != "accepted" {
		t.Errorf("first record: %+v", recs[0])
	}
	if recs[1].Height != 101 || recs[1].Status != "rejected: bad-prevblk" {
		t.Errorf("second record: %+v", recs[1])
	}
}

// A block whose delivery was cut short by a shutdown is delivered on the
// next start, as long as it is still recent.
func TestInterruptedSubmissionIsResumed(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "blocks")
	os.MkdirAll(dir, 0o755)
	recent := "block-500-" + time.Now().UTC().Add(-time.Minute).Format(timeLayout) + "-aa11"
	old := "block-400-" + time.Now().UTC().Add(-48*time.Hour).Format(timeLayout) + "-bb22"
	os.WriteFile(filepath.Join(dir, recent+".hex"), []byte("c0ffee\n"), 0o644)
	os.WriteFile(filepath.Join(dir, old+".hex"), []byte("deadbeef\n"), 0o644)

	node := &fakeNode{}
	s, err := Open(dir, node, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.Wait()
	if node.calls != 1 {
		t.Fatalf("node contacted %d times, want 1 (only the recent block)", node.calls)
	}
	byHash := map[string]string{}
	for _, r := range s.Records() {
		byHash[r.Hash] = r.Status
	}
	if byHash["aa11"] != "accepted" || byHash["bb22"] != "unknown" {
		t.Errorf("statuses: %v", byHash)
	}
	if raw, _ := os.ReadFile(filepath.Join(dir, recent+".result")); strings.TrimSpace(string(raw)) != "accepted" {
		t.Errorf("result file: %q", raw)
	}
	if s.Pending() != 0 {
		t.Errorf("pending = %d after all deliveries finished", s.Pending())
	}
}

func TestUnwritableDirectoryIsReported(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows permissions are not controlled by Unix mode bits")
	}
	if os.Getuid() == 0 {
		t.Skip("root can write anywhere")
	}
	parent := t.TempDir()
	os.Chmod(parent, 0o500)
	defer os.Chmod(parent, 0o700)
	if _, err := Open(filepath.Join(parent, "blocks"), &fakeNode{}, nil); err == nil {
		t.Fatal("an unwritable blocks folder was accepted")
	}
}

func TestBlockIsDeliveredWhenSavingFails(t *testing.T) {
	node := &fakeNode{}
	s, dir, accepted := open(t, node)
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	s.Found(100, "aa", []byte{1, 2}, "rig")
	s.Wait()
	records := s.Records()
	if node.calls != 1 || accepted.Load() != 1 || len(records) != 1 || records[0].Status != "accepted" || records[0].File != "" {
		t.Fatalf("disk failure prevented delivery: calls %d, accepted %d, records %+v", node.calls, accepted.Load(), records)
	}
}

func TestRestartMixedPendingAndCompleted(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UTC().Add(-time.Minute)
	for i := 0; i < 200; i++ {
		base := fmt.Sprintf("block-%d-%s-%s", 100+i, now.Add(time.Duration(i)*time.Nanosecond).Format(timeLayout), strings.Repeat("0", 64))
		if err := os.WriteFile(filepath.Join(dir, base+".hex"), []byte("deadbeef\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if i > 0 {
			if err := os.WriteFile(filepath.Join(dir, base+".result"), []byte("accepted\n"), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	s, err := Open(dir, &fakeNode{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.Wait()
	if len(s.Records()) != 200 {
		t.Fatal("records lost")
	}
	for i, r := range s.Records() {
		if r.Height != int64(100+i) || r.Status != "accepted" {
			t.Errorf("record %d after recovery: %+v", i, r)
		}
	}
}

type submitFunc func(context.Context, []byte) (string, error)

func (f submitFunc) SubmitBlock(ctx context.Context, block []byte) (string, error) {
	return f(ctx, block)
}

func TestSubmissionBurstLeavesRPCCapacity(t *testing.T) {
	const count = 12
	started := make(chan struct{}, count)
	release := make(chan struct{})
	var calls atomic.Int64
	node := submitFunc(func(context.Context, []byte) (string, error) {
		calls.Add(1)
		started <- struct{}{}
		<-release
		return "", nil
	})
	s, err := Open(t.TempDir(), node, nil)
	if err != nil {
		t.Fatal(err)
	}
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(func() { unblock(); s.Wait() })
	for i := 0; i < count; i++ {
		s.Found(int64(100+i), fmt.Sprintf("%064x", i), []byte{byte(i)}, "rig")
	}
	for i := 0; i < maxSubmissions; i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("block delivery did not start")
		}
	}
	select {
	case <-started:
		t.Fatal("too many simultaneous submitblock requests")
	case <-time.After(50 * time.Millisecond):
	}
	unblock()
	s.Wait()
	if calls.Load() != count || s.Pending() != 0 {
		t.Fatalf("delivered %d of %d blocks; %d pending", calls.Load(), count, s.Pending())
	}
	for _, r := range s.Records() {
		if r.Status != "accepted" {
			t.Errorf("block %s: %s", r.Hash, r.Status)
		}
	}
}

func TestQueuedSubmissionHonorsRetryDeadline(t *testing.T) {
	node := &fakeNode{}
	s, _, _ := open(t, node)
	s.retryFor = 20 * time.Millisecond
	// Keep the RPC slots occupied throughout this block's retry budget.
	for i := 0; i < maxSubmissions; i++ {
		s.submitSlots <- struct{}{}
	}
	t.Cleanup(func() {
		for i := 0; i < maxSubmissions; i++ {
			<-s.submitSlots
		}
		s.Wait()
	})
	s.Found(100, "aa", []byte{1}, "rig")
	done := make(chan struct{})
	go func() { s.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("a queued block waited beyond its retry budget")
	}
	if node.calls != 0 || s.Records()[0].Status != "not submitted: waited 20ms behind other block deliveries" {
		t.Fatalf("queued block: %d calls, records %+v", node.calls, s.Records())
	}
}

func TestAttemptInFlightAtTheDeadlineIsNotCutShort(t *testing.T) {
	// The node takes longer to validate the block than the retry budget
	// allows. Cancelling the request now could lose an accepted block.
	node := submitFunc(func(ctx context.Context, _ []byte) (string, error) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(100 * time.Millisecond):
			return "", nil
		}
	})
	s, err := Open(t.TempDir(), node, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.retryEvery, s.retryFor = time.Millisecond, 20*time.Millisecond
	s.Found(100, "aa", []byte{1}, "rig")
	s.Wait()
	if status := s.Records()[0].Status; status != "accepted" {
		t.Fatalf("status %q, want accepted", status)
	}
}
