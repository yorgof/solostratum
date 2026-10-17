package blocks

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log"
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

type submitAnswer struct {
	reason string
	err    error
}

type fakeNode struct {
	mu       sync.Mutex
	failures int    // transport failures before answering
	reason   string // the node's answer
	rpcErr   error
	calls    int
	onCall   func()
	answers  []submitAnswer // if set, each call returns the next one and then repeats the last
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
	if len(f.answers) > 0 {
		i := f.calls - f.failures - 1
		if i >= len(f.answers) {
			i = len(f.answers) - 1
		}
		return f.answers[i].reason, f.answers[i].err
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

func TestBlockIsSubmittedAndSaved(t *testing.T) {
	node := &fakeNode{}
	s, dir, accepted := open(t, node)
	block := []byte{0xde, 0xad, 0xbe, 0xef}
	s.Found(840000, "00000000aabb", block, "rig1")
	s.Wait()

	recs := s.Records()
	if len(recs) != 1 || recs[0].Status != "accepted" || recs[0].Height != 840000 || recs[0].Worker != "rig1" {
		t.Fatalf("records: %+v", recs)
	}
	if accepted.Load() != 1 {
		t.Errorf("accepted callback ran %d times", accepted.Load())
	}
	files := hexFiles(t, dir)
	if len(files) != 1 {
		t.Fatalf("%d block files, want 1", len(files))
	}
	if raw, _ := os.ReadFile(files[0]); strings.TrimSpace(string(raw)) != hex.EncodeToString(block) {
		t.Errorf("file content %q is not the block", raw)
	}
	name := filepath.Base(files[0])
	if !strings.HasPrefix(name, "block-840000-") || !strings.HasSuffix(name, "-00000000aabb.hex") || recs[0].File != name {
		t.Errorf("file name %s, recorded as %q", name, recs[0].File)
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
	first, err := s.save(1, "abcd", []byte{1}, now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.save(1, "abcd", []byte{2}, now)
	if err != nil {
		t.Fatal(err)
	}
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

// "inconclusive" means the node stored the block without making it the tip.
// A second submission would answer "duplicate", which must not turn a lost
// race into an accepted block.
func TestInconclusiveIsFinal(t *testing.T) {
	node := &fakeNode{answers: []submitAnswer{{reason: "inconclusive"}, {reason: "duplicate"}}}
	s, _, accepted := open(t, node)
	s.Found(100, "aa", []byte{1}, "rig")
	s.Wait()
	if node.calls != 1 || accepted.Load() != 0 || s.Records()[0].Status != "rejected: inconclusive" {
		t.Fatalf("calls %d, accepted %d, status %q", node.calls, accepted.Load(), s.Records()[0].Status)
	}
}

func TestNodeErrors(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		calls  int
		status string
	}{
		// A malformed block or request will not change on retry.
		{"decode", &rpc.Error{Code: rpc.CodeDeserialization, Message: "Block does not start with a coinbase"}, 1, "rejected: Block does not start with a coinbase"},
		{"parameter", &rpc.Error{Code: rpc.CodeInvalidParameter, Message: "bad parameter"}, 1, "rejected: bad parameter"},
		{"method", &rpc.Error{Code: rpc.CodeMethodNotFound, Message: "Method not found"}, 1, "rejected: Method not found"},
		// Anything else may be passing, and giving up would lose the block.
		{"warmup", &rpc.Error{Code: rpc.CodeInWarmup, Message: "Loading block index..."}, 2, "accepted"},
		{"unexpected", &rpc.Error{Code: -1, Message: "Unexpected error"}, 2, "accepted"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			node := &fakeNode{answers: []submitAnswer{{err: c.err}, {reason: ""}}}
			s, _, _ := open(t, node)
			s.Found(100, "aa", []byte{1}, "rig")
			s.Wait()
			if node.calls != c.calls || s.Records()[0].Status != c.status {
				t.Fatalf("calls %d, status %q", node.calls, s.Records()[0].Status)
			}
		})
	}
}

// Only a block whose delivery was interrupted is submitted again at start.
// A recorded answer, or giving up after the retry window, is final.
func TestOnlyInterruptedBlocksAreResumed(t *testing.T) {
	dir := t.TempDir()
	recent := time.Now().UTC().Add(-time.Minute)
	write := func(height int, hash, status string) {
		t.Helper()
		base := fmt.Sprintf("block-%d-%s-%s", height, recent.Format(timeLayout), hash)
		if err := os.WriteFile(filepath.Join(dir, base+".hex"), []byte(hash+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if status != "" {
			if err := os.WriteFile(filepath.Join(dir, base+".result"), []byte(status+"\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	write(100, "aa", "")
	write(101, "bb", "rejected: inconclusive")
	write(102, "cc", "not submitted: connection refused")

	node := &fakeNode{}
	s, err := Open(dir, node, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.Wait()
	got := map[string]string{}
	for _, r := range s.Records() {
		got[r.Hash] = r.Status
	}
	if node.calls != 1 || got["aa"] != "accepted" || got["bb"] != "rejected: inconclusive" || got["cc"] != "not submitted: connection refused" {
		t.Fatalf("calls %d, statuses %v", node.calls, got)
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

func TestGivingUpWhileQueuedKeepsTheLastError(t *testing.T) {
	// The node was unreachable; the budget then runs out while other
	// deliveries hold every slot. The record must name the real problem.
	called := make(chan struct{}, 1)
	node := submitFunc(func(context.Context, []byte) (string, error) {
		select {
		case called <- struct{}{}:
		default:
		}
		return "", errors.New("dial tcp: connection refused")
	})
	s, err := Open(t.TempDir(), node, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.retryEvery, s.retryFor = 50*time.Millisecond, 300*time.Millisecond
	s.Found(100, "aa", []byte{1}, "rig")
	<-called
	for i := 0; i < maxSubmissions; i++ {
		s.submitSlots <- struct{}{}
	}
	t.Cleanup(func() {
		for i := 0; i < maxSubmissions; i++ {
			<-s.submitSlots
		}
	})
	s.Wait()
	status := s.Records()[0].Status
	if !strings.Contains(status, "connection refused") || strings.Contains(status, "behind") {
		t.Fatalf("status %q does not name the node's unreachability", status)
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

// A bug hit while submitting is retried like a connection that failed, and
// leaves the submission slot free for the next block.
func TestPanicWhileSubmittingIsRetried(t *testing.T) {
	logs := captureLog(t)
	var calls atomic.Int64
	node := submitFunc(func(context.Context, []byte) (string, error) {
		if calls.Add(1) <= maxSubmissions {
			panic("bug in the RPC client")
		}
		return "", nil
	})
	s, err := Open(t.TempDir(), node, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.retryEvery = time.Millisecond
	for i := range 2 {
		s.Found(int64(100+i), fmt.Sprintf("%064x", i), []byte{byte(i)}, "rig")
	}
	s.Wait()
	for _, r := range s.Records() {
		if r.Status != "accepted" {
			t.Fatalf("block %s: %q", r.Hash, r.Status)
		}
	}
	if n := calls.Load(); n != maxSubmissions+2 {
		t.Fatalf("%d attempts, want %d", n, maxSubmissions+2)
	}
	if out := logs.String(); !strings.Contains(out, "ERROR: submitting a found block failed: bug in the RPC client") ||
		!strings.Contains(out, "internal error: bug in the RPC client. Retrying.") {
		t.Fatalf("log:\n%s", out)
	}
}

// A bug hit while saving costs the file, never the block: it is submitted
// from memory, as when the disk refuses it.
func TestPanicWhileSavingStillSubmits(t *testing.T) {
	logs := captureLog(t)
	node := &fakeNode{}
	s, dir, accepted := open(t, node)
	s.create = func(string) (*os.File, error) { panic("bug in the disk code") }
	s.Found(100, "aa", []byte{1, 2}, "rig")
	s.Wait()
	records := s.Records()
	if node.calls != 1 || accepted.Load() != 1 || len(records) != 1 || records[0].Status != "accepted" || records[0].File != "" {
		t.Fatalf("calls %d, accepted %d, records %+v", node.calls, accepted.Load(), records)
	}
	if len(hexFiles(t, dir)) != 0 {
		t.Fatal("a block file was written")
	}
	if out := logs.String(); !strings.Contains(out, "ERROR: saving a found block failed: bug in the disk code") {
		t.Fatalf("log:\n%s", out)
	}
}

// A block that could be neither saved nor delivered exists nowhere else, so
// it is logged in full, ready to be submitted by hand.
func TestBlockNeitherSavedNorDeliveredIsLogged(t *testing.T) {
	logs := captureLog(t)
	node := &fakeNode{failures: 1 << 30}
	s, _, _ := open(t, node)
	s.retryFor = 20 * time.Millisecond
	s.create = func(string) (*os.File, error) { return nil, errors.New("disk full") }
	s.Found(100, "aa", []byte{0xc0, 0xff, 0xee}, "rig")
	s.Wait()
	if status := s.Records()[0].Status; !strings.HasPrefix(status, "not submitted") {
		t.Fatalf("status %q", status)
	}
	if out := logs.String(); !strings.Contains(out, "Block aa was neither saved nor delivered. To submit it by hand, run: bitcoin-cli submitblock c0ffee") {
		t.Fatalf("log:\n%s", out)
	}
}

// A bug hit after the node accepted the block changes nothing about the
// block: it stays accepted, with its result file.
func TestPanicAfterAcceptanceKeepsTheAnswer(t *testing.T) {
	logs := captureLog(t)
	node := &fakeNode{}
	dir := filepath.Join(t.TempDir(), "blocks")
	s, err := Open(dir, node, func() { panic("bug in the new work") })
	if err != nil {
		t.Fatal(err)
	}
	s.Found(100, "aa", []byte{1}, "rig")
	s.Wait()
	if status := s.Records()[0].Status; status != "accepted" {
		t.Fatalf("status %q", status)
	}
	result, err := os.ReadFile(strings.TrimSuffix(hexFiles(t, dir)[0], ".hex") + ".result")
	if err != nil || strings.TrimSpace(string(result)) != "accepted" {
		t.Fatalf("result file %q, %v", result, err)
	}
	if out := logs.String(); !strings.Contains(out, "ERROR: delivering a found block failed: bug in the new work") {
		t.Fatalf("log:\n%s", out)
	}
}

// panicError hits a bug as soon as it is described.
type panicError struct{}

func (panicError) Error() string { panic("bug in describing an error") }

// A bug hit anywhere in an attempt, not only in the call to the node, is a
// failed attempt: it is retried within the same budget.
func TestPanicAnywhereInAnAttemptIsRetried(t *testing.T) {
	logs := captureLog(t)
	node := &fakeNode{answers: []submitAnswer{{err: panicError{}}, {reason: ""}}}
	s, dir, accepted := open(t, node)
	s.Found(100, "aa", []byte{1}, "rig")
	s.Wait()
	if node.calls != 2 || accepted.Load() != 1 || s.Records()[0].Status != "accepted" {
		t.Fatalf("calls %d, accepted %d, records %+v", node.calls, accepted.Load(), s.Records())
	}
	if result, err := os.ReadFile(strings.TrimSuffix(hexFiles(t, dir)[0], ".hex") + ".result"); err != nil || strings.TrimSpace(string(result)) != "accepted" {
		t.Fatalf("result file %q, %v", result, err)
	}
	if out := logs.String(); !strings.Contains(out, "ERROR: submitting a found block failed: bug in describing an error") {
		t.Fatalf("log:\n%s", out)
	}
}

// panicOnLog is a log output that hits a bug on a line containing text, to
// put a bug at a chosen place in the delivery.
type panicOnLog struct {
	text string
	out  syncBuffer
}

func (p *panicOnLog) Write(b []byte) (int, error) {
	if bytes.Contains(b, []byte(p.text)) {
		panic("bug while logging")
	}
	return p.out.Write(b)
}

// A bug hit between two attempts, before the node has answered, ends the
// delivery as not submitted and leaves the block without a result file, so
// that the next start delivers it.
func TestPanicBeforeTheAnswerLeavesTheBlockForTheNextStart(t *testing.T) {
	out := &panicOnLog{text: "Retrying."}
	log.SetOutput(out)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	s, dir, accepted := open(t, &fakeNode{failures: 1})
	s.Found(100, "aa", []byte{1}, "rig")
	s.Wait()
	within(t, "the file's goroutine to finish", s.files.Wait)
	rec := s.Records()[0]
	if rec.Status != "not submitted: internal error: bug while logging" || rec.File == "" || accepted.Load() != 0 {
		t.Fatalf("record %+v, accepted %d", rec, accepted.Load())
	}
	if _, err := os.Stat(filepath.Join(dir, strings.TrimSuffix(rec.File, ".hex")+".result")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a result file was written: %v", err)
	}
	if strings.Contains(out.out.String(), "neither saved nor delivered") {
		t.Fatalf("a saved block was reported as not saved:\n%s", out.out.String())
	}

	node := &fakeNode{}
	again, err := Open(dir, node, nil)
	if err != nil {
		t.Fatal(err)
	}
	again.Wait()
	if node.calls != 1 || again.Records()[0].Status != "accepted" {
		t.Fatalf("after a restart: %d calls, records %+v", node.calls, again.Records())
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

// A disk that never answers cannot hold a found block back: Found returns at
// once, the node gets the block straight away, and the delivery finishes
// without waiting for the file, so no result file is written.
func TestStuckDiskDoesNotHoldUpTheBlock(t *testing.T) {
	logs := captureLog(t)
	submitted := make(chan struct{}, 1)
	node := submitFunc(func(context.Context, []byte) (string, error) {
		submitted <- struct{}{}
		return "", nil
	})
	dir := t.TempDir()
	s, err := Open(dir, node, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.saveWait = 50 * time.Millisecond
	stuck := make(chan struct{})
	s.create = func(string) (*os.File, error) {
		<-stuck
		return nil, errors.New("the disk gave up")
	}
	within(t, "handing over a block", func() { s.Found(100, "aa", []byte{1}, "rig") })
	select {
	case <-submitted:
	case <-time.After(5 * time.Second):
		t.Fatal("the node never got the block")
	}
	within(t, "finishing the delivery", s.Wait)
	if rec := s.Records()[0]; rec.Status != "accepted" || rec.File != "" {
		t.Fatalf("record %+v", rec)
	}
	if results, _ := filepath.Glob(filepath.Join(dir, "*.result")); len(results) != 0 {
		t.Fatalf("result files %v", results)
	}
	if out := logs.String(); !strings.Contains(out, "WARNING: the disk has not finished with block aa within 50ms") {
		t.Fatalf("log:\n%s", out)
	}
	close(stuck)
	within(t, "the file's goroutine to finish", s.files.Wait)
}

// A slow disk does not delay a found block either. The node's answer is
// written next to the file once the file is there.
func TestSlowDiskDoesNotDelayTheBlock(t *testing.T) {
	captureLog(t)
	submitted := make(chan struct{}, 1)
	node := submitFunc(func(context.Context, []byte) (string, error) {
		submitted <- struct{}{}
		return "", nil
	})
	dir := t.TempDir()
	s, err := Open(dir, node, nil)
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	create := s.create
	s.create = func(path string) (*os.File, error) {
		<-release
		return create(path)
	}
	s.Found(100, "aa", []byte{1}, "rig")
	select {
	case <-submitted:
	case <-time.After(5 * time.Second):
		t.Fatal("the block waited for the disk")
	}
	close(release)
	within(t, "finishing the delivery", s.Wait)
	rec := s.Records()[0]
	if rec.Status != "accepted" || rec.File == "" {
		t.Fatalf("record %+v", rec)
	}
	if result, err := os.ReadFile(filepath.Join(dir, strings.TrimSuffix(rec.File, ".hex")+".result")); err != nil || strings.TrimSpace(string(result)) != "accepted" {
		t.Fatalf("result file %q, %v", result, err)
	}
}

// A file that the disk finishes only well after the node's answer still gets
// that answer next to it. Otherwise the next start would deliver the block
// again, and a block that lost its race, answered "inconclusive", would come
// back from the node as "duplicate" and be counted as accepted.
func TestLateFileStillGetsTheAnswer(t *testing.T) {
	captureLog(t)
	node := &fakeNode{answers: []submitAnswer{{reason: "inconclusive"}, {reason: "duplicate"}}}
	s, dir, _ := open(t, node)
	s.saveWait = 10 * time.Millisecond
	release := make(chan struct{})
	create := s.create
	s.create = func(path string) (*os.File, error) {
		<-release
		return create(path)
	}
	s.Found(100, "aa", []byte{1}, "rig")
	within(t, "finishing the delivery", s.Wait)
	if rec := s.Records()[0]; rec.Status != "rejected: inconclusive" || rec.File != "" {
		t.Fatalf("record before the file is saved: %+v", rec)
	}
	close(release)
	within(t, "the file's goroutine to finish", s.files.Wait)
	rec := s.Records()[0]
	if rec.File == "" {
		t.Fatal("the late file was not recorded")
	}
	if result, err := os.ReadFile(filepath.Join(dir, strings.TrimSuffix(rec.File, ".hex")+".result")); err != nil || strings.TrimSpace(string(result)) != "rejected: inconclusive" {
		t.Fatalf("result file %q, %v", result, err)
	}

	again, err := Open(dir, node, nil)
	if err != nil {
		t.Fatal(err)
	}
	again.Wait()
	if node.calls != 1 || again.Records()[0].Status != "rejected: inconclusive" {
		t.Fatalf("after a restart: %d calls, records %+v", node.calls, again.Records())
	}
}

// New work after an accepted block is asked for at once, whatever the disk
// does.
func TestNewWorkIsAskedForBeforeTheDiskIsDone(t *testing.T) {
	captureLog(t)
	refreshed := make(chan struct{}, 1)
	s, err := Open(t.TempDir(), &fakeNode{}, func() { refreshed <- struct{}{} })
	if err != nil {
		t.Fatal(err)
	}
	s.saveWait = time.Minute // what a delivery that waited for the disk first would wait
	release := make(chan struct{})
	create := s.create
	s.create = func(path string) (*os.File, error) {
		<-release
		return create(path)
	}
	s.Found(100, "aa", []byte{1}, "rig")
	select {
	case <-refreshed:
	case <-time.After(5 * time.Second):
		t.Fatal("asking for new work waited for the disk")
	}
	// The block has its answer: it is no longer counted as being delivered,
	// though the disk is still busy with it.
	for deadline := time.Now().Add(5 * time.Second); s.Pending() != 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("a block with its answer is still counted as being delivered")
		}
	}
	close(release)
	within(t, "finishing the delivery", s.Wait)
	within(t, "the file's goroutine to finish", s.files.Wait)
}
