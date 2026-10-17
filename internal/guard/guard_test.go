package guard

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// syncBuffer collects output from several goroutines.
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

func wait(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestRunTurnsAPanicIntoAnError(t *testing.T) {
	logs := captureLog(t)
	err := Run("doing the thing", func() { panic("boom") })
	if err == nil || err.Error() != "internal error: boom" {
		t.Fatalf("error %v, want the panic as an error", err)
	}
	if out := logs.String(); !strings.Contains(out, "ERROR: doing the thing failed: boom. This is a bug") || !strings.Contains(out, "guard_test.go") {
		t.Fatalf("the panic was not logged with its stack:\n%s", out)
	}
	// A runtime error, as a bug raises it.
	var m map[string]int
	if err := Run("writing to a nil map", func() { m["x"] = 1 }); err == nil || !strings.Contains(err.Error(), "nil map") {
		t.Fatalf("runtime error: %v", err)
	}
	ran := false
	if err := Run("doing nothing wrong", func() { ran = true }); err != nil || !ran {
		t.Fatalf("plain call: ran %v, error %v", ran, err)
	}
}

func TestQueueRunsEverythingInOrder(t *testing.T) {
	q := NewQueue("testing", 100)
	var got []int // only the queue's goroutine appends, until Done
	for i := range 100 {
		if !q.Post(func() { got = append(got, i) }) {
			t.Fatalf("function %d was refused", i)
		}
	}
	q.Close()
	wait(t, q.Done(), "the queue to finish")
	for i, v := range got {
		if v != i {
			t.Fatalf("ran %v, want 0 to 99 in order", got)
		}
	}
	if len(got) != 100 {
		t.Fatalf("ran %d functions, want 100", len(got))
	}
}

// Whatever holds up the queue, posting returns at once: a function that does
// not fit is dropped.
func TestQueueNeverWaits(t *testing.T) {
	q := NewQueue("testing", 2)
	entered, release := make(chan struct{}), make(chan struct{})
	var ran []int
	q.Post(func() { close(entered); <-release; ran = append(ran, 0) })
	wait(t, entered, "the first function to start")
	within(t, "posting to a busy queue", func() {
		for i := 1; i <= 2; i++ {
			if !q.Post(func() { ran = append(ran, i) }) {
				t.Errorf("function %d was refused although it fits", i)
			}
		}
		if q.Post(func() { ran = append(ran, 3) }) {
			t.Error("a function that does not fit was taken")
		}
	})
	close(release)
	q.Close()
	wait(t, q.Done(), "the queue to finish")
	if fmt.Sprint(ran) != "[0 1 2]" {
		t.Fatalf("ran %v, want [0 1 2]", ran)
	}
	if q.Post(func() {}) {
		t.Error("a closed queue took a function")
	}
	q.Close() // a second Close is harmless
}

// A panic is logged once and switches the queue off: what the functions work
// on may have been left broken.
func TestQueueSwitchesOffAfterAPanic(t *testing.T) {
	logs := captureLog(t)
	q := NewQueue("counting things", 10)
	var ran []string
	q.Post(func() { ran = append(ran, "before") })
	q.Post(func() { panic("boom") })
	q.Post(func() { ran = append(ran, "after") })
	q.Close()
	wait(t, q.Done(), "the queue to finish")
	if fmt.Sprint(ran) != "[before]" {
		t.Fatalf("ran %v, want only the function before the panic", ran)
	}
	out := logs.String()
	if !strings.Contains(out, "ERROR: counting things failed: boom") || !strings.Contains(out, "WARNING: counting things is switched off until solostratum is restarted") {
		t.Fatalf("log:\n%s", out)
	}
	if strings.Count(out, "ERROR:") != 1 {
		t.Fatalf("the failure was reported more than once:\n%s", out)
	}
}

// blockedOutput takes nothing until it is released, like a terminal paused
// with Ctrl-S.
type blockedOutput struct {
	entered chan struct{} // closed when the first write starts
	release chan struct{}
	once    sync.Once
	buf     syncBuffer
}

func newBlockedOutput() *blockedOutput {
	return &blockedOutput{entered: make(chan struct{}), release: make(chan struct{})}
}

func (b *blockedOutput) Write(p []byte) (int, error) {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return b.buf.Write(p)
}

func TestLogOutputNeverWaits(t *testing.T) {
	out := newBlockedOutput()
	o := NewLogOutput(out)
	logger := log.New(o, "", 0)
	logger.Print("first")
	wait(t, out.entered, "the first message to reach the output")
	// The output is stuck on the first message. The queue takes as many
	// messages as it holds and drops the rest, without waiting.
	within(t, "logging while the output is blocked", func() {
		for i := range logQueueSize + 5 {
			logger.Printf("message %d", i)
		}
	})
	close(out.release)
	last := fmt.Sprintf("message %d\n", logQueueSize-1)
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(out.buf.String(), last) {
		if time.Now().After(deadline) {
			t.Fatal("the queued messages were not written once the output was free")
		}
		time.Sleep(time.Millisecond)
	}
	logger.Print("after")
	o.Close(5 * time.Second)

	lines := strings.Split(strings.TrimSuffix(out.buf.String(), "\n"), "\n")
	if len(lines) != logQueueSize+3 {
		t.Fatalf("%d lines written, want %d", len(lines), logQueueSize+3)
	}
	if lines[0] != "first" || lines[1] != "message 0" || lines[logQueueSize]+"\n" != last {
		t.Fatalf("the queued messages are not in order: %q, %q, %q", lines[0], lines[1], lines[logQueueSize])
	}
	// The gap is marked where it happened.
	if note := lines[logQueueSize+1]; !strings.HasSuffix(note, "(5 log messages were lost here because the log output was blocked)") {
		t.Fatalf("note %q", note)
	}
	if lines[logQueueSize+2] != "after" {
		t.Fatalf("last line %q", lines[logQueueSize+2])
	}
	// Once closed, messages are dropped quietly.
	if n, err := o.Write([]byte("late\n")); n != 5 || err != nil {
		t.Fatalf("Write after Close: %d, %v", n, err)
	}
}

// Flush lets output that must come after the log wait for it, but not for
// an output that takes nothing.
func TestLogOutputFlush(t *testing.T) {
	out := newBlockedOutput()
	o := NewLogOutput(out)
	o.Write([]byte("logged\n"))
	wait(t, out.entered, "the message to reach the output")
	flushed := make(chan struct{})
	go func() { o.Flush(5 * time.Second); close(flushed) }()
	select {
	case <-flushed:
		t.Fatal("Flush returned before the message was written")
	case <-time.After(50 * time.Millisecond):
	}
	close(out.release)
	wait(t, flushed, "Flush to return")
	if got := out.buf.String(); got != "logged\n" {
		t.Fatalf("after Flush the output holds %q", got)
	}
	o.Close(5 * time.Second)

	blocked := newBlockedOutput()
	t.Cleanup(func() { close(blocked.release) })
	o = NewLogOutput(blocked)
	o.Write([]byte("stuck\n"))
	wait(t, blocked.entered, "the message to reach the output")
	within(t, "flushing while the output is blocked", func() { o.Flush(50 * time.Millisecond) })
}

// countingOutput counts the writes to out and those that failed.
type countingOutput struct {
	out          io.Writer
	calls, fails atomic.Int32
}

func (c *countingOutput) Write(p []byte) (int, error) {
	c.calls.Add(1)
	n, err := c.out.Write(p)
	if err != nil {
		c.fails.Add(1)
	}
	return n, err
}

// An output that has gone away, such as a pipe whose reader has closed,
// fails every write. Each message is still tried, nothing waits, and the
// log does not switch itself off.
func TestLogOutputSurvivesAnOutputThatIsGone(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	t.Cleanup(func() { w.Close() })
	out := &countingOutput{out: w}
	o := NewLogOutput(out)
	within(t, "logging to an output that is gone", func() {
		for range 3 {
			o.Write([]byte("message\n"))
			o.Flush(5 * time.Second)
		}
		o.Close(5 * time.Second)
	})
	if calls, fails := out.calls.Load(), out.fails.Load(); calls != 3 || fails != 3 {
		t.Fatalf("%d writes, %d of them failed; want 3 and 3", calls, fails)
	}
}

// The end of the program cannot be held up by an output that takes nothing.
func TestLogOutputCloseHasATimeLimit(t *testing.T) {
	out := newBlockedOutput()
	t.Cleanup(func() { close(out.release) })
	o := NewLogOutput(out)
	o.Write([]byte("stuck\n"))
	wait(t, out.entered, "the message to reach the output")
	within(t, "closing while the output is blocked", func() { o.Close(50 * time.Millisecond) })
}
