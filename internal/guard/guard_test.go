package guard

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yorgof/solostratum/internal/logtest"
)

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
	logs := logtest.Capture(t)
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

// A panic is logged and costs the function that raised it, nothing more.
func TestQueueCarriesOnAfterAPanic(t *testing.T) {
	logs := logtest.Capture(t)
	q := NewQueue("counting things", 10)
	var ran []string
	q.Post(func() { ran = append(ran, "before") })
	q.Post(func() { panic("boom") })
	q.Post(func() { ran = append(ran, "after") })
	q.Close()
	wait(t, q.Done(), "the queue to finish")
	if fmt.Sprint(ran) != "[before after]" {
		t.Fatalf("ran %v, want the functions before and after the panic", ran)
	}
	if out := logs.String(); strings.Count(out, "ERROR: counting things failed: boom") != 1 {
		t.Fatalf("log:\n%s", out)
	}
}

func TestQueueWaiting(t *testing.T) {
	q := NewQueue("testing", 5)
	entered, release := make(chan struct{}), make(chan struct{})
	q.Post(func() { close(entered); <-release })
	wait(t, entered, "the first function to start")
	for range 3 {
		q.Post(func() {})
	}
	if n := q.Waiting(); n != 3 {
		t.Fatalf("%d waiting, want 3: the one under way does not count", n)
	}
	close(release)
	q.Close()
	wait(t, q.Done(), "the queue to finish")
	if n := q.Waiting(); n != 0 {
		t.Fatalf("%d waiting after the queue finished", n)
	}
}

// Go returns at once, and its result can be waited for or not.
func TestGo(t *testing.T) {
	logs := logtest.Capture(t)
	release := make(chan struct{})
	var set int
	done := Go("doing the thing", func() { <-release; set = 1 })
	select {
	case <-done:
		t.Fatal("the result came before f returned")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil || set != 1 {
		t.Fatalf("error %v, set %d", err, set)
	}
	if err := <-Go("doing the other thing", func() { panic("boom") }); err == nil || err.Error() != "internal error: boom" {
		t.Fatalf("a panic: %v", err)
	}
	if out := logs.String(); !strings.Contains(out, "ERROR: doing the other thing failed: boom") {
		t.Fatalf("log:\n%s", out)
	}
	// Nobody waits for this one: it still finishes, without anything stuck.
	finished := make(chan struct{})
	Go("doing it unwatched", func() { close(finished) })
	wait(t, finished, "the unwatched function to finish")
}

// blockedOutput takes nothing until it is released, like a terminal paused
// with Ctrl-S.
type blockedOutput struct {
	entered chan struct{} // closed when the first write starts
	release chan struct{}
	once    sync.Once
	buf     logtest.Buffer
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
	if note := lines[logQueueSize+1]; !strings.HasSuffix(note, "(5 log messages were lost here because the log output did not take them)") {
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

// panickyOutput panics on the writes whose text contains bad.
type panickyOutput struct {
	bad   string
	calls atomic.Int32
	buf   logtest.Buffer
}

func (p *panickyOutput) Write(b []byte) (int, error) {
	p.calls.Add(1)
	if strings.Contains(string(b), p.bad) {
		panic("bug in the output")
	}
	return p.buf.Write(b)
}

// A panic in the output costs that message only. The log goes on, and marks
// the gap, for the line after it may be the last copy of a found block.
func TestLogOutputSurvivesAPanicInTheOutput(t *testing.T) {
	out := &panickyOutput{bad: "second"}
	o := NewLogOutput(out)
	for _, msg := range []string{"first\n", "second\n", "third\n"} {
		o.Write([]byte(msg))
		o.Flush(5 * time.Second) // so that the third message comes after the failed second one
	}
	o.Close(5 * time.Second)
	lines := strings.Split(strings.TrimSuffix(out.buf.String(), "\n"), "\n")
	if len(lines) != 3 || lines[0] != "first" || lines[2] != "third" ||
		!strings.HasSuffix(lines[1], "(1 log messages were lost here because the log output did not take them)") {
		t.Fatalf("output:\n%s", out.buf.String())
	}
}

// A gap that has been noted is not counted again when the message after the
// note fails as well.
func TestLogOutputCountsEachLostMessageOnce(t *testing.T) {
	out := &panickyOutput{bad: "second"}
	o := NewLogOutput(out)
	for _, msg := range []string{"second\n", "second\n", "third\n"} {
		o.Write([]byte(msg))
		o.Flush(5 * time.Second)
	}
	o.Close(5 * time.Second)
	// The first "second" is lost; the note about it is written before the
	// second "second", which is lost too; the note before "third" counts
	// that one only.
	got := out.buf.String()
	if strings.Count(got, "(1 log messages were lost here") != 2 || !strings.HasSuffix(got, "third\n") {
		t.Fatalf("output:\n%s", got)
	}
}

// An output that panics on every message must not keep the log busy for
// ever by reporting each panic to itself.
func TestLogOutputThatAlwaysPanicsStaysQuiet(t *testing.T) {
	out := &panickyOutput{bad: ""} // every text contains ""
	o := NewLogOutput(out)
	for range 3 {
		o.Write([]byte("message\n"))
	}
	within(t, "closing a log whose output always panics", func() { o.Close(5 * time.Second) })
	if n := out.calls.Load(); n != 3 {
		t.Fatalf("%d writes, want one for each message", n)
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
