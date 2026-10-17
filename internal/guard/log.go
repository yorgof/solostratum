package guard

import (
	"bytes"
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

// logQueueSize is how many messages may wait for the log output. The program
// logs a few lines a minute, so the queue only fills up when the output has
// stopped taking them.
const logQueueSize = 1024

// LogOutput is a log destination that never holds up the code that logs.
// Each message is written to the real output by a Queue. When that output
// takes no more, as with a terminal paused with Ctrl-S, a Windows console in
// which text is being selected, or a log collector that has stopped reading,
// the messages that do not fit are dropped and counted. Left alone, the
// program would stop at its next log line, and that could be the one about a
// found block, before the block is sent.
type LogOutput struct {
	out   io.Writer
	queue *Queue
	lost  atomic.Int64 // messages dropped or failed since the last one that was queued
}

// NewLogOutput starts a LogOutput that writes to out.
func NewLogOutput(out io.Writer) *LogOutput {
	return &LogOutput{out: out, queue: NewQueue("writing the log", logQueueSize)}
}

// Write queues one message. It never waits and never fails.
func (o *LogOutput) Write(p []byte) (int, error) {
	msg := bytes.Clone(p) // the logger reuses p
	lost := o.lost.Swap(0)
	if !o.queue.Post(func() { o.write(lost, msg) }) {
		o.lost.Add(lost + 1)
	}
	return len(p), nil
}

// write writes one message, and first a note where messages were dropped.
//
// A panic in the output costs this message only, and the note on the next
// one counts it. The log keeps no state that a panic could leave broken, so
// it must never fall silent: its last lines may be all that is left of a
// found block. The panic is not reported, nor left to the queue, which would
// report it to this very log: an output that panics on every message would
// then keep the queue busy for ever.
func (o *LogOutput) write(lost int64, msg []byte) {
	defer func() {
		if recover() != nil {
			o.lost.Add(lost + 1)
		}
	}()
	if lost > 0 {
		fmt.Fprintf(o.out, "%s(%d log messages were lost here because the log output did not take them)\n", time.Now().Format("2006/01/02 15:04:05 "), lost)
		lost = 0 // noted: a panic below costs this message alone
	}
	o.out.Write(msg)
}

// Flush waits until the messages logged so far have been written, at most
// for timeout, for output that must come after them.
func (o *LogOutput) Flush(timeout time.Duration) {
	written := make(chan struct{})
	if !o.queue.Post(func() { close(written) }) {
		return
	}
	select {
	case <-written:
	case <-time.After(timeout):
	}
}

// Close writes the messages still queued, waiting at most timeout for the
// output to take them. Messages logged afterwards are dropped.
func (o *LogOutput) Close(timeout time.Duration) {
	o.queue.Close()
	select {
	case <-o.queue.Done():
	case <-time.After(timeout):
	}
}
