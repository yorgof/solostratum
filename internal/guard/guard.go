// Package guard keeps a failure in one part of solostratum from stopping the
// parts that matter most: delivering a found block to the node, and serving
// the miners.
//
// Run turns a panic into an ordinary error of the piece of work that raised
// it. Every goroutine runs its work through it: for a loop, each pass; for
// anything else, the whole of it. The code around it then handles the error
// like any other failure of that work, so a bug ends one miner's connection,
// fails one attempt to submit a block or one update of the work, or switches
// off the statistics. A panic that nothing recovers ends the whole program,
// with every miner and any block that is being delivered.
//
// Queue lets code that must never wait, such as a miner's connection, hand
// work to code that might: the statistics, and the log output.
package guard

import (
	"fmt"
	"log"
	"runtime/debug"
	"sync"
)

// Run calls f and returns nil, or, if f panics, an error describing the
// panic. The panic is logged with the stack of the goroutine, so that the
// bug can be found. what names the work in that message, for example
// "serving a miner".
func Run(what string, f func()) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("internal error: %v", r)
			log.Printf("ERROR: %s failed: %v. This is a bug in solostratum; please report it with the lines below.\n%s", what, r, debug.Stack())
		}
	}()
	f()
	return nil
}

// Queue runs the functions posted to it one at a time, in the order they
// were posted, on a goroutine of its own. It is meant for work the program
// can do without. Post never waits: a function that does not fit is
// dropped. So whatever holds up the queue, be it a slow disk, a stuck lock
// or an output nobody reads, costs some of the queued work, never the time
// of the code that posts it.
//
// A panic in a queued function is logged and switches the queue off: the
// functions after it are dropped, since whatever they work on may have been
// left broken.
type Queue struct {
	what  string
	funcs chan func()
	done  chan struct{}

	mu     sync.RWMutex // held for reading while posting, and for writing to close
	closed bool
}

// NewQueue starts a queue that holds up to size functions. what names the
// work in log messages, for example "recording the statistics".
func NewQueue(what string, size int) *Queue {
	q := &Queue{what: what, funcs: make(chan func(), size), done: make(chan struct{})}
	go q.run()
	return q
}

// Post queues f and reports whether it was taken. It returns false at once
// when the queue is full or closed.
func (q *Queue) Post(f func()) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return false
	}
	select {
	case q.funcs <- f:
		return true
	default:
		return false
	}
}

// Close refuses further functions. Those already queued still run.
func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		close(q.funcs)
	}
}

// Done is closed once the queue is closed and every function queued before
// has run or been dropped. A queued function that never returns keeps it
// open, so wait for it with a time limit.
func (q *Queue) Done() <-chan struct{} { return q.done }

func (q *Queue) run() {
	defer close(q.done)
	off := false
	for f := range q.funcs {
		if off {
			continue
		}
		if Run(q.what, f) != nil {
			off = true
			log.Printf("WARNING: %s is switched off until solostratum is restarted. Mining is not affected.", q.what)
		}
	}
}
