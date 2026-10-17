package stratum

import (
	"log"
	"sync/atomic"
	"time"

	"github.com/yorgof/solostratum/internal/guard"
)

// statsQueueSize is how many connections and shares the statistics may fall
// behind before more are dropped. Recording one takes microseconds, so the
// queue only fills up when the statistics have stopped answering. It holds
// a disconnection for every miner the server takes.
const statsQueueSize = maxSessions

// queuedRecorder passes every call on to the statistics through a queue
// served by a goroutine of its own, so that a miner's connection never waits
// for them. Neither a slow disk nor a stuck lock nor a bug in the statistics
// can then delay a share, a found block or the next job. A panic in the
// statistics switches the recording off until the next restart. While they do
// not keep up, the calls that do not fit are dropped. Either is logged once.
type queuedRecorder struct {
	rec   Recorder
	queue *guard.Queue
	full  atomic.Bool // dropped calls have been reported
}

func newQueuedRecorder(rec Recorder, size int) *queuedRecorder {
	return &queuedRecorder{rec: rec, queue: guard.NewQueue("recording the statistics", size)}
}

func (q *queuedRecorder) post(call func()) {
	if !q.queue.Post(call) && !q.full.Swap(true) {
		log.Printf("WARNING: the statistics are not keeping up, so some connections and shares are not being counted. Mining is not affected.")
	}
}

func (q *queuedRecorder) Connected(worker string, at time.Time) {
	q.post(func() { q.rec.Connected(worker, at) })
}

func (q *queuedRecorder) Disconnected(worker string, at time.Time) {
	q.post(func() { q.rec.Disconnected(worker, at) })
}

func (q *queuedRecorder) Accepted(worker string, at time.Time, credit, shareDiff float64) {
	q.post(func() { q.rec.Accepted(worker, at, credit, shareDiff) })
}

func (q *queuedRecorder) Rejected(worker string, at time.Time) {
	q.post(func() { q.rec.Rejected(worker, at) })
}
