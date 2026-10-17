package stratum

import (
	"log"
	"sync/atomic"
	"time"

	"github.com/yorgof/solostratum/internal/guard"
)

// statsQueueSize is how many connections and shares the statistics may fall
// behind before more are dropped. Recording one takes microseconds, so the
// queue only fills up when the statistics have stopped answering.
const statsQueueSize = maxSessions

// queuedRecorder passes every call on to the statistics through a queue
// served by a goroutine of its own, so that a miner's connection never waits
// for them. Neither a slow disk nor a stuck lock nor a bug in the statistics
// can then delay a share, a found block or the next job; the statistics
// switch themselves off after a bug (see stats.Store).
//
// While they do not keep up, the calls that do not fit are dropped, and that
// is logged when it starts and when it ends. Shares may fill only half of the
// queue: the rest is kept for connections and disconnections, which are few,
// and a lost disconnection would count its worker as online until the next
// restart.
type queuedRecorder struct {
	rec       Recorder
	queue     *guard.Queue
	shareRoom int          // how many waiting calls shares may add to
	dropped   atomic.Int64 // calls dropped since the statistics last kept up
}

func newQueuedRecorder(rec Recorder, size int) *queuedRecorder {
	return &queuedRecorder{rec: rec, queue: guard.NewQueue("recording the statistics", size), shareRoom: size / 2}
}

// post queues call: a share if share is set, else a connection or a
// disconnection.
func (q *queuedRecorder) post(call func(), share bool) {
	if (share && q.queue.Waiting() >= q.shareRoom) || !q.queue.Post(call) {
		if q.dropped.Add(1) == 1 {
			log.Printf("WARNING: the statistics are not keeping up, so some connections and shares are not being counted. Mining is not affected.")
		}
		return
	}
	// Only a share shows that the statistics keep up again: connections and
	// disconnections still fit when shares no longer do.
	if share && q.dropped.Load() > 0 {
		if n := q.dropped.Swap(0); n > 0 {
			log.Printf("The statistics are keeping up again. %d connections and shares were not counted meanwhile.", n)
		}
	}
}

func (q *queuedRecorder) Connected(worker string, at time.Time) {
	q.post(func() { q.rec.Connected(worker, at) }, false)
}

func (q *queuedRecorder) Disconnected(worker string, at time.Time) {
	q.post(func() { q.rec.Disconnected(worker, at) }, false)
}

func (q *queuedRecorder) Accepted(worker string, at time.Time, credit, shareDiff float64) {
	q.post(func() { q.rec.Accepted(worker, at, credit, shareDiff) }, true)
}

func (q *queuedRecorder) Rejected(worker string, at time.Time) {
	q.post(func() { q.rec.Rejected(worker, at) }, true)
}
