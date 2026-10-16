package stratum

import (
	"log"
	"runtime/debug"
	"sync/atomic"
	"time"
)

// guardedRecorder hands every event to the statistics and keeps a failure
// there away from the miners. The statistics run on the miner's own
// connection, and a panic in a goroutine ends the whole program, with any
// block that is being delivered. A panic in the statistics is logged once,
// with its stack, and switches the recording off until the next restart.
type guardedRecorder struct {
	rec Recorder
	off atomic.Bool
}

// recoverPanic is deferred by every method below.
func (g *guardedRecorder) recoverPanic() {
	r := recover()
	if r == nil {
		return
	}
	if !g.off.Swap(true) {
		log.Printf("WARNING: the statistics code failed (%v) and is switched off until solostratum is restarted. Mining is not affected.\n%s", r, debug.Stack())
	}
}

func (g *guardedRecorder) Connected(worker string, at time.Time) {
	if g.off.Load() {
		return
	}
	defer g.recoverPanic()
	g.rec.Connected(worker, at)
}

func (g *guardedRecorder) Disconnected(worker string, at time.Time) {
	if g.off.Load() {
		return
	}
	defer g.recoverPanic()
	g.rec.Disconnected(worker, at)
}

func (g *guardedRecorder) Accepted(worker string, at time.Time, credit, shareDiff float64) {
	if g.off.Load() {
		return
	}
	defer g.recoverPanic()
	g.rec.Accepted(worker, at, credit, shareDiff)
}

func (g *guardedRecorder) Rejected(worker string, at time.Time) {
	if g.off.Load() {
		return
	}
	defer g.recoverPanic()
	g.rec.Rejected(worker, at)
}
