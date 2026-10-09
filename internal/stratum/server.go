// Package stratum implements the Stratum v1 mining protocol as spoken by
// Bitaxe and other ASIC miners.
package stratum

import (
	"context"
	"crypto/rand"
	"errors"
	"log"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yorgof/solostratum/internal/btc"
	"github.com/yorgof/solostratum/internal/work"
)

// Jobs supplies work. It is implemented by work.Manager.
type Jobs interface {
	Current() *work.Job
	Get(id string) *work.Job
	Healthy() bool
}

// BlockSink receives solved blocks. It is implemented by blocks.Store.
type BlockSink interface {
	Found(height int64, hash string, block []byte, worker string)
}

// Recorder receives every connection and share for the long-term
// statistics. It is implemented by stats.Store.
type Recorder interface {
	Connected(worker string, at time.Time)
	Disconnected(worker string, at time.Time)
	Accepted(worker string, at time.Time, credit, shareDiff float64)
	Rejected(worker string, at time.Time)
}

// Options configures a Server.
type Options struct {
	Network         btc.Network
	DefaultAddress  string
	DefaultScript   []byte
	StartDifficulty float64
	MinDifficulty   float64
}

// Server accepts miner connections and hands out work.
type Server struct {
	opts   Options
	jobs   Jobs
	blocks BlockSink
	stats  Recorder // nil when no statistics are kept

	mu       sync.Mutex
	sessions map[[work.Extranonce1Size]byte]*session
	listener net.Listener

	started  time.Time
	accepted atomic.Uint64
	rejected atomic.Uint64
	bestMu   sync.Mutex
	best     float64
}

const maxSessions = 4096

// NewServer creates a server; call Serve to start it.
func NewServer(opts Options, jobs Jobs, blocks BlockSink) *Server {
	return &Server{
		opts: opts, jobs: jobs, blocks: blocks,
		sessions: make(map[[work.Extranonce1Size]byte]*session),
		started:  time.Now(),
	}
}

// SetJobs sets the job source. It must be called before Serve.
func (s *Server) SetJobs(jobs Jobs) { s.jobs = jobs }

// SetRecorder makes the server report every connection and share to stats.
// It must be called before Serve.
func (s *Server) SetRecorder(stats Recorder) { s.stats = stats }

// Listen opens the listening socket.
func (s *Server) Listen(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.listener = l
	s.mu.Unlock()
	return nil
}

// Addr returns the address the server listens on.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener.Addr()
}

// Serve accepts connections until ctx is cancelled, then closes the miners
// and waits for their in-flight shares and block handoffs to finish.
func (s *Server) Serve(ctx context.Context) error {
	var sessions sync.WaitGroup
	defer sessions.Wait()
	// Close after the accept loop ends so even a connection accepted at
	// the same time as cancellation is included.
	defer s.closeAll()
	go func() {
		<-ctx.Done()
		s.listener.Close()
	}()
	go s.watchNode(ctx)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			log.Printf("Accepting a miner connection failed: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if !s.jobs.Healthy() {
			conn.Close() // lets the miner move on to its fallback pool
			continue
		}
		sess := s.register(conn)
		if sess == nil {
			conn.Close()
			continue
		}
		sessions.Add(1)
		go func() {
			defer sessions.Done()
			sess.run()
			s.unregister(sess)
		}()
	}
}

// watchNode disconnects miners while the node is unreachable. Mining on a
// template that can no longer be refreshed wastes power, and miners with a
// fallback pool configured switch to it once the connection drops.
func (s *Server) watchNode(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if s.jobs.Healthy() {
				continue
			}
			if n := s.closeAll(); n > 0 {
				log.Printf("The node is unreachable: disconnected %d miner(s) so they can use their fallback pool. They can reconnect once the node is back.", n)
			}
		}
	}
}

func (s *Server) register(conn net.Conn) *session {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.sessions) >= maxSessions {
		return nil
	}
	var id [work.Extranonce1Size]byte
	for {
		if _, err := rand.Read(id[:]); err != nil {
			return nil
		}
		if _, taken := s.sessions[id]; !taken {
			break
		}
	}
	sess := newSession(s, conn, id)
	s.sessions[id] = sess
	return sess
}

func (s *Server) unregister(sess *session) {
	s.mu.Lock()
	delete(s.sessions, sess.extranonce1)
	s.mu.Unlock()
}

func (s *Server) closeAll() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sess := range s.sessions {
		sess.close()
	}
	return len(s.sessions)
}

func (s *Server) snapshotSessions() []*session {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*session, 0, len(s.sessions))
	for _, sess := range s.sessions {
		out = append(out, sess)
	}
	return out
}

// Broadcast sends a new job to every miner.
func (s *Server) Broadcast(job *work.Job) {
	for _, sess := range s.snapshotSessions() {
		sess.sendJob(job, job.CleanJobs)
	}
}

func (s *Server) noteBest(diff float64) {
	s.bestMu.Lock()
	if diff > s.best {
		s.best = diff
	}
	s.bestMu.Unlock()
}

// MinerStatus describes one connected miner for the status page.
type MinerStatus struct {
	Worker     string    `json:"worker"`
	Payout     string    `json:"payout"`
	OwnPayout  bool      `json:"ownPayout"` // true if the miner chose its own payout address
	Address    string    `json:"address"`   // network address of the miner
	Client     string    `json:"client"`
	Connected  time.Time `json:"connected"`
	Difficulty float64   `json:"difficulty"`
	Hashrate   float64   `json:"hashrate"` // hashes per second
	Accepted   uint64    `json:"accepted"`
	Rejected   uint64    `json:"rejected"`
	// RejectReasons counts rejected shares by reason, to help diagnose a
	// misbehaving miner.
	RejectReasons map[string]uint64 `json:"rejectReasons,omitempty"`
	BestShare     float64           `json:"bestShare"`
	LastShare     time.Time         `json:"lastShare"`
}

// Status is a snapshot of the server for the status page.
type Status struct {
	Started   time.Time     `json:"started"`
	Hashrate  float64       `json:"hashrate"`
	Accepted  uint64        `json:"accepted"`
	Rejected  uint64        `json:"rejected"`
	BestShare float64       `json:"bestShare"`
	Miners    []MinerStatus `json:"miners"`
}

// Status returns a snapshot of all authorized miners.
func (s *Server) Status() Status {
	st := Status{Started: s.started, Accepted: s.accepted.Load(), Rejected: s.rejected.Load(), Miners: []MinerStatus{}}
	s.bestMu.Lock()
	st.BestShare = s.best
	s.bestMu.Unlock()
	now := time.Now()
	for _, sess := range s.snapshotSessions() {
		if m, ok := sess.status(now); ok {
			st.Miners = append(st.Miners, m)
			st.Hashrate += m.Hashrate
		}
	}
	sort.Slice(st.Miners, func(i, j int) bool {
		if st.Miners[i].Worker != st.Miners[j].Worker {
			return st.Miners[i].Worker < st.Miners[j].Worker
		}
		return st.Miners[i].Connected.Before(st.Miners[j].Connected)
	})
	return st
}
