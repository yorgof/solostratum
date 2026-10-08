package work

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/yorgof/solostratum/internal/btc"
	"github.com/yorgof/solostratum/internal/rpc"
)

const (
	// tipPollInterval is how often the chain tip is checked. It backs up the
	// long poll, which normally reports a new block first.
	tipPollInterval = 2 * time.Second
	// refreshInterval is how often a new job is issued for the same tip, to
	// pick up new transactions and a current timestamp.
	refreshInterval = 30 * time.Second
	// longPollWait bounds a single long-poll request.
	longPollWait = 3 * time.Minute
	// nodeTimeout is how long the node may be unreachable before miners are
	// disconnected so they can switch to their fallback pool.
	nodeTimeout = 90 * time.Second
	// keepJobs is how many jobs for the current tip stay valid. Pools
	// conventionally accept work on any job for the current tip, and slow
	// miners rely on that; 240 jobs cover two hours without a new block.
	keepJobs = 240
)

// Node is the part of the RPC client the manager needs.
type Node interface {
	GetBlockchainInfo(ctx context.Context) (rpc.ChainInfo, error)
	GetBlockTemplate(ctx context.Context, longPollID string, wait time.Duration) (*rpc.BlockTemplate, error)
	ProposeBlock(ctx context.Context, block []byte) (string, error)
}

// NodeState describes the node as last seen, for the status page.
type NodeState struct {
	Chain      string    `json:"chain"`
	Connected  bool      `json:"connected"`
	Error      string    `json:"error,omitempty"`
	Height     int64     `json:"height"`     // height of the block being mined
	Difficulty float64   `json:"difficulty"` // network difficulty
	TxCount    int       `json:"txCount"`
	RewardSats int64     `json:"rewardSats"`
	LastJob    time.Time `json:"lastJob"`
}

// Manager keeps the current job up to date with the node.
type Manager struct {
	node  Node
	tag   string
	chain string

	mu       sync.RWMutex
	current  *Job
	jobs     map[string]*Job
	order    []string
	nextID   uint64
	longPoll string
	lastErr  string
	lastOK   time.Time // last time a usable template was received
	tipGen   uint64    // counts published tip changes

	fetchMu  sync.Mutex          // serializes template processing
	txCache  map[btc.Hash][]byte // newest job's transactions by wtxid; guarded by fetchMu
	onJob    func(*Job)
	refreshC chan struct{}
}

// NewManager creates a manager. onJob is called, in order, for every new job.
func NewManager(node Node, chain, tag string, onJob func(*Job)) *Manager {
	return &Manager{
		node: node, tag: tag, chain: chain, onJob: onJob,
		jobs:     make(map[string]*Job),
		refreshC: make(chan struct{}, 1),
	}
}

// Current returns the newest job, or nil before the first template arrives.
func (m *Manager) Current() *Job {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.current
}

// Get returns a recent job by id.
func (m *Manager) Get(id string) *Job {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.jobs[id]
}

// State reports the node status for display.
func (m *Manager) State() NodeState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s := NodeState{Chain: m.chain, Error: m.lastErr, Connected: m.lastErr == "" && m.current != nil}
	if j := m.current; j != nil {
		s.Height, s.Difficulty, s.TxCount, s.RewardSats, s.LastJob = j.Height, j.NetDiff, j.TxCount, j.Value, j.Created
	}
	return s
}

// Refresh asks for a new template right away, for example after a block was
// found.
func (m *Manager) Refresh() {
	select {
	case m.refreshC <- struct{}{}:
	default:
	}
}

// Fetch gets a template from the node and publishes a job if it is news.
func (m *Manager) Fetch(ctx context.Context) error {
	return m.fetch(ctx, "")
}

func (m *Manager) fetch(ctx context.Context, longPollID string) error {
	m.mu.RLock()
	gen := m.tipGen
	m.mu.RUnlock()
	t, err := m.node.GetBlockTemplate(ctx, longPollID, longPollWait)
	if err != nil {
		// The regular poll is the authority on node health; a failed long
		// poll is simply retried.
		if longPollID == "" && ctx.Err() == nil {
			m.setError(err)
		}
		return err
	}
	return m.publish(t, gen)
}

func isTimeout(err error) bool {
	var t interface{ Timeout() bool }
	return errors.As(err, &t) && t.Timeout() || errors.Is(err, context.DeadlineExceeded)
}

// setOK records that the node delivered a usable template.
func (m *Manager) setOK() {
	m.mu.Lock()
	recovered := m.lastErr != ""
	m.lastErr = ""
	m.lastOK = time.Now()
	m.mu.Unlock()
	if recovered {
		log.Printf("Node connection restored.")
	}
}

// Healthy reports whether the node has delivered usable work recently. When
// it has not, miners are better served by their fallback pool.
func (m *Manager) Healthy() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.current != nil && time.Since(m.lastOK) < nodeTimeout
}

func (m *Manager) setError(err error) {
	msg := rpc.Explain(err)
	m.mu.Lock()
	changed := m.lastErr != msg
	m.lastErr = msg
	m.mu.Unlock()
	if changed {
		log.Printf("Node problem: %s. Retrying; miners keep their current work.", msg)
	}
}

// publish turns a template into the current job unless it is stale. gen is
// the tip generation at the time the template was requested.
func (m *Manager) publish(t *rpc.BlockTemplate, gen uint64) error {
	m.fetchMu.Lock()
	defer m.fetchMu.Unlock()

	prev, err := btc.HashFromDisplayHex(t.PreviousBlockHash)
	if err != nil {
		m.setError(err)
		return err
	}
	m.mu.Lock()
	cur := m.current
	raced := gen != m.tipGen
	m.nextID++
	seq := m.nextID
	m.mu.Unlock()

	newTip := cur == nil || cur.PrevHash != prev
	if newTip && raced {
		// The tip changed while this request was under way, so this answer
		// may describe a tip that is already gone. Heights cannot settle
		// it (a reorganisation may lead to a lower height), so ask again.
		m.Refresh()
		return nil
	}
	if !newTip && (uint32(t.CurTime) < cur.CurTime || time.Since(cur.Created) < time.Second) {
		// An older answer overtaken by a newer one, or the long poll and
		// the regular poll answering together.
		m.mu.Lock()
		m.longPoll = t.LongPollID
		m.mu.Unlock()
		m.setOK()
		return nil
	}
	job, txCache, err := newJob(strconv.FormatUint(seq, 16), t, m.tag, time.Now(), m.txCache)
	m.txCache = txCache
	if err != nil {
		m.setError(err)
		return err
	}
	job.Seq = seq
	m.setOK()
	job.CleanJobs = newTip

	m.mu.Lock()
	if newTip {
		// Work on the old tip is worthless now.
		m.jobs = make(map[string]*Job)
		m.order = m.order[:0]
	}
	m.jobs[job.ID] = job
	m.order = append(m.order, job.ID)
	for len(m.order) > keepJobs {
		delete(m.jobs, m.order[0])
		m.order = m.order[1:]
	}
	m.current = job
	m.longPoll = t.LongPollID
	if newTip {
		m.tipGen++
	}
	m.mu.Unlock()

	if newTip {
		log.Printf("New block to mine: height %d, %d transactions, reward %s BTC, network difficulty %s",
			job.Height, job.TxCount, formatBTC(job.Value), formatDiff(job.NetDiff))
	}
	m.onJob(job)
	return nil
}

// Run keeps jobs current until ctx is cancelled.
func (m *Manager) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.longPollLoop(ctx)
	}()

	ticker := time.NewTicker(tipPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case <-m.refreshC:
			m.Fetch(ctx)
		case <-ticker.C:
			cur := m.Current()
			if cur == nil || time.Since(cur.Created) >= refreshInterval {
				m.Fetch(ctx)
				continue
			}
			info, err := m.node.GetBlockchainInfo(ctx)
			if err != nil {
				if ctx.Err() == nil {
					m.setError(err)
				}
				continue
			}
			// Fetch right away on a new tip, and after a failure so that
			// recovery is noticed without waiting for the next refresh.
			tip, err := btc.HashFromDisplayHex(info.BestBlockHash)
			if (err == nil && tip != cur.PrevHash) || m.State().Error != "" {
				m.Fetch(ctx)
			}
		}
	}
}

// longPollLoop waits on the node's long poll, which answers the moment a new
// block arrives.
func (m *Manager) longPollLoop(ctx context.Context) {
	for ctx.Err() == nil {
		m.mu.RLock()
		id := m.longPoll
		m.mu.RUnlock()
		if id == "" {
			sleep(ctx, time.Second)
			continue
		}
		if err := m.fetch(ctx, id); err != nil && !isTimeout(err) {
			sleep(ctx, 5*time.Second) // the regular poll reports the problem
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// SelfTest builds a complete block from the current template and asks the
// node to validate everything except the proof of work. It catches any
// mismatch between this program and the node before hash power is spent.
func (m *Manager) SelfTest(ctx context.Context, payoutScript []byte) error {
	var reason string
	for attempt := 0; attempt < 3; attempt++ {
		if err := m.Fetch(ctx); err != nil {
			return fmt.Errorf("could not get a block template: %s", rpc.Explain(err))
		}
		job := m.Current()
		coinbase := job.Coinbase(payoutScript, make([]byte, Extranonce1Size), make([]byte, Extranonce2Size))
		header := job.Header(coinbase, job.Version, job.CurTime, 0)
		var err error
		reason, err = m.node.ProposeBlock(ctx, job.Block(header, coinbase))
		if err != nil {
			return fmt.Errorf("could not ask the node to check a test block: %s", rpc.Explain(err))
		}
		if reason == "" {
			return nil
		}
		// The chain tip may have moved between the two calls; try again.
		sleep(ctx, time.Second)
	}
	return &SelfTestError{Reason: reason}
}

// SelfTestError means the node found a block built by this program invalid.
// Mining must not start in that case.
type SelfTestError struct{ Reason string }

func (e *SelfTestError) Error() string {
	return fmt.Sprintf("the node rejected a test block built by this program (reason: %s). Mining was not started, because blocks found this way would be lost. Please report this problem", e.Reason)
}

func formatBTC(sats int64) string {
	return strconv.FormatFloat(float64(sats)/1e8, 'f', 8, 64)
}

func formatDiff(d float64) string {
	units := []struct {
		v float64
		s string
	}{{1e15, "P"}, {1e12, "T"}, {1e9, "G"}, {1e6, "M"}, {1e3, "k"}}
	for _, u := range units {
		if d >= u.v {
			return strconv.FormatFloat(d/u.v, 'f', 2, 64) + u.s
		}
	}
	return strconv.FormatFloat(d, 'g', 4, 64)
}
