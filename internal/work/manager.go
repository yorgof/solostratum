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
	"github.com/yorgof/solostratum/internal/guard"
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
	// keepJobs is how many jobs for the current tip stay valid for ordinary
	// shares. Pools conventionally accept work on any job for the current
	// tip, and slow miners rely on that; 240 jobs cover two hours without
	// a new block.
	keepJobs = 240
	// retiredJobGrace is how long a job that no longer pays ordinary shares
	// is kept, so a block solved on it that is still on its way from the
	// miner can be rebuilt. Miners switch to new work within seconds.
	retiredJobGrace = time.Minute
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

	// mu is held with defer: a panic while updating the work is recovered
	// (see update), and must not leave it held, since every miner, the
	// broadcast of new work and the health check need it.
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

// Get returns a job by id. A job that no longer pays shares is still
// returned for retiredJobGrace, so a solved block can be rebuilt after the
// tip has moved on.
func (m *Manager) Get(id string) *Job {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if job := m.jobs[id]; job != nil && !job.expired(time.Now()) {
		return job
	}
	return nil
}

// pruneLocked drops retired jobs past retiredJobGrace.
func (m *Manager) pruneLocked(now time.Time) {
	for id, job := range m.jobs {
		if job.expired(now) {
			delete(m.jobs, id)
		}
	}
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
	gen := m.generation()
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

// generation returns the number of tip changes published so far.
func (m *Manager) generation() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tipGen
}

// setOK records that the node delivered a usable template.
func (m *Manager) setOK() {
	if m.clearError() {
		log.Printf("Node connection restored.")
	}
}

// clearError forgets the node's problem, notes that the node has delivered,
// and reports whether there was a problem.
func (m *Manager) clearError() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	had := m.lastErr != ""
	m.lastErr, m.lastOK = "", time.Now()
	return had
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
	if m.replaceError(msg) {
		log.Printf("Node problem: %s. Retrying; miners keep their current work.", msg)
	}
}

// replaceError records msg as the node's problem and reports whether it is a
// new one.
func (m *Manager) replaceError(msg string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	changed := m.lastErr != msg
	m.lastErr = msg
	return changed
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
	cur, raced, seq := m.next(gen)
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
		m.setLongPoll(t.LongPollID)
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
	m.install(job, t.LongPollID, time.Now())

	if newTip {
		log.Printf("New block to mine: height %d, %d transactions, reward %s BTC, network difficulty %s",
			job.Height, job.TxCount, formatBTC(job.Value), formatDiff(job.NetDiff))
	}
	m.onJob(job)
	return nil
}

// next returns the current job, whether the tip has changed since generation
// gen, and the sequence number for a new job.
func (m *Manager) next(gen uint64) (cur *Job, raced bool, seq uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextID++
	return m.current, gen != m.tipGen, m.nextID
}

func (m *Manager) setLongPoll(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.longPoll = id
}

// install makes job the current job, with longPoll the node's long-poll id
// for it, and drops the jobs that are no longer kept.
func (m *Manager) install(job *Job, longPoll string, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if job.CleanJobs {
		// Ordinary shares for the old tip are stale. A block solved on it
		// may still be on its way: those jobs stay until pruneLocked drops
		// them.
		for _, id := range m.order {
			if old := m.jobs[id]; old != nil {
				old.Retire(now)
			}
		}
		m.order = m.order[:0]
	}
	m.jobs[job.ID] = job
	m.order = append(m.order, job.ID)
	for len(m.order) > keepJobs {
		if old := m.jobs[m.order[0]]; old != nil {
			old.Retire(now)
		}
		m.order = m.order[1:]
	}
	m.pruneLocked(now)
	m.current = job
	m.longPoll = longPoll
	if job.CleanJobs {
		m.tipGen++
	}
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
			m.update(func() { m.Fetch(ctx) })
		case <-ticker.C:
			m.update(func() { m.poll(ctx) })
		}
	}
}

// update runs one pass of Run. A panic in it is a bug, and counts as a failed
// update: it is logged and reported like a node problem, and the next pass
// tries again. Miners keep their current work meanwhile, and are disconnected
// if no update succeeds within nodeTimeout, as when the node is down.
func (m *Manager) update(f func()) {
	if err := guard.Run("updating the work", f); err != nil {
		m.setError(err)
	}
}

// poll checks the chain tip, and fetches a new template when the job is due
// for a refresh, the tip has moved or the last update failed.
func (m *Manager) poll(ctx context.Context) {
	cur := m.Current()
	if cur == nil || time.Since(cur.Created) >= refreshInterval {
		m.Fetch(ctx)
		return
	}
	info, err := m.node.GetBlockchainInfo(ctx)
	if err != nil {
		if ctx.Err() == nil {
			m.setError(err)
		}
		return
	}
	// Fetch right away on a new tip, and after a failure so that recovery is
	// noticed without waiting for the next refresh.
	tip, err := btc.HashFromDisplayHex(info.BestBlockHash)
	if (err == nil && tip != cur.PrevHash) || m.State().Error != "" {
		m.Fetch(ctx)
	}
}

// longPollLoop waits on the node's long poll, which answers the moment a new
// block arrives. A panic in one long poll counts as a failed one.
func (m *Manager) longPollLoop(ctx context.Context) {
	for ctx.Err() == nil {
		id := m.longPollID()
		if id == "" {
			sleep(ctx, time.Second)
			continue
		}
		var err error
		if failed := guard.Run("waiting for a new block", func() { err = m.fetch(ctx, id) }); failed != nil {
			err = failed
		}
		if err != nil && !isTimeout(err) {
			sleep(ctx, 5*time.Second) // the regular poll reports the problem
		}
	}
}

func (m *Manager) longPollID() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.longPoll
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
