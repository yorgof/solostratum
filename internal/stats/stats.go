// Package stats keeps a permanent record of what every worker has mined.
//
// The statistics folder holds two kinds of plain-text files:
//
//   - workers.json has each worker's lifetime totals. It is replaced as a
//     whole whenever the totals have changed.
//   - history-<year>-<month>.jsonl has one line per worker for every
//     interval in which that worker was connected. Lines are only ever
//     appended.
//
// A history line counts what happened in the interval that starts at "t".
// Lines add up: after a restart the same worker and interval can appear
// twice. "work" is the sum of the accepted shares' difficulties, so the
// average hashrate over any period is work * 2^32 / seconds. "online" is
// the number of seconds the worker had at least one connection.
package stats

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Interval is the length of one history interval, and how often the files
// are written. Writing this rarely is gentle on SD cards.
const Interval = 5 * time.Minute

// MaxSeries is how many workers a Series lists one by one. Any further
// workers are added together.
const MaxSeries = 8

const (
	// maxWorkers limits how many worker names are remembered, so a client
	// that keeps inventing names cannot fill the disk.
	maxWorkers = 10000
	// maxOffline limits the list of workers that are not connected.
	maxOffline = 100
	// recent is how far back a Series looks for workers. It is the longest
	// period worth charting, so a worker keeps its place in every chart.
	recent = 30 * 24 * time.Hour

	totalsFile = "workers.json"
)

// Counts are the numbers kept per worker, for its lifetime and for each
// interval.
type Counts struct {
	Accepted    uint64  `json:"accepted"`
	Rejected    uint64  `json:"rejected"`
	Work        float64 `json:"work"` // sum of the accepted shares' difficulties
	BestShare   float64 `json:"bestShare"`
	Connections uint64  `json:"connections"` // times a miner logged in
	Online      int64   `json:"online"`      // seconds with at least one connection
}

func (c *Counts) add(d Counts) {
	c.Accepted += d.Accepted
	c.Rejected += d.Rejected
	c.Work += d.Work
	c.BestShare = max(c.BestShare, d.BestShare)
	c.Connections += d.Connections
	c.Online += d.Online
}

// Totals are one worker's lifetime numbers.
type Totals struct {
	FirstSeen time.Time `json:"firstSeen"`
	LastSeen  time.Time `json:"lastSeen"`
	Counts
}

// Worker is a known worker with its lifetime numbers.
type Worker struct {
	Worker string `json:"worker"`
	Totals
}

// Sample is one line of a history file: what a worker did in the interval
// starting at Time.
type Sample struct {
	Time   time.Time `json:"t"`
	Worker string    `json:"worker"`
	Counts
}

// Series is what the workers did over a period, cut into steps of equal
// length. The period ends at the last full interval.
type Series struct {
	Start time.Time `json:"start"`
	Step  int64     `json:"step"` // seconds per step
	Steps int       `json:"steps"`
	// Since is where the statistics begin, when that is after Start. The
	// step it lies in is averaged over the part that was recorded.
	Since time.Time `json:"since"`
	// Workers are the first MaxSeries workers seen recently, in the order
	// they were first seen. A worker keeps its place whatever the period.
	Workers []WorkerSeries `json:"workers"`
	// Other is every further worker added together.
	Other  *WorkerSeries `json:"other,omitempty"`
	Others int           `json:"others"` // number of workers in Other
}

// WorkerSeries is one worker's part of a Series.
type WorkerSeries struct {
	Worker   string    `json:"worker"`
	Hashrate []float64 `json:"hashrate"` // average hashes per second in each step
	Counts             // sums over the whole period
}

type sampleKey struct {
	start  int64 // start of the interval, in Unix seconds
	worker string
}

// presence tracks a worker that is connected right now.
type presence struct {
	sessions int
	since    time.Time // online time has been counted up to here
}

type seriesKey struct{ span, step time.Duration }

type cachedSeries struct {
	end    time.Time
	series Series
}

// Store collects shares and connections in memory and writes them to disk
// once per Interval.
type Store struct {
	dir string

	// io is held while the files are written or read. It is taken before
	// mu and never held by a miner's connection, so a slow disk does not
	// delay mining.
	io    sync.Mutex
	cache map[seriesKey]cachedSeries

	mu      sync.Mutex
	totals  map[string]*Totals
	changed bool                  // totals differ from the file on disk
	pending map[sampleKey]*Sample // history not yet written
	live    map[string]*presence
	full    bool // the maxWorkers warning has been logged
}

// Open prepares the statistics folder, verifies it is writable and loads
// the totals of earlier runs.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	probe, err := os.CreateTemp(dir, ".write-test-*")
	if err != nil {
		return nil, fmt.Errorf("stats folder is not writable: %w", err)
	}
	probe.Close()
	os.Remove(probe.Name())

	s := &Store{
		dir: dir, cache: map[seriesKey]cachedSeries{},
		totals: map[string]*Totals{}, pending: map[sampleKey]*Sample{}, live: map[string]*presence{},
	}
	path := filepath.Join(dir, totalsFile)
	raw, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(raw, &s.totals); err != nil || s.totals == nil {
		// Statistics are not worth refusing to mine over. Keep the damaged
		// file for a person to look at and start counting again.
		aside := path + ".damaged-" + time.Now().UTC().Format("20060102T150405Z")
		if err := os.Rename(path, aside); err != nil {
			return nil, fmt.Errorf("%s is damaged and could not be moved out of the way: %w", path, err)
		}
		log.Printf("WARNING: %s was damaged and has been renamed to %s. The lifetime totals start again from zero.", path, filepath.Base(aside))
		s.totals = map[string]*Totals{}
	}
	for worker, t := range s.totals {
		if t == nil {
			delete(s.totals, worker)
		}
	}
	return s, nil
}

// Accepted records an accepted share. credit is the difficulty the share
// counts for, shareDiff the difficulty its hash actually reached.
func (s *Store) Accepted(worker string, at time.Time, credit, shareDiff float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordLocked(worker, at, Counts{Accepted: 1, Work: credit, BestShare: shareDiff})
}

// Rejected records a rejected share.
func (s *Store) Rejected(worker string, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordLocked(worker, at, Counts{Rejected: 1})
}

// Connected records that a miner has logged in as worker.
func (s *Store) Connected(worker string, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.recordLocked(worker, at, Counts{Connections: 1}) {
		return
	}
	p := s.live[worker]
	if p == nil {
		p = &presence{since: at.UTC().Truncate(time.Second)}
		s.live[worker] = p
	}
	p.sessions++
}

// Disconnected records that one of the worker's connections has ended.
func (s *Store) Disconnected(worker string, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.live[worker]
	if p == nil {
		return
	}
	if p.sessions--; p.sessions == 0 {
		s.countOnlineLocked(worker, p, at)
		delete(s.live, worker)
	}
}

// recordLocked adds d to the worker's totals and to the interval at lies
// in. It reports false for a new worker once maxWorkers are known.
func (s *Store) recordLocked(worker string, at time.Time, d Counts) bool {
	at = at.UTC().Truncate(time.Second)
	t := s.totals[worker]
	if t == nil {
		if len(s.totals) >= maxWorkers {
			if !s.full {
				s.full = true
				log.Printf("WARNING: statistics are already kept for %d workers. Workers with new names are not recorded.", maxWorkers)
			}
			return false
		}
		t = &Totals{FirstSeen: at}
		s.totals[worker] = t
	}
	if at.After(t.LastSeen) {
		t.LastSeen = at
	}
	t.add(d)
	s.changed = true

	start := at.Truncate(Interval)
	key := sampleKey{start.Unix(), worker}
	sample := s.pending[key]
	if sample == nil {
		sample = &Sample{Time: start, Worker: worker}
		s.pending[key] = sample
	}
	sample.add(d)
	return true
}

// countOnlineLocked counts the time since p.since as online time, interval
// by interval, and marks the worker as seen until then.
func (s *Store) countOnlineLocked(worker string, p *presence, until time.Time) {
	until = until.UTC().Truncate(time.Second)
	for p.since.Before(until) {
		end := p.since.Truncate(Interval).Add(Interval)
		if end.After(until) {
			end = until
		}
		s.recordLocked(worker, p.since, Counts{Online: int64(end.Sub(p.since) / time.Second)})
		p.since = end
	}
	if t := s.totals[worker]; t != nil && until.After(t.LastSeen) {
		t.LastSeen = until
	}
}

func (s *Store) countAllOnlineLocked(until time.Time) {
	for worker, p := range s.live {
		s.countOnlineLocked(worker, p, until)
	}
}

// Offline returns the workers that have mined here before and are not
// connected now, the most recently seen first.
func (s *Store) Offline() []Worker {
	s.mu.Lock()
	out := make([]Worker, 0, len(s.totals))
	for worker, t := range s.totals {
		if s.live[worker] == nil {
			out = append(out, Worker{worker, *t})
		}
	}
	s.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if !out[i].LastSeen.Equal(out[j].LastSeen) {
			return out[i].LastSeen.After(out[j].LastSeen)
		}
		return out[i].Worker < out[j].Worker
	})
	return out[:min(len(out), maxOffline)]
}

// Series returns what the workers did in the span before now, in steps of
// the given length. Both must be whole multiples of Interval.
func (s *Store) Series(now time.Time, span, step time.Duration) (Series, error) {
	end := now.UTC().Truncate(Interval)
	start := end.Add(-span)
	s.io.Lock()
	defer s.io.Unlock()
	key := seriesKey{span, step}
	if c, ok := s.cache[key]; ok && c.end.Equal(end) {
		return c.series, nil
	}

	s.mu.Lock()
	s.countAllOnlineLocked(now)
	since := end
	var known []Worker
	for worker, t := range s.totals {
		if t.FirstSeen.Before(since) {
			since = t.FirstSeen
		}
		if !t.LastSeen.Before(end.Add(-recent)) {
			known = append(known, Worker{worker, *t})
		}
	}
	samples := make([]Sample, 0, len(s.pending))
	for _, sample := range s.pending {
		samples = append(samples, *sample)
	}
	s.mu.Unlock()
	sort.Slice(known, func(i, j int) bool {
		if !known[i].FirstSeen.Equal(known[j].FirstSeen) {
			return known[i].FirstSeen.Before(known[j].FirstSeen)
		}
		return known[i].Worker < known[j].Worker
	})

	out := Series{Start: start, Step: int64(step / time.Second), Steps: int(span / step), Workers: []WorkerSeries{}}
	place := make(map[string]int, len(known))
	for i, w := range known {
		place[w.Worker] = min(i, MaxSeries)
		if i < MaxSeries {
			out.Workers = append(out.Workers, WorkerSeries{Worker: w.Worker, Hashrate: make([]float64, out.Steps)})
		}
	}
	other := &WorkerSeries{Hashrate: make([]float64, out.Steps)}
	add := func(sample Sample) {
		if sample.Time.Before(start) || !sample.Time.Before(end) {
			return
		}
		if sample.Time.Before(since.Truncate(Interval)) {
			since = sample.Time // history older than the totals know of
		}
		ws := other
		if i, ok := place[sample.Worker]; ok && i < MaxSeries {
			ws = &out.Workers[i]
		}
		ws.add(sample.Counts)
		// Work is collected here and turned into a hashrate below.
		ws.Hashrate[int(sample.Time.Sub(start)/step)] += sample.Work
	}
	if err := s.readHistory(start, end, add); err != nil {
		return Series{}, err
	}
	for _, sample := range samples {
		add(sample)
	}
	// The step in which the statistics begin covers less time than the
	// others. Very short stretches say little, hence the lower limit.
	out.Since = start
	partial, covered := -1, step
	if since.After(start) {
		out.Since = since
		partial = int(since.Sub(start) / step)
		covered = max(start.Add(time.Duration(partial+1)*step).Sub(since), time.Minute)
	}
	hashrate := func(ws *WorkerSeries) {
		for i, work := range ws.Hashrate {
			seconds := step.Seconds()
			if i == partial {
				seconds = covered.Seconds()
			}
			ws.Hashrate[i] = math.Round(work * 4294967296 / seconds)
		}
	}
	for i := range out.Workers {
		hashrate(&out.Workers[i])
	}
	if out.Others = max(len(known)-MaxSeries, 0); out.Others > 0 || other.Counts != (Counts{}) {
		hashrate(other)
		out.Other = other
	}
	s.cache[key] = cachedSeries{end, out}
	return out, nil
}

func historyFile(month time.Time) string {
	return "history-" + month.Format("2006-01") + ".jsonl"
}

// readHistory calls add for every stored line of the months that overlap
// the time from start to end.
func (s *Store) readHistory(start, end time.Time, add func(Sample)) error {
	for month := time.Date(start.Year(), start.Month(), 1, 0, 0, 0, 0, time.UTC); month.Before(end); month = month.AddDate(0, 1, 0) {
		f, err := os.Open(filepath.Join(s.dir, historyFile(month)))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		lines := bufio.NewScanner(f)
		for lines.Scan() {
			var sample Sample
			// A power cut can leave half a line behind; skip it.
			if json.Unmarshal(lines.Bytes(), &sample) == nil {
				add(sample)
			}
		}
		err = lines.Err()
		f.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// Run writes the statistics to disk at the end of every interval until ctx
// is cancelled, and once more on the way out.
func (s *Store) Run(ctx context.Context) {
	for {
		now := time.Now()
		timer := time.NewTimer(now.Truncate(Interval).Add(Interval).Sub(now))
		select {
		case <-ctx.Done():
			timer.Stop()
			s.flush(time.Now(), true)
			return
		case <-timer.C:
			s.flush(time.Now(), false)
		}
	}
}

// flush writes the totals and every interval that has ended. The interval
// still in progress is kept in memory, unless final is set.
func (s *Store) flush(now time.Time, final bool) {
	s.io.Lock()
	defer s.io.Unlock()
	s.mu.Lock()
	s.countAllOnlineLocked(now)
	current := now.UTC().Truncate(Interval)
	var samples []Sample
	for key, sample := range s.pending {
		if final || sample.Time.Before(current) {
			samples = append(samples, *sample)
			delete(s.pending, key)
		}
	}
	var totals []byte
	if s.changed {
		var err error
		if totals, err = json.MarshalIndent(s.totals, "", "  "); err != nil {
			log.Printf("WARNING: could not encode the worker totals: %v", err)
		}
		s.changed = false
	}
	s.mu.Unlock()

	if err := s.appendHistory(samples); err != nil {
		log.Printf("WARNING: could not write the mining history: %v", err)
	}
	if totals != nil {
		if err := s.replaceTotals(totals); err != nil {
			log.Printf("WARNING: could not write the worker totals: %v", err)
			s.mu.Lock()
			s.changed = true // try again at the next interval
			s.mu.Unlock()
		}
	}
}

// appendHistory adds one line per sample to the file of the sample's month.
func (s *Store) appendHistory(samples []Sample) error {
	sort.Slice(samples, func(i, j int) bool {
		if !samples[i].Time.Equal(samples[j].Time) {
			return samples[i].Time.Before(samples[j].Time)
		}
		return samples[i].Worker < samples[j].Worker
	})
	var name string
	var lines bytes.Buffer
	write := func() error {
		if lines.Len() == 0 {
			return nil
		}
		f, err := os.OpenFile(filepath.Join(s.dir, name), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		_, err = f.Write(lines.Bytes())
		lines.Reset()
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		return err
	}
	for _, sample := range samples {
		if month := historyFile(sample.Time); month != name {
			if err := write(); err != nil {
				return err
			}
			name = month
		}
		line, err := json.Marshal(sample)
		if err != nil {
			return err
		}
		lines.Write(line)
		lines.WriteByte('\n')
	}
	return write()
}

// replaceTotals swaps in a new totals file. The new contents are forced
// onto the disk first, so a power cut leaves either the old file or the new
// one, never half of one.
func (s *Store) replaceTotals(data []byte) error {
	path := filepath.Join(s.dir, totalsFile)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	_, err = f.Write(append(data, '\n'))
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		os.Remove(tmp)
	}
	return err
}
