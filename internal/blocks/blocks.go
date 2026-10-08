// Package blocks saves every solved block to disk and delivers it to the
// node, retrying until the node has given a definite answer.
package blocks

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yorgof/solostratum/internal/rpc"
)

// Submitter is the part of the RPC client the store needs.
type Submitter interface {
	SubmitBlock(ctx context.Context, block []byte) (string, error)
}

// Record describes one found block.
type Record struct {
	Time   time.Time `json:"time"`
	Height int64     `json:"height"`
	Hash   string    `json:"hash"`
	File   string    `json:"file"`
	Status string    `json:"status"` // "accepted", "submitting", or the rejection reason
	Worker string    `json:"worker,omitempty"`
}

// Store keeps found blocks.
type Store struct {
	dir        string
	node       Submitter
	onAccepted func()

	// retry schedule for submissions that could not reach the node
	retryEvery time.Duration
	retryFor   time.Duration

	mu      sync.Mutex
	records []Record
	wg      sync.WaitGroup
	pending atomic.Int64
	// Leave RPC capacity for template updates and node health checks, even
	// when regtest miners find many blocks at once.
	submitSlots chan struct{}
}

const timeLayout = "20060102T150405.000000000Z"
const maxSubmissions = 2

// statusUnknown marks a block from an earlier run whose result was never
// recorded.
const statusUnknown = "unknown"

// Open prepares the blocks directory, verifies it is writable and loads the
// blocks found by earlier runs.
func Open(dir string, node Submitter, onAccepted func()) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	probe, err := os.CreateTemp(dir, ".write-test-*")
	if err != nil {
		return nil, fmt.Errorf("blocks folder is not writable: %w", err)
	}
	probe.Close()
	os.Remove(probe.Name())

	s := &Store{
		dir: dir, node: node, onAccepted: onAccepted,
		retryEvery: 2 * time.Second, retryFor: 30 * time.Minute,
		submitSlots: make(chan struct{}, maxSubmissions),
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var loaded []Record
	for _, e := range entries {
		if r, ok := s.parseName(e.Name()); ok {
			loaded = append(loaded, r)
		}
	}
	sort.Slice(loaded, func(i, j int) bool { return loaded[i].Time.Before(loaded[j].Time) })
	for _, r := range loaded {
		// A block without a result file was still being delivered when the
		// program stopped. If it is recent enough to matter, try again.
		if r.Status == statusUnknown && time.Since(r.Time) < s.retryFor {
			if raw, err := os.ReadFile(filepath.Join(dir, r.File)); err == nil {
				if block, err := hex.DecodeString(strings.TrimSpace(string(raw))); err == nil {
					log.Printf("Resuming delivery of block %s found before the last shutdown.", r.Hash)
					r.Status = "submitting"
					s.track(r, strings.TrimSuffix(r.File, ".hex"), block)
					continue
				}
			}
		}
		s.mu.Lock()
		s.records = append(s.records, r)
		s.mu.Unlock()
	}
	return s, nil
}

// parseName recovers a record from "block-<height>-<time>-<hash>.hex".
func (s *Store) parseName(name string) (Record, bool) {
	base, ok := strings.CutSuffix(name, ".hex")
	if !ok {
		return Record{}, false
	}
	parts := strings.Split(base, "-")
	if len(parts) < 4 || parts[0] != "block" {
		return Record{}, false
	}
	height, err1 := strconv.ParseInt(parts[1], 10, 64)
	when, err2 := time.Parse(timeLayout, parts[2])
	if err1 != nil || err2 != nil {
		return Record{}, false
	}
	r := Record{Time: when, Height: height, Hash: parts[3], File: name, Status: statusUnknown}
	if raw, err := os.ReadFile(filepath.Join(s.dir, base+".result")); err == nil {
		r.Status = strings.TrimSpace(string(raw))
	}
	return r, true
}

// Records returns all found blocks, oldest first.
func (s *Store) Records() []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Record(nil), s.records...)
}

// Found saves a solved block to its own file and then submits it. The file
// is written before the node is contacted, so the block can be submitted by
// hand ("bitcoin-cli submitblock <contents>") if everything else fails.
// Forcing the file onto the physical disk can be slow on SD cards, so that
// part runs alongside the submission rather than delaying it.
func (s *Store) Found(height int64, hash string, block []byte, worker string) {
	now := time.Now().UTC()
	base, file, err := s.save(height, hash, block, now)
	if err != nil {
		// Still try to submit: the block only exists in memory now.
		log.Printf("WARNING: could not save the found block to disk: %v", err)
	} else {
		log.Printf("Block saved to %s", filepath.Join(s.dir, base+".hex"))
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := file.Sync(); err != nil {
				log.Printf("WARNING: could not flush the block file to disk: %v", err)
			}
			file.Close()
		}()
	}
	rec := Record{Time: now, Height: height, Hash: hash, Status: "submitting", Worker: worker}
	if base != "" {
		rec.File = base + ".hex"
	}
	s.track(rec, base, block)
}

// track adds a record and delivers its block in the background.
func (s *Store) track(rec Record, base string, block []byte) {
	s.mu.Lock()
	s.records = append(s.records, rec)
	index := len(s.records) - 1
	s.mu.Unlock()

	s.pending.Add(1)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.pending.Add(-1)
		status := s.submit(rec.Hash, block)
		s.mu.Lock()
		s.records[index].Status = status
		s.mu.Unlock()
		if base != "" {
			if err := os.WriteFile(filepath.Join(s.dir, base+".result"), []byte(status+"\n"), 0o644); err != nil {
				log.Printf("Could not write the block result file: %v", err)
			}
		}
		if status == "accepted" && s.onAccepted != nil {
			s.onAccepted()
		}
	}()
}

// Pending returns the number of blocks still being delivered to the node.
func (s *Store) Pending() int { return int(s.pending.Load()) }

// Wait blocks until all submissions and disk flushes have finished. Callers
// must finish handing off blocks through Found before calling Wait.
func (s *Store) Wait() { s.wg.Wait() }

// save writes the block as hex to a new file that is never overwritten: the
// name carries the height, the time to the nanosecond and the block hash,
// and the file is created exclusively. The open file is returned so the
// caller can flush it to disk.
func (s *Store) save(height int64, hash string, block []byte, now time.Time) (string, *os.File, error) {
	base := fmt.Sprintf("block-%d-%s-%s", height, now.Format(timeLayout), hash)
	for attempt := 1; ; attempt++ {
		name := base
		if attempt > 1 {
			name = fmt.Sprintf("%s-%d", base, attempt)
		}
		f, err := os.OpenFile(filepath.Join(s.dir, name+".hex"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if errors.Is(err, fs.ErrExist) && attempt < 100 {
			continue
		}
		if err != nil {
			return "", nil, err
		}
		if _, err := f.WriteString(hex.EncodeToString(block) + "\n"); err != nil {
			f.Close()
			return "", nil, err
		}
		return name, f, nil
	}
}

// submit delivers the block and returns its final status. The retry budget
// covers waiting for a slot and the pauses between attempts, never an attempt
// itself: the node may be validating the block at that very moment, and the
// RPC call has its own timeout.
func (s *Store) submit(hash string, block []byte) string {
	ctx, cancel := context.WithTimeout(context.Background(), s.retryFor)
	defer cancel()
	giveUp := func(attempt int, why string) string {
		log.Printf("Giving up submitting block %s after %d attempts: %s", hash, attempt, why)
		return "not submitted: " + why
	}
	for attempt := 1; ; attempt++ {
		select {
		case s.submitSlots <- struct{}{}:
		case <-ctx.Done():
			return giveUp(attempt-1, fmt.Sprintf("waited %s behind other block deliveries", s.retryFor))
		}
		reason, err := s.node.SubmitBlock(context.Background(), block)
		<-s.submitSlots
		switch {
		case err == nil && (reason == "" || reason == "duplicate"):
			// "duplicate" means the node already has this exact block,
			// which happens when a retry follows a lost answer.
			log.Printf("BLOCK ACCEPTED by the node: %s", hash)
			return "accepted"
		case err == nil:
			log.Printf("Block %s was REJECTED by the node: %s", hash, reason)
			return "rejected: " + reason
		}
		var rpcErr *rpc.Error
		if errors.As(err, &rpcErr) && rpcErr.Code != rpc.CodeInWarmup {
			log.Printf("Block %s could not be submitted: %s", hash, rpc.Explain(err))
			return "rejected: " + rpcErr.Message
		}
		if ctx.Err() != nil {
			return giveUp(attempt, rpc.Explain(err))
		}
		log.Printf("Could not reach the node to submit block %s (attempt %d): %s. Retrying.", hash, attempt, rpc.Explain(err))
		select {
		case <-ctx.Done():
			return giveUp(attempt, rpc.Explain(err))
		case <-time.After(s.retryEvery):
		}
	}
}
