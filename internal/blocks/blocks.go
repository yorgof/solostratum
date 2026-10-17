// Package blocks delivers every solved block to the node, retrying until the
// node has given a definite answer, and at the same time saves it to disk.
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

	"github.com/yorgof/solostratum/internal/guard"
	"github.com/yorgof/solostratum/internal/rpc"
)

// Submitter is the part of the RPC client the store needs. SubmitBlock must
// bound its own duration: the store never cancels an attempt in flight,
// since the node may be validating the block at that very moment.
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
	saveWait   time.Duration // saveWait; tests shorten it

	mu      sync.Mutex
	records []Record
	wg      sync.WaitGroup // the deliveries; the blocks' files are not counted (see track)
	pending atomic.Int64   // deliveries without the node's answer yet
	// files counts the goroutines that look after the blocks' files. Nothing
	// in the program waits for them, as a stuck disk must hold nothing up;
	// tests do, to see that none is left behind.
	files sync.WaitGroup
	// Leave RPC capacity for template updates and node health checks, even
	// when regtest miners find many blocks at once.
	submitSlots chan struct{}

	// create makes a new block file; tests replace it to make saving fail or
	// stall.
	create func(path string) (*os.File, error)
}

const timeLayout = "20060102T150405.000000000Z"
const maxSubmissions = 2

// saveWait is how long a delivery waits, once the node has answered, for the
// block's file and the answer next to it. A slow or stuck disk is never
// waited for longer: the file and the answer are then written when it lets
// them.
const saveWait = 5 * time.Second

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
		retryEvery: 2 * time.Second, retryFor: 30 * time.Minute, saveWait: saveWait,
		submitSlots: make(chan struct{}, maxSubmissions),
		create: func(path string) (*os.File, error) {
			return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		},
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

// Found submits a solved block to the node and, at the same time, saves it to
// a file of its own, from which it can be submitted by hand ("bitcoin-cli
// submitblock <contents>") if everything else fails. It returns at once and
// never touches the disk itself: the submission starts from memory, so a slow
// or stuck disk cannot hold the block back.
func (s *Store) Found(height int64, hash string, block []byte, worker string) {
	s.track(Record{Time: time.Now().UTC(), Height: height, Hash: hash, Status: "submitting", Worker: worker}, "", block)
}

// track adds a record, then delivers its block and looks after its file in the
// background, each on a goroutine of its own. base names the block's file if
// it is saved already, as for a block resumed at the start.
//
// Only the delivery is counted in wg, and it never touches the disk, so Wait
// cannot hang on a disk that has stopped. The file's goroutine saves the
// block, and writes the node's answer next to it once both are there, however
// late that is. The delivery waits for it up to saveWait after the answer, so
// with a working disk the file and its answer are on disk when Wait returns.
func (s *Store) track(rec Record, base string, block []byte) {
	s.mu.Lock()
	s.records = append(s.records, rec)
	index := len(s.records) - 1
	s.mu.Unlock()

	answered := make(chan string, 1) // the node's answer; closed without one if there is none
	filed := make(chan string, 1)    // the file's name, or "" if the block could not be saved
	s.files.Add(1)
	go func() {
		defer s.files.Done()
		var file string
		guard.Run("saving a found block", func() { file = s.keep(index, rec, base, block, answered) })
		filed <- file
	}()
	s.pending.Add(1)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.deliver(index, rec.Hash, block, answered, filed)
	}()
}

// keep looks after a block's file. It saves the block, unless base already
// names its file, and once the delivery has the node's answer it writes that
// answer next to the file. Without an answer, after an internal error, it
// writes none, so that the next start delivers the block again. It returns
// the file's name, or "" if the block could not be saved.
func (s *Store) keep(index int, rec Record, base string, block []byte, answered <-chan string) string {
	if base == "" {
		var err error
		if base, err = s.save(rec.Height, rec.Hash, block, rec.Time); err != nil {
			log.Printf("WARNING: could not save the found block to disk: %v", err)
			return ""
		}
		s.setFile(index, base+".hex")
	}
	if status, ok := <-answered; ok {
		if err := os.WriteFile(filepath.Join(s.dir, base+".result"), []byte(status+"\n"), 0o644); err != nil {
			log.Printf("Could not write the block result file: %v", err)
		}
	}
	return base
}

// deliver submits the block, records the node's answer and hands it to the
// block's file, and asks for new work after an accepted block. It never
// touches the disk. A panic in it ends the delivery: an answer from the node
// stands, and without one the block is left without a result file, so that
// the next start delivers it again.
func (s *Store) deliver(index int, hash string, block []byte, answered chan<- string, filed <-chan string) {
	var status string // set once the node has answered or the delivery has given up
	failed := guard.Run("delivering a found block", func() {
		defer close(answered)
		status = s.submit(hash, block)
		s.setStatus(index, status)
		answered <- status
		if status == "accepted" && s.onAccepted != nil {
			s.onAccepted()
		}
	})
	if failed != nil && status == "" {
		// The node may not have the block.
		status = "not submitted: " + failed.Error()
		s.setStatus(index, status)
	}
	s.pending.Add(-1)
	var base string
	select {
	case base = <-filed:
	case <-time.After(s.saveWait):
		log.Printf("WARNING: the disk has not finished with block %s within %s. Is it stuck? The block's file, and the node's answer next to it, are written as soon as it lets them.", hash, s.saveWait)
	}
	if base == "" && strings.HasPrefix(status, "not submitted") {
		// The block is not on disk, or not yet: this is the last place it
		// is known to exist.
		log.Printf("Block %s was neither saved nor delivered. To submit it by hand, run: bitcoin-cli submitblock %x", hash, block)
	}
}

func (s *Store) setStatus(index int, status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[index].Status = status
}

func (s *Store) setFile(index int, file string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[index].File = file
}

// Pending returns the number of blocks still being delivered to the node.
func (s *Store) Pending() int { return int(s.pending.Load()) }

// Wait blocks until every delivery has finished, which includes waiting a
// bounded time for each block's file and the node's answer next to it (see
// track). Callers must finish handing off blocks through Found before calling
// Wait.
func (s *Store) Wait() { s.wg.Wait() }

// save writes the block as hex to a new file that is never overwritten, and
// forces it onto the disk. The name carries the height, the time to the
// nanosecond and the block hash, and the file is created exclusively. It
// returns the name without ".hex".
func (s *Store) save(height int64, hash string, block []byte, now time.Time) (string, error) {
	base := fmt.Sprintf("block-%d-%s-%s", height, now.Format(timeLayout), hash)
	for attempt := 1; ; attempt++ {
		name := base
		if attempt > 1 {
			name = fmt.Sprintf("%s-%d", base, attempt)
		}
		f, err := s.create(filepath.Join(s.dir, name+".hex"))
		if errors.Is(err, fs.ErrExist) && attempt < 100 {
			continue
		}
		if err != nil {
			return "", err
		}
		if _, err := f.WriteString(hex.EncodeToString(block) + "\n"); err != nil {
			f.Close()
			return "", err
		}
		log.Printf("Block saved to %s", filepath.Join(s.dir, name+".hex"))
		if err := f.Sync(); err != nil {
			log.Printf("WARNING: could not flush the block file to disk: %v", err)
		}
		f.Close()
		return name, nil
	}
}

// submit delivers the block and returns its final status. The retry budget
// covers waiting for a slot and the pauses between attempts, never an attempt
// itself: the node may be validating the block at that very moment, and the
// RPC call has its own timeout. A panic during an attempt is a bug, and the
// attempt is retried like one that could not reach the node: giving up could
// lose the block.
func (s *Store) submit(hash string, block []byte) string {
	ctx, cancel := context.WithTimeout(context.Background(), s.retryFor)
	defer cancel()
	giveUp := func(attempt int, why string) string {
		log.Printf("Giving up submitting block %s after %d attempts: %s", hash, attempt, why)
		return "not submitted: " + why
	}
	var lastWhy string // why the latest attempt failed
	for attempt := 1; ; attempt++ {
		select {
		case s.submitSlots <- struct{}{}:
		case <-ctx.Done():
			if attempt > 1 {
				return giveUp(attempt-1, lastWhy)
			}
			return giveUp(0, fmt.Sprintf("waited %s behind other block deliveries", s.retryFor))
		}
		var status, why string
		if failed := guard.Run("submitting a found block", func() { status, why = s.attempt(hash, block) }); failed != nil {
			why = failed.Error()
		}
		<-s.submitSlots
		if status != "" {
			return status
		}
		lastWhy = why
		if ctx.Err() != nil {
			return giveUp(attempt, why)
		}
		log.Printf("Could not submit block %s (attempt %d): %s. Retrying.", hash, attempt, why)
		select {
		case <-ctx.Done():
			return giveUp(attempt, why)
		case <-time.After(s.retryEvery):
		}
	}
}

// attempt submits the block once. It returns the block's final status, or ""
// and why the attempt failed when it is worth repeating.
func (s *Store) attempt(hash string, block []byte) (status, why string) {
	reason, err := s.node.SubmitBlock(context.Background(), block)
	switch {
	case err == nil && (reason == "" || reason == "duplicate"):
		// "duplicate" means the node already has this exact block, which
		// happens when a retry follows a lost answer.
		log.Printf("BLOCK ACCEPTED by the node: %s", hash)
		return "accepted", ""
	case err == nil:
		// Every other answer is final. "inconclusive" means the node stored
		// the block without making it the tip, typically because another
		// block at the same height arrived first. Submitting it again would
		// return "duplicate", which here does not mean the block won.
		log.Printf("Block %s was REJECTED by the node: %s", hash, reason)
		return "rejected: " + reason, ""
	case permanentSubmitError(err):
		log.Printf("Block %s could not be submitted: %s", hash, rpc.Explain(err))
		var rpcErr *rpc.Error
		errors.As(err, &rpcErr)
		return "rejected: " + rpcErr.Message, ""
	}
	return "", rpc.Explain(err)
}

// permanentSubmitError reports a node error that will not change on retry:
// the block could not be decoded, or the request itself is malformed.
// Consensus rejections arrive as result strings, not as errors. Any other
// error is retried, since giving up on a block that may be valid loses it.
func permanentSubmitError(err error) bool {
	var rpcErr *rpc.Error
	if !errors.As(err, &rpcErr) {
		return false
	}
	switch rpcErr.Code {
	case rpc.CodeDeserialization, rpc.CodeTypeError, rpc.CodeInvalidParameter,
		rpc.CodeParseError, rpc.CodeInvalidRequest, rpc.CodeMethodNotFound, rpc.CodeInvalidParams:
		return true
	}
	return false
}
