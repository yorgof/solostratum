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
	writeWait  time.Duration // see writeWait; tests change it
	saveWait   time.Duration // see saveWait; tests shorten it
	fileWait   time.Duration // see fileWait; tests shorten it

	mu      sync.Mutex
	records []Record
	// inFlight holds the blocks whose delivery is under way, by their index
	// in records, so that one that exists only in memory can be written in
	// full to the log (see logInFull).
	inFlight map[int]*flight
	wg       sync.WaitGroup // the deliveries, which never touch the disk
	pending  atomic.Int64   // deliveries without the node's answer yet
	files    sync.WaitGroup // the goroutines that look after the blocks' files
	// Leave RPC capacity for template updates and node health checks, even
	// when regtest miners find many blocks at once.
	submitSlots chan struct{}

	// create makes a new block file; tests replace it to make saving fail or
	// stall.
	create func(path string) (*os.File, error)
}

// flight is a block whose delivery is under way.
type flight struct {
	block  []byte
	logged bool // written in full to the log
}

const timeLayout = "20060102T150405.000000000Z"
const maxSubmissions = 2

// writeWait is how long a delivery waits for the block's file to be written,
// though not yet forced onto the disk, before it sends the block. On a
// working disk that takes a few milliseconds, and the block is then on disk
// before the node is contacted: from there even a crash that nothing can
// catch, such as running out of memory, cannot lose it. A slow or stuck disk
// holds the block back for this long at most.
const writeWait = 50 * time.Millisecond

// saveWait is how long a delivery waits, once the node has answered, for the
// block's file and the answer next to it. A slow or stuck disk is never
// waited for longer: the file and the answer are then written when it lets
// them.
const saveWait = 5 * time.Second

// fileWait is how long Wait, at the end of the program, gives the disk to
// finish the blocks' files once every delivery is over. Without the answer
// next to it, a block is delivered again at the next start, and a block that
// lost its race would then come back from the node as "duplicate" and be
// counted as accepted.
const fileWait = 10 * time.Second

// statusUnknown marks a block from an earlier run whose result was never
// recorded.
const statusUnknown = "unknown"

// Open prepares the blocks directory, verifies it is writable and loads the
// blocks found by earlier runs. It delivers none of them: see Resume.
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
		writeWait: writeWait, saveWait: saveWait, fileWait: fileWait,
		inFlight:    map[int]*flight{},
		submitSlots: make(chan struct{}, maxSubmissions),
		create: func(path string) (*os.File, error) {
			return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		},
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if r, ok := s.parseName(e.Name()); ok {
			s.records = append(s.records, r)
		}
	}
	sort.Slice(s.records, func(i, j int) bool { return s.records[i].Time.Before(s.records[j].Time) })
	return s, nil
}

// Resume delivers again each block from an earlier run that never got the
// node's answer, if it was found recently enough to matter: within retryFor,
// the time a delivery keeps trying. -check does not call it, as it does not
// mine: a solostratum that is running may be delivering those very blocks.
func (s *Store) Resume() {
	s.mu.Lock()
	var due []int
	for i, r := range s.records {
		if r.Status == statusUnknown && time.Since(r.Time) < s.retryFor {
			due = append(due, i)
		}
	}
	s.mu.Unlock()
	for _, i := range due {
		r := s.record(i)
		raw, err := os.ReadFile(filepath.Join(s.dir, r.File))
		var block []byte
		if err == nil {
			block, err = hex.DecodeString(strings.TrimSpace(string(raw)))
		}
		if err != nil {
			log.Printf("WARNING: could not read block %s, found before the last shutdown, from %s: %v", r.Hash, r.File, err)
			continue
		}
		log.Printf("Resuming delivery of block %s found before the last shutdown.", r.Hash)
		r.Status = "submitting"
		s.setStatus(i, r.Status)
		s.start(i, r, strings.TrimSuffix(r.File, ".hex"), block)
	}
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

// Found saves a solved block to a file of its own, from which it can be
// submitted by hand ("bitcoin-cli submitblock <contents>") if everything else
// fails, and submits it to the node. It returns at once and never touches the
// disk itself. The submission waits for the file to be written, so that the
// block is on disk before the node is contacted, but never longer than
// writeWait: a slow or stuck disk cannot hold the block back.
func (s *Store) Found(height int64, hash string, block []byte, worker string) {
	rec := Record{Time: time.Now().UTC(), Height: height, Hash: hash, Status: "submitting", Worker: worker}
	s.mu.Lock()
	s.records = append(s.records, rec)
	index := len(s.records) - 1
	s.mu.Unlock()
	s.start(index, rec, "", block)
}

// start delivers the block of records[index] and looks after its file in the
// background, each on a goroutine of its own. base names the block's file if
// it is saved already, as for a block resumed at the start.
//
// The delivery never touches the disk. The file's goroutine saves the block,
// and writes the node's answer next to it once both are there, however late
// that is. The delivery waits up to writeWait for the file to be written
// before it sends the block, up to saveWait after the answer for the rest, and
// Wait up to fileWait more, so with a working disk the file and its answer
// are on disk when Wait returns. A block that the disk has not taken within
// saveWait goes to the log in full, in case the program dies before its
// delivery ends.
func (s *Store) start(index int, rec Record, base string, block []byte) {
	s.mu.Lock()
	s.inFlight[index] = &flight{block: block}
	s.mu.Unlock()
	answered := make(chan string, 1) // the block's status; closed without one if there is none
	written := make(chan struct{})   // closed once the file is written, or cannot be
	markWritten := sync.OnceFunc(func() { close(written) })
	if base != "" {
		markWritten()
	}
	s.files.Add(1)
	filed := guard.Go("saving a found block", func() {
		defer s.files.Done()
		defer markWritten()
		s.keep(index, rec, base, block, answered, markWritten)
	})
	wait := s.saveWait
	time.AfterFunc(wait, func() {
		guard.Run("logging a found block", func() { s.logInFull(index, fmt.Sprintf("is not saved to disk after %s", wait)) })
	})
	s.pending.Add(1)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.deliver(index, rec.Hash, block, written, answered, filed)
	}()
}

// keep looks after a block's file. It saves the block, unless base already
// names its file, calling written once the file is written, and once the
// delivery has the block's status it writes it next to the file. A block that
// cannot be saved goes to the log in full at once.
func (s *Store) keep(index int, rec Record, base string, block []byte, answered <-chan string, written func()) {
	if base == "" {
		var err error
		if base, err = s.save(rec.Height, rec.Hash, block, rec.Time, written); err != nil {
			written() // nothing to wait for
			log.Printf("WARNING: could not save the found block to disk: %v", err)
			s.logInFull(index, "could not be saved")
			return
		}
		s.setFile(index, base+".hex")
	}
	if status, ok := <-answered; ok {
		if err := os.WriteFile(filepath.Join(s.dir, base+".result"), []byte(status+"\n"), 0o644); err != nil {
			log.Printf("Could not write the block result file: %v", err)
		}
	}
}

// deliver submits the block, records its status and hands it to the block's
// file, and asks for new work after an accepted block. It never touches the
// disk: it waits for the file to be written up to writeWait, then sends the
// block whatever the disk does. A panic before the node has answered is a
// bug, and the submission starts again within the same retryFor: giving up
// could lose the block. A panic after the answer changes nothing about it.
func (s *Store) deliver(index int, hash string, block []byte, written <-chan struct{}, answered chan<- string, filed <-chan error) {
	select {
	case <-written:
	case <-time.After(s.writeWait):
		log.Printf("WARNING: the disk has not written block %s within %s. Sending it to the node without waiting any longer.", hash, s.writeWait)
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.retryFor)
	defer cancel()
	var status string  // the node's answer, or why the delivery gave up
	var lastWhy string // why the submission last failed, if it did
	for {
		failed := guard.Run("delivering a found block", func() { status = s.submit(ctx, hash, block, lastWhy) })
		if failed == nil {
			break
		}
		lastWhy = failed.Error()
		select {
		case <-ctx.Done():
		case <-time.After(s.retryEvery):
		}
		if ctx.Err() != nil {
			log.Printf("Giving up submitting block %s: %s", hash, lastWhy)
			status = "not submitted: " + lastWhy
			break
		}
		log.Printf("Delivering block %s again.", hash)
	}
	guard.Run("delivering a found block", func() {
		defer close(answered)
		s.setStatus(index, status)
		answered <- status
		if status == "accepted" && s.onAccepted != nil {
			s.onAccepted()
		}
	})
	s.pending.Add(-1)
	select {
	case <-filed:
	case <-time.After(s.saveWait):
		log.Printf("WARNING: the disk has not finished with block %s within %s. Is it stuck? The block's file, and the node's answer next to it, are written as soon as it lets them.", hash, s.saveWait)
	}
	if strings.HasPrefix(status, "not submitted") && !s.logInFull(index, "was neither saved nor delivered") && s.record(index).File == "" {
		log.Printf("Block %s was neither saved nor delivered. It is written in full further up in the log, ready to be submitted by hand.", hash)
	}
	s.mu.Lock()
	delete(s.inFlight, index)
	s.mu.Unlock()
}

func (s *Store) record(index int) Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.records[index]
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

// LogUnsaved writes to the log in full, ready to be submitted by hand, each
// block whose delivery is under way and that exists only in memory (see
// atRiskLocked), unless the log has it already. It returns how many such
// blocks there are. The end of the program calls it first thing: quitting
// before those deliveries end would otherwise lose the blocks, unless the
// node has them already.
func (s *Store) LogUnsaved() int {
	s.mu.Lock()
	var due []int
	for i := range s.inFlight {
		if s.atRiskLocked(i) {
			due = append(due, i)
		}
	}
	s.mu.Unlock()
	for _, i := range due {
		s.logInFull(i, "is not saved to disk yet")
	}
	return len(due)
}

// atRiskLocked reports whether the block of records[index] exists only in
// memory: it is not on disk, and the node has not settled it, as it has when
// it accepted or rejected it. s.mu must be held.
func (s *Store) atRiskLocked(index int) bool {
	r := s.records[index]
	return r.File == "" && (r.Status == "submitting" || strings.HasPrefix(r.Status, "not submitted"))
}

// logInFull writes the block of records[index] to the log, ready to be
// submitted by hand, if its delivery is under way, it exists only in memory
// and the log does not have it yet. why says what is the matter with it. It
// reports whether it wrote the block.
func (s *Store) logInFull(index int, why string) bool {
	s.mu.Lock()
	f := s.inFlight[index]
	if f == nil || f.logged || !s.atRiskLocked(index) {
		s.mu.Unlock()
		return false
	}
	f.logged = true
	hash := s.records[index].Hash
	s.mu.Unlock()
	log.Printf("Block %s %s. To submit it by hand, run: bitcoin-cli submitblock %x", hash, why, f.block)
	return true
}

// Wait blocks until every delivery has finished, which includes waiting a
// bounded time for each block's file and the node's answer next to it (see
// start). It then gives the disk up to fileWait more to finish them. Callers
// must finish handing off blocks through Found before calling Wait.
func (s *Store) Wait() {
	s.wg.Wait()
	select {
	case <-guard.Go("waiting for the found blocks' files", s.files.Wait):
	case <-time.After(s.fileWait):
		log.Printf("WARNING: the disk has not finished the found blocks' files within %s. Quitting anyway: a block whose file has no result next to it is delivered again at the next start.", s.fileWait)
	}
}

// save writes the block as hex to a new file that is never overwritten, calls
// written, and forces the file onto the disk. The name carries the height,
// the time to the nanosecond and the block hash, and the file is created
// exclusively. It returns the name without ".hex".
func (s *Store) save(height int64, hash string, block []byte, now time.Time, written func()) (string, error) {
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
		written()
		log.Printf("Block saved to %s", filepath.Join(s.dir, name+".hex"))
		if err := f.Sync(); err != nil {
			log.Printf("WARNING: could not flush the block file to disk: %v", err)
		}
		f.Close()
		return name, nil
	}
}

// submit delivers the block and returns its final status. ctx, the retry
// budget, covers waiting for a slot and the pauses between attempts, never an
// attempt itself: the node may be validating the block at that very moment,
// and the RPC call has its own timeout. A panic during an attempt is a bug,
// and the attempt is retried like one that could not reach the node: giving
// up could lose the block. lastWhy is why the submission last failed, when it
// starts again after a bug.
func (s *Store) submit(ctx context.Context, hash string, block []byte, lastWhy string) string {
	giveUp := func(attempt int, why string) string {
		log.Printf("Giving up submitting block %s after %d attempts: %s", hash, attempt, why)
		return "not submitted: " + why
	}
	for attempt := 1; ; attempt++ {
		status, why, ok := s.attemptInSlot(ctx, hash, block)
		if !ok {
			if lastWhy != "" {
				return giveUp(attempt-1, lastWhy)
			}
			return giveUp(0, fmt.Sprintf("waited %s behind other block deliveries", s.retryFor))
		}
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

// attemptInSlot waits for a free submission slot, then submits the block once
// (see attempt). A panic in the attempt fails it. It reports false if ctx
// ends before a slot is free. The slot is given back whatever happens: two
// lost slots would hold every later block back.
func (s *Store) attemptInSlot(ctx context.Context, hash string, block []byte) (status, why string, ok bool) {
	select {
	case s.submitSlots <- struct{}{}:
	case <-ctx.Done():
		return "", "", false
	}
	defer func() { <-s.submitSlots }()
	if failed := guard.Run("submitting a found block", func() { status, why = s.attempt(hash, block) }); failed != nil {
		why = failed.Error()
	}
	return status, why, true
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
