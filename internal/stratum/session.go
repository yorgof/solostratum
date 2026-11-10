package stratum

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"math"
	"math/big"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/yorgof/solostratum/internal/btc"
	"github.com/yorgof/solostratum/internal/guard"
	"github.com/yorgof/solostratum/internal/work"
)

const (
	// versionRollingMask is the set of block version bits miners may change
	// (BIP320).
	versionRollingMask = 0x1fffe000

	maxLineLength = 16 * 1024
	// idleTimeout is how long a miner may send nothing before its connection
	// is taken for dead. A miner that finds no share sends nothing, so the
	// time starts again whenever its difficulty is lowered for that reason
	// (see sendJob): it is only given up once easier work has not helped.
	idleTimeout = 10 * time.Minute
	// loginTimeout is how long a connection may take to subscribe and
	// authorize, so idle connections cannot crowd out real miners.
	loginTimeout = 60 * time.Second
	writeTimeout = 30 * time.Second
	// maxNTimeRoll is how far a miner may move the block time forward.
	maxNTimeRoll = 600

	// Automatic difficulty aims for one share every sharePeriod.
	sharePeriod    = 6 * time.Second
	retargetWindow = 30 * time.Second
	retargetShares = 30
	quietWindow    = 60 * time.Second // wait this long before lowering when no share arrived
	// maxQuietSteps limits how fast the difficulty falls for a miner that
	// stays silent: by 8, then by 64, then by 512 for every quietWindow.
	maxQuietSteps = 3
)

// Stratum error codes.
const (
	errOther        = 20
	errJobNotFound  = 21
	errDuplicate    = 22
	errLowDiff      = 23
	errUnauthorized = 24
	errNotSubbed    = 25
)

type request struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type session struct {
	srv         *Server
	conn        net.Conn
	extranonce1 [work.Extranonce1Size]byte
	remote      string
	connected   time.Time

	out       chan []byte
	closeOnce sync.Once
	done      chan struct{}
	failed    atomic.Bool // the connection was closed because of a bug

	// mu is held with defer wherever its holder calls out of the session, to
	// the job source or to send a message: a panic there must not leave it
	// held, since the manager's Broadcast and the status page take it too.
	mu           sync.Mutex
	subscribed   bool
	authorized   bool
	client       string
	worker       string
	payout       string
	payoutScript []byte
	ownPayout    bool
	versionMask  uint32

	diff     float64            // difficulty the miner should be using
	floor    float64            // lowest difficulty while the miner delivers shares
	minDiff  float64            // absolute lowest difficulty
	sentDiff float64            // last difficulty announced to the miner
	jobDiff  map[string]float64 // lowest difficulty announced while a job was live
	lastSeq  uint64             // sequence number of the newest job sent

	windowStart  time.Time
	windowShares int
	windowWork   float64 // sum of the difficulties the window's shares were credited at
	quietWindows int     // windows in a row that ended without a share
	// held is set from the moment the miner is told a new difficulty until
	// the next job is sent, and stops the difficulty from being adjusted
	// meanwhile (see retargetLocked).
	held bool
	// clock is time.Now, except in tests that play through minutes of mining.
	clock func() time.Time

	accepted      uint64
	rejected      uint64
	rejectReasons map[string]uint64 // rejection message -> count
	bestShare     float64
	lastShare     time.Time
	rate          rateMeter
}

func newSession(srv *Server, conn net.Conn, id [work.Extranonce1Size]byte) *session {
	host, _, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil {
		host = conn.RemoteAddr().String()
	}
	now := time.Now()
	return &session{
		srv: srv, conn: conn, extranonce1: id, remote: host, connected: now,
		out:         make(chan []byte, 64),
		done:        make(chan struct{}),
		diff:        srv.opts.StartDifficulty,
		floor:       srv.opts.MinDifficulty,
		minDiff:     srv.opts.MinDifficulty,
		jobDiff:     make(map[string]float64),
		windowStart: now,
		payout:      srv.opts.DefaultAddress, payoutScript: srv.opts.DefaultScript,
	}
}

// now is the time by the session's clock.
func (s *session) now() time.Time {
	if s.clock != nil {
		return s.clock()
	}
	return time.Now()
}

func (s *session) close() {
	s.closeOnce.Do(func() {
		close(s.done)
		s.conn.Close()
	})
}

// run serves the connection until it closes. A panic while serving it is a
// bug; it ends this connection only, never the program or another miner.
func (s *session) run() {
	defer s.close()
	go func() {
		if guard.Run("writing to a miner", s.writeLoop) != nil {
			s.fail()
		}
	}()
	if guard.Run("serving a miner", s.serve) != nil {
		s.fail()
	}

	s.mu.Lock()
	authorized, worker := s.authorized, s.worker
	s.mu.Unlock()
	if s.failed.Load() {
		log.Printf("Miner %s was disconnected after an internal error. The other miners are not affected.", s.label())
	} else if authorized {
		log.Printf("Miner disconnected: %s", s.label())
	}
	if authorized && s.srv.stats != nil {
		s.srv.stats.Disconnected(worker, time.Now())
	}
}

// serve handles the miner's messages until the connection closes.
func (s *session) serve() {
	reader := bufio.NewReaderSize(s.conn, maxLineLength)
	for {
		timeout := loginTimeout
		if s.ready() {
			timeout = idleTimeout
		}
		s.conn.SetReadDeadline(time.Now().Add(timeout))
		line, err := reader.ReadSlice('\n')
		if err != nil {
			if errors.Is(err, bufio.ErrBufferFull) {
				log.Printf("Miner %s sent an over-long message; disconnecting.", s.label())
			}
			return
		}
		var req request
		if err := json.Unmarshal(line, &req); err != nil {
			if len(strings.TrimSpace(string(line))) == 0 {
				continue
			}
			s.reply(nil, nil, errOther, "Invalid JSON")
			continue
		}
		if req.Method == "" {
			continue // an answer to one of our notifications
		}
		s.handle(&req)
	}
}

// fail ends a connection whose handling has hit a bug.
func (s *session) fail() {
	s.failed.Store(true)
	s.close()
}

func (s *session) ready() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.subscribed && s.authorized
}

func (s *session) loggedIn() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.authorized
}

func (s *session) writeLoop() {
	for {
		select {
		case <-s.done:
			return
		case msg := <-s.out:
			s.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if _, err := s.conn.Write(msg); err != nil {
				s.close()
				return
			}
		}
	}
}

// send queues a message. A miner that does not read its messages is
// disconnected rather than allowed to stall everyone else.
func (s *session) send(v any) {
	msg, err := json.Marshal(v)
	if err != nil {
		return
	}
	select {
	case s.out <- append(msg, '\n'):
	case <-s.done:
	default:
		s.close()
	}
}

func (s *session) reply(id json.RawMessage, result any, errCode int, errMsg string) {
	if id == nil {
		id = json.RawMessage("null")
	}
	msg := map[string]any{"id": id, "result": result, "error": nil}
	if errCode != 0 {
		msg["error"] = []any{errCode, errMsg, nil}
	}
	s.send(msg)
}

func (s *session) notify(method string, params ...any) {
	s.send(map[string]any{"id": nil, "method": method, "params": params})
}

func (s *session) label() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.worker != "" {
		return fmt.Sprintf("%s (%s)", s.worker, s.remote)
	}
	return s.remote
}

func (s *session) handle(req *request) {
	var params []json.RawMessage
	if len(req.Params) > 0 {
		// Tolerate anything that is not an array by treating it as empty.
		json.Unmarshal(req.Params, &params)
	}
	switch req.Method {
	case "mining.configure":
		s.handleConfigure(req, params)
	case "mining.subscribe":
		s.handleSubscribe(req, params)
	case "mining.authorize":
		s.handleAuthorize(req, params)
	case "mining.suggest_difficulty":
		s.handleSuggestDifficulty(req, params)
	case "mining.extranonce.subscribe":
		// The extranonce never changes during a connection.
		s.reply(req.ID, true, 0, "")
	case "mining.submit":
		s.handleSubmit(req, params)
	default:
		s.reply(req.ID, nil, errOther, "Unknown method")
	}
}

func stringParam(params []json.RawMessage, i int) (string, bool) {
	if i >= len(params) {
		return "", false
	}
	var v string
	if err := json.Unmarshal(params[i], &v); err != nil {
		return "", false
	}
	return v, true
}

// handleConfigure negotiates protocol extensions (BIP310). Only version
// rolling is supported.
func (s *session) handleConfigure(req *request, params []json.RawMessage) {
	var extensions []string
	var options map[string]json.RawMessage
	if len(params) > 0 {
		json.Unmarshal(params[0], &extensions)
	}
	if len(params) > 1 {
		json.Unmarshal(params[1], &options)
	}
	result := map[string]any{}
	for _, ext := range extensions {
		if ext != "version-rolling" {
			result[ext] = false
			continue
		}
		mask := uint32(versionRollingMask)
		var requested string
		if raw, ok := options["version-rolling.mask"]; ok && json.Unmarshal(raw, &requested) == nil {
			if v, err := strconv.ParseUint(requested, 16, 32); err == nil {
				mask &= uint32(v)
			}
		}
		s.mu.Lock()
		s.versionMask = mask
		s.mu.Unlock()
		result["version-rolling"] = true
		result["version-rolling.mask"] = fmt.Sprintf("%08x", mask)
	}
	s.reply(req.ID, result, 0, "")
}

func (s *session) handleSubscribe(req *request, params []json.RawMessage) {
	client, _ := stringParam(params, 0)
	client = cleanString(client, 64)
	id := hex.EncodeToString(s.extranonce1[:])
	s.mu.Lock()
	s.subscribed = true
	s.client = client
	ready := s.authorized
	s.mu.Unlock()

	s.reply(req.ID, []any{
		[]any{[]any{"mining.set_difficulty", id}, []any{"mining.notify", id}},
		id,
		work.Extranonce2Size,
	}, 0, "")
	if ready {
		s.sendCurrentJob()
	}
}

func (s *session) handleAuthorize(req *request, params []json.RawMessage) {
	user, _ := stringParam(params, 0)
	user = cleanString(user, 128)

	// The first authorize decides the payout for the whole connection.
	// Work already handed out was built for that address, so it must not
	// change underneath the miner.
	s.mu.Lock()
	already := s.authorized
	s.mu.Unlock()
	if already {
		s.reply(req.ID, true, 0, "")
		return
	}

	// A username that is (or starts with) a valid address for this network
	// selects where this miner's block reward goes: "address" or
	// "address.workername". Anything else is just a name.
	payout, script, own := s.srv.opts.DefaultAddress, s.srv.opts.DefaultScript, false
	candidate, _, _ := strings.Cut(user, ".")
	if sc, err := btc.AddressToScript(candidate, s.srv.opts.Network); err == nil {
		payout, script, own = candidate, sc, true
	}
	if user == "" {
		user = "miner"
	}

	s.mu.Lock()
	s.authorized = true
	s.worker = user
	s.payout, s.payoutScript, s.ownPayout = payout, script, own
	ready := s.subscribed
	client := s.client
	s.mu.Unlock()

	s.reply(req.ID, true, 0, "")
	note := "default payout address"
	if own {
		note = "paying to its own address " + payout
	}
	log.Printf("Miner connected: %s from %s [%s], %s", user, s.remote, client, note)
	if ready {
		s.sendCurrentJob()
	}
	// Reports to the statistics are only queued (see queuedRecorder), so they
	// never delay a miner. The session handles one message at a time, so this
	// one still comes before any share the miner submits.
	if s.srv.stats != nil {
		s.srv.stats.Connected(user, time.Now())
	}
}

func (s *session) handleSuggestDifficulty(req *request, params []json.RawMessage) {
	var d float64
	if len(params) == 0 || json.Unmarshal(params[0], &d) != nil || d <= 0 || math.IsNaN(d) || math.IsInf(d, 0) {
		s.reply(req.ID, nil, errOther, "Invalid difficulty")
		return
	}
	// The suggestion becomes the starting point and, as long as the miner
	// keeps delivering shares, the lower bound.
	d = max(d, s.srv.opts.MinDifficulty)
	s.reply(req.ID, true, 0, "")
	s.mu.Lock()
	defer s.mu.Unlock()
	s.diff, s.floor = d, d
	s.windowStart, s.windowShares, s.windowWork, s.quietWindows = s.now(), 0, 0, 0
	s.announceDifficultyLocked()
}

// announceDifficulty is announceDifficultyLocked for a caller that does not
// hold s.mu.
func (s *session) announceDifficulty() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.announceDifficultyLocked()
}

// announceDifficultyLocked tells the miner its difficulty if it changed.
// Only mining.set_difficulty is sent: re-sending the current job would make
// the miner start that job over and repeat work it has already done.
// Firmware differs on whether a new difficulty applies to the job in hand
// or only to the next one, so for jobs already sent the lower of the old
// and new value is accepted, and the difficulty is not adjusted again until
// the next job has gone out (see held).
func (s *session) announceDifficultyLocked() {
	if !s.subscribed || !s.authorized {
		return
	}
	diff := s.diff
	if job := s.srv.jobs.Current(); job != nil {
		// A share can never need to be harder than the block itself.
		diff = math.Min(diff, job.NetDiff)
	}
	if diff == s.sentDiff {
		return
	}
	s.notify("mining.set_difficulty", diff)
	s.sentDiff = diff
	s.held = true
	for id, d := range s.jobDiff {
		if diff < d {
			s.jobDiff[id] = diff
		}
	}
}

func (s *session) sendCurrentJob() {
	if job := s.srv.jobs.Current(); job != nil {
		s.sendJob(job, true)
	}
}

// sendJob announces the difficulty (if it changed) and the job.
func (s *session) sendJob(job *work.Job, clean bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.subscribed || !s.authorized {
		return
	}
	if job.Seq < s.lastSeq {
		return // a newer job has already been sent
	}
	s.lastSeq = job.Seq
	now := s.now()
	if s.held {
		// The first job since the miner was told a new difficulty. Every
		// miner works at that difficulty from this job on, so its speed can
		// be measured again, starting now.
		s.held = false
		s.windowStart, s.windowShares, s.windowWork = now, 0, 0
	} else if before := s.diff; s.retargetLocked(now) && s.diff < before {
		// Lowered for a miner that is not finding shares, and so says
		// nothing: give the easier work time to show before the silence
		// counts as a dead connection (see idleTimeout). This job carries
		// the new difficulty, so there is nothing to hold.
		s.conn.SetReadDeadline(time.Now().Add(idleTimeout))
	}

	// A share can never need to be harder than the block itself.
	diff := math.Min(s.diff, job.NetDiff)
	if diff != s.sentDiff {
		s.notify("mining.set_difficulty", diff)
		s.sentDiff = diff
	}
	if job.CleanJobs {
		clear(s.jobDiff) // jobs for the old tip are gone
	}
	if prev, ok := s.jobDiff[job.ID]; !ok || diff < prev {
		s.jobDiff[job.ID] = diff
	}
	// Forget jobs the manager no longer keeps.
	if len(s.jobDiff) > 256 {
		for id := range s.jobDiff {
			if s.srv.jobs.Get(id) == nil {
				delete(s.jobDiff, id)
			}
		}
	}

	branches := make([]string, len(job.Branches))
	for i, b := range job.Branches {
		branches[i] = hex.EncodeToString(b[:])
	}
	s.notify("mining.notify",
		job.ID,
		job.StratumPrevHash(),
		hex.EncodeToString(job.Coinbase1()),
		hex.EncodeToString(job.Coinbase2(s.payoutScript)),
		branches,
		fmt.Sprintf("%08x", job.Version),
		fmt.Sprintf("%08x", job.Bits),
		fmt.Sprintf("%08x", job.CurTime),
		clean || job.CleanJobs,
	)
}

// retargetLocked adjusts the difficulty towards one share per sharePeriod.
// It reports whether the difficulty changed.
//
// It goes by the work the miner delivered, not by the number of its shares:
// the sum of the difficulties the window's shares were credited at, per
// second, is the miner's hashrate.
//
// That only holds while it is known what difficulty the miner works at, and
// after a change it is not. Firmware differs on whether a new difficulty
// applies to the job in hand or only to the next one, so until the next job
// has gone out a miner may be working at either. Its shares then mislead
// whichever way they are read. After a raise, a miner still at the old
// difficulty keeps sending shares at the old rate; counted one by one, each
// 30 of them raised the difficulty by 16 again, and a fast miner ended up
// with a difficulty no hardware can meet. Weighed by their difficulty, the
// odd share that happens to meet the new difficulty counts for all of it,
// and one such share outweighs a window of the others. So the callers do not
// call this at all between a change and the next job (see held): they start
// a new window at that job, from which every miner works at the difficulty
// it was last told.
func (s *session) retargetLocked(now time.Time) bool {
	elapsed := now.Sub(s.windowStart)
	if s.windowShares < retargetShares && elapsed < retargetWindow {
		return false
	}
	if s.windowShares == 0 && elapsed < quietWindow {
		return false
	}
	seconds := math.Max(elapsed.Seconds(), 0.5)
	shares, work := s.windowShares, s.windowWork
	s.windowStart, s.windowShares, s.windowWork = now, 0, 0

	if shares == 0 {
		// Silence says only that the miner is much slower than assumed, not
		// by how much, so each silent window in a row lowers the difficulty
		// further than the last. A miner far below its starting difficulty
		// then gets work it can do within a few minutes. A miner that
		// suggested a difficulty and cannot produce a share at it goes below
		// its suggestion too.
		s.quietWindows++
		next := s.diff / math.Pow(8, float64(min(s.quietWindows, maxQuietSteps)))
		next = math.Max(roundDifficulty(next), s.minDiff)
		if next == s.diff {
			return false
		}
		s.diff = next
		return true
	}
	s.quietWindows = 0

	// ideal is the difficulty at which this hashrate gives one share per
	// sharePeriod.
	ideal := work / seconds * sharePeriod.Seconds()
	ratio := ideal / s.diff
	if ratio > 0.5 && ratio < 2 {
		return false // close enough; avoid flapping
	}
	// A miner that suggested a difficulty keeps it as its lower bound while
	// it delivers shares.
	next := s.diff * math.Min(math.Max(ratio, 1.0/8), 16)
	next = math.Max(roundDifficulty(next), s.floor)
	if next == s.diff {
		return false
	}
	s.diff = next
	return true
}

// roundDifficulty snaps difficulties of 1 and above to a power of two so the
// numbers miners display stay tidy.
func roundDifficulty(d float64) float64 {
	if d < 1 {
		return d
	}
	return math.Exp2(math.Round(math.Log2(d)))
}

// hexUint32 reads a 32-bit number written as hex. Miners send eight digits.
// Fewer are read too, as the same number without its leading zeros: turning a
// share away over how its numbers are written could turn away a block.
func hexUint32(s string) (uint32, bool) {
	if len(s) == 0 || len(s) > 8 {
		return 0, false
	}
	v, err := strconv.ParseUint(s, 16, 32)
	return uint32(v), err == nil
}

// submission is a mining.submit as the miner sent it.
type submission struct {
	jobID       string
	extranonce2 []byte
	ntime       uint32
	nonce       uint32
	versionBits uint32 // 0 if the miner sent none
}

// parseSubmit reads the parameters of a mining.submit: the worker's name,
// which is not used, then the job id, the extranonce2, the time, the nonce
// and, from a miner that rolls the version, the version bits. It returns what
// is wrong with them, or "".
func parseSubmit(params []json.RawMessage) (submission, string) {
	jobID, ok1 := stringParam(params, 1)
	en2Hex, ok2 := stringParam(params, 2)
	ntimeHex, ok3 := stringParam(params, 3)
	nonceHex, ok4 := stringParam(params, 4)
	if !(ok1 && ok2 && ok3 && ok4) {
		return submission{}, "Malformed submit"
	}
	extranonce2, err := hex.DecodeString(en2Hex)
	ntime, okT := hexUint32(ntimeHex)
	nonce, okN := hexUint32(nonceHex)
	if err != nil || len(extranonce2) != work.Extranonce2Size || !okT || !okN {
		return submission{}, "Malformed submit"
	}
	sub := submission{jobID: jobID, extranonce2: extranonce2, ntime: ntime, nonce: nonce}
	if bitsHex, ok := stringParam(params, 5); ok {
		if sub.versionBits, ok = hexUint32(bitsHex); !ok {
			return submission{}, "Malformed version bits"
		}
	}
	return sub, ""
}

// versionReadings returns the block versions a miner may have hashed for a
// job of the given version when it submits bits, and whether the bits keep to
// mask, the bits it was allowed to change.
//
// BIP310 says the submitted bits replace the masked bits of the job's
// version. Some firmware sends the XOR against the job's version instead. The
// two only differ when the job's version already has bits inside the mask.
// Both are returned, and the one that produces the better hash is the one the
// miner hashed: the other is a random number.
//
// Bits outside the mask break the protocol, and such a share is not paid.
// The header behind it may still be a block, though, and the network does not
// ask which bits a miner was allowed to change. So for those the readings
// are returned that a miner rolling too many bits, or sending the whole
// version, may have meant, for the caller to test against the network target
// and nothing else.
func versionReadings(jobVersion, mask, bits uint32) (versions []uint32, inMask bool) {
	add := func(v uint32) {
		for _, known := range versions {
			if known == v {
				return
			}
		}
		versions = append(versions, v)
	}
	if bits&^mask == 0 {
		add(jobVersion&^mask | bits)
		add(jobVersion ^ bits)
		return versions, true
	}
	add(jobVersion ^ bits)                     // changed bits, sent as a difference
	add(jobVersion | bits)                     // changed bits, added to the job's version
	add(jobVersion&^versionRollingMask | bits) // BIP310 with every bit BIP320 allows
	add(bits)                                  // the whole version
	return versions, false
}

// solution is a submitted share worked out against its job: the header the
// miner hashed, and what that hash is worth.
type solution struct {
	header    [80]byte
	hash      btc.Hash
	hashInt   *big.Int
	version   uint32
	shareDiff float64
	isBlock   bool // the hash meets the network target
}

// solve builds the header a submission stands for, in each of the given
// readings of its version, and returns the one with the best hash.
func solve(job *work.Job, coinbase []byte, sub submission, versions []uint32) solution {
	var best solution
	for i, v := range versions {
		header := job.Header(coinbase, v, sub.ntime, sub.nonce)
		hash := btc.SHA256d(header[:])
		if hashInt := btc.HashToInt(hash); i == 0 || hashInt.Cmp(best.hashInt) < 0 {
			best = solution{header: header, hash: hash, hashInt: hashInt, version: v}
		}
	}
	best.shareDiff = btc.DifficultyFromTarget(best.hashInt)
	best.isBlock = best.hashInt.Cmp(job.Target) <= 0
	return best
}

// rejectShare answers a mining.submit with an error and counts the share as
// rejected.
func (s *session) rejectShare(req *request, code int, msg string) {
	s.mu.Lock()
	s.rejected++
	if s.rejectReasons == nil {
		s.rejectReasons = make(map[string]uint64)
	}
	s.rejectReasons[msg]++
	worker := s.worker
	s.mu.Unlock()
	s.srv.rejected.Add(1)
	s.reply(req.ID, nil, code, msg)
	// A miner that has not logged in yet has no name to record it under.
	if s.srv.stats != nil && worker != "" {
		s.srv.stats.Rejected(worker, time.Now())
	}
}

// handleSubmit takes a share. It goes in this order, and the order matters:
//
//  1. Read the share and find its job. Without both, there is no header.
//  2. Build the header and hash it.
//  3. If the hash meets the network target, the share is a block, and from
//     here on nothing but being a repeat can turn it away. Only the node can
//     judge a block, and refusing the header here would throw the block away.
//  4. Anything else is an ordinary share, and must keep to the rules that
//     make it worth paying: the mask, a job that is current, a time close to
//     the job's, and the difficulty the miner was given.
//
// A rule for ordinary shares must never be checked before step 3.
func (s *session) handleSubmit(req *request, params []json.RawMessage) {
	s.mu.Lock()
	subscribed, authorized := s.subscribed, s.authorized
	payoutScript, mask, worker := s.payoutScript, s.versionMask, s.worker
	s.mu.Unlock()
	if mask == 0 {
		// Some firmware rolls the version without negotiating it first.
		// Refusing such a share could throw away a valid block.
		mask = versionRollingMask
	}
	// A miner that has not subscribed and logged in was never sent a job.
	if !subscribed {
		s.rejectShare(req, errNotSubbed, "Not subscribed")
		return
	}
	if !authorized {
		s.rejectShare(req, errUnauthorized, "Unauthorized worker")
		return
	}

	// 1. The share and its job.
	sub, problem := parseSubmit(params)
	if problem != "" {
		s.rejectShare(req, errOther, problem)
		return
	}
	job := s.srv.jobs.Get(sub.jobID)
	if job == nil {
		s.rejectShare(req, errJobNotFound, "Job not found (stale)")
		return
	}

	// 2. The header and its hash.
	versions, inMask := versionReadings(job.Version, mask, sub.versionBits)
	coinbase := job.Coinbase(payoutScript, s.extranonce1[:], sub.extranonce2)
	sol := solve(job, coinbase, sub, versions)

	s.mu.Lock()
	assigned, known := s.jobDiff[sub.jobID]
	if !known {
		assigned = math.Min(s.diff, job.NetDiff)
	}
	current := s.sentDiff
	s.mu.Unlock()

	// 4. The rules for an ordinary share. (3. is that a block skips them.)
	if !sol.isBlock {
		if code, msg := ordinaryShareProblem(job, sub, sol, inMask, assigned); msg != "" {
			s.rejectShare(req, code, msg)
			return
		}
	}
	if !job.MarkShare(s.extranonce1[:], sub.extranonce2, sub.ntime, sub.nonce, sol.version) {
		s.rejectShare(req, errDuplicate, "Duplicate share")
		return
	}
	if sol.isBlock {
		log.Printf("*** BLOCK FOUND by %s: height %d, hash %s ***", worker, job.Height, sol.hash)
		s.srv.blocks.Found(job.Height, sol.hash.String(), job.Block(sol.header, coinbase), worker)
	}

	s.acceptShare(req, worker, creditFor(assigned, current, sol.shareDiff), sol.shareDiff)
}

// creditFor returns the difficulty an accepted share counts for: the one the
// miner was working at, as far as that can be told. assigned is the lowest
// difficulty announced while the share's job was live, and what the share
// had to meet; current is the difficulty last announced. A share that meets
// a higher current difficulty is taken to have been mined at it.
func creditFor(assigned, current, shareDiff float64) float64 {
	if current > assigned && shareDiff >= current {
		return current
	}
	return assigned
}

// ordinaryShareProblem checks a share that is not a block against the rules
// for ordinary shares. It returns the Stratum error for the first one the
// share breaks, or an empty message.
func ordinaryShareProblem(job *work.Job, sub submission, sol solution, inMask bool, assigned float64) (code int, msg string) {
	switch {
	case !inMask:
		return errOther, "Version bits outside the allowed mask"
	case !job.AcceptsShares():
		return errJobNotFound, "Job not found (stale)"
	// A short time window: new work arrives every few seconds, so rolling
	// the time further gains nothing.
	case sub.ntime < job.CurTime || sub.ntime > job.CurTime+maxNTimeRoll:
		return errOther, "Time out of range"
	case sol.hashInt.Cmp(btc.TargetFromDifficulty(assigned)) > 0:
		return errLowDiff, "Difficulty too low"
	}
	return 0, ""
}

// acceptShare counts an accepted share, tells the miner, and adjusts its
// difficulty. credit is the difficulty the share counts for, shareDiff the
// difficulty its hash reached.
func (s *session) acceptShare(req *request, worker string, credit, shareDiff float64) {
	now := s.now()
	s.mu.Lock()
	s.accepted++
	s.windowShares++
	s.windowWork += credit
	s.lastShare = now
	s.rate.add(now, credit)
	if shareDiff > s.bestShare {
		s.bestShare = shareDiff
	}
	// Not between a change of the difficulty and the next job: until then
	// the miner may be working at the old one or the new (see retargetLocked).
	changed := !s.held && s.retargetLocked(now)
	s.mu.Unlock()
	s.srv.accepted.Add(1)
	s.srv.noteBest(shareDiff)
	s.reply(req.ID, true, 0, "")

	if changed {
		s.announceDifficulty()
	}
	if s.srv.stats != nil {
		s.srv.stats.Accepted(worker, now, credit, shareDiff)
	}
}

func (s *session) status(now time.Time) (MinerStatus, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.authorized {
		return MinerStatus{}, false
	}
	return MinerStatus{
		Worker: s.worker, Payout: s.payout, OwnPayout: s.ownPayout,
		Address: s.remote, Client: s.client, Connected: s.connected,
		Difficulty: s.sentDiff, Hashrate: s.rate.hashrate(now, s.connected),
		Accepted: s.accepted, Rejected: s.rejected, RejectReasons: maps.Clone(s.rejectReasons),
		BestShare: s.bestShare, LastShare: s.lastShare,
	}, true
}

// cleanString makes a miner-supplied string safe to log and display: it
// drops control characters and limits the length.
func cleanString(s string, n int) string {
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f || r == utf8.RuneError {
			return -1
		}
		return r
	}, strings.TrimSpace(s))
	if len(s) > n {
		s = s[:n]
		for len(s) > 0 && !utf8.ValidString(s) {
			s = s[:len(s)-1]
		}
	}
	return s
}

// rateMeter estimates hashrate from accepted shares over the last minutes.
type rateMeter struct {
	buckets [rateBuckets]struct {
		minute int64
		work   float64 // sum of share difficulties
	}
}

const rateBuckets = 10

func (r *rateMeter) add(now time.Time, difficulty float64) {
	minute := now.Unix() / 60
	b := &r.buckets[minute%rateBuckets]
	if b.minute != minute {
		b.minute, b.work = minute, 0
	}
	b.work += difficulty
}

// hashrate returns hashes per second. A share of difficulty d stands for
// d * 2^32 hashes on average.
func (r *rateMeter) hashrate(now time.Time, since time.Time) float64 {
	minute := now.Unix() / 60
	var total float64
	for _, b := range r.buckets {
		if minute-b.minute < rateBuckets {
			total += b.work
		}
	}
	// The window covers the nine full minutes before this one plus the
	// part of the current minute that has passed, or less for a new miner.
	window := float64((rateBuckets-1)*60 + now.Unix()%60 + 1)
	if age := now.Sub(since).Seconds(); age < window {
		window = math.Max(age, 1)
	}
	return total * 4294967296 / window
}
