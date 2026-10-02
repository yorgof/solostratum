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
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/yorgof/solostratum/internal/btc"
	"github.com/yorgof/solostratum/internal/work"
)

const (
	// versionRollingMask is the set of block version bits miners may change
	// (BIP320).
	versionRollingMask = 0x1fffe000

	maxLineLength = 16 * 1024
	idleTimeout   = 10 * time.Minute
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

func (s *session) close() {
	s.closeOnce.Do(func() {
		close(s.done)
		s.conn.Close()
	})
}

// run serves the connection until it closes.
func (s *session) run() {
	defer s.close()
	go s.writeLoop()

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
			break
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

	s.mu.Lock()
	authorized := s.authorized
	s.mu.Unlock()
	if authorized {
		log.Printf("Miner disconnected: %s", s.label())
	}
}

func (s *session) ready() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.subscribed && s.authorized
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
	id := hex.EncodeToString(s.extranonce1[:])
	s.mu.Lock()
	s.subscribed = true
	s.client = cleanString(client, 64)
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
	s.diff, s.floor = d, d
	s.windowStart, s.windowShares = time.Now(), 0
	s.announceDifficultyLocked()
	s.mu.Unlock()
}

// announceDifficultyLocked tells the miner its difficulty if it changed.
// Only mining.set_difficulty is sent: re-sending the current job would make
// the miner start that job over and repeat work it has already done.
// Firmware differs on whether a new difficulty applies to the job in hand
// or only to the next one, so for jobs already sent the lower of the old
// and new value is accepted.
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
	s.retargetLocked(time.Now())

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
func (s *session) retargetLocked(now time.Time) bool {
	elapsed := now.Sub(s.windowStart)
	if s.windowShares < retargetShares && elapsed < retargetWindow {
		return false
	}
	if s.windowShares == 0 && elapsed < quietWindow {
		return false
	}
	seconds := math.Max(elapsed.Seconds(), 0.5)
	shares := s.windowShares
	ratio := float64(shares) / seconds * sharePeriod.Seconds()
	s.windowStart, s.windowShares = now, 0
	if ratio > 0.5 && ratio < 2 {
		return false // close enough; avoid flapping
	}
	// A miner that suggested a difficulty keeps it as its lower bound,
	// unless it cannot produce any share at that level.
	floor := s.floor
	if shares == 0 {
		floor = s.minDiff
	}
	next := s.diff * math.Min(math.Max(ratio, 1.0/8), 16)
	next = math.Max(roundDifficulty(next), floor)
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

func hexUint32(s string) (uint32, bool) {
	if len(s) != 8 {
		return 0, false
	}
	v, err := strconv.ParseUint(s, 16, 32)
	return uint32(v), err == nil
}

func (s *session) handleSubmit(req *request, params []json.RawMessage) {
	reject := func(code int, msg string) {
		s.mu.Lock()
		s.rejected++
		if s.rejectReasons == nil {
			s.rejectReasons = make(map[string]uint64)
		}
		s.rejectReasons[msg]++
		s.mu.Unlock()
		s.srv.rejected.Add(1)
		s.reply(req.ID, nil, code, msg)
	}

	s.mu.Lock()
	subscribed, authorized := s.subscribed, s.authorized
	payoutScript, mask, worker := s.payoutScript, s.versionMask, s.worker
	s.mu.Unlock()
	if mask == 0 {
		// Some firmware rolls the version without negotiating it first.
		// Refusing such a share could throw away a valid block.
		mask = versionRollingMask
	}
	if !subscribed {
		reject(errNotSubbed, "Not subscribed")
		return
	}
	if !authorized {
		reject(errUnauthorized, "Unauthorized worker")
		return
	}

	jobID, ok1 := stringParam(params, 1)
	en2Hex, ok2 := stringParam(params, 2)
	ntimeHex, ok3 := stringParam(params, 3)
	nonceHex, ok4 := stringParam(params, 4)
	if !(ok1 && ok2 && ok3 && ok4) {
		reject(errOther, "Malformed submit")
		return
	}
	extranonce2, err := hex.DecodeString(en2Hex)
	ntime, okT := hexUint32(ntimeHex)
	nonce, okN := hexUint32(nonceHex)
	if err != nil || len(extranonce2) != work.Extranonce2Size || !okT || !okN {
		reject(errOther, "Malformed submit")
		return
	}
	var versionBits uint32
	if bitsHex, ok := stringParam(params, 5); ok {
		if versionBits, ok = hexUint32(bitsHex); !ok {
			reject(errOther, "Malformed version bits")
			return
		}
		if versionBits&^mask != 0 {
			reject(errOther, "Version bits outside the allowed mask")
			return
		}
	}

	job := s.srv.jobs.Get(jobID)
	if job == nil {
		reject(errJobNotFound, "Job not found (stale)")
		return
	}
	// The block time may only move forward a little. Moving it further
	// gains nothing, since new work arrives every few seconds.
	if ntime < job.CurTime || ntime > job.CurTime+maxNTimeRoll {
		reject(errOther, "Time out of range")
		return
	}

	// BIP310 says the submitted bits replace the masked bits of the job's
	// version. Some firmware sends the XOR against the job's version
	// instead. The two only differ when the job's version already has bits
	// inside the mask, so accept whichever one produces the better hash.
	versions := []uint32{job.Version&^mask | versionBits}
	if alt := job.Version ^ versionBits; alt != versions[0] {
		versions = append(versions, alt)
	}
	coinbase := job.Coinbase(payoutScript, s.extranonce1[:], extranonce2)
	var header [80]byte
	var hash btc.Hash
	var version uint32
	for i, v := range versions {
		h := job.Header(coinbase, v, ntime, nonce)
		sum := btc.SHA256d(h[:])
		if i == 0 || btc.HashToInt(sum).Cmp(btc.HashToInt(hash)) < 0 {
			header, hash, version = h, sum, v
		}
	}
	hashInt := btc.HashToInt(hash)
	shareDiff := btc.DifficultyFromTarget(hashInt)
	isBlock := hashInt.Cmp(job.Target) <= 0

	s.mu.Lock()
	assigned, known := s.jobDiff[jobID]
	if !known {
		assigned = math.Min(s.diff, job.NetDiff)
	}
	current := s.sentDiff
	s.mu.Unlock()

	if !isBlock && hashInt.Cmp(btc.TargetFromDifficulty(assigned)) > 0 {
		reject(errLowDiff, "Difficulty too low")
		return
	}
	if !job.MarkShare(s.extranonce1[:], extranonce2, ntime, nonce, version) {
		reject(errDuplicate, "Duplicate share")
		return
	}

	// Credit the share at the difficulty the miner was actually working at.
	credit := assigned
	if current > assigned && shareDiff >= current {
		credit = current
	}
	now := time.Now()
	s.mu.Lock()
	s.accepted++
	s.windowShares++
	s.lastShare = now
	s.rate.add(now, credit)
	if shareDiff > s.bestShare {
		s.bestShare = shareDiff
	}
	changed := s.retargetLocked(now)
	s.mu.Unlock()
	s.srv.accepted.Add(1)
	s.srv.noteBest(shareDiff)
	s.reply(req.ID, true, 0, "")

	if isBlock {
		log.Printf("*** BLOCK FOUND by %s: height %d, hash %s ***", worker, job.Height, hash)
		s.srv.blocks.Found(job.Height, hash.String(), job.Block(header, coinbase), worker)
	}
	if changed {
		s.mu.Lock()
		s.announceDifficultyLocked()
		s.mu.Unlock()
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
