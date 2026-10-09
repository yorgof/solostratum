// Package axesim is a software stand-in for a Bitaxe, used by the tests.
//
// It sends the same Stratum messages in the same order as the Bitaxe
// firmware (ESP-Miner / AxeOS) and builds block headers from mining.notify
// the way the firmware does. It deliberately shares no code with the server
// side of this project, so a mistake in the server's byte ordering cannot be
// mirrored here and go unnoticed.
package axesim

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net"
	"strconv"
	"sync"
	"time"
)

// Options describes the simulated device.
type Options struct {
	User              string
	Password          string
	SuggestDifficulty uint32 // 0 sends no mining.suggest_difficulty
	ExtranonceSub     bool   // send mining.extranonce.subscribe
	NoVersionRolling  bool   // skip mining.configure, like very old firmware
	Timeout           time.Duration
}

// Job is a parsed mining.notify.
type Job struct {
	ID        string
	PrevHash  string
	Coinbase1 string
	Coinbase2 string
	Branches  []string
	Version   uint32
	NBits     uint32
	NTime     uint32
	CleanJobs bool
	Seq       int // increases with every notify received
}

// Share is a solution ready to submit.
type Share struct {
	JobID       string
	Extranonce2 string
	NTime       uint32
	Nonce       uint32
	VersionBits uint32
	Header      [80]byte
	Hash        [32]byte // as hashed; reverse for display
}

// Result is the server's answer to a submit.
type Result struct {
	Accepted bool
	Code     int
	Message  string
}

// Miner is one simulated device connection.
type Miner struct {
	conn    net.Conn
	opts    Options
	timeout time.Duration

	writeMu sync.Mutex
	nextID  int

	mu          sync.Mutex
	cond        *sync.Cond
	pending     map[int]chan json.RawMessage
	extranonce1 string
	en2Len      int
	versionMask uint32
	difficulty  float64
	job         *Job
	seq         int
	closed      bool
	en2Counter  uint64
	notifies    []Job
}

type message struct {
	ID     *int              `json:"id"`
	Method string            `json:"method"`
	Params []json.RawMessage `json:"params"`
	Result json.RawMessage   `json:"result"`
	Error  json.RawMessage   `json:"error"`
}

// Dial connects and performs the firmware's start-up sequence:
// mining.configure, mining.subscribe, mining.authorize, and after the
// authorize answer mining.suggest_difficulty and mining.extranonce.subscribe.
func Dial(addr string, opts Options) (*Miner, error) {
	if opts.Timeout == 0 {
		opts.Timeout = 10 * time.Second
	}
	conn, err := net.DialTimeout("tcp", addr, opts.Timeout)
	if err != nil {
		return nil, err
	}
	m := &Miner{conn: conn, opts: opts, timeout: opts.Timeout, pending: make(map[int]chan json.RawMessage), difficulty: 1}
	m.cond = sync.NewCond(&m.mu)
	go m.readLoop()

	var configure, subscribe, authorize chan json.RawMessage
	if !opts.NoVersionRolling {
		configure = m.request(`{"id":%d,"method":"mining.configure","params":[["version-rolling"],{"version-rolling.mask":"ffffffff"}]}`)
	}
	subscribe = m.request(`{"id":%d,"method":"mining.subscribe","params":["bitaxe/BM1370/v2.14.1"]}`)
	authorize = m.request(`{"id":%d,"method":"mining.authorize","params":[%s,%s]}`, quote(opts.User), quote(opts.Password))

	if configure != nil {
		raw, err := m.await(configure)
		if err != nil {
			return nil, m.fail("mining.configure: %w", err)
		}
		var res struct {
			Rolling bool   `json:"version-rolling"`
			Mask    string `json:"version-rolling.mask"`
		}
		if err := json.Unmarshal(raw, &res); err != nil || !res.Rolling {
			return nil, m.fail("mining.configure: unexpected result %s", raw)
		}
		mask, err := strconv.ParseUint(res.Mask, 16, 32)
		if err != nil {
			return nil, m.fail("mining.configure: bad mask %q", res.Mask)
		}
		m.mu.Lock()
		m.versionMask = uint32(mask)
		m.mu.Unlock()
	}

	raw, err := m.await(subscribe)
	if err != nil {
		return nil, m.fail("mining.subscribe: %w", err)
	}
	var sub []json.RawMessage
	var en1 string
	var en2Len int
	if json.Unmarshal(raw, &sub) != nil || len(sub) < 3 || json.Unmarshal(sub[1], &en1) != nil || json.Unmarshal(sub[2], &en2Len) != nil {
		return nil, m.fail("mining.subscribe: unexpected result %s", raw)
	}
	m.mu.Lock()
	m.extranonce1, m.en2Len = en1, en2Len
	m.mu.Unlock()

	raw, err = m.await(authorize)
	if err != nil {
		return nil, m.fail("mining.authorize: %w", err)
	}
	if string(raw) != "true" {
		return nil, m.fail("mining.authorize: rejected (%s)", raw)
	}
	if opts.SuggestDifficulty > 0 {
		if _, err := m.await(m.request(`{"id":%d,"method":"mining.suggest_difficulty","params":[%d]}`, opts.SuggestDifficulty)); err != nil {
			return nil, m.fail("mining.suggest_difficulty: %w", err)
		}
	}
	if opts.ExtranonceSub {
		if _, err := m.await(m.request(`{"id":%d,"method":"mining.extranonce.subscribe","params":[]}`)); err != nil {
			return nil, m.fail("mining.extranonce.subscribe: %w", err)
		}
	}
	return m, nil
}

func quote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func (m *Miner) fail(format string, args ...any) error {
	m.Close()
	return fmt.Errorf(format, args...)
}

// Close drops the connection.
func (m *Miner) Close() { m.conn.Close() }

// request sends one line; the first format argument is the message id.
func (m *Miner) request(format string, args ...any) chan json.RawMessage {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	m.nextID++
	id := m.nextID
	ch := make(chan json.RawMessage, 1)
	m.mu.Lock()
	if m.closed {
		close(ch)
	} else {
		m.pending[id] = ch
	}
	m.mu.Unlock()
	line := fmt.Sprintf(format, append([]any{id}, args...)...) + "\n"
	m.conn.SetWriteDeadline(time.Now().Add(m.timeout))
	// A failed write is noticed by the read loop, which closes ch.
	m.conn.Write([]byte(line))
	return ch
}

// await returns the "result" of an answer, or an error built from "error".
func (m *Miner) await(ch chan json.RawMessage) (json.RawMessage, error) {
	select {
	case raw, ok := <-ch:
		if !ok {
			return nil, errors.New("connection closed")
		}
		var msg message
		if err := json.Unmarshal(raw, &msg); err != nil {
			return nil, err
		}
		if len(msg.Error) > 0 && string(msg.Error) != "null" {
			return nil, &ServerError{Raw: string(msg.Error)}
		}
		return msg.Result, nil
	case <-time.After(m.timeout):
		return nil, errors.New("timed out waiting for the server's answer")
	}
}

// ServerError is a Stratum error answer.
type ServerError struct{ Raw string }

func (e *ServerError) Error() string { return "server error " + e.Raw }

func (m *Miner) readLoop() {
	reader := bufio.NewReaderSize(m.conn, 1<<20)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			m.mu.Lock()
			m.closed = true
			for id, ch := range m.pending {
				close(ch)
				delete(m.pending, id)
			}
			m.cond.Broadcast()
			m.mu.Unlock()
			return
		}
		var msg message
		if json.Unmarshal(line, &msg) != nil {
			continue
		}
		m.mu.Lock()
		switch msg.Method {
		case "":
			if msg.ID != nil {
				if ch, ok := m.pending[*msg.ID]; ok {
					ch <- json.RawMessage(append([]byte(nil), line...))
					delete(m.pending, *msg.ID)
				}
			}
		case "mining.set_difficulty":
			if len(msg.Params) > 0 {
				json.Unmarshal(msg.Params[0], &m.difficulty)
			}
		case "mining.set_extranonce":
			if len(msg.Params) >= 2 {
				json.Unmarshal(msg.Params[0], &m.extranonce1)
				json.Unmarshal(msg.Params[1], &m.en2Len)
			}
		case "mining.set_version_mask":
			var s string
			if len(msg.Params) > 0 && json.Unmarshal(msg.Params[0], &s) == nil {
				if v, err := strconv.ParseUint(s, 16, 32); err == nil {
					m.versionMask = uint32(v)
				}
			}
		case "mining.notify":
			if job, err := parseNotify(msg.Params); err == nil {
				m.seq++
				job.Seq = m.seq
				m.job = job
				m.notifies = append(m.notifies, *job)
			}
		}
		m.cond.Broadcast()
		m.mu.Unlock()
	}
}

// parseNotify mirrors the firmware: version, nbits and ntime are parsed as
// big-endian hex numbers; clean_jobs is the last parameter.
func parseNotify(p []json.RawMessage) (*Job, error) {
	if len(p) < 8 {
		return nil, errors.New("not enough params in mining.notify")
	}
	j := &Job{}
	var version, nbits, ntime string
	for i, dst := range []any{&j.ID, &j.PrevHash, &j.Coinbase1, &j.Coinbase2, &j.Branches, &version, &nbits, &ntime} {
		if err := json.Unmarshal(p[i], dst); err != nil {
			return nil, err
		}
	}
	for i, dst := range []*uint32{&j.Version, &j.NBits, &j.NTime} {
		v, err := strconv.ParseUint([]string{version, nbits, ntime}[i], 16, 32)
		if err != nil {
			return nil, err
		}
		*dst = uint32(v)
	}
	json.Unmarshal(p[len(p)-1], &j.CleanJobs)
	return j, nil
}

// Closed reports whether the server has dropped the connection.
func (m *Miner) Closed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

// Difficulty returns the last mining.set_difficulty value.
func (m *Miner) Difficulty() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.difficulty
}

// VersionMask returns the negotiated version-rolling mask.
func (m *Miner) VersionMask() uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.versionMask
}

// Extranonce1 returns the extranonce assigned by the server.
func (m *Miner) Extranonce1() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.extranonce1
}

// Notifies returns every mining.notify received so far.
func (m *Miner) Notifies() []Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Job(nil), m.notifies...)
}

// WaitJob returns the newest job with a sequence number above afterSeq that
// satisfies ok (nil accepts any job).
func (m *Miner) WaitJob(afterSeq int, timeout time.Duration, ok func(*Job) bool) (*Job, error) {
	deadline := time.Now().Add(timeout)
	timer := time.AfterFunc(timeout, func() {
		m.mu.Lock()
		m.cond.Broadcast()
		m.mu.Unlock()
	})
	defer timer.Stop()
	m.mu.Lock()
	defer m.mu.Unlock()
	for {
		if m.job != nil && m.job.Seq > afterSeq && (ok == nil || ok(m.job)) {
			j := *m.job
			return &j, nil
		}
		if m.closed {
			return nil, errors.New("connection closed while waiting for a job")
		}
		if time.Now().After(deadline) {
			return nil, errors.New("timed out waiting for a job")
		}
		m.cond.Wait()
	}
}

// SHA256d returns the double SHA-256 used for block and transaction hashes.
func SHA256d(b []byte) [32]byte { return sha256d(b) }

func sha256d(b []byte) [32]byte {
	first := sha256.Sum256(b)
	return sha256.Sum256(first[:])
}

// truediffone is the difficulty-1 target as used by the firmware.
var truediffone, _ = new(big.Float).SetString("26959535291011309493156476344723991336010898738574164086137773096960")

// HashDifficulty converts a header hash to pool difficulty the way the
// firmware's hash_to_pdiff does (the hash read as a little-endian number).
func HashDifficulty(hash [32]byte) float64 {
	rev := make([]byte, 32)
	for i := range hash {
		rev[31-i] = hash[i]
	}
	v := new(big.Float).SetInt(new(big.Int).SetBytes(rev))
	if v.Sign() == 0 {
		return math.MaxUint32
	}
	d, _ := new(big.Float).Quo(truediffone, v).Float64()
	return d
}

// NetworkDifficulty converts the job's nbits to difficulty.
func (j *Job) NetworkDifficulty() float64 {
	exp := j.NBits >> 24
	mant := new(big.Int).SetUint64(uint64(j.NBits & 0x007fffff))
	target := mant.Lsh(mant, uint(8*(exp-3)))
	d, _ := new(big.Float).Quo(truediffone, new(big.Float).SetInt(target)).Float64()
	return d
}

// incrementBitmask steps through version combinations, wrapping within the
// negotiated mask without changing any of the other version bits.
func incrementBitmask(value, mask uint32) uint32 {
	for bit := uint32(1); bit != 0; bit <<= 1 {
		if mask&bit != 0 {
			value ^= bit
			if value&bit != 0 {
				break
			}
		}
	}
	return value
}

// Mine searches for a share of at least minDifficulty for job, trying up to
// maxHashes headers. With a version mask it rolls the version as the ASIC
// does. It returns false if nothing was found.
func (m *Miner) Mine(job *Job, minDifficulty float64, maxHashes int) (Share, bool) {
	m.mu.Lock()
	en1, en2Len, mask := m.extranonce1, m.en2Len, m.versionMask
	m.en2Counter++
	counter := m.en2Counter
	m.mu.Unlock()

	// extranonce_2_generate: the counter's little-endian bytes as hex.
	var counterBytes [8]byte
	binary.LittleEndian.PutUint64(counterBytes[:], counter)
	en2 := make([]byte, en2Len)
	copy(en2, counterBytes[:])
	en2Hex := hex.EncodeToString(en2)

	// calculate_coinbase_tx_hash + calculate_merkle_root_hash.
	coinbase, err := hex.DecodeString(job.Coinbase1 + en1 + en2Hex + job.Coinbase2)
	if err != nil {
		return Share{}, false
	}
	root := sha256d(coinbase)
	for _, b := range job.Branches {
		branch, err := hex.DecodeString(b)
		if err != nil || len(branch) != 32 {
			return Share{}, false
		}
		root = sha256d(append(root[:], branch...))
	}

	// construct_bm_job: hex2bin the previous hash, then byte-swap each
	// 32-bit word. Numbers go into the header as little-endian integers.
	prev, err := hex.DecodeString(job.PrevHash)
	if err != nil || len(prev) != 32 {
		return Share{}, false
	}
	for i := 0; i < 32; i += 4 {
		prev[i], prev[i+1], prev[i+2], prev[i+3] = prev[i+3], prev[i+2], prev[i+1], prev[i]
	}
	var header [80]byte
	copy(header[4:], prev)
	copy(header[36:], root[:])
	binary.LittleEndian.PutUint32(header[68:], job.NTime)
	binary.LittleEndian.PutUint32(header[72:], job.NBits)

	version := job.Version
	for tries := 0; tries < maxHashes; {
		binary.LittleEndian.PutUint32(header[0:], version)
		for nonce := uint32(0); nonce < 4096 && tries < maxHashes; nonce, tries = nonce+1, tries+1 {
			binary.LittleEndian.PutUint32(header[76:], nonce)
			hash := sha256d(header[:])
			if HashDifficulty(hash) >= minDifficulty {
				return Share{
					JobID: job.ID, Extranonce2: en2Hex, NTime: job.NTime, Nonce: nonce,
					VersionBits: version ^ job.Version, Header: header, Hash: hash,
				}, true
			}
		}
		if mask == 0 {
			// Without version rolling only the nonce changes.
			for nonce := uint32(4096); tries < maxHashes; nonce, tries = nonce+1, tries+1 {
				binary.LittleEndian.PutUint32(header[76:], nonce)
				hash := sha256d(header[:])
				if HashDifficulty(hash) >= minDifficulty {
					return Share{JobID: job.ID, Extranonce2: en2Hex, NTime: job.NTime, Nonce: nonce, Header: header, Hash: hash}, true
				}
			}
			break
		}
		version = incrementBitmask(version, mask)
	}
	return Share{}, false
}

// Submit sends a share exactly as the firmware formats it.
func (m *Miner) Submit(s Share) (Result, error) {
	var ch chan json.RawMessage
	if m.opts.NoVersionRolling {
		ch = m.request(`{"id":%d,"method":"mining.submit","params":[%s,%s,"%s","%08x","%08x"]}`,
			quote(m.opts.User), quote(s.JobID), s.Extranonce2, s.NTime, s.Nonce)
	} else {
		ch = m.request(`{"id":%d,"method":"mining.submit","params":[%s,%s,"%s","%08x","%08x","%08x"]}`,
			quote(m.opts.User), quote(s.JobID), s.Extranonce2, s.NTime, s.Nonce, s.VersionBits)
	}
	raw, err := m.await(ch)
	var srvErr *ServerError
	if errors.As(err, &srvErr) {
		res := Result{Message: srvErr.Raw}
		var parts []json.RawMessage
		if json.Unmarshal([]byte(srvErr.Raw), &parts) == nil && len(parts) >= 2 {
			json.Unmarshal(parts[0], &res.Code)
			json.Unmarshal(parts[1], &res.Message)
		}
		return res, nil
	}
	if err != nil {
		return Result{}, err
	}
	return Result{Accepted: string(raw) == "true"}, nil
}

// DisplayHash returns a hash the way block explorers show it.
func DisplayHash(h [32]byte) string {
	rev := make([]byte, 32)
	for i := range h {
		rev[31-i] = h[i]
	}
	return hex.EncodeToString(rev)
}
