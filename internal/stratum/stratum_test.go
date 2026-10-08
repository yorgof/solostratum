package stratum

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yorgof/solostratum/internal/axesim"
	"github.com/yorgof/solostratum/internal/btc"
	"github.com/yorgof/solostratum/internal/rpc"
	"github.com/yorgof/solostratum/internal/work"
)

const (
	regtestBits = "207fffff" // half of all hashes are blocks
	diff1Bits   = "1d00ffff" // network difficulty 1
	// lowDifficulty needs about 40,000 hashes per share: fast for a test,
	// yet far from "every hash is a share".
	lowDifficulty = 0.00001
)

var (
	regtest, _       = btc.NetworkByChain("regtest")
	defaultAddress   = "bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080"
	defaultScript, _ = btc.AddressToScript(defaultAddress, regtest)
	otherAddress     = "mipcBbFg9gMiCh81Kj8tqqdgoZub1ZJRfn"
	otherScript, _   = btc.AddressToScript(otherAddress, regtest)
)

type fakeJobs struct {
	mu        sync.Mutex
	jobs      map[string]*work.Job
	current   *work.Job
	unhealthy bool
}

func (f *fakeJobs) Current() *work.Job {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.current
}

func (f *fakeJobs) Get(id string) *work.Job {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.jobs[id]
}

func (f *fakeJobs) Healthy() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.unhealthy
}

func (f *fakeJobs) add(j *work.Job) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.jobs == nil {
		f.jobs = map[string]*work.Job{}
	}
	if j.CleanJobs {
		f.jobs = map[string]*work.Job{}
	}
	f.jobs[j.ID] = j
	f.current = j
}

type foundBlock struct {
	height int64
	hash   string
	block  []byte
	worker string
}

type fakeSink struct{ ch chan foundBlock }

func (f *fakeSink) Found(height int64, hash string, block []byte, worker string) {
	f.ch <- foundBlock{height, hash, block, worker}
}

// recorded is one call to the Recorder.
type recorded struct {
	event             string
	worker            string
	credit, shareDiff float64
}

type fakeRecorder struct {
	mu     sync.Mutex
	events []recorded
}

func (f *fakeRecorder) add(e recorded) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, e)
}

func (f *fakeRecorder) Connected(worker string, at time.Time) {
	f.add(recorded{event: "connected", worker: worker})
}

func (f *fakeRecorder) Disconnected(worker string, at time.Time) {
	f.add(recorded{event: "disconnected", worker: worker})
}

func (f *fakeRecorder) Accepted(worker string, at time.Time, credit, shareDiff float64) {
	f.add(recorded{"accepted", worker, credit, shareDiff})
}

func (f *fakeRecorder) Rejected(worker string, at time.Time) {
	f.add(recorded{event: "rejected", worker: worker})
}

func (f *fakeRecorder) all() []recorded {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recorded(nil), f.events...)
}

func makeJob(t *testing.T, id, bits string, version uint32, txs int, clean bool) *work.Job {
	t.Helper()
	tmpl := &rpc.BlockTemplate{
		Version:           version,
		PreviousBlockHash: "00000000000000000001a0b1c2d3e4f5061728394a5b6c7d8e9fa0b1c2d3e4f5",
		CoinbaseValue:     312500000,
		Bits:              bits,
		CurTime:           1700000000,
		MinTime:           1699990000,
		Height:            840000,
		WitnessCommitment: "6a24aa21a9ede2f61c3f71d1defd3fa999dfa36953755c690689799962b48bebd836974e8cf9",
	}
	for i := 0; i < txs; i++ {
		data := []byte{0x02, 0, 0, 0, byte(i)}
		id := btc.SHA256d(data)
		tmpl.Transactions = append(tmpl.Transactions, rpc.TemplateTx{Data: hex.EncodeToString(data), TxID: id.String()})
	}
	job, err := work.NewJob(id, tmpl, "/test/", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	job.CleanJobs = clean
	return job
}

type harness struct {
	t     *testing.T
	srv   *Server
	jobs  *fakeJobs
	sink  *fakeSink
	stats *fakeRecorder
	addr  string
}

func newHarness(t *testing.T, startDiff float64, job *work.Job) *harness {
	t.Helper()
	h := &harness{t: t, jobs: &fakeJobs{}, sink: &fakeSink{ch: make(chan foundBlock, 16)}, stats: &fakeRecorder{}}
	h.jobs.add(job)
	h.srv = NewServer(Options{
		Network: regtest, DefaultAddress: defaultAddress, DefaultScript: defaultScript,
		StartDifficulty: startDiff, MinDifficulty: startDiff / 100,
	}, h.jobs, h.sink)
	h.srv.SetRecorder(h.stats)
	if err := h.srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	h.addr = h.srv.Addr().String()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { h.srv.Serve(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return h
}

func (h *harness) dial(opts axesim.Options) *axesim.Miner {
	h.t.Helper()
	if opts.User == "" {
		opts.User = "tester"
	}
	opts.Timeout = 5 * time.Second
	m, err := axesim.Dial(h.addr, opts)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(m.Close)
	return m
}

func firstJob(t *testing.T, m *axesim.Miner) *axesim.Job {
	t.Helper()
	job, err := m.WaitJob(0, 5*time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	return job
}

func mustMine(t *testing.T, m *axesim.Miner, job *axesim.Job, diff float64) axesim.Share {
	t.Helper()
	share, ok := m.Mine(job, diff, 50_000_000)
	if !ok {
		t.Fatal("no share found")
	}
	return share
}

func mustSubmit(t *testing.T, m *axesim.Miner, s axesim.Share) axesim.Result {
	t.Helper()
	res, err := m.Submit(s)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func TestShareAcceptedAndCounted(t *testing.T) {
	h := newHarness(t, lowDifficulty, makeJob(t, "1", diff1Bits, 0x20000000, 3, true))
	m := h.dial(axesim.Options{ExtranonceSub: true})
	job := firstJob(t, m)
	if got := m.Difficulty(); got != lowDifficulty {
		t.Fatalf("set_difficulty = %v, want %v", got, lowDifficulty)
	}
	if m.VersionMask() != versionRollingMask {
		t.Fatalf("version mask %08x, want %08x", m.VersionMask(), versionRollingMask)
	}
	if !job.CleanJobs {
		t.Error("the first job for a miner must have clean_jobs set")
	}
	if len(job.Branches) != 2 {
		t.Errorf("3 transactions need 2 merkle branches, got %d", len(job.Branches))
	}

	share := mustMine(t, m, job, lowDifficulty)
	if res := mustSubmit(t, m, share); !res.Accepted {
		t.Fatalf("valid share rejected: %+v", res)
	}
	st := h.srv.Status()
	if st.Accepted != 1 || st.Rejected != 0 || len(st.Miners) != 1 {
		t.Fatalf("status after one share: %+v", st)
	}
	miner := st.Miners[0]
	if miner.Worker != "tester" || miner.OwnPayout || miner.Payout != defaultAddress || miner.Accepted != 1 {
		t.Errorf("miner status: %+v", miner)
	}
	if miner.BestShare < lowDifficulty || miner.Hashrate <= 0 {
		t.Errorf("best share %v / hashrate %v not recorded", miner.BestShare, miner.Hashrate)
	}
	if !strings.HasPrefix(miner.Client, "bitaxe/") {
		t.Errorf("client = %q", miner.Client)
	}
	select {
	case b := <-h.sink.ch:
		t.Fatalf("a share far below network difficulty was treated as a block: %+v", b.hash)
	default:
	}
}

func TestLowDifficultyDuplicateAndStale(t *testing.T) {
	h := newHarness(t, lowDifficulty, makeJob(t, "1", diff1Bits, 0x20000000, 0, true))
	m := h.dial(axesim.Options{})
	job := firstJob(t, m)

	// The very first header tried almost certainly misses the target.
	weak := mustMine(t, m, job, 0)
	if axesim.HashDifficulty(weak.Hash) >= lowDifficulty {
		t.Skip("first hash happened to meet the target")
	}
	if res := mustSubmit(t, m, weak); res.Accepted || res.Code != errLowDiff {
		t.Fatalf("weak share: %+v, want error %d", res, errLowDiff)
	}

	good := mustMine(t, m, job, lowDifficulty)
	if res := mustSubmit(t, m, good); !res.Accepted {
		t.Fatalf("good share rejected: %+v", res)
	}
	if res := mustSubmit(t, m, good); res.Accepted || res.Code != errDuplicate {
		t.Fatalf("duplicate share: %+v, want error %d", res, errDuplicate)
	}

	stale := good
	stale.JobID = "ffff"
	if res := mustSubmit(t, m, stale); res.Accepted || res.Code != errJobNotFound {
		t.Fatalf("stale share: %+v, want error %d", res, errJobNotFound)
	}

	early := good
	early.NTime = job.NTime - 1
	if res := mustSubmit(t, m, early); res.Accepted {
		t.Fatal("share with a block time before the job's was accepted")
	}
	late := good
	late.NTime = job.NTime + maxNTimeRoll + 1
	if res := mustSubmit(t, m, late); res.Accepted {
		t.Fatal("share with a block time far in the future was accepted")
	}

	outside := good
	outside.VersionBits = 0x00000001
	if res := mustSubmit(t, m, outside); res.Accepted {
		t.Fatal("version bits outside the mask were accepted")
	}

	st := h.srv.Status()
	if st.Accepted != 1 || st.Rejected != 6 {
		t.Errorf("accepted/rejected = %d/%d, want 1/6", st.Accepted, st.Rejected)
	}
	reasons := st.Miners[0].RejectReasons
	if reasons["Difficulty too low"] != 1 || reasons["Duplicate share"] != 1 || reasons["Job not found (stale)"] != 1 || reasons["Time out of range"] != 2 {
		t.Errorf("reject reasons: %v", reasons)
	}
}

// A second connection must not be able to replay another miner's share: the
// extranonce differs, so the same numbers give a different hash.
func TestConnectionsAndSharesAreRecorded(t *testing.T) {
	h := newHarness(t, lowDifficulty, makeJob(t, "1", diff1Bits, 0x20000000, 0, true))

	// A miner that has not logged in has no name to be recorded under.
	c := h.raw()
	c.send(`{"id":1,"method":"mining.submit","params":["w","1","00000000","6553f100","00000000"]}`)
	if msg := c.answer(1); msg["error"] == nil {
		t.Fatal("submit before subscribe was not rejected")
	}
	c.conn.Close()

	worker := otherAddress + ".rig"
	m := h.dial(axesim.Options{User: worker})
	good := mustMine(t, m, firstJob(t, m), lowDifficulty)
	if res := mustSubmit(t, m, good); !res.Accepted {
		t.Fatalf("good share rejected: %+v", res)
	}
	if res := mustSubmit(t, m, good); res.Accepted {
		t.Fatal("duplicate share accepted")
	}
	m.Close()

	want := []recorded{
		{event: "connected", worker: worker},
		{"accepted", worker, lowDifficulty, axesim.HashDifficulty(good.Hash)},
		{event: "rejected", worker: worker},
		{event: "disconnected", worker: worker},
	}
	var got []recorded
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got = h.stats.all(); len(got) >= len(want) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(got) != len(want) {
		t.Fatalf("recorded: %+v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("event %d recorded as %+v, want %+v", i+1, got[i], want[i])
		}
	}
}

func TestSharesAreBoundToTheConnection(t *testing.T) {
	h := newHarness(t, lowDifficulty, makeJob(t, "1", diff1Bits, 0x20000000, 1, true))
	a, b := h.dial(axesim.Options{}), h.dial(axesim.Options{})
	if a.Extranonce1() == b.Extranonce1() {
		t.Fatal("two connections got the same extranonce")
	}
	share := mustMine(t, a, firstJob(t, a), lowDifficulty)
	firstJob(t, b)
	if res := mustSubmit(t, b, share); res.Accepted {
		t.Fatal("a share mined on another connection was accepted")
	}
	if res := mustSubmit(t, a, share); !res.Accepted {
		t.Fatalf("own share rejected: %+v", res)
	}
}

func TestBlockFound(t *testing.T) {
	for _, txs := range []int{0, 1, 2, 5, 300} {
		t.Run(fmt.Sprintf("%dtxs", txs), func(t *testing.T) {
			serverJob := makeJob(t, "a", regtestBits, 0x20000000, txs, true)
			h := newHarness(t, 1024, serverJob)
			m := h.dial(axesim.Options{User: "rig1"})
			job := firstJob(t, m)
			// A share never needs to be harder than the block itself.
			if got, want := m.Difficulty(), serverJob.NetDiff; got != want {
				t.Fatalf("set_difficulty = %v, want the network difficulty %v", got, want)
			}
			share := mustMine(t, m, job, job.NetworkDifficulty())
			if res := mustSubmit(t, m, share); !res.Accepted {
				t.Fatalf("block share rejected: %+v", res)
			}
			var found foundBlock
			select {
			case found = <-h.sink.ch:
			case <-time.After(5 * time.Second):
				t.Fatal("block was not reported")
			}
			if found.height != 840000 || found.worker != "rig1" {
				t.Errorf("found: height %d worker %q", found.height, found.worker)
			}
			if found.hash != axesim.DisplayHash(share.Hash) {
				t.Errorf("hash %s, miner computed %s", found.hash, axesim.DisplayHash(share.Hash))
			}
			if !bytes.Equal(found.block[:80], share.Header[:]) {
				t.Errorf("block header differs from the header the miner hashed:\n server %x\n miner  %x", found.block[:80], share.Header)
			}
			checkBlock(t, found.block, serverJob, txs, defaultScript)
		})
	}
}

// checkBlock parses a serialized block far enough to verify its structure:
// transaction count, witness-form coinbase, payout output, and that the
// merkle root in the header commits to the coinbase's transaction id.
func checkBlock(t *testing.T, block []byte, job *work.Job, txs int, payoutScript []byte) {
	t.Helper()
	rest := block[80:]
	var count uint64
	switch rest[0] {
	case 0xfd:
		count, rest = uint64(rest[1])|uint64(rest[2])<<8, rest[3:]
	default:
		count, rest = uint64(rest[0]), rest[1:]
	}
	if count != uint64(txs+1) {
		t.Fatalf("transaction count %d, want %d", count, txs+1)
	}
	if !bytes.Equal(rest[:6], []byte{2, 0, 0, 0, 0x00, 0x01}) {
		t.Fatalf("coinbase does not start with version 2 and the segwit marker: %x", rest[:6])
	}
	if !bytes.Contains(rest, append([]byte{byte(len(payoutScript))}, payoutScript...)) {
		t.Error("payout script not found in the coinbase")
	}
	// Rebuild the id-form coinbase: drop marker/flag and the witness.
	cbLen := len(rest) - 5*txs // every test transaction is 5 bytes
	cb := rest[:cbLen]
	witness := cb[len(cb)-38 : len(cb)-4]
	if witness[0] != 1 || witness[1] != 32 || !bytes.Equal(witness[2:], make([]byte, 32)) {
		t.Fatalf("unexpected coinbase witness %x", witness)
	}
	stripped := append(append(append([]byte{}, cb[:4]...), cb[6:len(cb)-38]...), cb[len(cb)-4:]...)
	root := btc.MerkleRootFromBranches(btc.SHA256d(stripped), job.Branches)
	if !bytes.Equal(block[36:68], root[:]) {
		t.Error("header merkle root does not commit to the coinbase transaction id")
	}
	if !bytes.Equal(block[4:36], job.PrevHash[:]) {
		t.Error("header previous-block hash is not in internal byte order")
	}
}

func TestPayoutAddressFromUsername(t *testing.T) {
	serverJob := makeJob(t, "1", regtestBits, 0x20000000, 0, true)
	cases := []struct {
		user   string
		script []byte
		own    bool
	}{
		{otherAddress, otherScript, true},
		{otherAddress + ".garage", otherScript, true},
		{"garage", defaultScript, false},
		{"", defaultScript, false},
		// A mainnet address on regtest is not a valid payout address here.
		{"1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa.rig", defaultScript, false},
		// A typo in the address must not pay to a wrong destination.
		{strings.Replace(otherAddress, "9", "8", 1), defaultScript, false},
	}
	for _, c := range cases {
		t.Run(c.user, func(t *testing.T) {
			h := newHarness(t, 1024, serverJob)
			opts := axesim.Options{User: c.user}
			if c.user == "" {
				opts.User = " "
			}
			m := h.dial(opts)
			job := firstJob(t, m)
			cb2, _ := hex.DecodeString(job.Coinbase2)
			if !bytes.Contains(cb2, append([]byte{byte(len(c.script))}, c.script...)) {
				t.Fatalf("coinbase does not pay the expected script %x", c.script)
			}
			st := h.srv.Status().Miners[0]
			if st.OwnPayout != c.own {
				t.Errorf("ownPayout = %v, want %v", st.OwnPayout, c.own)
			}
			share := mustMine(t, m, job, job.NetworkDifficulty())
			if res := mustSubmit(t, m, share); !res.Accepted {
				t.Fatalf("share rejected: %+v", res)
			}
			found := <-h.sink.ch
			checkBlock(t, found.block, serverJob, 0, c.script)
		})
	}
}

func TestVersionRolling(t *testing.T) {
	// The later versions already have bits inside the rolling mask, which
	// is where the two ways of encoding rolled bits differ.
	for _, version := range []uint32{0x20000000, 0x20002000, 0x21e04000} {
		t.Run(fmt.Sprintf("%08x", version), func(t *testing.T) {
			serverJob := makeJob(t, "1", diff1Bits, version, 2, true)
			h := newHarness(t, lowDifficulty, serverJob)
			m := h.dial(axesim.Options{})
			job := firstJob(t, m)
			rolled := false
			for i := 0; i < 6; i++ {
				share := mustMine(t, m, job, lowDifficulty)
				rolled = rolled || share.VersionBits != 0
				if share.VersionBits&^versionRollingMask != 0 {
					t.Fatalf("simulator rolled outside the mask: %08x", share.VersionBits)
				}
				if res := mustSubmit(t, m, share); !res.Accepted {
					t.Fatalf("share with version bits %08x rejected: %+v", share.VersionBits, res)
				}
			}
			if !rolled {
				t.Error("no share used a rolled version; the test did not exercise version rolling")
			}
		})
	}
}

func TestMinerWithoutVersionRolling(t *testing.T) {
	h := newHarness(t, lowDifficulty, makeJob(t, "1", diff1Bits, 0x20000000, 0, true))
	m := h.dial(axesim.Options{NoVersionRolling: true})
	job := firstJob(t, m)
	if res := mustSubmit(t, m, mustMine(t, m, job, lowDifficulty)); !res.Accepted {
		t.Fatalf("share rejected: %+v", res)
	}
}

func TestSuggestDifficulty(t *testing.T) {
	h := newHarness(t, 1024, makeJob(t, "1", "1701d936", 0x20000000, 0, true))
	m := h.dial(axesim.Options{SuggestDifficulty: 1000})
	deadline := time.Now().Add(5 * time.Second)
	for m.Difficulty() != 1000 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := m.Difficulty(); got != 1000 {
		t.Fatalf("difficulty after suggesting 1000 = %v", got)
	}
	// Suggesting a difficulty must not restart the job the miner already
	// has: only the difficulty is announced.
	if n := len(m.Notifies()); n != 1 {
		t.Errorf("%d mining.notify messages after connecting, want 1", n)
	}
	// While the miner delivers shares, the suggestion is the floor for
	// automatic adjustment.
	sess := h.srv.snapshotSessions()[0]
	sess.mu.Lock()
	sess.windowStart = time.Now().Add(-10 * time.Minute)
	sess.windowShares = 1
	changed := sess.retargetLocked(time.Now())
	diff := sess.diff
	sess.mu.Unlock()
	if changed || diff != 1000 {
		t.Errorf("difficulty dropped below the miner's suggestion: %v", diff)
	}
	// A miner that cannot produce a single share at its own suggestion is
	// helped down, instead of being left silent until it times out.
	sess.mu.Lock()
	sess.windowStart = time.Now().Add(-10 * time.Minute)
	sess.windowShares = 0
	changed = sess.retargetLocked(time.Now())
	diff = sess.diff
	sess.mu.Unlock()
	if !changed || diff >= 1000 {
		t.Errorf("difficulty stayed at %v for a miner producing no shares", diff)
	}
}

// A lowered difficulty must be honoured for the job the miner already has,
// and a raised one must not invalidate shares already under way.
func TestDifficultyChangeAppliesToCurrentJob(t *testing.T) {
	h := newHarness(t, lowDifficulty*8, makeJob(t, "1", diff1Bits, 0x20000000, 0, true))
	m := h.dial(axesim.Options{})
	job := firstJob(t, m)
	sess := h.srv.snapshotSessions()[0]

	setDiff := func(d float64) {
		sess.mu.Lock()
		sess.diff = d
		sess.announceDifficultyLocked()
		sess.mu.Unlock()
		deadline := time.Now().Add(5 * time.Second)
		for m.Difficulty() != d && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if m.Difficulty() != d {
			t.Fatalf("miner was not told about difficulty %v", d)
		}
	}

	setDiff(lowDifficulty)
	share := mustMine(t, m, job, lowDifficulty)
	if res := mustSubmit(t, m, share); !res.Accepted {
		t.Fatalf("share at the lowered difficulty rejected: %+v", res)
	}
	setDiff(lowDifficulty * 4)
	share = mustMine(t, m, job, lowDifficulty)
	if res := mustSubmit(t, m, share); !res.Accepted {
		t.Fatalf("share at the difficulty the job was issued with rejected after a raise: %+v", res)
	}
	if n := len(m.Notifies()); n != 1 {
		t.Errorf("difficulty changes re-sent the job (%d notifies)", n)
	}
}

// The payout is fixed by the first authorize. A later authorize on the same
// connection must not redirect work that was already handed out.
func TestSecondAuthorizeDoesNotChangePayout(t *testing.T) {
	serverJob := makeJob(t, "1", regtestBits, 0x20000000, 0, true)
	h := newHarness(t, 1024, serverJob)
	c := h.raw()
	c.send(`{"id":1,"method":"mining.subscribe","params":["x"]}`)
	c.answer(1)
	c.send(`{"id":2,"method":"mining.authorize","params":["first","x"]}`)
	c.answer(2)
	c.send(fmt.Sprintf(`{"id":3,"method":"mining.authorize","params":[%q,"x"]}`, otherAddress))
	if msg := c.answer(3); msg["result"] != true {
		t.Fatalf("second authorize: %v", msg)
	}
	st := h.srv.Status().Miners[0]
	if st.Worker != "first" || st.OwnPayout || st.Payout != defaultAddress {
		t.Errorf("payout changed by a second authorize: %+v", st)
	}
}

func TestOlderJobIsNotSentAfterNewer(t *testing.T) {
	h := newHarness(t, lowDifficulty, makeJob(t, "1", diff1Bits, 0x20000000, 0, true))
	m := h.dial(axesim.Options{})
	firstJob(t, m)
	older := makeJob(t, "2", diff1Bits, 0x20000000, 0, false)
	newer := makeJob(t, "3", diff1Bits, 0x20000000, 0, false)
	older.Seq, newer.Seq = 2, 3
	h.jobs.add(newer)
	h.srv.Broadcast(newer)
	h.srv.Broadcast(older)
	if _, err := m.WaitJob(1, 5*time.Second, func(j *axesim.Job) bool { return j.ID == "3" }); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	for _, j := range m.Notifies() {
		if j.ID == "2" {
			t.Fatal("an older job was sent after a newer one")
		}
	}
}

func TestCleanString(t *testing.T) {
	if got := cleanString("a\nb\x1b[31m\rc\x00d\x7f", 64); got != "ab[31mcd" {
		t.Errorf("cleanString = %q", got)
	}
	if got := cleanString("  rig1  ", 64); got != "rig1" {
		t.Errorf("cleanString = %q", got)
	}
	if got := cleanString(strings.Repeat("é", 40), 9); got != "éééé" {
		t.Errorf("truncation split a character: %q", got)
	}
}

func TestNewJobBroadcast(t *testing.T) {
	h := newHarness(t, lowDifficulty, makeJob(t, "1", diff1Bits, 0x20000000, 0, true))
	m := h.dial(axesim.Options{})
	first := firstJob(t, m)
	old := mustMine(t, m, first, lowDifficulty)

	refresh := makeJob(t, "2", diff1Bits, 0x20000000, 4, false)
	h.jobs.add(refresh)
	h.srv.Broadcast(refresh)
	second, err := m.WaitJob(first.Seq, 5*time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	if second.ID != "2" || second.CleanJobs {
		t.Fatalf("refresh job: id %q clean %v", second.ID, second.CleanJobs)
	}
	// Work on the previous job of the same tip is still good.
	if res := mustSubmit(t, m, old); !res.Accepted {
		t.Fatalf("share for the previous job rejected: %+v", res)
	}

	tip := makeJob(t, "3", diff1Bits, 0x20000000, 0, true)
	h.jobs.add(tip)
	h.srv.Broadcast(tip)
	third, err := m.WaitJob(second.Seq, 5*time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !third.CleanJobs {
		t.Fatal("job for a new tip must set clean_jobs")
	}
	late := mustMine(t, m, second, lowDifficulty)
	if res := mustSubmit(t, m, late); res.Accepted || res.Code != errJobNotFound {
		t.Fatalf("share for a job on the old tip: %+v, want error %d", res, errJobNotFound)
	}
}

func TestMinersDisconnectedWhileNodeIsDown(t *testing.T) {
	h := newHarness(t, lowDifficulty, makeJob(t, "1", diff1Bits, 0x20000000, 0, true))
	m := h.dial(axesim.Options{})
	firstJob(t, m)

	h.jobs.mu.Lock()
	h.jobs.unhealthy = true
	h.jobs.mu.Unlock()
	if n := h.srv.closeAll(); n != 1 {
		t.Fatalf("closed %d sessions, want 1", n)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !m.Closed() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !m.Closed() {
		t.Fatal("miner still connected")
	}
	// New connections are turned away until the node is back.
	if _, err := axesim.Dial(h.addr, axesim.Options{User: "x", Timeout: time.Second}); err == nil {
		t.Fatal("a miner could start mining while the node is unreachable")
	}
	h.jobs.mu.Lock()
	h.jobs.unhealthy = false
	h.jobs.mu.Unlock()
	back := h.dial(axesim.Options{})
	firstJob(t, back)
}

// rawClient speaks to the server line by line, for malformed input.
type rawClient struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
}

func (h *harness) raw() *rawClient {
	conn, err := net.Dial("tcp", h.addr)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { conn.Close() })
	return &rawClient{h.t, conn, bufio.NewReader(conn)}
}

func (c *rawClient) send(line string) { fmt.Fprintf(c.conn, "%s\n", line) }

// answer reads until the reply with the given id arrives.
func (c *rawClient) answer(id int) map[string]any {
	c.t.Helper()
	c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		line, err := c.r.ReadBytes('\n')
		if err != nil {
			c.t.Fatalf("waiting for answer %d: %v", id, err)
		}
		var msg map[string]any
		if err := json.Unmarshal(line, &msg); err != nil {
			c.t.Fatalf("server sent invalid JSON: %s", line)
		}
		if got, ok := msg["id"].(float64); ok && int(got) == id && msg["method"] == nil {
			return msg
		}
	}
}

func TestMalformedInputIsHandled(t *testing.T) {
	h := newHarness(t, lowDifficulty, makeJob(t, "1", diff1Bits, 0x20000000, 0, true))
	c := h.raw()

	c.send(`{"id":1,"method":"mining.submit","params":["w","1","00000000","6553f100","00000000"]}`)
	if msg := c.answer(1); msg["error"] == nil {
		t.Fatal("submit before subscribe was not rejected")
	}
	c.send(`this is not json`)
	c.send(``)
	c.send(`{"id":2,"method":"mining.subscribe","params":[]}`)
	if msg := c.answer(2); msg["error"] != nil {
		t.Fatalf("subscribe after garbage failed: %v", msg)
	}
	c.send(`{"id":3,"method":"mining.submit","params":["w","1","00000000","6553f100","00000000"]}`)
	if msg := c.answer(3); msg["error"] == nil {
		t.Fatal("submit before authorize was not rejected")
	}
	c.send(`{"id":4,"method":"mining.authorize","params":["w","x"]}`)
	c.answer(4)

	for i, bad := range []string{
		`{"id":%d,"method":"mining.submit","params":[]}`,
		`{"id":%d,"method":"mining.submit","params":"nope"}`,
		`{"id":%d,"method":"mining.submit","params":["w","1",5,6,7]}`,
		`{"id":%d,"method":"mining.submit","params":["w","1","zz","6553f100","00000000"]}`,
		`{"id":%d,"method":"mining.submit","params":["w","1","0000000000","6553f100","00000000"]}`,
		`{"id":%d,"method":"mining.submit","params":["w","1","00000000","6553f1","00000000"]}`,
		`{"id":%d,"method":"mining.submit","params":["w","1","00000000","6553f100","0000000g"]}`,
		`{"id":%d,"method":"mining.submit","params":["w","1","00000000","6553f100","00000000","xyz"]}`,
		`{"id":%d,"method":"mining.suggest_difficulty","params":[-5]}`,
		`{"id":%d,"method":"mining.suggest_difficulty","params":["a"]}`,
		`{"id":%d,"method":"mining.configure","params":[5,6]}`,
		`{"id":%d,"method":"mining.does_not_exist","params":[]}`,
	} {
		id := 10 + i
		c.send(fmt.Sprintf(bad, id))
		msg := c.answer(id)
		if strings.Contains(bad, "configure") {
			continue // answered with an empty result
		}
		if msg["error"] == nil {
			t.Errorf("malformed request was not rejected: %s", fmt.Sprintf(bad, id))
		}
	}

	// The connection is still usable after all of that.
	c.send(`{"id":90,"method":"mining.extranonce.subscribe","params":[]}`)
	if msg := c.answer(90); msg["result"] != true {
		t.Fatalf("connection unusable after malformed input: %v", msg)
	}

	// An endless line must end the connection instead of using memory.
	big := h.raw()
	big.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	big.conn.Write(bytes.Repeat([]byte("a"), 4*maxLineLength))
	big.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := big.r.ReadByte(); err == nil {
		t.Fatal("over-long line did not close the connection")
	}
}

func TestRetarget(t *testing.T) {
	newSess := func(diff, floor float64) *session {
		return &session{diff: diff, floor: floor, minDiff: floor, windowStart: time.Unix(1000, 0)}
	}
	at := func(seconds float64) time.Time {
		return time.Unix(1000, 0).Add(time.Duration(seconds * float64(time.Second)))
	}

	s := newSess(1024, 0.001)
	if s.retargetLocked(at(10)) {
		t.Error("retargeted before the window ended")
	}

	// 30 shares in 3 seconds is 60 times too fast: raise by the maximum step.
	s = newSess(1024, 0.001)
	s.windowShares = 30
	if !s.retargetLocked(at(3)) || s.diff != 1024*16 {
		t.Errorf("fast miner: difficulty %v, want %v", s.diff, 1024*16)
	}

	// Five shares in 30 seconds is the goal: leave it alone.
	s = newSess(1024, 0.001)
	s.windowShares = 5
	if s.retargetLocked(at(30)) || s.diff != 1024 {
		t.Errorf("on-target miner was retargeted to %v", s.diff)
	}

	// One share in 30 seconds is five times too slow.
	s = newSess(1024, 0.001)
	s.windowShares = 1
	if !s.retargetLocked(at(30)) || s.diff != 256 {
		t.Errorf("slow miner: difficulty %v, want 256", s.diff)
	}

	// No shares: wait for the longer quiet window, then drop sharply.
	s = newSess(1024, 0.001)
	if s.retargetLocked(at(45)) {
		t.Error("lowered with no shares before the quiet window passed")
	}
	if !s.retargetLocked(at(61)) || s.diff != 128 {
		t.Errorf("silent miner: difficulty %v, want 128", s.diff)
	}

	// Never below the floor, and tiny miners get fractional difficulty.
	s = newSess(0.004, 0.001)
	if !s.retargetLocked(at(61)) || s.diff != 0.001 {
		t.Errorf("difficulty %v, want the floor 0.001", s.diff)
	}
	if s.retargetLocked(at(200)) {
		t.Error("retargeted although already at the floor")
	}
}

func TestRoundDifficulty(t *testing.T) {
	for in, want := range map[float64]float64{0.3: 0.3, 1: 1, 1000: 1024, 1500: 2048, 1400: 1024, 3e6: 4194304} {
		if got := roundDifficulty(in); got != want {
			t.Errorf("roundDifficulty(%v) = %v, want %v", in, got, want)
		}
	}
}

func TestRateMeter(t *testing.T) {
	var r rateMeter
	start := time.Unix(1_000_000*60, 0) // on a minute boundary
	// One difficulty-1024 share every 6 seconds for 20 minutes is
	// 1024 * 2^32 / 6 hashes per second.
	want := 1024 * 4294967296.0 / 6
	now := start
	for i := 0; i < 200; i++ {
		now = start.Add(time.Duration(i) * 6 * time.Second)
		r.add(now, 1024)
	}
	if got := r.hashrate(now, start); math.Abs(got-want)/want > 0.02 {
		t.Errorf("hashrate %v, want about %v", got, want)
	}
	// A miner that just connected is measured over its own lifetime.
	var fresh rateMeter
	fresh.add(start.Add(6*time.Second), 1024)
	fresh.add(start.Add(12*time.Second), 1024)
	if got := fresh.hashrate(start.Add(12*time.Second), start); math.Abs(got-want)/want > 0.02 {
		t.Errorf("fresh miner hashrate %v, want about %v", got, want)
	}
	// Old shares age out.
	if got := r.hashrate(now.Add(30*time.Minute), start); got != 0 {
		t.Errorf("hashrate after 30 idle minutes = %v, want 0", got)
	}
}

type blockingSink struct{ entered, release chan struct{} }

func (s *blockingSink) Found(int64, string, []byte, string) {
	close(s.entered)
	<-s.release
}

func TestShutdownWaitsForBlockHandoffAndDisconnect(t *testing.T) {
	job := makeJob(t, "1", regtestBits, 0x20000000, 0, true)
	jobs := &fakeJobs{}
	jobs.add(job)
	sink := &blockingSink{make(chan struct{}), make(chan struct{})}
	recorder := &fakeRecorder{}
	srv := NewServer(Options{
		Network: regtest, DefaultAddress: defaultAddress, DefaultScript: defaultScript,
		StartDifficulty: 1, MinDifficulty: 0.001,
	}, jobs, sink)
	srv.SetRecorder(recorder)
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { srv.Serve(ctx); close(done) }()
	unblock := sync.OnceFunc(func() { close(sink.release) })
	t.Cleanup(func() {
		cancel()
		unblock()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("server did not finish shutdown")
		}
	})
	m, err := axesim.Dial(srv.Addr().String(), axesim.Options{User: "rig", Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	share := mustMine(t, m, firstJob(t, m), job.NetDiff)
	if result := mustSubmit(t, m, share); !result.Accepted {
		t.Fatal(result)
	}
	select {
	case <-sink.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("block handoff did not start")
	}
	cancel()
	select {
	case <-done:
		t.Fatal("server exited while a solved block was still being handed off")
	case <-time.After(50 * time.Millisecond):
	}
	unblock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not finish after the block handoff")
	}
	events := recorder.all()
	if len(events) != 3 || events[0].event != "connected" || events[1].event != "accepted" || events[2].event != "disconnected" {
		t.Fatalf("shutdown returned before all statistics were recorded: %+v", events)
	}
	if len(srv.Status().Miners) != 0 {
		t.Fatal("a miner is still registered after shutdown")
	}
}
