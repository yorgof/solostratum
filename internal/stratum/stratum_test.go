package stratum

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yorgof/solostratum/internal/axesim"
	"github.com/yorgof/solostratum/internal/btc"
	"github.com/yorgof/solostratum/internal/guard"
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
	stop  func() // shuts the server down and waits for it; runs once
}

func newHarness(t *testing.T, startDiff float64, job *work.Job) *harness {
	t.Helper()
	return newHarnessWith(t, startDiff, job, harnessOptions{})
}

// newHarnessWithRecorder reports to rec instead of the harness's own fake
// recorder.
func newHarnessWithRecorder(t *testing.T, startDiff float64, job *work.Job, rec Recorder) *harness {
	t.Helper()
	return newHarnessWith(t, startDiff, job, harnessOptions{recorder: rec})
}

type harnessOptions struct {
	recorder Recorder                        // instead of the harness's own fake recorder
	jobs     func(*fakeJobs) Jobs            // wraps the fake job source
	listener func(net.Listener) net.Listener // wraps the listening socket
}

func newHarnessWith(t *testing.T, startDiff float64, job *work.Job, o harnessOptions) *harness {
	t.Helper()
	h := &harness{t: t, jobs: &fakeJobs{}, sink: &fakeSink{ch: make(chan foundBlock, 16)}, stats: &fakeRecorder{}}
	h.jobs.add(job)
	var jobs Jobs = h.jobs
	if o.jobs != nil {
		jobs = o.jobs(h.jobs)
	}
	h.srv = NewServer(Options{
		Network: regtest, DefaultAddress: defaultAddress, DefaultScript: defaultScript,
		StartDifficulty: startDiff, MinDifficulty: startDiff / 100,
	}, jobs, h.sink)
	rec := o.recorder
	if rec == nil {
		rec = h.stats
	}
	h.srv.SetRecorder(rec)
	if err := h.srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	h.addr = h.srv.Addr().String()
	if o.listener != nil {
		h.srv.listener = o.listener(h.srv.listener)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { h.srv.Serve(ctx); close(done) }()
	h.stop = sync.OnceFunc(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("the server did not shut down")
		}
	})
	t.Cleanup(h.stop)
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

// A job that no longer pays shares can still deliver a block. An ordinary
// share on that job is stale.
func TestStaleJobStillSubmitsABlock(t *testing.T) {
	serverJob := makeJob(t, "a", regtestBits, 0x20000000, 1, true)
	serverJob.Retire(time.Now())
	h := newHarness(t, 1024, serverJob)
	m := h.dial(axesim.Options{User: "rig1"})
	job := firstJob(t, m)

	ordinary := makeJob(t, "b", diff1Bits, 0x20000000, 0, false)
	ordinary.Retire(time.Now())
	h.jobs.add(ordinary)
	var weak axesim.Share
	for {
		weak = mustMine(t, m, jobFromServer(t, ordinary), lowDifficulty)
		if axesim.HashDifficulty(weak.Hash) < ordinary.NetDiff {
			break
		}
	}
	if res := mustSubmit(t, m, weak); res.Accepted || res.Code != errJobNotFound {
		t.Fatalf("ordinary share on a retired job: %+v", res)
	}

	share := mustMine(t, m, job, job.NetworkDifficulty())
	if res := mustSubmit(t, m, share); !res.Accepted {
		t.Fatalf("block on a retired job rejected: %+v", res)
	}
	select {
	case found := <-h.sink.ch:
		if found.hash != axesim.DisplayHash(share.Hash) {
			t.Errorf("hash %s, miner computed %s", found.hash, axesim.DisplayHash(share.Hash))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("block on a retired job was not submitted")
	}
}

// jobFromServer builds the miner-side view of a job the server already has,
// using the coinbase parts the server would have announced.
func jobFromServer(t *testing.T, job *work.Job) *axesim.Job {
	t.Helper()
	return &axesim.Job{
		ID:        job.ID,
		PrevHash:  job.StratumPrevHash(),
		Coinbase1: hex.EncodeToString(job.Coinbase1()),
		Coinbase2: hex.EncodeToString(job.Coinbase2(defaultScript)),
		Version:   job.Version,
		NBits:     job.Bits,
		NTime:     job.CurTime,
	}
}

// A header that meets the network target is handed to the node whatever its
// time: only the node can judge the time against its own clock.
func TestBlockIsSubmittedWhateverItsTime(t *testing.T) {
	serverJob := makeJob(t, "a", regtestBits, 0x20000000, 0, true)
	h := newHarness(t, 1024, serverJob)
	m := h.dial(axesim.Options{User: "rig1"})
	notified := firstJob(t, m)

	for _, ntime := range []uint32{serverJob.CurTime + maxNTimeRoll + 1, serverJob.MinTime - 1, math.MaxUint32} {
		job := *notified
		job.NTime = ntime
		share := mustMine(t, m, &job, job.NetworkDifficulty())
		if res := mustSubmit(t, m, share); !res.Accepted {
			t.Fatalf("block with time %d was rejected: %+v", ntime, res)
		}
		select {
		case found := <-h.sink.ch:
			if found.hash != axesim.DisplayHash(share.Hash) {
				t.Errorf("hash %s, miner computed %s", found.hash, axesim.DisplayHash(share.Hash))
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("block with time %d was not submitted", ntime)
		}
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
	// The block is handed off before the miner gets its answer, so the
	// answer only comes once the handoff below is released.
	go m.Submit(share)
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
	select {
	case <-srv.StatisticsDone():
	case <-time.After(5 * time.Second):
		t.Fatal("the statistics were not told everything after shutdown")
	}
	events := recorder.all()
	if len(events) != 3 || events[0].event != "connected" || events[1].event != "accepted" || events[2].event != "disconnected" {
		t.Fatalf("the statistics missed events of the shutdown: %+v", events)
	}
	if len(srv.Status().Miners) != 0 {
		t.Fatal("a miner is still registered after shutdown")
	}
}

// syncBuffer collects log output from several goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func captureLog(t *testing.T) *syncBuffer {
	t.Helper()
	out := &syncBuffer{}
	log.SetOutput(out)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	return out
}

// within fails the test unless f returns. It is for calls that must never
// wait: a broken version of them blocks forever.
func within(t *testing.T, what string, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); f() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s blocked", what)
	}
}

func waitClosed(t *testing.T, m *axesim.Miner) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !m.Closed() {
		if time.Now().After(deadline) {
			t.Fatal("the connection was not closed")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// mineBlock mines a block with m on a job for regtestBits and checks that it
// is accepted and handed over.
func (h *harness) mineBlock(m *axesim.Miner, job *axesim.Job) {
	h.t.Helper()
	share := mustMine(h.t, m, job, job.NetworkDifficulty())
	if res := mustSubmit(h.t, m, share); !res.Accepted {
		h.t.Fatalf("block rejected: %+v", res)
	}
	select {
	case found := <-h.sink.ch:
		if found.hash != axesim.DisplayHash(share.Hash) {
			h.t.Fatalf("handed over block %s, mined %s", found.hash, axesim.DisplayHash(share.Hash))
		}
	case <-time.After(5 * time.Second):
		h.t.Fatal("the block was not handed over")
	}
}

// statisticsDone waits until the statistics have been told everything, after
// the server has stopped.
func (h *harness) statisticsDone() {
	h.t.Helper()
	h.stop()
	select {
	case <-h.srv.StatisticsDone():
	case <-time.After(5 * time.Second):
		h.t.Fatal("the statistics were not told everything after shutdown")
	}
}

// panicRecorder panics when it is told about the event named by panicOn.
type panicRecorder struct {
	panicOn string
	calls   atomic.Int32
}

func (p *panicRecorder) event(name string) {
	p.calls.Add(1)
	if name == p.panicOn {
		panic("statistics bug on " + name)
	}
}

func (p *panicRecorder) Connected(string, time.Time)                  { p.event("connected") }
func (p *panicRecorder) Disconnected(string, time.Time)               { p.event("disconnected") }
func (p *panicRecorder) Accepted(string, time.Time, float64, float64) { p.event("accepted") }
func (p *panicRecorder) Rejected(string, time.Time)                   { p.event("rejected") }

// A bug in the statistics must never reach the miners: a panic would end
// the whole program, and with it any block being delivered. It switches the
// recording off instead.
func TestStatisticsPanicNeverReachesMiners(t *testing.T) {
	for _, event := range []string{"connected", "accepted", "rejected", "disconnected"} {
		t.Run(event, func(t *testing.T) {
			logs := captureLog(t)
			rec := &panicRecorder{panicOn: event}
			h := newHarnessWithRecorder(t, 1024, makeJob(t, "a", regtestBits, 0x20000000, 0, true), rec)
			m := h.dial(axesim.Options{})
			job := firstJob(t, m)
			share := mustMine(t, m, job, job.NetworkDifficulty())
			if res := mustSubmit(t, m, share); !res.Accepted {
				t.Fatalf("block rejected after a statistics failure: %+v", res)
			}
			select {
			case <-h.sink.ch:
			case <-time.After(5 * time.Second):
				t.Fatal("the block was not handed over")
			}
			if res := mustSubmit(t, m, share); res.Accepted || res.Code != errDuplicate {
				t.Fatalf("duplicate share: %+v", res)
			}
			h.statisticsDone()
			// Nothing after the event that panicked reached the statistics.
			want := map[string]int32{"connected": 1, "accepted": 2, "rejected": 3, "disconnected": 4}[event]
			if got := rec.calls.Load(); got != want {
				t.Fatalf("%d calls reached the statistics, want %d (all up to the panic, none after)", got, want)
			}
			if out := logs.String(); !strings.Contains(out, "ERROR: recording the statistics failed: statistics bug on "+event) ||
				!strings.Contains(out, "recording the statistics is switched off") {
				t.Fatalf("log:\n%s", out)
			}
		})
	}
}

// stuckRecorder never returns from its first call, like statistics whose
// lock was left held.
type stuckRecorder struct {
	entered chan struct{} // closed when the first call starts
	release chan struct{}
	once    sync.Once
	calls   atomic.Int32
}

func newStuckRecorder() *stuckRecorder {
	return &stuckRecorder{entered: make(chan struct{}), release: make(chan struct{})}
}

func (s *stuckRecorder) event() {
	s.calls.Add(1)
	s.once.Do(func() { close(s.entered) })
	<-s.release
}

func (s *stuckRecorder) Connected(string, time.Time)                  { s.event() }
func (s *stuckRecorder) Disconnected(string, time.Time)               { s.event() }
func (s *stuckRecorder) Accepted(string, time.Time, float64, float64) { s.event() }
func (s *stuckRecorder) Rejected(string, time.Time)                   { s.event() }

// Statistics that stop answering must not hold up anything: shares are
// answered, blocks are handed over and new miners get work.
func TestStuckStatisticsNeverHoldUpMining(t *testing.T) {
	rec := newStuckRecorder()
	h := newHarnessWithRecorder(t, 1024, makeJob(t, "a", regtestBits, 0x20000000, 0, true), rec)
	t.Cleanup(func() { close(rec.release) })
	m := h.dial(axesim.Options{User: "rig"})
	job := firstJob(t, m)
	select {
	case <-rec.entered: // stuck on the miner's connection
	case <-time.After(5 * time.Second):
		t.Fatal("the statistics were never told about the connection")
	}
	for range 3 {
		h.mineBlock(m, job)
	}
	late := h.dial(axesim.Options{User: "late"})
	h.mineBlock(late, firstJob(t, late))
	if n := rec.calls.Load(); n != 1 {
		t.Fatalf("%d calls reached the stuck statistics, want 1", n)
	}
}

// While the statistics do not keep up, what does not fit in their queue is
// dropped, and that is reported once.
func TestStatisticsQueueDropsWhatDoesNotFit(t *testing.T) {
	logs := captureLog(t)
	rec := newStuckRecorder()
	q := newQueuedRecorder(rec, 2)
	q.Connected("rig", time.Now())
	select {
	case <-rec.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the statistics were never called")
	}
	within(t, "reporting to stuck statistics", func() {
		for range 10 {
			q.Accepted("rig", time.Now(), 1, 1)
		}
	})
	close(rec.release)
	q.queue.Close()
	select {
	case <-q.queue.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the queue did not finish")
	}
	if n := rec.calls.Load(); n != 3 {
		t.Fatalf("%d calls reached the statistics, want 3: the one under way and the two that fit", n)
	}
	if n := strings.Count(logs.String(), "the statistics are not keeping up"); n != 1 {
		t.Fatalf("the drops were reported %d times, want once:\n%s", n, logs.String())
	}
}

// brokenJobs is a job source with a bug: while breakOn matches a call, the
// call panics.
type brokenJobs struct {
	*fakeJobs
	mu      sync.Mutex
	breakOn func(method, id string) bool
}

func (b *brokenJobs) set(breakOn func(method, id string) bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.breakOn = breakOn
}

func (b *brokenJobs) check(method, id string) {
	b.mu.Lock()
	broken := b.breakOn != nil && b.breakOn(method, id)
	b.mu.Unlock()
	if broken {
		panic("bug in " + method)
	}
}

func (b *brokenJobs) Current() *work.Job      { b.check("Current", ""); return b.fakeJobs.Current() }
func (b *brokenJobs) Get(id string) *work.Job { b.check("Get", id); return b.fakeJobs.Get(id) }
func (b *brokenJobs) Healthy() bool           { b.check("Healthy", ""); return b.fakeJobs.Healthy() }

// newBrokenHarness starts a server on a regtest job whose job source has a
// bug that the test switches on.
func newBrokenHarness(t *testing.T) (*harness, *brokenJobs) {
	t.Helper()
	broken := &brokenJobs{}
	h := newHarnessWith(t, 1024, makeJob(t, "a", regtestBits, 0x20000000, 0, true), harnessOptions{
		jobs: func(f *fakeJobs) Jobs { broken.fakeJobs = f; return broken },
	})
	return h, broken
}

// A bug hit while serving one miner ends that miner's connection only. The
// others keep mining, new miners still get work, and the statistics hear
// that the miner left.
func TestPanicServingAMinerEndsOnlyThatConnection(t *testing.T) {
	logs := captureLog(t)
	h, jobs := newBrokenHarness(t)
	other := h.dial(axesim.Options{User: "other"})
	job := firstJob(t, other)
	victim := h.dial(axesim.Options{User: "victim"})
	share := mustMine(t, victim, firstJob(t, victim), job.NetworkDifficulty())
	share.JobID = "boom"
	jobs.set(func(method, id string) bool { return method == "Get" && id == "boom" })
	if res, err := victim.Submit(share); err == nil {
		t.Fatalf("the share that hit the bug was answered: %+v", res)
	}
	waitClosed(t, victim)

	h.mineBlock(other, job)
	newcomer := h.dial(axesim.Options{User: "newcomer"})
	h.mineBlock(newcomer, firstJob(t, newcomer))

	h.statisticsDone()
	left := false
	for _, e := range h.stats.all() {
		left = left || e == recorded{event: "disconnected", worker: "victim"}
	}
	if !left {
		t.Fatalf("the statistics never heard that the miner left: %+v", h.stats.all())
	}
	if out := logs.String(); !strings.Contains(out, "ERROR: serving a miner failed: bug in Get") ||
		!strings.Contains(out, "Miner victim (127.0.0.1) was disconnected after an internal error. The other miners are not affected.") {
		t.Fatalf("log:\n%s", out)
	}
}

// A bug hit while sending a new job to one miner ends that miner's
// connection only. The others get the job and mine on it.
func TestPanicSendingWorkEndsOnlyThatMiner(t *testing.T) {
	logs := captureLog(t)
	h, jobs := newBrokenHarness(t)
	victim := h.dial(axesim.Options{User: "victim"})
	firstJob(t, victim)
	other := h.dial(axesim.Options{User: "other"})
	firstJob(t, other)
	// Only the victim's connection remembers more than 256 jobs, which makes
	// sendJob look them up, and only those lookups hit the bug.
	for _, sess := range h.srv.snapshotSessions() {
		sess.mu.Lock()
		if sess.worker == "victim" {
			for i := range 300 {
				sess.jobDiff[fmt.Sprintf("old%d", i)] = 1
			}
		}
		sess.mu.Unlock()
	}
	jobs.set(func(method, id string) bool { return method == "Get" && strings.HasPrefix(id, "old") })
	next := makeJob(t, "b", regtestBits, 0x20000000, 0, false)
	h.jobs.add(next)
	h.srv.Broadcast(next)

	job, err := other.WaitJob(1, 5*time.Second, func(j *axesim.Job) bool { return j.ID == "b" })
	if err != nil {
		t.Fatal(err)
	}
	h.mineBlock(other, job)
	waitClosed(t, victim)
	if out := logs.String(); !strings.Contains(out, "ERROR: sending new work to a miner failed: bug in Get") {
		t.Fatalf("log:\n%s", out)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(logs.String(), "Miner victim (127.0.0.1) was disconnected after an internal error.") {
		if time.Now().After(deadline) {
			t.Fatalf("log:\n%s", logs.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// firstConnBroken hands out its first connection wrapped by wrap.
type firstConnBroken struct {
	net.Listener
	wrap func(net.Conn) net.Conn
	once sync.Once
}

func (l *firstConnBroken) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	first := false
	l.once.Do(func() { first = true })
	if first {
		return l.wrap(conn), nil
	}
	return conn, nil
}

// breakFirstConn makes a harness hand out its first connection wrapped by
// wrap.
func breakFirstConn(wrap func(net.Conn) net.Conn) harnessOptions {
	return harnessOptions{listener: func(l net.Listener) net.Listener { return &firstConnBroken{Listener: l, wrap: wrap} }}
}

// panicAddrConn is a connection that hits a bug as soon as it is looked at.
type panicAddrConn struct{ net.Conn }

func (panicAddrConn) RemoteAddr() net.Addr { panic("bug in RemoteAddr") }

// panicWriteConn is a connection that hits a bug when it is written to.
type panicWriteConn struct{ net.Conn }

func (panicWriteConn) Write([]byte) (int, error) { panic("bug in Write") }

// A bug hit while taking on a connection drops that connection, and the
// server goes on accepting others.
func TestPanicAcceptingAConnectionDropsOnlyIt(t *testing.T) {
	logs := captureLog(t)
	h := newHarnessWith(t, 1024, makeJob(t, "a", regtestBits, 0x20000000, 0, true),
		breakFirstConn(func(c net.Conn) net.Conn { return panicAddrConn{c} }))
	if m, err := axesim.Dial(h.addr, axesim.Options{User: "first", Timeout: 5 * time.Second}); err == nil {
		m.Close()
		t.Fatal("the connection that hit the bug was served")
	}
	m := h.dial(axesim.Options{User: "second"})
	h.mineBlock(m, firstJob(t, m))
	if out := logs.String(); !strings.Contains(out, "ERROR: accepting a miner failed: bug in RemoteAddr") {
		t.Fatalf("log:\n%s", out)
	}
}

// A bug hit while writing to one miner ends that miner's connection only.
func TestPanicWritingToAMinerEndsOnlyThatConnection(t *testing.T) {
	logs := captureLog(t)
	h := newHarnessWith(t, 1024, makeJob(t, "a", regtestBits, 0x20000000, 0, true),
		breakFirstConn(func(c net.Conn) net.Conn { return panicWriteConn{c} }))
	if m, err := axesim.Dial(h.addr, axesim.Options{User: "first", Timeout: 5 * time.Second}); err == nil {
		m.Close()
		t.Fatal("a miner that cannot be written to was served")
	}
	m := h.dial(axesim.Options{User: "second"})
	h.mineBlock(m, firstJob(t, m))
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(logs.String(), "ERROR: writing to a miner failed: bug in Write") ||
		!strings.Contains(logs.String(), "was disconnected after an internal error") {
		if time.Now().After(deadline) {
			t.Fatalf("log:\n%s", logs.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A bug hit while checking the node is logged, and the next check runs as
// usual.
func TestPanicCheckingTheNodeIsContained(t *testing.T) {
	logs := captureLog(t)
	h, jobs := newBrokenHarness(t)
	m := h.dial(axesim.Options{User: "rig"})
	job := firstJob(t, m)
	jobs.set(func(method, _ string) bool { return method == "Healthy" })
	h.srv.checkNode()
	jobs.set(nil)
	h.srv.checkNode()
	h.mineBlock(m, job)
	if out := logs.String(); !strings.Contains(out, "ERROR: checking the node failed: bug in Healthy") {
		t.Fatalf("log:\n%s", out)
	}
}

// A bug hit while a miner's lock is held must not leave it held: the
// manager's Broadcast and the status page take it too, and would wait for it
// forever.
func TestPanicUnderTheSessionLockReleasesIt(t *testing.T) {
	// lockIsFree waits for the bug to end the connection, then takes the
	// lock the way Broadcast and the status page do.
	lockIsFree := func(t *testing.T, h *harness, jobs *brokenJobs, sess *session) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !sess.failed.Load() {
			if time.Now().After(deadline) {
				t.Fatal("the bug was not hit")
			}
			time.Sleep(5 * time.Millisecond)
		}
		jobs.set(nil)
		within(t, "reading the miner's status", func() { sess.status(time.Now()) })
		within(t, "sending the miner a job", func() { sess.sendJob(h.jobs.Current(), false) })
	}

	t.Run("suggested difficulty", func(t *testing.T) {
		captureLog(t)
		h, jobs := newBrokenHarness(t)
		c := h.raw()
		c.send(`{"id":1,"method":"mining.subscribe","params":["x"]}`)
		c.answer(1)
		c.send(`{"id":2,"method":"mining.authorize","params":["victim","x"]}`)
		c.answer(2)
		// Messages are handled in order: once this is answered, the first job
		// has been sent.
		c.send(`{"id":3,"method":"mining.extranonce.subscribe","params":[]}`)
		c.answer(3)
		sess := h.srv.snapshotSessions()[0]
		jobs.set(func(method, _ string) bool { return method == "Current" })
		c.send(`{"id":4,"method":"mining.suggest_difficulty","params":[2048]}`)
		lockIsFree(t, h, jobs, sess)
	})

	t.Run("difficulty change after a share", func(t *testing.T) {
		captureLog(t)
		h, jobs := newBrokenHarness(t)
		m := h.dial(axesim.Options{User: "victim"})
		job := firstJob(t, m)
		sess := h.srv.snapshotSessions()[0]
		// One share in ten minutes is far too slow: the next share lowers the
		// difficulty, and announcing that hits the bug.
		sess.mu.Lock()
		sess.windowStart, sess.windowShares = time.Now().Add(-10*time.Minute), 1
		sess.mu.Unlock()
		jobs.set(func(method, _ string) bool { return method == "Current" })
		// The bug ends the connection right after the answer is queued, so
		// the miner may or may not get it. The block is handed over first.
		m.Submit(mustMine(t, m, job, job.NetworkDifficulty()))
		select {
		case <-h.sink.ch:
		case <-time.After(5 * time.Second):
			t.Fatal("the block was not handed over")
		}
		lockIsFree(t, h, jobs, sess)
	})
}

// stuckOutput takes nothing, like a terminal paused with Ctrl-S.
type stuckOutput struct{ release chan struct{} }

func (s stuckOutput) Write(p []byte) (int, error) {
	<-s.release
	return len(p), nil
}

// A log output that takes nothing must not hold up mining either. Through
// guard.LogOutput, which main sets up, miners still connect, get work and
// hand over blocks.
func TestStuckLogNeverHoldsUpMining(t *testing.T) {
	out := stuckOutput{make(chan struct{})}
	logs := guard.NewLogOutput(out)
	log.SetOutput(logs)
	t.Cleanup(func() {
		log.SetOutput(os.Stderr)
		close(out.release)
		logs.Close(5 * time.Second)
	})
	h := newHarness(t, 1024, makeJob(t, "a", regtestBits, 0x20000000, 0, true))
	for i := range 3 {
		m := h.dial(axesim.Options{User: fmt.Sprintf("rig%d", i)})
		h.mineBlock(m, firstJob(t, m))
	}
}
