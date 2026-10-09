package work

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yorgof/solostratum/internal/btc"
	"github.com/yorgof/solostratum/internal/rpc"
)

const commitment = "6a24aa21a9ede2f61c3f71d1defd3fa999dfa36953755c690689799962b48bebd836974e8cf9"

var payout = []byte{0x00, 0x14, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}

func template(prev byte, height int64, curTime int64, txs int) *rpc.BlockTemplate {
	t := &rpc.BlockTemplate{
		Version:           0x20000000,
		Rules:             []string{"csv", "!segwit", "taproot"},
		PreviousBlockHash: strings.Repeat(fmt.Sprintf("%02x", prev), 32),
		CoinbaseValue:     312500000 + 4242,
		Bits:              "1d00ffff",
		CurTime:           curTime,
		MinTime:           curTime - 3600,
		Height:            height,
		WitnessCommitment: commitment,
		LongPollID:        fmt.Sprintf("lp-%d-%d", prev, curTime),
	}
	for i := 0; i < txs; i++ {
		data := []byte{2, 0, 0, 0, byte(i), byte(i >> 8)}
		t.Transactions = append(t.Transactions, rpc.TemplateTx{Data: hex.EncodeToString(data), TxID: btc.SHA256d(data).String(), Hash: btc.SHA256d(data).String()})
	}
	return t
}

func mustJob(t *testing.T, tmpl *rpc.BlockTemplate, tag string) *Job {
	t.Helper()
	j, err := NewJob("1", tmpl, tag, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func TestCoinbaseLayout(t *testing.T) {
	for _, height := range []int64{1, 16, 17, 200, 840000, 8388608} {
		j := mustJob(t, template(0xab, height, 1700000000, 3), "/tag/")
		en1, en2 := []byte{0xa1, 0xa2, 0xa3, 0xa4}, []byte{0xb1, 0xb2, 0xb3, 0xb4}
		cb := j.Coinbase(payout, en1, en2)

		// The miner concatenates coinbase1 + extranonce1 + extranonce2 +
		// coinbase2; that must be the same transaction.
		joined := append(append(append(append([]byte{}, j.Coinbase1()...), en1...), en2...), j.Coinbase2(payout)...)
		if !bytes.Equal(cb, joined) {
			t.Fatal("Coinbase differs from coinbase1+extranonce+coinbase2")
		}

		r := bytes.NewReader(cb)
		next := func(n int) []byte {
			b := make([]byte, n)
			if _, err := r.Read(b); err != nil {
				t.Fatalf("height %d: coinbase too short", height)
			}
			return b
		}
		if v := binary.LittleEndian.Uint32(next(4)); v != 2 {
			t.Errorf("version %d", v)
		}
		if n := next(1)[0]; n != 1 {
			t.Fatalf("input count %d (a segwit marker here would give miners the wrong transaction id)", n)
		}
		if !bytes.Equal(next(36), append(make([]byte, 32), 0xff, 0xff, 0xff, 0xff)) {
			t.Error("previous output is not null")
		}
		script := next(int(next(1)[0]))
		wantScript := btc.AppendHeightPush(nil, height)
		wantScript = append(wantScript, 8)
		wantScript = append(append(wantScript, en1...), en2...)
		wantScript = append(append(wantScript, 5), "/tag/"...)
		if !bytes.Equal(script, wantScript) {
			t.Errorf("height %d: script %x, want %x", height, script, wantScript)
		}
		if len(script) < 2 || len(script) > 100 {
			t.Errorf("script length %d outside the consensus range 2..100", len(script))
		}
		if seq := binary.LittleEndian.Uint32(next(4)); seq != 0xfffffffe {
			t.Errorf("sequence %08x", seq)
		}
		if n := next(1)[0]; n != 2 {
			t.Fatalf("output count %d, want 2", n)
		}
		if v := binary.LittleEndian.Uint64(next(8)); v != 312504242 {
			t.Errorf("payout value %d", v)
		}
		if s := next(int(next(1)[0])); !bytes.Equal(s, payout) {
			t.Errorf("payout script %x", s)
		}
		if v := binary.LittleEndian.Uint64(next(8)); v != 0 {
			t.Errorf("commitment value %d", v)
		}
		want, _ := hex.DecodeString(commitment)
		if s := next(int(next(1)[0])); !bytes.Equal(s, want) {
			t.Errorf("commitment script %x", s)
		}
		if lock := binary.LittleEndian.Uint32(next(4)); int64(lock) != height-1 {
			t.Errorf("lock time %d, want %d", lock, height-1)
		}
		if r.Len() != 0 {
			t.Errorf("%d trailing bytes", r.Len())
		}
		// Extranonce1 must sit exactly at the end of coinbase1.
		if last := j.Coinbase1()[len(j.Coinbase1())-1]; last != Extranonce1Size+Extranonce2Size {
			t.Errorf("coinbase1 ends with %02x, want the extranonce push opcode", last)
		}
	}
}

func TestBlockSerialization(t *testing.T) {
	for _, txs := range []int{0, 1, 251, 252, 253, 300} {
		tmpl := template(0x11, 500, 1700000000, txs)
		j := mustJob(t, tmpl, "")
		cb := j.Coinbase(payout, make([]byte, 4), make([]byte, 4))
		header := j.Header(cb, j.Version, j.CurTime, 7)
		block := j.Block(header, cb)

		if !bytes.Equal(block[:80], header[:]) {
			t.Fatal("block does not start with the header")
		}
		rest := block[80:]
		wantCount := btc.AppendVarInt(nil, uint64(txs+1))
		if !bytes.HasPrefix(rest, wantCount) {
			t.Fatalf("%d txs: count bytes %x, want %x", txs, rest[:3], wantCount)
		}
		rest = rest[len(wantCount):]
		// Witness form: version, marker+flag, body, witness, lock time.
		wantCB := append([]byte{}, cb[:4]...)
		wantCB = append(wantCB, 0, 1)
		wantCB = append(wantCB, cb[4:len(cb)-4]...)
		wantCB = append(wantCB, 1, 32)
		wantCB = append(wantCB, make([]byte, 32)...)
		wantCB = append(wantCB, cb[len(cb)-4:]...)
		if !bytes.HasPrefix(rest, wantCB) {
			t.Fatal("coinbase in block is not the witness serialization")
		}
		rest = rest[len(wantCB):]
		for i, tx := range tmpl.Transactions {
			data, _ := hex.DecodeString(tx.Data)
			if !bytes.HasPrefix(rest, data) {
				t.Fatalf("transaction %d missing or out of order", i)
			}
			rest = rest[len(data):]
		}
		if len(rest) != 0 {
			t.Fatalf("%d trailing bytes", len(rest))
		}

		// The header commits to the transaction ids via the merkle root.
		ids := []btc.Hash{btc.SHA256d(cb)}
		for _, tx := range tmpl.Transactions {
			id, _ := btc.HashFromDisplayHex(tx.TxID)
			ids = append(ids, id)
		}
		root := btc.MerkleRoot(ids)
		if !bytes.Equal(header[36:68], root[:]) {
			t.Fatalf("%d txs: header merkle root does not match the transactions", txs)
		}
	}
}

func TestBlockWithoutWitnessCommitment(t *testing.T) {
	tmpl := template(0x11, 500, 1700000000, 1)
	tmpl.WitnessCommitment = ""
	j := mustJob(t, tmpl, "x")
	cb := j.Coinbase(payout, make([]byte, 4), make([]byte, 4))
	block := j.Block(j.Header(cb, j.Version, j.CurTime, 0), cb)
	if !bytes.Equal(block[81:81+len(cb)], cb) {
		t.Fatal("without a witness commitment the coinbase must be serialized plainly")
	}
	if n := cb[4+1+36+1+int(cb[41])+4]; n != 1 {
		t.Fatalf("output count %d, want 1", n)
	}
}

func TestJobFields(t *testing.T) {
	tmpl := template(0, 840000, 1700000000, 5)
	tmpl.PreviousBlockHash = "000000000000000000011a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f"
	j := mustJob(t, tmpl, "")
	if j.NetDiff != 1 || j.Bits != 0x1d00ffff || j.TxCount != 5 || len(j.Branches) != 3 {
		t.Errorf("job: diff %v bits %08x txs %d branches %d", j.NetDiff, j.Bits, j.TxCount, len(j.Branches))
	}
	// Stratum sends the hash in wire order with each 4-byte word reversed:
	// the display hash split into 8-character groups, groups in reverse
	// order.
	want := "3c4d5e6f" + "f8091a2b" + "b4c5d6e7" + "708192a3" + "3c4d5e6f" + "00011a2b" + "00000000" + "00000000"
	if got := j.StratumPrevHash(); got != want {
		t.Errorf("stratum prevhash\n got  %s\n want %s", got, want)
	}
	if j.PrevHash.String() != tmpl.PreviousBlockHash {
		t.Error("PrevHash does not round-trip")
	}
}

func TestTemplateRejections(t *testing.T) {
	mutate := func(f func(*rpc.BlockTemplate)) *rpc.BlockTemplate {
		tmpl := template(1, 100, 1700000000, 2)
		f(tmpl)
		return tmpl
	}
	cases := map[string]*rpc.BlockTemplate{
		"signet rule":      mutate(func(t *rpc.BlockTemplate) { t.Rules = append(t.Rules, "!signet") }),
		"unknown rule":     mutate(func(t *rpc.BlockTemplate) { t.Rules = []string{"!futurefork"} }),
		"bad prev hash":    mutate(func(t *rpc.BlockTemplate) { t.PreviousBlockHash = "abc" }),
		"bad bits":         mutate(func(t *rpc.BlockTemplate) { t.Bits = "xyz" }),
		"zero target":      mutate(func(t *rpc.BlockTemplate) { t.Bits = "00000000" }),
		"bad tx data":      mutate(func(t *rpc.BlockTemplate) { t.Transactions[1].Data = "0g" }),
		"bad txid":         mutate(func(t *rpc.BlockTemplate) { t.Transactions[0].TxID = "00" }),
		"bad commitment":   mutate(func(t *rpc.BlockTemplate) { t.WitnessCommitment = "zz" }),
		"negative value":   mutate(func(t *rpc.BlockTemplate) { t.CoinbaseValue = -1 }),
		"no time":          mutate(func(t *rpc.BlockTemplate) { t.CurTime = 0 }),
		"negative height":  mutate(func(t *rpc.BlockTemplate) { t.Height = -1 }),
		"time beyond 2106": mutate(func(t *rpc.BlockTemplate) { t.CurTime = 1 << 33 }),
	}
	for name, tmpl := range cases {
		if _, err := NewJob("1", tmpl, "", time.Now()); err == nil {
			t.Errorf("%s: template accepted", name)
		}
	}
	if _, err := NewJob("1", template(1, 100, 1700000000, 0), strings.Repeat("x", 90), time.Now()); err == nil {
		t.Error("over-long coinbase script accepted")
	}
}

func TestMarkShare(t *testing.T) {
	j := mustJob(t, template(1, 100, 1700000000, 0), "")
	a, b := []byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}
	if !j.MarkShare(a, b, 1, 2, 3) {
		t.Fatal("first share reported as duplicate")
	}
	if j.MarkShare(a, b, 1, 2, 3) {
		t.Fatal("duplicate share not detected")
	}
	for _, different := range []bool{
		j.MarkShare(b, b, 1, 2, 3), j.MarkShare(a, a, 1, 2, 3), j.MarkShare(a, b, 9, 2, 3),
		j.MarkShare(a, b, 1, 9, 3), j.MarkShare(a, b, 1, 2, 9),
	} {
		if !different {
			t.Fatal("a share differing in one field was treated as a duplicate")
		}
	}
}

// ---- Manager ----

type fakeNode struct {
	mu        sync.Mutex
	templates []*rpc.BlockTemplate // served in order; the last one repeats
	err       error
	proposals [][]byte
	reasons   []string // answers to ProposeBlock, in order
}

func (f *fakeNode) GetBlockchainInfo(ctx context.Context) (rpc.ChainInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return rpc.ChainInfo{}, f.err
	}
	return rpc.ChainInfo{Chain: "regtest", BestBlockHash: f.templates[0].PreviousBlockHash}, nil
}

func (f *fakeNode) GetBlockTemplate(ctx context.Context, longPollID string, wait time.Duration) (*rpc.BlockTemplate, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	t := f.templates[0]
	if len(f.templates) > 1 {
		f.templates = f.templates[1:]
	}
	return t, nil
}

func (f *fakeNode) ProposeBlock(ctx context.Context, block []byte) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.proposals = append(f.proposals, block)
	if len(f.reasons) == 0 {
		return "", nil
	}
	r := f.reasons[0]
	f.reasons = f.reasons[1:]
	return r, nil
}

func newTestManager(node *fakeNode) (*Manager, *[]*Job) {
	var published []*Job
	m := NewManager(node, "regtest", "/t/", func(j *Job) { published = append(published, j) })
	return m, &published
}

func TestManagerPublishing(t *testing.T) {
	ctx := context.Background()
	node := &fakeNode{templates: []*rpc.BlockTemplate{template(1, 100, 1000, 1)}}
	m, published := newTestManager(node)

	if m.Current() != nil || m.Healthy() {
		t.Fatal("manager has work before the first template")
	}
	if err := m.Fetch(ctx); err != nil {
		t.Fatal(err)
	}
	first := m.Current()
	if first == nil || !first.CleanJobs || !m.Healthy() || len(*published) != 1 {
		t.Fatalf("first job: %+v", first)
	}

	// The same template again within a second is not news.
	m.Fetch(ctx)
	if len(*published) != 1 {
		t.Fatal("identical template published twice")
	}

	// A later template on the same tip is a refresh, not a clean job.
	first.Created = first.Created.Add(-time.Minute)
	node.templates = []*rpc.BlockTemplate{template(1, 100, 1030, 4)}
	m.Fetch(ctx)
	refresh := m.Current()
	if len(*published) != 2 || refresh.CleanJobs || refresh.ID == first.ID || refresh.TxCount != 4 {
		t.Fatalf("refresh job: %+v", refresh)
	}
	if m.Get(first.ID) != first {
		t.Fatal("the previous job on the same tip must stay valid")
	}

	// An out-of-order answer with an older timestamp is ignored.
	refresh.Created = refresh.Created.Add(-time.Minute)
	node.templates = []*rpc.BlockTemplate{template(1, 100, 1010, 2)}
	m.Fetch(ctx)
	if len(*published) != 2 {
		t.Fatal("an older template replaced a newer one")
	}

	// A new tip invalidates everything before it.
	node.templates = []*rpc.BlockTemplate{template(2, 101, 1040, 0)}
	m.Fetch(ctx)
	tip := m.Current()
	if len(*published) != 3 || !tip.CleanJobs || tip.Height != 101 {
		t.Fatalf("new tip job: %+v", tip)
	}
	if m.Get(first.ID) != nil || m.Get(refresh.ID) != nil {
		t.Fatal("jobs for the old tip are still accepted")
	}

	// An answer that was requested before the tip changed and would switch
	// the tip again cannot be trusted; it is dropped and a fresh template
	// is requested instead.
	staleGen := m.tipGen - 1
	if err := m.publish(template(1, 100, 1050, 0), staleGen); err != nil {
		t.Fatal(err)
	}
	if m.Current() != tip {
		t.Fatal("a template requested before the tip changed replaced the current tip")
	}
	select {
	case <-m.refreshC:
	default:
		t.Fatal("no fresh template was requested after dropping a raced answer")
	}

	// A reorganisation is followed even when it leads to a lower height.
	tip.Created = tip.Created.Add(-time.Minute)
	node.templates = []*rpc.BlockTemplate{template(3, 99, 1060, 0)}
	m.Fetch(ctx)
	if cur := m.Current(); cur == tip || !cur.CleanJobs || cur.Height != 99 {
		t.Fatal("a reorganisation to a lower height was ignored")
	}
	if m.Current().Seq <= tip.Seq {
		t.Fatal("job sequence numbers must increase")
	}

	st := m.State()
	if !st.Connected || st.Height != 99 || st.Chain != "regtest" || st.Error != "" {
		t.Errorf("state: %+v", st)
	}
}

func TestManagerKeepsOnlyRecentJobs(t *testing.T) {
	node := &fakeNode{}
	m, _ := newTestManager(node)
	var ids []string
	for i := 0; i < keepJobs+5; i++ {
		node.templates = []*rpc.BlockTemplate{template(1, 100, int64(1000+i), 0)}
		if cur := m.Current(); cur != nil {
			cur.Created = cur.Created.Add(-time.Minute)
		}
		m.Fetch(context.Background())
		ids = append(ids, m.Current().ID)
	}
	for i, id := range ids {
		if kept := m.Get(id) != nil; kept != (i >= len(ids)-keepJobs) {
			t.Errorf("job %d kept=%v", i, kept)
		}
	}
}

// Jobs for the same tip share the bytes of the transactions they have in
// common, so keeping many of them costs little memory.
func TestJobsShareTransactionData(t *testing.T) {
	first, cache, err := newJob("1", template(1, 100, 1000, 50), "", time.Now(), nil)
	if err != nil {
		t.Fatal(err)
	}
	next := template(1, 100, 1030, 60) // the same 50 transactions plus 10 new ones
	second, cache2, err := newJob("2", next, "", time.Now(), cache)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if &first.txData[i][0] != &second.txData[i][0] {
			t.Fatalf("transaction %d was copied instead of shared", i)
		}
	}
	if len(cache2) != 60 {
		t.Fatalf("cache holds %d transactions, want 60", len(cache2))
	}
	// The block built from shared data is identical to one built fresh.
	fresh := mustJob(t, next, "")
	cb := fresh.Coinbase(payout, make([]byte, 4), make([]byte, 4))
	h := fresh.Header(cb, fresh.Version, fresh.CurTime, 1)
	if !bytes.Equal(fresh.Block(h, cb), second.Block(h, cb)) {
		t.Fatal("block built from shared transaction data differs")
	}
	// Same txid but different bytes (other witness): must not be reused.
	changed := template(1, 100, 1060, 60)
	changed.Transactions[3].Data += "00"
	data, _ := hex.DecodeString(changed.Transactions[3].Data)
	changed.Transactions[3].Hash = btc.SHA256d(data).String()
	third, _, err := newJob("3", changed, "", time.Now(), cache2)
	if err != nil {
		t.Fatal(err)
	}
	if len(third.txData[3]) != len(second.txData[3])+1 {
		t.Fatal("stale cached bytes were used for a transaction whose data changed")
	}
}

func TestManagerErrorState(t *testing.T) {
	node := &fakeNode{templates: []*rpc.BlockTemplate{template(1, 100, 1000, 0)}}
	m, _ := newTestManager(node)
	m.Fetch(context.Background())

	node.err = errors.New("dial tcp 127.0.0.1:8332: connect: connection refused")
	if err := m.Fetch(context.Background()); err == nil {
		t.Fatal("expected an error")
	}
	st := m.State()
	if st.Connected || !strings.Contains(st.Error, "connection refused") {
		t.Errorf("state while node is down: %+v", st)
	}
	if m.Current() == nil {
		t.Error("the current job must be kept while the node is briefly unreachable")
	}
	m.mu.Lock()
	m.lastOK = time.Now().Add(-nodeTimeout - time.Second)
	m.mu.Unlock()
	if m.Healthy() {
		t.Error("manager still healthy long after the node went away")
	}

	node.err = nil
	m.Current().Created = time.Now().Add(-time.Minute)
	node.templates = []*rpc.BlockTemplate{template(1, 100, 1100, 0)}
	if err := m.Fetch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st := m.State(); !st.Connected || st.Error != "" || !m.Healthy() {
		t.Errorf("state after recovery: %+v", st)
	}
}

// A node that answers but whose templates cannot be used (for example after
// a soft fork this program does not know) must not count as healthy, so
// miners are released to their fallback pool.
func TestUnusableTemplatesAreNotHealthy(t *testing.T) {
	node := &fakeNode{templates: []*rpc.BlockTemplate{template(1, 100, 1000, 0)}}
	m, published := newTestManager(node)
	m.Fetch(context.Background())

	bad := template(2, 101, 1100, 0)
	bad.Rules = append(bad.Rules, "!futurefork")
	node.templates = []*rpc.BlockTemplate{bad}
	before := m.lastOK
	if err := m.Fetch(context.Background()); err == nil {
		t.Fatal("unusable template accepted")
	}
	if len(*published) != 1 || !m.lastOK.Equal(before) {
		t.Fatal("an unusable template refreshed the health timer or produced a job")
	}
	if st := m.State(); st.Connected || !strings.Contains(st.Error, "futurefork") {
		t.Errorf("state: %+v", st)
	}
}

func TestSelfTest(t *testing.T) {
	node := &fakeNode{templates: []*rpc.BlockTemplate{template(1, 100, 1000, 3)}}
	m, _ := newTestManager(node)
	if err := m.SelfTest(context.Background(), payout); err != nil {
		t.Fatal(err)
	}
	if len(node.proposals) != 1 || len(node.proposals[0]) < 80+1+100 {
		t.Fatalf("proposals: %d", len(node.proposals))
	}

	// One "tip moved" style answer is retried.
	node.reasons = []string{"inconclusive-not-best-prevblk", ""}
	if err := m.SelfTest(context.Background(), payout); err != nil {
		t.Fatalf("self-test did not retry: %v", err)
	}

	// A persistent rejection is a hard stop.
	node.reasons = []string{"bad-txnmrklroot", "bad-txnmrklroot", "bad-txnmrklroot"}
	err := m.SelfTest(context.Background(), payout)
	var st *SelfTestError
	if !errors.As(err, &st) || st.Reason != "bad-txnmrklroot" {
		t.Fatalf("error %v, want SelfTestError", err)
	}
}

func witnessTx(w byte) (string, []byte) {
	// Two structurally valid witness serializations with identical non-witness
	// bytes and same length, differing only in a one-byte witness stack item.
	body := append([]byte{1}, make([]byte, 32)...)
	body = append(body, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 1)
	body = append(body, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0x51)
	base := append([]byte{2, 0, 0, 0}, body...)
	base = append(base, 0, 0, 0, 0)
	raw := append([]byte{2, 0, 0, 0, 0, 1}, body...)
	raw = append(raw, 1, 1, w, 0, 0, 0, 0)
	return btc.SHA256d(base).String(), raw
}

func TestWrongWitnessHashFieldDoesNotStopMining(t *testing.T) {
	// A node or proxy that reports a hash that does not match the bytes,
	// or no hash at all, must not leave the miners on a stale tip: the
	// bytes decide.
	for _, field := range []string{strings.Repeat("01", 32), "00", "not a hash"} {
		t.Run(field, func(t *testing.T) {
			tmpl := template(1, 100, 1000, 3)
			data, _ := hex.DecodeString(tmpl.Transactions[1].Data)
			tmpl.Transactions[1].Hash = field
			job, cache, err := newJob("1", tmpl, "", time.Now(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(job.txData[1], data) {
				t.Fatal("the transaction bytes were not taken from the template")
			}
			if wrong, err := btc.HashFromDisplayHex(field); err == nil {
				if _, ok := cache[wrong]; ok {
					t.Fatal("the reported hash was used as the cache key")
				}
			}
			if cached, ok := cache[btc.SHA256d(data)]; !ok || &cached[0] != &job.txData[1][0] {
				t.Fatal("the bytes are not cached under their real hash")
			}
			// The next template from the same node reuses the bytes all the same.
			again, _, err := newJob("2", tmpl, "", time.Now(), cache)
			if err != nil {
				t.Fatal(err)
			}
			if &again.txData[1][0] != &job.txData[1][0] {
				t.Fatal("unchanged bytes were not shared between jobs")
			}
		})
	}
}

func TestCacheTracksWitnessChangesOfTheSameLength(t *testing.T) {
	for _, includeHash := range []bool{true, false} {
		t.Run(fmt.Sprintf("hash=%v", includeHash), func(t *testing.T) {
			txid, firstBytes := witnessTx(1)
			_, nextBytes := witnessTx(2)
			makeTemplate := func(data []byte) *rpc.BlockTemplate {
				tmpl := template(1, 100, 1000, 0)
				tx := rpc.TemplateTx{TxID: txid, Data: hex.EncodeToString(data)}
				if includeHash {
					tx.Hash = btc.SHA256d(data).String()
				}
				tmpl.Transactions = []rpc.TemplateTx{tx}
				return tmpl
			}
			first, cache, err := newJob("1", makeTemplate(firstBytes), "", time.Now(), nil)
			if err != nil {
				t.Fatal(err)
			}
			tmpl := makeTemplate(nextBytes)
			next, cache, err := newJob("2", tmpl, "", time.Now(), cache)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(first.txData[0], firstBytes) || !bytes.Equal(next.txData[0], nextBytes) {
				t.Fatal("cached jobs did not preserve their own witness bytes")
			}
			fresh := mustJob(t, tmpl, "")
			cb := fresh.Coinbase(payout, make([]byte, 4), make([]byte, 4))
			header := fresh.Header(cb, fresh.Version, fresh.CurTime, 0)
			if !bytes.Equal(next.Block(header, cb), fresh.Block(header, cb)) {
				t.Fatal("cached block differs from the current template")
			}
			again, _, err := newJob("3", tmpl, "", time.Now(), cache)
			if err != nil {
				t.Fatal(err)
			}
			if &again.txData[0][0] != &next.txData[0][0] {
				t.Fatal("unchanged witness bytes were not shared")
			}
		})
	}
}

func TestCachedTemplateDataIsAuthoritative(t *testing.T) {
	txid, before := witnessTx(1)
	_, after := witnessTx(2)
	for _, tc := range []struct {
		name, data string
		wantError  bool
	}{
		{"changed witness with stale hash", hex.EncodeToString(after), false},
		{"different length with stale hash", hex.EncodeToString(append(after, 0)), false},
		{"uppercase hex", strings.ToUpper(hex.EncodeToString(before)), false},
		{"invalid hex with cached hash", "zz", true},
		{"odd length with cached hash", "0", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmpl := template(1, 100, 1000, 0)
			tmpl.Transactions = []rpc.TemplateTx{{
				TxID: txid, Hash: btc.SHA256d(before).String(), Data: hex.EncodeToString(before),
			}}
			first, cache, err := newJob("1", tmpl, "", time.Now(), nil)
			if err != nil {
				t.Fatal(err)
			}
			tmpl.Transactions[0].Data = tc.data
			cached, next, err := newJob("2", tmpl, "", time.Now(), cache)
			if tc.wantError {
				if err == nil {
					t.Fatal("a cache hit hid malformed transaction data")
				}
				if len(next) != 1 || !bytes.Equal(next[btc.SHA256d(before)], before) {
					t.Fatal("a rejected template changed the cache")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			fresh := mustJob(t, tmpl, "")
			cb := fresh.Coinbase(payout, make([]byte, 4), make([]byte, 4))
			header := fresh.Header(cb, fresh.Version, fresh.CurTime, 0)
			if !bytes.Equal(cached.Block(header, cb), fresh.Block(header, cb)) {
				t.Fatal("the same template builds different blocks with and without cached transactions")
			}
			data, _ := hex.DecodeString(tc.data)
			if len(next) != 1 || !bytes.Equal(next[btc.SHA256d(data)], data) {
				t.Fatal("the updated cache does not use the actual transaction hash")
			}
			if !bytes.Equal(first.txData[0], before) {
				t.Fatal("updating the cache mutated an earlier job")
			}
			if bytes.Equal(data, before) && &cached.txData[0][0] != &first.txData[0][0] {
				t.Fatal("unchanged bytes were not shared")
			}
		})
	}
}

// Cache state must never change which block a template builds or whether
// malformed transaction data is rejected.
func FuzzCachedTemplateMatchesFresh(f *testing.F) {
	before := []byte{0xab, 0xcd}
	hash := btc.SHA256d(before).String()
	for _, data := range []string{"abcd", "ABCD", "abce", "", "0", "zz"} {
		f.Add(data, hash)
	}
	f.Add("abcd", "")
	f.Add("abcd", "not a hash")
	f.Fuzz(func(t *testing.T, data, reportedHash string) {
		if len(data) > 8192 || len(reportedHash) > 128 {
			t.Skip()
		}
		tmpl := template(1, 100, 1000, 0)
		tmpl.Transactions = []rpc.TemplateTx{{TxID: hash, Hash: reportedHash, Data: data}}
		cache := map[btc.Hash][]byte{btc.SHA256d(before): before}
		fresh, _, freshErr := newJob("1", tmpl, "", time.Now(), nil)
		cached, _, cachedErr := newJob("1", tmpl, "", time.Now(), cache)
		if (freshErr == nil) != (cachedErr == nil) {
			t.Fatalf("cache changed validation: fresh %v, cached %v", freshErr, cachedErr)
		}
		if freshErr == nil {
			cb := fresh.Coinbase(payout, make([]byte, 4), make([]byte, 4))
			header := fresh.Header(cb, fresh.Version, fresh.CurTime, 0)
			if !bytes.Equal(fresh.Block(header, cb), cached.Block(header, cb)) {
				t.Fatal("cache changed the serialized block")
			}
		}
	})
}
