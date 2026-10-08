//go:build e2e

// Package e2e runs solostratum against real Bitcoin Core nodes in Docker and
// checks that the blocks it produces are accepted.
//
// Run it with "make e2e" (needs Docker and Go). See README.md in this folder.
package e2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/yorgof/solostratum/internal/axesim"
)

const (
	rpcUser = "e2e"
	rpcPass = "e2e-password"
	label   = "solostratum-e2e"
)

var (
	binary     string
	coreImages = []string{"bitcoin/bitcoin:31", "bitcoin/bitcoin:29"}
)

func TestMain(m *testing.M) {
	if v := strings.Fields(os.Getenv("E2E_CORE_IMAGES")); len(v) > 0 {
		coreImages = v
	}
	if err := exec.Command("docker", "version").Run(); err != nil {
		fmt.Fprintln(os.Stderr, "e2e: Docker is required and does not seem to be running:", err)
		os.Exit(1)
	}
	dir, err := os.MkdirTemp("", "solostratum-e2e-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	binary = filepath.Join(dir, "solostratum")
	build := exec.Command("go", "build", "-o", binary, "..")
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "e2e: building solostratum failed: %v\n%s", err, out)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// ---- helpers: commands ----

func docker(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func eventually(t *testing.T, timeout time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for: %s", timeout, what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// ---- helpers: Bitcoin Core in Docker ----

type node struct {
	t       *testing.T
	name    string
	rpcPort int
}

func startNode(t *testing.T, image string) *node {
	t.Helper()
	n := &node{t: t, name: fmt.Sprintf("%s-%d-%d", label, os.Getpid(), time.Now().UnixNano()), rpcPort: freePort(t)}
	docker(t, "run", "-d", "--name", n.name, "--label", label,
		"-p", fmt.Sprintf("127.0.0.1:%d:18443", n.rpcPort),
		image,
		"-regtest", "-server", "-rpcbind=0.0.0.0", "-rpcallowip=0.0.0.0/0",
		"-rpcuser="+rpcUser, "-rpcpassword="+rpcPass, "-fallbackfee=0.0002")
	t.Cleanup(func() {
		if t.Failed() {
			out, _ := exec.Command("docker", "logs", "--tail", "40", n.name).CombinedOutput()
			t.Logf("bitcoind log tail:\n%s", out)
		}
		exec.Command("docker", "rm", "-f", "-v", n.name).Run()
	})
	n.waitReady()
	n.cli("-named", "createwallet", "wallet_name=e2e", "load_on_startup=true")
	// 101 blocks make the first coinbase spendable; a few more give the
	// wallet several coins to work with.
	n.cli("generatetoaddress", "110", n.newAddress("bech32"))
	return n
}

func (n *node) waitReady() {
	n.t.Helper()
	eventually(n.t, 60*time.Second, "bitcoind to answer RPC", func() bool {
		_, err := n.tryCLI("getblockchaininfo")
		return err == nil
	})
}

func (n *node) tryCLI(args ...string) (string, error) {
	full := append([]string{"exec", n.name, "bitcoin-cli", "-regtest", "-rpcuser=" + rpcUser, "-rpcpassword=" + rpcPass}, args...)
	out, err := exec.Command("docker", full...).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func (n *node) cli(args ...string) string {
	n.t.Helper()
	out, err := n.tryCLI(args...)
	if err != nil {
		n.t.Fatalf("bitcoin-cli %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

func (n *node) cliJSON(dst any, args ...string) {
	n.t.Helper()
	if err := json.Unmarshal([]byte(n.cli(args...)), dst); err != nil {
		n.t.Fatalf("bitcoin-cli %s: %v", strings.Join(args, " "), err)
	}
}

// stop shuts bitcoind down cleanly, as a user restarting their node would,
// and waits for the container to exit.
func (n *node) stop() {
	n.t.Helper()
	n.cli("stop")
	docker(n.t, "wait", n.name)
}

func (n *node) start() {
	n.t.Helper()
	docker(n.t, "start", n.name)
	n.waitReady()
}

func (n *node) newAddress(kind string) string { return n.cli("getnewaddress", "", kind) }

func (n *node) height() int {
	h, _ := strconv.Atoi(n.cli("getblockcount"))
	return h
}

func (n *node) tip() string { return n.cli("getbestblockhash") }

// fillMempool creates count transactions paying to a mix of address types,
// so blocks contain both witness and non-witness transactions.
func (n *node) fillMempool(count int) {
	n.t.Helper()
	kinds := []string{"bech32", "legacy", "bech32m", "p2sh-segwit"}
	for i := 0; i < count; i++ {
		n.cli("sendtoaddress", n.newAddress(kinds[i%len(kinds)]), "0.01")
	}
	var info struct {
		Size int `json:"size"`
	}
	n.cliJSON(&info, "getmempoolinfo")
	if info.Size < count {
		n.t.Fatalf("mempool has %d transactions, want at least %d", info.Size, count)
	}
}

// fundManyCoins splits the wallet's money into many confirmed coins so that
// hundreds of independent transactions can be created afterwards.
func (n *node) fundManyCoins(count int) {
	n.t.Helper()
	for done := 0; done < count; {
		batch := min(100, count-done)
		outputs := map[string]float64{}
		for i := 0; i < batch; i++ {
			outputs[n.newAddress("bech32")] = 0.05
		}
		raw, _ := json.Marshal(outputs)
		n.cli("sendmany", "", string(raw))
		done += batch
	}
	n.cli("generatetoaddress", "1", n.newAddress("bech32"))
}

type coreBlock struct {
	Hash       string `json:"hash"`
	Height     int    `json:"height"`
	NTx        int    `json:"nTx"`
	VersionHex string `json:"versionHex"`
	Tx         []struct {
		TxID string `json:"txid"`
		Vin  []struct {
			Coinbase string `json:"coinbase"`
		} `json:"vin"`
		Vout []struct {
			Value        float64 `json:"value"`
			ScriptPubKey struct {
				Address string `json:"address"`
				Hex     string `json:"hex"`
			} `json:"scriptPubKey"`
		} `json:"vout"`
	} `json:"tx"`
}

func (n *node) block(hash string) coreBlock {
	n.t.Helper()
	var b coreBlock
	n.cliJSON(&b, "getblock", hash, "2")
	return b
}

// ---- helpers: solostratum ----

type bridge struct {
	t           *testing.T
	dir         string
	stratumAddr string
	stratumPort int
	statusURL   string
	cmd         *exec.Cmd
	logs        *lockedBuffer
	exited      chan struct{}
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func writeConfig(t *testing.T, dir string, n *node, payout string, stratumPort, statusPort int, extra ...string) string {
	t.Helper()
	lines := []string{
		fmt.Sprintf("node_url = http://127.0.0.1:%d", n.rpcPort),
		"node_user = " + rpcUser,
		"node_password = " + rpcPass,
		"payout_address = " + payout,
		fmt.Sprintf("stratum_listen = 127.0.0.1:%d", stratumPort),
		fmt.Sprintf("status_listen = 127.0.0.1:%d", statusPort),
		"coinbase_tag = /e2e-test/",
	}
	lines = append(lines, extra...)
	path := filepath.Join(dir, "solostratum.conf")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func startBridge(t *testing.T, n *node, payout string, extra ...string) *bridge {
	t.Helper()
	b := &bridge{t: t, dir: t.TempDir(), stratumPort: freePort(t), logs: &lockedBuffer{}, exited: make(chan struct{})}
	statusPort := freePort(t)
	b.stratumAddr = fmt.Sprintf("127.0.0.1:%d", b.stratumPort)
	b.statusURL = fmt.Sprintf("http://127.0.0.1:%d", statusPort)
	conf := writeConfig(t, b.dir, n, payout, b.stratumPort, statusPort, extra...)

	b.cmd = exec.Command(binary, "-config", conf)
	b.cmd.Stdout, b.cmd.Stderr = b.logs, b.logs
	if err := b.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { b.cmd.Wait(); close(b.exited) }()
	t.Cleanup(func() {
		b.stop()
		if t.Failed() {
			t.Logf("solostratum log:\n%s", b.logs.String())
		}
	})
	eventually(t, 30*time.Second, "solostratum to become ready", func() bool {
		select {
		case <-b.exited:
			t.Fatalf("solostratum exited during start-up:\n%s", b.logs.String())
		default:
		}
		resp, err := http.Get(b.statusURL + "/api/status")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})
	return b
}

func (b *bridge) stop() {
	select {
	case <-b.exited:
		return
	default:
	}
	b.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-b.exited:
	case <-time.After(10 * time.Second):
		b.cmd.Process.Kill()
		<-b.exited
		b.t.Error("solostratum did not shut down within 10 seconds of SIGTERM")
	}
}

type statusReport struct {
	PayoutAddress string `json:"payoutAddress"`
	Node          struct {
		Chain      string  `json:"chain"`
		Connected  bool    `json:"connected"`
		Error      string  `json:"error"`
		Height     int     `json:"height"`
		Difficulty float64 `json:"difficulty"`
		TxCount    int     `json:"txCount"`
		RewardSats int64   `json:"rewardSats"`
	} `json:"node"`
	Pool struct {
		Accepted uint64 `json:"accepted"`
		Rejected uint64 `json:"rejected"`
		Miners   []struct {
			Worker    string `json:"worker"`
			Payout    string `json:"payout"`
			OwnPayout bool   `json:"ownPayout"`
			Accepted  uint64 `json:"accepted"`
		} `json:"miners"`
	} `json:"pool"`
	Blocks []struct {
		Height int    `json:"height"`
		Hash   string `json:"hash"`
		File   string `json:"file"`
		Status string `json:"status"`
		Worker string `json:"worker"`
	} `json:"blocks"`
	Offline []struct {
		Worker      string `json:"worker"`
		Accepted    uint64 `json:"accepted"`
		Connections uint64 `json:"connections"`
	} `json:"offline"`
	History bool `json:"history"`
}

func (b *bridge) status() statusReport {
	b.t.Helper()
	resp, err := http.Get(b.statusURL + "/api/status")
	if err != nil {
		b.t.Fatal(err)
	}
	defer resp.Body.Close()
	var r statusReport
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		b.t.Fatal(err)
	}
	return r
}

func (b *bridge) dial(user string) *axesim.Miner {
	b.t.Helper()
	m, err := axesim.Dial(b.stratumAddr, axesim.Options{User: user, Password: "x", SuggestDifficulty: 1000, ExtranonceSub: true})
	if err != nil {
		b.t.Fatalf("connecting the simulated Bitaxe: %v", err)
	}
	b.t.Cleanup(m.Close)
	return m
}

// blockFiles returns the saved blocks: hash -> file contents.
func (b *bridge) blockFiles() map[string]string {
	b.t.Helper()
	files, _ := filepath.Glob(filepath.Join(b.dir, "blocks", "block-*.hex"))
	out := map[string]string{}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			b.t.Fatal(err)
		}
		name := strings.TrimSuffix(filepath.Base(f), ".hex")
		out[name[strings.LastIndex(name, "-")+1:]] = strings.TrimSpace(string(raw))
	}
	return out
}

// ---- helpers: mining ----

// waitJobOnTip waits until the miner holds a job that builds on the node's
// current tip and includes at least minTxs transactions (judged by the
// number of merkle branches).
func waitJobOnTip(t *testing.T, m *axesim.Miner, n *node, minTxs int) *axesim.Job {
	t.Helper()
	wantBranches := 0
	for 1<<wantBranches < minTxs+1 {
		wantBranches++
	}
	tip := n.tip()
	job, err := m.WaitJob(0, 45*time.Second, func(j *axesim.Job) bool {
		return prevHashOf(j) == tip && len(j.Branches) >= wantBranches
	})
	if err != nil {
		t.Fatalf("no job on tip %s with %d+ transactions: %v", tip, minTxs, err)
	}
	return job
}

// prevHashOf undoes Stratum's word-swapped encoding the way the firmware
// does, and returns the hash as Bitcoin Core displays it.
func prevHashOf(j *axesim.Job) string {
	raw := []byte(j.PrevHash)
	if len(raw) != 64 {
		return ""
	}
	out := make([]byte, 0, 64)
	for word := 7; word >= 0; word-- {
		out = append(out, raw[word*8:word*8+8]...)
	}
	return string(out)
}

// mineBlock finds and submits one block through the simulated Bitaxe and
// waits until Bitcoin Core reports it as the chain tip.
func mineBlock(t *testing.T, m *axesim.Miner, n *node, minTxs int) (axesim.Share, coreBlock) {
	t.Helper()
	job := waitJobOnTip(t, m, n, minTxs)
	share, ok := m.Mine(job, job.NetworkDifficulty(), 1_000_000)
	if !ok {
		t.Fatal("simulated Bitaxe found no block (regtest needs about two hashes)")
	}
	res, err := m.Submit(share)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Accepted {
		t.Fatalf("share rejected by solostratum: %+v", res)
	}
	hash := axesim.DisplayHash(share.Hash)
	eventually(t, 15*time.Second, "Bitcoin Core to accept block "+hash, func() bool { return n.tip() == hash })
	return share, n.block(hash)
}

// checkCoinbase verifies who gets paid and how much.
func checkCoinbase(t *testing.T, n *node, b coreBlock, wantAddress string) {
	t.Helper()
	cb := b.Tx[0]
	if got := cb.Vout[0].ScriptPubKey.Address; got != wantAddress {
		t.Errorf("block %d pays %s, want %s", b.Height, got, wantAddress)
	}
	var stats struct {
		Subsidy  int64 `json:"subsidy"`
		TotalFee int64 `json:"totalfee"`
	}
	n.cliJSON(&stats, "getblockstats", strconv.Itoa(b.Height), `["subsidy","totalfee"]`)
	if got, want := int64(math.Round(cb.Vout[0].Value*1e8)), stats.Subsidy+stats.TotalFee; got != want {
		t.Errorf("block %d reward %d sat, want subsidy+fees = %d sat", b.Height, got, want)
	}
	if tag := fmt.Sprintf("%x", "/e2e-test/"); !strings.Contains(cb.Vin[0].Coinbase, tag) {
		t.Errorf("coinbase script %s does not contain the configured tag", cb.Vin[0].Coinbase)
	}
}

// ---- tests ----

func TestE2E(t *testing.T) {
	for _, image := range coreImages {
		t.Run(strings.NewReplacer("/", "_", ":", "_").Replace(image), func(t *testing.T) {
			t.Parallel()
			t.Run("PayoutAddressTypes", func(t *testing.T) { t.Parallel(); testPayoutAddressTypes(t, image) })
			t.Run("UsernameOverride", func(t *testing.T) { t.Parallel(); testUsernameOverride(t, image) })
			t.Run("ManyTransactions", func(t *testing.T) { t.Parallel(); testManyTransactions(t, image) })
			t.Run("ConsecutiveBlocksAndFiles", func(t *testing.T) { t.Parallel(); testConsecutiveBlocks(t, image) })
			t.Run("NewBlockFromNetwork", func(t *testing.T) { t.Parallel(); testNewBlockFromNetwork(t, image) })
			t.Run("FirmwareCode", func(t *testing.T) { t.Parallel(); testFirmwareCode(t, image) })
			t.Run("Cpuminer", func(t *testing.T) { t.Parallel(); testCpuminer(t, image) })
			t.Run("NodeOutage", func(t *testing.T) { t.Parallel(); testNodeOutage(t, image) })
			t.Run("StartupChecks", func(t *testing.T) { t.Parallel(); testStartupChecks(t, image) })
		})
	}
}

// Every address type Bitcoin Core can generate must work as payout address,
// with empty, small and mixed blocks.
func testPayoutAddressTypes(t *testing.T, image string) {
	n := startNode(t, image)
	for i, kind := range []string{"legacy", "p2sh-segwit", "bech32", "bech32m"} {
		payout := n.newAddress(kind)
		txs := []int{0, 1, 2, 5}[i]
		n.fillMempool(txs)
		b := startBridge(t, n, payout)
		m := b.dial("bitaxe")
		_, block := mineBlock(t, m, n, txs)
		if block.NTx != txs+1 {
			t.Errorf("%s: block has %d transactions, want %d", kind, block.NTx, txs+1)
		}
		checkCoinbase(t, n, block, payout)
		var mempool struct {
			Size int `json:"size"`
		}
		n.cliJSON(&mempool, "getmempoolinfo")
		if mempool.Size != 0 {
			t.Errorf("%s: %d transactions left in the mempool after the block", kind, mempool.Size)
		}
		st := b.status()
		if len(st.Blocks) != 1 || st.Blocks[0].Status != "accepted" || st.Blocks[0].Hash != block.Hash || st.Blocks[0].Worker != "bitaxe" {
			t.Errorf("%s: status page blocks: %+v", kind, st.Blocks)
		}
		if st.Node.Chain != "regtest" || !st.Node.Connected || st.PayoutAddress != payout {
			t.Errorf("%s: status page node: %+v", kind, st.Node)
		}
		m.Close()
		b.stop()
	}
}

// A miner may direct its reward to its own address through its username.
func testUsernameOverride(t *testing.T, image string) {
	n := startNode(t, image)
	defaultAddr, own := n.newAddress("bech32"), n.newAddress("bech32m")
	b := startBridge(t, n, defaultAddr)

	cases := []struct{ user, pays string }{
		{own + ".garage", own},
		{own, own},
		{"garage", defaultAddr},
		// A mainnet address is not valid on this network: default applies.
		{"bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4.rig", defaultAddr},
		// One changed character breaks the checksum: default applies.
		{own[:len(own)-1] + flip(own[len(own)-1]), defaultAddr},
	}
	for _, c := range cases {
		m := b.dial(c.user)
		n.fillMempool(1)
		_, block := mineBlock(t, m, n, 1)
		checkCoinbase(t, n, block, c.pays)
		st := b.status()
		if len(st.Pool.Miners) != 1 || st.Pool.Miners[0].OwnPayout != (c.pays == own) || st.Pool.Miners[0].Payout != c.pays {
			t.Errorf("user %q: status page miners: %+v", c.user, st.Pool.Miners)
		}
		m.Close()
		eventually(t, 5*time.Second, "miner to disappear from the status page", func() bool { return len(b.status().Pool.Miners) == 0 })
		// A miner that has left is remembered, the last to leave first.
		eventually(t, 5*time.Second, "miner to be listed as offline", func() bool {
			off := b.status().Offline
			return len(off) > 0 && off[0].Worker == c.user && off[0].Accepted > 0 && off[0].Connections == 1
		})
	}
}

func flip(c byte) string {
	if c == 'q' {
		return "p"
	}
	return "q"
}

// More than 252 transactions changes the transaction-count encoding and
// gives a deep merkle tree.
func testManyTransactions(t *testing.T, image string) {
	n := startNode(t, image)
	const txs = 300
	n.fundManyCoins(txs)
	n.fillMempool(txs)
	payout := n.newAddress("bech32")
	b := startBridge(t, n, payout)
	m := b.dial("bitaxe")
	_, block := mineBlock(t, m, n, txs)
	if block.NTx != txs+1 {
		t.Fatalf("block has %d transactions, want %d", block.NTx, txs+1)
	}
	checkCoinbase(t, n, block, payout)
}

// Several blocks in a row over one connection: each must be accepted, and
// each must be saved to its own file holding exactly the block Core stored.
func testConsecutiveBlocks(t *testing.T, image string) {
	n := startNode(t, image)
	payout := n.newAddress("bech32")
	b := startBridge(t, n, payout)
	m := b.dial("bitaxe")

	const count = 6
	start := n.height()
	var hashes []string
	rolled := false
	for i := 0; i < count; i++ {
		n.fillMempool(i % 3)
		share, block := mineBlock(t, m, n, i%3)
		checkCoinbase(t, n, block, payout)
		hashes = append(hashes, block.Hash)
		if share.VersionBits != 0 {
			rolled = true
			// The rolled version must be what ended up in the chain.
			want := fmt.Sprintf("%08x", uint32(0x20000000)|share.VersionBits)
			if block.VersionHex != want {
				t.Errorf("block version %s, want %s", block.VersionHex, want)
			}
		}
	}
	_ = rolled // version rolling on regtest depends on luck; covered by unit tests
	if got := n.height(); got != start+count {
		t.Fatalf("chain grew by %d blocks, want %d", got-start, count)
	}

	eventually(t, 10*time.Second, "all result files", func() bool {
		st := b.status()
		if len(st.Blocks) != count {
			return false
		}
		for _, blk := range st.Blocks {
			if blk.Status != "accepted" {
				return false
			}
		}
		return true
	})
	files := b.blockFiles()
	if len(files) != count {
		t.Fatalf("%d block files on disk, want %d", len(files), count)
	}
	for _, hash := range hashes {
		saved, ok := files[hash]
		if !ok {
			t.Errorf("no file for block %s", hash)
			continue
		}
		if raw := n.cli("getblock", hash, "0"); raw != saved {
			t.Errorf("saved file for %s differs from the block Bitcoin Core stored", hash)
		}
	}
	results, _ := filepath.Glob(filepath.Join(b.dir, "blocks", "*.result"))
	if len(results) != count {
		t.Errorf("%d result files, want %d", len(results), count)
	}

	// The charts on the status page are fed from the mining history.
	var series struct {
		Steps   int `json:"steps"`
		Workers []struct {
			Worker      string    `json:"worker"`
			Connections uint64    `json:"connections"`
			Hashrate    []float64 `json:"hashrate"`
		} `json:"workers"`
	}
	resp, err := http.Get(b.statusURL + "/api/history?range=24h")
	if err != nil {
		t.Fatal(err)
	}
	err = json.NewDecoder(resp.Body).Decode(&series)
	resp.Body.Close()
	if err != nil || !b.status().History || series.Steps != 96 || len(series.Workers) != 1 ||
		series.Workers[0].Worker != "bitaxe" || len(series.Workers[0].Hashrate) != 96 {
		t.Errorf("history (%v): %+v", err, series)
	}

	// A restart must not lose the history of found blocks.
	m.Close()
	b.stop()
	if !strings.Contains(b.logs.String(), "Shutting down") {
		t.Error("no clean shutdown message in the log")
	}

	// Shutting down writes the mining history: the worker's lifetime totals
	// and its shares per interval.
	var totals map[string]struct {
		Accepted    uint64 `json:"accepted"`
		Connections uint64 `json:"connections"`
	}
	raw, err := os.ReadFile(filepath.Join(b.dir, "stats", "workers.json"))
	if err != nil || json.Unmarshal(raw, &totals) != nil || totals["bitaxe"].Accepted < count || totals["bitaxe"].Connections != 1 {
		t.Errorf("worker totals after shutdown: %s (%v)", raw, err)
	}
	history, _ := filepath.Glob(filepath.Join(b.dir, "stats", "history-*.jsonl"))
	if len(history) == 0 {
		t.Fatal("no history file after shutdown")
	}
	if lines, _ := os.ReadFile(history[0]); !strings.Contains(string(lines), `"worker":"bitaxe"`) {
		t.Errorf("history after shutdown: %s", lines)
	}
}

// When someone else finds a block, miners must be moved to the new tip
// quickly and work on the old tip must be refused.
func testNewBlockFromNetwork(t *testing.T, image string) {
	n := startNode(t, image)
	b := startBridge(t, n, n.newAddress("bech32"))
	m := b.dial("bitaxe")
	old := waitJobOnTip(t, m, n, 0)
	oldShare, ok := m.Mine(old, old.NetworkDifficulty(), 1_000_000)
	if !ok {
		t.Fatal("no share found")
	}

	begin := time.Now()
	n.cli("generatetoaddress", "1", n.newAddress("bech32"))
	newTip := n.tip()
	job, err := m.WaitJob(old.Seq, 10*time.Second, func(j *axesim.Job) bool { return prevHashOf(j) == newTip })
	if err != nil {
		t.Fatalf("miner was not given work on the new tip: %v", err)
	}
	elapsed := time.Since(begin)
	t.Logf("new work reached the miner %s after the block appeared", elapsed.Round(time.Millisecond))
	if !job.CleanJobs {
		t.Error("the job for the new tip did not set clean_jobs")
	}
	if elapsed > 3*time.Second {
		t.Errorf("switching miners to the new tip took %s", elapsed)
	}

	res, err := m.Submit(oldShare)
	if err != nil {
		t.Fatal(err)
	}
	if res.Accepted {
		t.Fatal("a share for the old tip was accepted after a new block arrived")
	}
	if n.tip() != newTip {
		t.Fatal("the stale share changed the chain")
	}
	// Mining continues normally on the new tip.
	mineBlock(t, m, n, 0)
}

// The header is built by the Bitaxe firmware's own C code (see
// firmware/Dockerfile) and must produce a block Core accepts.
func testFirmwareCode(t *testing.T, image string) {
	buildImage(t, "solostratum-e2e-firmware", "firmware")
	n := startNode(t, image)
	payout := n.newAddress("bech32m")
	b := startBridge(t, n, payout)
	n.fillMempool(4)

	for round := 0; round < 3; round++ {
		m := b.dial("firmware")
		job := waitJobOnTip(t, m, n, map[int]int{0: 4, 1: 0, 2: 0}[round])
		args := []string{"run", "--rm", "solostratum-e2e-firmware",
			job.Coinbase1, job.Coinbase2, m.Extranonce1(), "4", job.PrevHash,
			fmt.Sprintf("%08x", job.Version), fmt.Sprintf("%08x", job.NBits), fmt.Sprintf("%08x", job.NTime),
			fmt.Sprintf("%08x", m.VersionMask()),
			strconv.FormatFloat(job.NetworkDifficulty(), 'g', 17, 64),
			strconv.Itoa(round + 1),
		}
		args = append(args, job.Branches...)
		out := strings.Fields(docker(t, args...))
		if len(out) != 6 {
			t.Fatalf("unexpected firmware miner output: %q", out)
		}
		en2, ntime, nonce, versionBits, headerHex := out[0], out[1], out[2], out[3], out[4]

		parse := func(s string) uint32 { v, _ := strconv.ParseUint(s, 16, 32); return uint32(v) }
		share := axesim.Share{JobID: job.ID, Extranonce2: en2, NTime: parse(ntime), Nonce: parse(nonce), VersionBits: parse(versionBits)}
		res, err := m.Submit(share)
		if err != nil {
			t.Fatal(err)
		}
		if !res.Accepted {
			t.Fatalf("share built by firmware code was rejected: %+v", res)
		}
		// The block hash is the hash of the header the firmware assembled.
		header, err := hex.DecodeString(headerHex)
		if err != nil || len(header) != 80 {
			t.Fatalf("bad header from firmware miner: %q", headerHex)
		}
		hash := axesim.DisplayHash(axesim.SHA256d(header))
		eventually(t, 15*time.Second, "Bitcoin Core to accept the firmware-built block "+hash, func() bool { return n.tip() == hash })
		checkCoinbase(t, n, n.block(hash), payout)
		m.Close()
	}
}

var imageBuilds sync.Map // image name -> *sync.Once

func buildImage(t *testing.T, name, dir string) {
	t.Helper()
	once, _ := imageBuilds.LoadOrStore(name, &sync.Once{})
	var buildErr error
	var buildOut []byte
	once.(*sync.Once).Do(func() {
		buildOut, buildErr = exec.Command("docker", "build", "-q", "-t", name, dir).CombinedOutput()
	})
	if buildErr != nil {
		t.Fatalf("building %s: %v\n%s", name, buildErr, buildOut)
	}
	if err := exec.Command("docker", "image", "inspect", name).Run(); err != nil {
		t.Fatalf("image %s is not available (its build failed earlier in this run)", name)
	}
}

// pacedMiner forwards the miner's messages unchanged, at most ten per
// second. On regtest half of all hashes solve a block; an unrestricted CPU
// miner can queue thousands of blocks before Core announces the first new
// tip. Pacing keeps this interoperability test independent of CPU speed.
func pacedMiner(t *testing.T, upstream string) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		miner, err := l.Accept()
		if err != nil {
			return
		}
		defer miner.Close()
		pool, err := (&net.Dialer{}).DialContext(ctx, "tcp", upstream)
		if err != nil {
			return
		}
		stop := context.AfterFunc(ctx, func() { miner.Close(); pool.Close() })
		defer stop()
		replies := make(chan struct{})
		go func() {
			defer close(replies)
			io.Copy(miner, pool)
			miner.Close()
		}()
		defer func() { pool.Close(); miner.Close(); <-replies }()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		reader := bufio.NewReader(miner)
		for {
			line, err := reader.ReadBytes('\n')
			if err != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			if _, err := pool.Write(line); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		l.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("miner relay did not stop")
		}
	})
	return l.Addr().String()
}

// cpuminer is an independent Stratum client. Its blocks must be accepted by
// Core, with only the message rate limited for regtest.
func testCpuminer(t *testing.T, image string) {
	buildImage(t, "solostratum-e2e-cpuminer", "cpuminer")
	n := startNode(t, image)
	payout := n.newAddress("bech32")
	n.fillMempool(3)
	b := startBridge(t, n, payout)
	start := n.height()
	addr := pacedMiner(t, b.stratumAddr)

	name := fmt.Sprintf("%s-cpuminer-%d-%d", label, os.Getpid(), time.Now().UnixNano())
	docker(t, "run", "-d", "--name", name, "--label", label,
		"--network", "host",
		"solostratum-e2e-cpuminer",
		"-a", "sha256d", "-t", "1", "-r", "2", "-R", "1",
		"-o", "stratum+tcp://"+addr,
		"-u", "cpuminer", "-p", "x")
	t.Cleanup(func() {
		if t.Failed() {
			out, _ := exec.Command("docker", "logs", "--tail", "30", name).CombinedOutput()
			t.Logf("cpuminer log tail:\n%s", out)
		}
		exec.Command("docker", "rm", "-f", name).Run()
	})

	eventually(t, 60*time.Second, "cpuminer to find a block that Bitcoin Core accepts", func() bool { return n.height() > start })
	exec.Command("docker", "stop", "-t", "1", name).Run()

	first := n.block(n.cli("getblockhash", strconv.Itoa(start+1)))
	checkCoinbase(t, n, first, payout)
	if first.NTx != 4 {
		t.Errorf("first cpuminer block has %d transactions, want 4", first.NTx)
	}
	st := b.status()
	accepted := 0
	for _, blk := range st.Blocks {
		if blk.Status == "accepted" {
			accepted++
		}
	}
	t.Logf("cpuminer: chain grew by %d blocks; solostratum recorded %d found, %d accepted", n.height()-start, len(st.Blocks), accepted)
	if accepted == 0 {
		t.Error("no block from cpuminer was recorded as accepted")
	}
}

// A block found while the node is unreachable must be kept and delivered
// once the node is back; during a long outage miners are disconnected so
// they can use their fallback pool, and service resumes afterwards.
func testNodeOutage(t *testing.T, image string) {
	n := startNode(t, image)
	payout := n.newAddress("bech32")
	b := startBridge(t, n, payout)
	m := b.dial("bitaxe")
	job := waitJobOnTip(t, m, n, 0)
	startHeight := n.height()

	n.stop()

	share, ok := m.Mine(job, job.NetworkDifficulty(), 1_000_000)
	if !ok {
		t.Fatal("no share found")
	}
	res, err := m.Submit(share)
	if err != nil || !res.Accepted {
		t.Fatalf("share while node is down: %+v, %v", res, err)
	}
	hash := axesim.DisplayHash(share.Hash)
	eventually(t, 10*time.Second, "the block to be saved to disk while the node is down", func() bool {
		_, saved := b.blockFiles()[hash]
		return saved
	})
	if st := b.status(); len(st.Blocks) != 1 || st.Blocks[0].Status != "submitting" {
		t.Fatalf("status while node is down: %+v", st.Blocks)
	}

	n.start()
	eventually(t, 60*time.Second, "the saved block to be delivered after the node came back", func() bool {
		tip, err := n.tryCLI("getbestblockhash")
		return err == nil && tip == hash
	})
	if n.height() != startHeight+1 {
		t.Fatalf("height %d, want %d", n.height(), startHeight+1)
	}
	checkCoinbase(t, n, n.block(hash), payout)
	eventually(t, 10*time.Second, "status 'accepted'", func() bool { return b.status().Blocks[0].Status == "accepted" })

	// Long outage: miners are sent away, then welcomed back.
	n.stop()
	eventually(t, 150*time.Second, "miners to be disconnected during a long node outage", m.Closed)
	if st := b.status(); st.Node.Connected || st.Node.Error == "" {
		t.Errorf("status page does not report the node problem: %+v", st.Node)
	}
	if late, err := axesim.Dial(b.stratumAddr, axesim.Options{User: "late", Timeout: 2 * time.Second}); err == nil {
		late.Close()
		t.Error("a miner could connect while the node is down")
	}
	n.start()
	var back *axesim.Miner
	eventually(t, 60*time.Second, "miners to be able to reconnect", func() bool {
		back, err = axesim.Dial(b.stratumAddr, axesim.Options{User: "bitaxe", Timeout: 2 * time.Second})
		return err == nil
	})
	defer back.Close()
	_, block := mineBlock(t, back, n, 0)
	checkCoinbase(t, n, block, payout)
}

// Wrong settings must be refused with a clear message instead of mining
// into the void.
func testStartupChecks(t *testing.T, image string) {
	n := startNode(t, image)
	good := n.newAddress("bech32")

	check := func(payout string, extra ...string) (string, error) {
		conf := writeConfig(t, t.TempDir(), n, payout, freePort(t), freePort(t), extra...)
		out, err := exec.Command(binary, "-config", conf, "-check").CombinedOutput()
		return string(out), err
	}
	expectFailure := func(name, wantText, payout string, extra ...string) {
		out, err := check(payout, extra...)
		if err == nil {
			t.Errorf("%s: start-up check passed, want failure. Output:\n%s", name, out)
		} else if !strings.Contains(out, wantText) {
			t.Errorf("%s: output does not mention %q:\n%s", name, wantText, out)
		}
	}

	if out, err := check(good); err != nil || !strings.Contains(out, "Self-test passed") {
		t.Fatalf("check with good settings failed: %v\n%s", err, out)
	}
	expectFailure("mainnet address", "not usable on the node's network", "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4")
	expectFailure("mistyped address", "checksum mismatch", good[:len(good)-1]+flip(good[len(good)-1]))

	// Wrong password: rewrite the file with a bad one.
	dir := t.TempDir()
	conf := writeConfig(t, dir, n, good, freePort(t), freePort(t))
	raw, _ := os.ReadFile(conf)
	os.WriteFile(conf, bytes.Replace(raw, []byte(rpcPass), []byte("wrong"), 1), 0o600)
	if out, err := exec.Command(binary, "-config", conf, "-check").CombinedOutput(); err == nil || !strings.Contains(string(out), "rejected the login") {
		t.Errorf("wrong password: %v\n%s", err, out)
	}

	// Nothing listening at node_url.
	os.WriteFile(conf, bytes.Replace(raw, []byte(strconv.Itoa(n.rpcPort)), []byte(strconv.Itoa(freePort(t))), 1), 0o600)
	if out, err := exec.Command(binary, "-config", conf, "-check").CombinedOutput(); err == nil || !strings.Contains(string(out), "Cannot reach the node") {
		t.Errorf("unreachable node: %v\n%s", err, out)
	}

	// The status page itself.
	b := startBridge(t, n, good)

	// The Docker health check reports a running, working instance as
	// healthy and anything else as unhealthy.
	runningConf := filepath.Join(b.dir, "solostratum.conf")
	if out, err := exec.Command(binary, "-config", runningConf, "-healthcheck").CombinedOutput(); err != nil {
		t.Errorf("health check of a running instance failed: %v\n%s", err, out)
	}
	if out, err := exec.Command(binary, "-config", conf, "-healthcheck").CombinedOutput(); err == nil {
		t.Errorf("health check passed although nothing is running with those settings:\n%s", out)
	}
	resp, err := http.Get(b.statusURL + "/")
	if err != nil {
		t.Fatal(err)
	}
	page, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !bytes.Contains(page, []byte("<title>solostratum</title>")) {
		t.Errorf("status page: HTTP %d", resp.StatusCode)
	}
	if bytes.Contains(page, []byte("http://")) || bytes.Contains(page, []byte("https://")) {
		t.Error("status page references an external resource")
	}
	if resp, err := http.Post(b.statusURL+"/api/status", "application/json", strings.NewReader("{}")); err == nil {
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("POST to the status API returned %d, want 405", resp.StatusCode)
		}
		resp.Body.Close()
	}
}
