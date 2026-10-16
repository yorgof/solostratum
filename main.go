// solostratum is a solo-mining Stratum server: it connects to your own
// Bitcoin Core node and gives work to miners such as the Bitaxe. Any block
// they find pays the full reward to your address.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/yorgof/solostratum/internal/blocks"
	"github.com/yorgof/solostratum/internal/btc"
	"github.com/yorgof/solostratum/internal/config"
	"github.com/yorgof/solostratum/internal/rpc"
	"github.com/yorgof/solostratum/internal/stats"
	"github.com/yorgof/solostratum/internal/status"
	"github.com/yorgof/solostratum/internal/stratum"
	"github.com/yorgof/solostratum/internal/work"
)

// version is overridden at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	configPath := flag.String("config", "", "path to the settings file (default: "+config.FileName+" next to the program)")
	check := flag.Bool("check", false, "check the settings and the node, then exit")
	healthcheck := flag.Bool("healthcheck", false, "ask the running solostratum whether it is healthy, then exit (used by Docker)")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println("solostratum", version)
		return
	}
	if flag.NArg() > 0 {
		fatal("Unexpected argument %q. Run with -help to see the options.", flag.Arg(0))
	}
	log.SetFlags(log.Ldate | log.Ltime)
	if *healthcheck {
		if err := runHealthcheck(*configPath); err != nil {
			fatal("%v", err)
		}
		fmt.Println("healthy")
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	restoreSignals = stop
	if err := run(ctx, *configPath, *check); err != nil {
		fatal("%v", err)
	}
}

// restoreSignals stops intercepting Ctrl-C and SIGTERM.
var restoreSignals = func() {}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "\nERROR: "+format+"\n", args...)
	os.Exit(1)
}

func run(ctx context.Context, configPath string, checkOnly bool) error {
	explicit := configPath != ""
	if !explicit {
		configPath = config.DefaultPath()
	}
	cfg, err := config.Load(configPath)
	if errors.Is(err, fs.ErrNotExist) {
		if explicit {
			return fmt.Errorf("settings file not found: %s", configPath)
		}
		return createConfig(configPath)
	}
	if err != nil {
		return fmt.Errorf("problem in settings file %s:\n  %v", configPath, err)
	}
	log.Printf("solostratum %s, settings from %s", version, configPath)

	node := rpc.New(cfg.NodeURL, cfg.NodeUser, cfg.NodePassword, cfg.NodeCookieFile)
	info, err := waitForNode(ctx, node, cfg.NodeURL, checkOnly)
	if err != nil {
		return err
	}
	network, ok := btc.NetworkByChain(info.Chain)
	if !ok {
		return fmt.Errorf("the node is on an unknown network %q", info.Chain)
	}
	if info.Chain == "signet" {
		return errors.New("the node is on signet, where blocks must be signed by the network's operators and cannot be mined this way. Use mainnet, testnet4 or regtest")
	}
	payoutScript, err := btc.AddressToScript(cfg.PayoutAddress, network)
	if err != nil {
		return fmt.Errorf("payout_address %q is not usable on the node's network (%s): %v", cfg.PayoutAddress, info.Chain, err)
	}
	log.Printf("Node %s is on %s at block %d. Rewards go to %s", cfg.NodeURL, chainName(info.Chain), info.Blocks, cfg.PayoutAddress)

	// The store may deliver a block left over from the last run before the
	// manager exists, hence the indirection.
	var managerRef atomic.Pointer[work.Manager]
	store, err := blocks.Open(cfg.BlocksDir, node, func() {
		if m := managerRef.Load(); m != nil {
			m.Refresh()
		}
	})
	if err != nil {
		return fmt.Errorf("cannot use blocks_dir %s: %v", cfg.BlocksDir, err)
	}
	// From here on a block may be on its way to the node: one left over from
	// the last run is resumed as soon as the store opens. However run ends,
	// including a failed start, the node gets its answer first.
	defer finishDeliveries(store, cfg.BlocksDir)
	server := stratum.NewServer(stratum.Options{
		Network: network, DefaultAddress: cfg.PayoutAddress, DefaultScript: payoutScript,
		StartDifficulty: cfg.StartDifficulty, MinDifficulty: cfg.MinDifficulty,
	}, nil, store)
	manager := work.NewManager(node, info.Chain, cfg.CoinbaseTag, server.Broadcast)
	server.SetJobs(manager)
	managerRef.Store(manager)
	// The statistics are not worth stopping the mining for. Only -check, which
	// does not mine, treats a folder it cannot use as an error.
	var history *stats.Store
	if cfg.StatsDir != "" {
		opened, err := stats.Open(cfg.StatsDir)
		switch {
		case err == nil:
			history = opened
			server.SetRecorder(history)
		case checkOnly:
			return fmt.Errorf("cannot use stats_dir %s: %v\n  (change stats_dir in the settings file, or leave it empty to keep no history)", cfg.StatsDir, err)
		default:
			log.Printf("WARNING: cannot use stats_dir %s: %v\n  No statistics are kept during this run. Mining is not affected. Fix the folder, or change stats_dir in the settings file, and restart to get them back.", cfg.StatsDir, err)
		}
	}

	if err := selfTest(ctx, manager, payoutScript, checkOnly); err != nil {
		return err
	}
	log.Printf("Self-test passed: the node accepts the blocks this program builds.")
	if checkOnly {
		fmt.Println("Everything looks good.")
		return nil
	}

	if err := server.Listen(cfg.StratumListen); err != nil {
		return fmt.Errorf("cannot listen for miners on %s: %v\n  (is another program, or another copy of solostratum, using that port?)", cfg.StratumListen, err)
	}
	_, stratumPort, _ := net.SplitHostPort(server.Addr().String())
	var statusListener net.Listener
	if cfg.StatusListen != "" {
		if statusListener, err = net.Listen("tcp", cfg.StatusListen); err != nil {
			log.Printf("WARNING: cannot serve the status page on %s: %v\n  There is no status page during this run. Mining is not affected. Change status_listen in the settings file, or free that port, and restart to get it back.", cfg.StatusListen, err)
			statusListener = nil
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); manager.Run(ctx) }()
	go func() { defer wg.Done(); server.Serve(ctx) }()
	// The statistics writer is not in wg: the end of the program does not wait
	// for it beyond a limit (see saveStatistics).
	var writerDone chan struct{}
	if history != nil {
		writerDone = make(chan struct{})
		go func() {
			defer close(writerDone)
			guarded("The statistics writer", func() { history.Run(ctx) })
		}()
	}
	if statusListener != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			guarded("The status page", func() {
				src := status.Sources{
					Version: version, PayoutAddress: cfg.PayoutAddress, StratumPort: stratumPort,
					Node: manager.State, Pool: server.Status, Blocks: store.Records,
				}
				if history != nil {
					src.Offline, src.History, src.BestShares = history.Offline, history.Series, history.BestShares
				}
				err := status.Serve(ctx, statusListener, src)
				if err != nil {
					log.Printf("Status page stopped: %v", err)
				}
			})
		}()
		_, statusPort, _ := net.SplitHostPort(statusListener.Addr().String())
		log.Printf("Status page: http://%s:%s", displayHost(cfg.StatusListen), statusPort)
	}
	log.Printf("Ready. Point your miners at stratum+tcp://%s:%s", displayHost(cfg.StratumListen), stratumPort)

	<-ctx.Done()
	log.Printf("Shutting down.")
	// Give the default signal behaviour back at once, so a second Ctrl-C
	// ends the program even if a miner's last share or a disk write hangs.
	// A found block is on disk by then, and a recent one is delivered
	// again at the next start.
	restoreSignals()
	wg.Wait()
	if history != nil {
		saveStatistics(writerDone, func() { history.Flush(time.Now()) }, statsSaveTimeout)
	}
	return nil // finishDeliveries runs now
}

// finishDeliveries waits until the node has answered for every found block
// that is still being delivered. It runs however run ends.
func finishDeliveries(store *blocks.Store, dir string) {
	// A second Ctrl-C must end the program, whatever the first one did.
	restoreSignals()
	if n := store.Pending(); n > 0 {
		log.Printf("Still delivering %d found block(s) to the node. They are saved in %s. Press Ctrl-C to quit anyway.", n, dir)
	}
	store.Wait()
}

// statsSaveTimeout is how long the last write of the statistics may take. They
// are not worth holding up the delivery of a found block or the end of the
// program, so a disk that has hung costs the last minutes of them instead.
const statsSaveTimeout = 5 * time.Second

// saveStatistics runs the last write of the statistics, after the writer's own
// last write, for at most timeout. Miners recorded their last shares and their
// disconnection while the server drained, after the writer's final write, so
// that write alone is not enough.
func saveStatistics(writerDone <-chan struct{}, flush func(), timeout time.Duration) {
	saved := make(chan struct{})
	go func() {
		defer close(saved)
		guarded("The last write of the statistics", func() {
			<-writerDone
			flush()
		})
	}()
	select {
	case <-saved:
	case <-time.After(timeout):
		log.Printf("WARNING: the statistics could not be saved within %s. Is the disk stuck? The last minutes of them are lost. Mining and found blocks are not affected.", timeout)
	}
}

// guarded runs f, which mining does not depend on, so that a panic in it is
// logged and ends only f. Left alone, a panic in any goroutine ends the whole
// program: every miner, and any block that is being delivered.
func guarded(name string, f func()) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("WARNING: %s failed (%v) and has stopped. Mining is not affected.\n%s", name, r, debug.Stack())
		}
	}()
	f()
}

// runHealthcheck asks the solostratum instance that uses the same settings
// file, through its status page, whether it has current work from the node.
func runHealthcheck(configPath string) error {
	if configPath == "" {
		configPath = config.DefaultPath()
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("problem in settings file %s:\n  %v", configPath, err)
	}
	if cfg.StatusListen == "" {
		return errors.New("the health check needs the status page, but status_listen is empty")
	}
	host, port, _ := net.SplitHostPort(cfg.StatusListen)
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = "127.0.0.1"
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("http://" + net.JoinHostPort(host, port) + "/api/status")
	if err != nil {
		return fmt.Errorf("solostratum is not answering: %v", err)
	}
	defer resp.Body.Close()
	var report status.Report
	if err := json.NewDecoder(resp.Body).Decode(&report); err != nil {
		return fmt.Errorf("unreadable status answer: %v", err)
	}
	if !report.Node.Connected {
		return fmt.Errorf("node problem: %s", report.Node.Error)
	}
	return nil
}

// createConfig writes a commented settings file for the user to fill in.
func createConfig(path string) error {
	if err := os.WriteFile(path, []byte(config.Example), 0o600); err != nil {
		return fmt.Errorf("no settings file found, and one could not be created at %s: %v", path, err)
	}
	fmt.Printf(`Welcome to solostratum.

A settings file was created for you:

  %s

Open it in a text editor, fill in your node's address and login and your
Bitcoin payout address, then start solostratum again.
`, path)
	return nil
}

// waitForNode returns once the node answers and has finished syncing. Wrong
// credentials are fatal; a node that is down or still syncing is waited for.
func waitForNode(ctx context.Context, node *rpc.Client, url string, checkOnly bool) (rpc.ChainInfo, error) {
	var last string
	for {
		info, err := node.GetBlockchainInfo(ctx)
		var msg string
		switch {
		case err == nil && !info.InitialBlockDownload:
			return info, nil
		case err == nil:
			msg = fmt.Sprintf("The node is still downloading the blockchain (%.1f%% done, block %d of %d). Mining starts when it has caught up.",
				info.VerificationProgress*100, info.Blocks, info.Headers)
		default:
			// A rejected login or a missing cookie file will not fix itself;
			// anything else (node starting, busy, restarting) is waited out.
			var httpErr *rpc.HTTPError
			loginProblem := errors.As(err, &httpErr) && (httpErr.Status == 401 || httpErr.Status == 403)
			if loginProblem || errors.Is(err, fs.ErrNotExist) {
				return info, fmt.Errorf("cannot use the node at %s: %s", url, rpc.Explain(err))
			}
			msg = fmt.Sprintf("Cannot reach the node at %s: %s.", url, rpc.Explain(err))
		}
		if checkOnly {
			return info, errors.New(msg)
		}
		if msg != last {
			log.Print(msg + " Waiting...")
			last = msg
		}
		select {
		case <-ctx.Done():
			return info, errors.New("stopped while waiting for the node")
		case <-time.After(5 * time.Second):
		}
	}
}

// selfTest retries while the node is temporarily unable to produce templates
// (for example while it has no peers yet).
func selfTest(ctx context.Context, manager *work.Manager, payoutScript []byte, checkOnly bool) error {
	var last string
	for {
		err := manager.SelfTest(ctx, payoutScript)
		if err == nil {
			return nil
		}
		var fatalErr *work.SelfTestError
		if errors.As(err, &fatalErr) || checkOnly {
			return err
		}
		if msg := err.Error(); msg != last {
			log.Printf("Not ready yet: %s. Waiting...", msg)
			last = msg
		}
		select {
		case <-ctx.Done():
			return errors.New("stopped while waiting for the node")
		case <-time.After(5 * time.Second):
		}
	}
}

func chainName(chain string) string {
	if chain == "main" {
		return "mainnet"
	}
	return chain
}

// displayHost turns a listen address into something a person can type into
// a miner: the wildcard address becomes this computer's LAN address.
func displayHost(listen string) string {
	host, _, _ := net.SplitHostPort(listen)
	if ip := net.ParseIP(host); host != "" && (ip == nil || !ip.IsUnspecified()) {
		return host
	}
	// Ask the routing table which local address is used for outside
	// traffic. No packet is sent for a UDP "connection".
	if conn, err := net.Dial("udp", "192.0.2.1:9"); err == nil {
		defer conn.Close()
		if addr, ok := conn.LocalAddr().(*net.UDPAddr); ok && !addr.IP.IsLoopback() {
			return addr.IP.String()
		}
	}
	return "<this computer's IP address>"
}
