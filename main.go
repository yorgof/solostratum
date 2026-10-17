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
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/yorgof/solostratum/internal/blocks"
	"github.com/yorgof/solostratum/internal/btc"
	"github.com/yorgof/solostratum/internal/config"
	"github.com/yorgof/solostratum/internal/guard"
	"github.com/yorgof/solostratum/internal/rpc"
	"github.com/yorgof/solostratum/internal/stats"
	"github.com/yorgof/solostratum/internal/status"
	"github.com/yorgof/solostratum/internal/stratum"
	"github.com/yorgof/solostratum/internal/work"
)

// version is overridden at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	// An output that goes away, such as a closed terminal or a log collector
	// that has stopped, must not end the program: Go exits on a write to a
	// closed standard output or error unless SIGPIPE is ignored. The write
	// then just fails.
	signal.Ignore(syscall.SIGPIPE)
	// Logging must never hold up the program, whatever happens to its output
	// (see guard.LogOutput).
	logOutput = guard.NewLogOutput(os.Stderr)
	log.SetOutput(logOutput)
	defer logOutput.Close(logCloseTimeout)

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

// logOutput takes everything the program logs. main sets it up.
var logOutput *guard.LogOutput

// logCloseTimeout is how long the end of the program waits for the log
// output to take the last messages.
const logCloseTimeout = 2 * time.Second

func fatal(format string, args ...any) {
	msg := fmt.Sprintf("\nERROR: "+format+"\n", args...)
	if logOutput == nil {
		fmt.Fprint(os.Stderr, msg)
		os.Exit(1)
	}
	// After the messages logged before it, and without waiting forever for
	// an output that takes nothing.
	logOutput.Write([]byte(msg))
	logOutput.Close(logCloseTimeout)
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
	// the last run is resumed at once. However run ends, including a failed
	// start, the node gets its answer first. -check does not mine, and leaves
	// those blocks to the solostratum that may be delivering them right now.
	defer finishDeliveries(store, cfg.BlocksDir)
	if !checkOnly {
		store.Resume()
	}
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
		opened, err := openStatistics(ctx, func() (*stats.Store, error) { return stats.Open(cfg.StatsDir) }, statsOpenTimeout)
		switch {
		case err == nil:
			history = opened
			server.SetRecorder(history)
		case ctx.Err() != nil:
			return fmt.Errorf("stopped while waiting for stats_dir %s", cfg.StatsDir)
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
		flushLog() // the log comes first
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
			log.Printf("WARNING: cannot serve the status page on %s: %v\n  Mining is not affected. solostratum tries again every %s, and the status page, with the Docker health check that reads it, starts as soon as it can. To use another port, change status_listen in the settings file and restart.", cfg.StatusListen, err, statusRetryEvery)
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
			guarded("the statistics writer", func() { history.Run(ctx) })
		}()
	}
	if cfg.StatusListen != "" {
		src := status.Sources{
			Version: version, PayoutAddress: cfg.PayoutAddress, StratumPort: stratumPort,
			Node: manager.State, Pool: server.Status, Miners: server.Miners, Blocks: store.Records,
		}
		if history != nil {
			src.Offline, src.History, src.BestShares = history.Offline, history.Series, history.BestShares
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			guarded("the status page", func() { serveStatusPage(ctx, statusListener, cfg.StatusListen, src, statusRetryEvery) })
		}()
		if statusListener != nil {
			logStatusPage(cfg.StatusListen, statusListener)
		}
	}
	log.Printf("Ready. Point your miners at stratum+tcp://%s:%s", displayHost(cfg.StratumListen), stratumPort)

	<-ctx.Done()
	log.Printf("Shutting down.")
	// A found block that is not on disk yet goes to the log in full before
	// anything else, as the next Ctrl-C can quit (see finishDeliveries).
	if store.LogUnsaved() > 0 {
		flushLog()
	}
	// Give the default signal behaviour back at once, so a second Ctrl-C
	// ends the program even if a miner's last share or a disk write hangs.
	// A found block that is saved by then and was not answered is delivered
	// again at the next start, if that comes soon enough.
	restoreSignals()
	wg.Wait()
	if history != nil {
		// After the writer's own last write, and after the last shares and
		// disconnections the miners reported while the server drained.
		saveStatistics(statsSaveTimeout, func() { history.Flush(time.Now()) }, writerDone, server.StatisticsDone())
	}
	return nil // finishDeliveries runs now
}

// statusRetryEvery is how often the status page tries again for a port that
// is taken, as by an old copy of solostratum that has not quite stopped.
const statusRetryEvery = 30 * time.Second

// serveStatusPage serves the status page on ln until ctx ends. Without ln, as
// when its port was taken at the start, it first tries to listen on addr
// every retry, until it can. A reason it cannot that differs from the last
// one is logged.
func serveStatusPage(ctx context.Context, ln net.Listener, addr string, src status.Sources, retry time.Duration) {
	var last string // why the last try failed
	for ln == nil {
		select {
		case <-ctx.Done():
			return
		case <-time.After(retry):
		}
		var err error
		if ln, err = net.Listen("tcp", addr); err == nil {
			logStatusPage(addr, ln)
		} else if err.Error() != last {
			last = err.Error()
			log.Printf("WARNING: still cannot serve the status page on %s: %v. Trying again every %s.", addr, err, retry)
		}
	}
	if err := status.Serve(ctx, ln, src); err != nil {
		log.Printf("Status page stopped: %v", err)
	}
}

func logStatusPage(addr string, ln net.Listener) {
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	log.Printf("Status page: http://%s:%s", displayHost(addr), port)
}

// finishDeliveries waits until the node has answered for every found block
// that is still being delivered. It runs however run ends.
func finishDeliveries(store *blocks.Store, dir string) {
	// A second Ctrl-C must end the program, whatever the first one did. Any
	// block that exists only in memory, such as one a miner handed over
	// while the server was letting the miners go, is in the log first.
	restoreSignals()
	unsaved := store.LogUnsaved()
	if n := store.Pending(); n > 0 {
		log.Printf("Still delivering %d found block(s) to the node.", n)
	}
	switch {
	case unsaved > 0:
		log.Printf("WARNING: %d found block(s) are not saved in %s. Quitting now loses them, unless the node has them already: the log has each in full, ready to be submitted by hand. Press Ctrl-C to quit anyway.", unsaved, dir)
		flushLog() // those lines must be out before anyone quits on reading this
	case store.Pending() > 0:
		log.Printf("Press Ctrl-C to quit anyway: they are saved in %s, and delivered again if solostratum starts again within 30 minutes of finding them.", dir)
	}
	store.Wait()
}

// flushLog waits, within a limit, until what was logged so far is out.
func flushLog() {
	if logOutput != nil {
		logOutput.Flush(logCloseTimeout)
	}
}

// statsSaveTimeout is how long the last write of the statistics may take. They
// are not worth holding up the delivery of a found block or the end of the
// program, so a disk that has hung costs the last minutes of them instead.
const statsSaveTimeout = 5 * time.Second

// statsOpenTimeout is how long the start waits for the statistics folder. A
// folder that does not answer, such as one on a mount that has hung, must not
// keep the miners waiting.
const statsOpenTimeout = 10 * time.Second

// openStatistics runs open, which opens the statistics, for at most timeout or
// until ctx ends. A result that comes later is ignored.
func openStatistics(ctx context.Context, open func() (*stats.Store, error), timeout time.Duration) (*stats.Store, error) {
	var store *stats.Store
	var err error
	select {
	case failed := <-guard.Go("opening the statistics", func() { store, err = open() }):
		if failed != nil {
			return nil, failed
		}
		return store, err
	case <-time.After(timeout):
		return nil, fmt.Errorf("the folder did not answer within %s", timeout)
	case <-ctx.Done():
		return nil, errors.New("stopped while waiting for the folder")
	}
}

// saveStatistics runs flush, the last write of the statistics, once every
// channel in after is closed, and gives all of it at most timeout.
func saveStatistics(timeout time.Duration, flush func(), after ...<-chan struct{}) {
	saved := guard.Go("saving the statistics", func() {
		for _, ch := range after {
			<-ch
		}
		flush()
	})
	select {
	case err := <-saved:
		if err != nil {
			log.Printf("WARNING: the statistics could not be saved (%v). The last minutes of them are lost. Mining and found blocks are not affected.", err)
		}
	case <-time.After(timeout):
		log.Printf("WARNING: the statistics could not be saved within %s. Is the disk stuck? The last minutes of them are lost. Mining and found blocks are not affected.", timeout)
	}
}

// guarded runs f, a part of the program that mining does not depend on. A
// panic in it is logged and stops f only.
func guarded(what string, f func()) {
	if guard.Run(what, f) != nil {
		log.Printf("WARNING: %s has stopped until solostratum is restarted. Mining is not affected.", what)
	}
}

// runHealthcheck asks the solostratum instance that uses the same settings
// file, through /api/health on its status page, whether it has current work
// from the node.
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
	resp, err := client.Get("http://" + net.JoinHostPort(host, port) + "/api/health")
	if err != nil {
		return fmt.Errorf("solostratum is not answering: %v", err)
	}
	defer resp.Body.Close()
	var health status.Health
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		return fmt.Errorf("unreadable status answer: %v", err)
	}
	if !health.Node.Connected {
		return fmt.Errorf("node problem: %s", health.Node.Error)
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
