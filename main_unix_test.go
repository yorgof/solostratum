//go:build !windows

package main

import (
	"bytes"
	"context"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yorgof/solostratum/internal/logtest"
	"github.com/yorgof/solostratum/internal/stats"
)

// helperMainArgs, set in the environment, makes TestHelperMain run main with
// these arguments, so that a test can run the real program in a subprocess.
const helperMainArgs = "SOLOSTRATUM_TEST_MAIN_ARGS"

// TestHelperMain is not a test of its own: run in a subprocess with
// helperMainArgs set, it runs main with them and exits.
func TestHelperMain(t *testing.T) {
	args := os.Getenv(helperMainArgs)
	if args == "" {
		return
	}
	os.Args = append([]string{"solostratum"}, strings.Fields(args)...)
	flag.CommandLine = flag.NewFlagSet("solostratum", flag.ExitOnError)
	main()
	os.Exit(0)
}

// A standard output whose reader has gone must not end the program: Go dies
// of SIGPIPE on such a write unless the signal is ignored, as main does.
func TestClosedOutputDoesNotEndTheProgram(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	defer w.Close()
	var stderr bytes.Buffer
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperMain$")
	cmd.Env = append(os.Environ(), helperMainArgs+"=-version", "GORACE=atexit_sleep_ms=0")
	cmd.Stdout, cmd.Stderr = w, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("solostratum -version with its output closed: %v\n%s", err, stderr.String())
	}
}

// hangingStatsDir makes a statistics folder whose best-share file is a FIFO:
// reading it hangs until something writes to it, as reading a folder on a
// mount that has hung would.
func hangingStatsDir(t *testing.T) (dir, fifo string) {
	t.Helper()
	dir = t.TempDir()
	fifo = filepath.Join(dir, "best-shares.json")
	if err := syscall.Mkfifo(fifo, 0o644); err != nil {
		t.Fatal(err)
	}
	return dir, fifo
}

// release lets a reader that hangs on fifo go on with an empty list, which
// it reads without changing the folder that is about to be removed. It runs
// in the background, as opening a FIFO for writing waits for a reader.
func release(fifo string) {
	go func() {
		if f, err := os.OpenFile(fifo, os.O_WRONLY, 0); err == nil {
			f.WriteString("[]\n")
			f.Close()
		}
	}()
}

// The real folder that hangs is given up on after the time limit.
func TestOpenStatisticsGivesUpOnAFolderThatHangs(t *testing.T) {
	logtest.Capture(t)
	dir, fifo := hangingStatsDir(t)
	t.Cleanup(func() { release(fifo) })
	done := make(chan error, 1)
	go func() {
		_, err := openStatistics(context.Background(), func() (*stats.Store, error) { return stats.Open(dir) }, 100*time.Millisecond)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "did not answer within") {
			t.Fatalf("error %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("opening a folder that hangs did not give up")
	}
}

// A statistics folder that hangs while solostratum starts must not keep it
// from stopping.
func TestStartStopsWhileTheStatisticsFolderHangs(t *testing.T) {
	logtest.Capture(t)
	dir, fifo := hangingStatsDir(t)
	node, _ := startupNode(t, "regtest")
	path := settingsFile(t, node.URL, map[string]string{"stats_dir": dir, "status_listen": ""})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(ctx, path, false) }()

	// Once this open returns, the statistics are reading the FIFO, and they
	// keep waiting for data until it is closed.
	opened := make(chan *os.File, 1)
	go func() {
		if f, err := os.OpenFile(fifo, os.O_WRONLY, 0); err == nil {
			opened <- f
		}
	}()
	var writer *os.File
	select {
	case writer = <-opened:
		t.Cleanup(func() {
			writer.WriteString("[]\n")
			writer.Close()
		})
	case err := <-done:
		t.Fatalf("run returned before reading the statistics: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("the statistics were never read")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "stopped while waiting for stats_dir") {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a statistics folder that hangs kept solostratum from stopping")
	}
}
