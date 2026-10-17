//go:build !windows

package blocks

import (
	"io"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yorgof/solostratum/internal/logtest"
)

// A disk that stops while the node's answer is written next to the block,
// shown by a FIFO in place of the result file, holds up nothing: the new work
// after the accepted block has been asked for, and Wait returns.
func TestStuckResultFileHoldsUpNothing(t *testing.T) {
	logtest.Capture(t)
	var refreshed int
	s, err := Open(t.TempDir(), &fakeNode{}, func() { refreshed++ })
	if err != nil {
		t.Fatal(err)
	}
	s.saveWait, s.fileWait = 50*time.Millisecond, 50*time.Millisecond
	fifo := make(chan string, 1)
	create := s.create
	s.create = func(path string) (*os.File, error) {
		f, err := create(path)
		if err == nil {
			result := strings.TrimSuffix(path, ".hex") + ".result"
			if err := syscall.Mkfifo(result, 0o644); err != nil {
				t.Error(err)
			}
			fifo <- result
		}
		return f, err
	}
	s.Found(100, "aa", []byte{1}, "rig")
	within(t, "finishing the delivery", s.Wait)
	if rec := s.Records()[0]; rec.Status != "accepted" || refreshed != 1 {
		t.Fatalf("record %+v, new work asked for %d times", rec, refreshed)
	}
	// Reading the FIFO lets the stuck write finish, and nothing is left behind.
	result := <-fifo
	within(t, "reading the result", func() {
		f, err := os.Open(result)
		if err != nil {
			t.Error(err)
			return
		}
		defer f.Close()
		if answer, _ := io.ReadAll(f); strings.TrimSpace(string(answer)) != "accepted" {
			t.Errorf("result %q", answer)
		}
	})
	within(t, "the file's goroutine to finish", s.files.Wait)
}
