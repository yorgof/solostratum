// Package logtest captures what the program logs, for tests.
package logtest

import (
	"bytes"
	"log"
	"sync"
	"testing"
)

// Buffer collects log output from several goroutines.
type Buffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *Buffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *Buffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// Capture returns what the program logs until the test ends. The log then
// goes back to where it went before.
func Capture(t testing.TB) *Buffer {
	t.Helper()
	out := &Buffer{}
	prev := log.Writer()
	log.SetOutput(out)
	t.Cleanup(func() { log.SetOutput(prev) })
	return out
}
