// Package status serves the read-only status web page.
package status

import (
	"context"
	_ "embed"
	"encoding/json"
	"net"
	"net/http"
	"time"

	"github.com/yorgof/solostratum/internal/blocks"
	"github.com/yorgof/solostratum/internal/stratum"
	"github.com/yorgof/solostratum/internal/work"
)

//go:embed index.html
var page []byte

// Report is the JSON document behind the page.
type Report struct {
	Version       string          `json:"version"`
	Now           time.Time       `json:"now"`
	PayoutAddress string          `json:"payoutAddress"`
	StratumPort   string          `json:"stratumPort"`
	Node          work.NodeState  `json:"node"`
	Pool          stratum.Status  `json:"pool"`
	Blocks        []blocks.Record `json:"blocks"`
}

// Sources are the functions the page reads its data from.
type Sources struct {
	Version       string
	PayoutAddress string
	StratumPort   string
	Node          func() work.NodeState
	Pool          func() stratum.Status
	Blocks        func() []blocks.Record
}

// Handler returns the HTTP handler for the status page.
func Handler(src Sources) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "read only", http.StatusMethodNotAllowed)
			return
		}
		found := src.Blocks()
		if found == nil {
			found = []blocks.Record{}
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		json.NewEncoder(w).Encode(Report{
			Version: src.Version, Now: time.Now(), PayoutAddress: src.PayoutAddress, StratumPort: src.StratumPort,
			Node: src.Node(), Pool: src.Pool(), Blocks: found,
		})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "read only", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'; img-src data:")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Write(page)
	})
	return mux
}

// Serve runs the status page on l until ctx is cancelled.
func Serve(ctx context.Context, l net.Listener, src Sources) error {
	srv := &http.Server{
		Handler:           Handler(src),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Shutdown(shutdown)
	}()
	if err := srv.Serve(l); err != http.ErrServerClosed {
		return err
	}
	return nil
}
