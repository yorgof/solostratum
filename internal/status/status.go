// Package status serves the read-only status web page.
package status

import (
	"context"
	_ "embed"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/yorgof/solostratum/internal/blocks"
	"github.com/yorgof/solostratum/internal/stats"
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
	Offline       []stats.Worker  `json:"offline"` // known workers that are not connected
	History       bool            `json:"history"` // whether /api/history is available
}

// Sources are the functions the page reads its data from.
type Sources struct {
	Version       string
	PayoutAddress string
	StratumPort   string
	Node          func() work.NodeState
	Pool          func() stratum.Status
	Blocks        func() []blocks.Record
	// Offline and History come from the statistics. Both are nil when no
	// statistics are kept.
	Offline func() []stats.Worker
	History func(now time.Time, span, step time.Duration) (stats.Series, error)
}

// ranges are the periods a history chart can cover, each with the width of
// one point. All are whole multiples of stats.Interval.
var ranges = map[string]struct{ span, step time.Duration }{
	"24h": {24 * time.Hour, 15 * time.Minute},
	"7d":  {7 * 24 * time.Hour, time.Hour},
	"30d": {30 * 24 * time.Hour, 4 * time.Hour},
}

// readOnly refuses everything but GET and HEAD.
func readOnly(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "read only", http.StatusMethodNotAllowed)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(v)
}

// Handler returns the HTTP handler for the status page.
func Handler(src Sources) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		if !readOnly(w, r) {
			return
		}
		found := src.Blocks()
		if found == nil {
			found = []blocks.Record{}
		}
		offline := []stats.Worker{}
		if src.Offline != nil {
			offline = src.Offline()
		}
		writeJSON(w, Report{
			Version: src.Version, Now: time.Now(), PayoutAddress: src.PayoutAddress, StratumPort: src.StratumPort,
			Node: src.Node(), Pool: src.Pool(), Blocks: found, Offline: offline, History: src.History != nil,
		})
	})
	mux.HandleFunc("/api/history", func(w http.ResponseWriter, r *http.Request) {
		if !readOnly(w, r) {
			return
		}
		if src.History == nil {
			http.Error(w, "no history is kept: stats_dir is empty", http.StatusNotFound)
			return
		}
		period, ok := ranges[r.URL.Query().Get("range")]
		if !ok {
			http.Error(w, "range must be 24h, 7d or 30d", http.StatusBadRequest)
			return
		}
		series, err := src.History(time.Now(), period.span, period.step)
		if err != nil {
			log.Printf("Reading the mining history failed: %v", err)
			http.Error(w, "the history could not be read", http.StatusInternalServerError)
			return
		}
		writeJSON(w, series)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		if !readOnly(w, r) {
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
