package status

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yorgof/solostratum/internal/blocks"
	"github.com/yorgof/solostratum/internal/stats"
	"github.com/yorgof/solostratum/internal/stratum"
	"github.com/yorgof/solostratum/internal/work"
)

func testServer(t *testing.T, found []blocks.Record) *httptest.Server {
	return serve(t, found, nil)
}

// serve starts the status page; change may adjust its sources first.
func serve(t *testing.T, found []blocks.Record, change func(*Sources)) *httptest.Server {
	src := Sources{
		Version: "1.2.3", PayoutAddress: "bc1qexample", StratumPort: "3333",
		Node: func() work.NodeState { return work.NodeState{Chain: "main", Connected: true, Height: 900000} },
		Pool: func() stratum.Status {
			return stratum.Status{Started: time.Now(), Miners: []stratum.MinerStatus{{Worker: `<script>alert(1)</script>`}}}
		},
		Blocks: func() []blocks.Record { return found },
	}
	if change != nil {
		change(&src)
	}
	srv := httptest.NewServer(Handler(src))
	t.Cleanup(srv.Close)
	return srv
}

func TestAPI(t *testing.T) {
	srv := testServer(t, nil)
	resp, err := http.Get(srv.URL + "/api/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("content type %q", ct)
	}
	var r map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		t.Fatal(err)
	}
	if r["version"] != "1.2.3" || r["payoutAddress"] != "bc1qexample" || r["stratumPort"] != "3333" {
		t.Errorf("report: %v", r)
	}
	// Lists must be arrays, never null, so the page can iterate them.
	for _, list := range []string{"blocks", "offline"} {
		if _, ok := r[list].([]any); !ok {
			t.Errorf("%s is %T, want an array", list, r[list])
		}
	}
	// Without statistics there is no history to offer.
	if r["history"] != false {
		t.Errorf("history is %v without statistics", r["history"])
	}
	if node := r["node"].(map[string]any); node["chain"] != "main" || node["height"] != float64(900000) {
		t.Errorf("node: %v", node)
	}
}

func TestPageIsSelfContainedAndReadOnly(t *testing.T) {
	srv := testServer(t, nil)
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	html := string(body)
	if resp.StatusCode != 200 || !strings.Contains(html, "<title>solostratum</title>") {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if csp := resp.Header.Get("Content-Security-Policy"); !strings.Contains(csp, "default-src 'none'") {
		t.Errorf("CSP %q", csp)
	}
	// Nothing may be loaded from elsewhere.
	if m := regexp.MustCompile(`(?i)(src|href)\s*=\s*["']?(https?:)?//`).FindString(html); m != "" {
		t.Errorf("page references an external resource: %s", m)
	}
	// Dark only: no light theme, and the browser is told so.
	if !strings.Contains(html, `<meta name="color-scheme" content="dark">`) || strings.Contains(html, "prefers-color-scheme") {
		t.Error("page is not dark-only")
	}
	// Miner-supplied text must never be inserted as HTML.
	if strings.Contains(html, "innerHTML =") || strings.Contains(html, "insertAdjacentHTML") || strings.Contains(html, "document.write") {
		t.Error("page builds HTML from data")
	}

	for _, path := range []string{"/", "/api/status", "/api/history?range=24h"} {
		for _, method := range []string{"POST", "PUT", "DELETE"} {
			req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader("{}"))
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusMethodNotAllowed {
				t.Errorf("%s %s = %d, want 405", method, path, resp.StatusCode)
			}
		}
	}
	if resp, _ := http.Get(srv.URL + "/solostratum.conf"); resp.StatusCode != http.StatusNotFound {
		t.Errorf("unknown path returned %d", resp.StatusCode)
	}
}

func TestHistoryAPI(t *testing.T) {
	type call struct{ span, step time.Duration }
	var mu sync.Mutex
	var calls []call
	var failing error
	srv := serve(t, nil, func(src *Sources) {
		src.Offline = func() []stats.Worker {
			return []stats.Worker{{Worker: "gone", Totals: stats.Totals{Counts: stats.Counts{Accepted: 7}}}}
		}
		src.History = func(now time.Time, span, step time.Duration) (stats.Series, error) {
			mu.Lock()
			defer mu.Unlock()
			calls = append(calls, call{span, step})
			return stats.Series{Step: int64(step / time.Second), Steps: int(span / step), Workers: []stats.WorkerSeries{}}, failing
		}
	})
	get := func(path string) (int, map[string]any) {
		t.Helper()
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var body map[string]any
		json.NewDecoder(resp.Body).Decode(&body)
		return resp.StatusCode, body
	}

	_, report := get("/api/status")
	offline, _ := report["offline"].([]any)
	if report["history"] != true || len(offline) != 1 || offline[0].(map[string]any)["worker"] != "gone" || offline[0].(map[string]any)["accepted"] != float64(7) {
		t.Errorf("status with statistics: history %v, offline %v", report["history"], report["offline"])
	}

	// Every period is a whole number of steps, and every step a whole
	// number of the intervals the statistics are kept in.
	want := map[string]call{
		"24h": {24 * time.Hour, 15 * time.Minute},
		"7d":  {7 * 24 * time.Hour, time.Hour},
		"30d": {30 * 24 * time.Hour, 4 * time.Hour},
	}
	for name, period := range want {
		mu.Lock()
		calls = nil
		mu.Unlock()
		code, body := get("/api/history?range=" + name)
		mu.Lock()
		if code != 200 || len(calls) != 1 || calls[0] != period {
			t.Fatalf("%s: status %d, asked for %+v, want %+v", name, code, calls, period)
		}
		mu.Unlock()
		if body["steps"] != float64(period.span/period.step) || period.span%period.step != 0 || period.step%stats.Interval != 0 {
			t.Errorf("%s: %v steps of %v", name, body["steps"], period.step)
		}
		if _, ok := body["workers"].([]any); !ok {
			t.Errorf("%s: workers is %T, want an array", name, body["workers"])
		}
	}
	for _, path := range []string{"/api/history", "/api/history?range=1y", "/api/history?range="} {
		if code, _ := get(path); code != http.StatusBadRequest {
			t.Errorf("%s = %d, want 400", path, code)
		}
	}
	mu.Lock()
	failing = errors.New("disk on fire")
	mu.Unlock()
	if code, _ := get("/api/history?range=24h"); code != http.StatusInternalServerError {
		t.Errorf("unreadable history = %d, want 500", code)
	}
}

func TestHistoryIsOffWithoutStatistics(t *testing.T) {
	srv := testServer(t, nil)
	resp, err := http.Get(srv.URL + "/api/history?range=24h")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("history without statistics = %d, want 404", resp.StatusCode)
	}
}

func TestServeCancellationAndListenerFailure(t *testing.T) {
	for _, broken := range []bool{false, true} {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		if broken {
			l.Close()
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- Serve(ctx, l, Sources{}) }()
		if !broken {
			client := &http.Client{Timeout: time.Second}
			resp, err := client.Head("http://" + l.Addr().String() + "/")
			if err != nil {
				cancel()
				t.Fatal(err)
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil || len(body) != 0 || resp.StatusCode != 200 {
				t.Errorf("HEAD: status %d, body %q, error %v", resp.StatusCode, body, err)
			}
			cancel()
		}
		select {
		case err := <-done:
			if broken != (err != nil) {
				t.Errorf("closed listener %v: %v", broken, err)
			}
		case <-time.After(5 * time.Second):
			t.Error("status server did not stop")
		}
		cancel()
		l.Close()
	}
}
