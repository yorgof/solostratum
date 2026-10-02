package status

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/yorgof/solostratum/internal/blocks"
	"github.com/yorgof/solostratum/internal/stratum"
	"github.com/yorgof/solostratum/internal/work"
)

func testServer(t *testing.T, found []blocks.Record) *httptest.Server {
	src := Sources{
		Version: "v1.2.3", PayoutAddress: "bc1qexample", StratumPort: "3333",
		Node: func() work.NodeState { return work.NodeState{Chain: "main", Connected: true, Height: 900000} },
		Pool: func() stratum.Status {
			return stratum.Status{Started: time.Now(), Miners: []stratum.MinerStatus{{Worker: `<script>alert(1)</script>`}}}
		},
		Blocks: func() []blocks.Record { return found },
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
	if r["version"] != "v1.2.3" || r["payoutAddress"] != "bc1qexample" || r["stratumPort"] != "3333" {
		t.Errorf("report: %v", r)
	}
	// Lists must be arrays, never null, so the page can iterate them.
	if _, ok := r["blocks"].([]any); !ok {
		t.Errorf("blocks is %T, want an array", r["blocks"])
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

	for _, path := range []string{"/", "/api/status"} {
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
