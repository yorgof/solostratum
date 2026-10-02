package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeCore answers JSON-RPC like Bitcoin Core for the methods used here.
type fakeCore struct {
	t        *testing.T
	user     string
	pass     string
	lastReq  map[string]any
	answer   func(method string, params []any) (status int, body string)
	requests int
}

func (f *fakeCore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.requests++
	user, pass, ok := r.BasicAuth()
	if !ok || user != f.user || pass != f.pass {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	raw, _ := io.ReadAll(r.Body)
	if err := json.Unmarshal(raw, &f.lastReq); err != nil {
		f.t.Errorf("request is not JSON: %s", raw)
	}
	params, _ := f.lastReq["params"].([]any)
	status, body := f.answer(f.lastReq["method"].(string), params)
	w.WriteHeader(status)
	io.WriteString(w, body)
}

func newFake(t *testing.T) (*fakeCore, *Client) {
	f := &fakeCore{t: t, user: "u", pass: "p"}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, New(srv.URL, "u", "p", "")
}

func TestGetBlockchainInfo(t *testing.T) {
	f, c := newFake(t)
	f.answer = func(method string, _ []any) (int, string) {
		if method != "getblockchaininfo" {
			t.Errorf("method %q", method)
		}
		return 200, `{"result":{"chain":"main","blocks":900000,"headers":900001,"bestblockhash":"00ab","initialblockdownload":true,"verificationprogress":0.5},"error":null,"id":1}`
	}
	info, err := c.GetBlockchainInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.Chain != "main" || info.Blocks != 900000 || !info.InitialBlockDownload || info.BestBlockHash != "00ab" {
		t.Errorf("info: %+v", info)
	}
}

func TestGetBlockTemplateRequestAndDecode(t *testing.T) {
	f, c := newFake(t)
	f.answer = func(method string, params []any) (int, string) {
		req := params[0].(map[string]any)
		rules, _ := req["rules"].([]any)
		if method != "getblocktemplate" || len(rules) != 1 || rules[0] != "segwit" {
			t.Errorf("request: %s %v", method, params)
		}
		return 200, `{"result":{"version":536870912,"rules":["csv","!segwit"],"previousblockhash":"aa","transactions":[{"data":"0102","txid":"bb","fee":250}],
			"coinbasevalue":312500250,"longpollid":"lp1","target":"00","mintime":10,"curtime":20,"bits":"1d00ffff","height":7,"default_witness_commitment":"6a24"},"error":null,"id":1}`
	}
	tmpl, err := c.GetBlockTemplate(context.Background(), "", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if tmpl.Version != 0x20000000 || tmpl.Height != 7 || tmpl.CoinbaseValue != 312500250 || tmpl.LongPollID != "lp1" ||
		len(tmpl.Transactions) != 1 || tmpl.Transactions[0].Data != "0102" || tmpl.WitnessCommitment != "6a24" || tmpl.Bits != "1d00ffff" {
		t.Errorf("template: %+v", tmpl)
	}
	if _, has := f.lastReq["params"].([]any)[0].(map[string]any)["longpollid"]; has {
		t.Error("longpollid sent although none was given")
	}
	c.GetBlockTemplate(context.Background(), "lp1", time.Second)
	if got := f.lastReq["params"].([]any)[0].(map[string]any)["longpollid"]; got != "lp1" {
		t.Errorf("longpollid = %v", got)
	}
}

func TestSubmitAndPropose(t *testing.T) {
	f, c := newFake(t)
	result := `null`
	f.answer = func(method string, params []any) (int, string) {
		return 200, `{"result":` + result + `,"error":null,"id":1}`
	}
	reason, err := c.SubmitBlock(context.Background(), []byte{0xab, 0xcd})
	if err != nil || reason != "" {
		t.Fatalf("accepted block: %q, %v", reason, err)
	}
	if f.lastReq["method"] != "submitblock" || f.lastReq["params"].([]any)[0] != "abcd" {
		t.Errorf("submit request: %v", f.lastReq)
	}
	result = `"high-hash"`
	if reason, err = c.SubmitBlock(context.Background(), []byte{1}); err != nil || reason != "high-hash" {
		t.Fatalf("rejected block: %q, %v", reason, err)
	}

	result = `null`
	if reason, err = c.ProposeBlock(context.Background(), []byte{0x01, 0x02}); err != nil || reason != "" {
		t.Fatalf("valid proposal: %q, %v", reason, err)
	}
	req := f.lastReq["params"].([]any)[0].(map[string]any)
	if f.lastReq["method"] != "getblocktemplate" || req["mode"] != "proposal" || req["data"] != "0102" {
		t.Errorf("proposal request: %v", f.lastReq)
	}
	result = `"bad-txnmrklroot"`
	if reason, _ = c.ProposeBlock(context.Background(), []byte{1}); reason != "bad-txnmrklroot" {
		t.Fatalf("invalid proposal: %q", reason)
	}
}

func TestErrors(t *testing.T) {
	f, c := newFake(t)

	// Bitcoin Core reports RPC errors with HTTP 500 and a JSON body.
	f.answer = func(string, []any) (int, string) {
		return 500, `{"result":null,"error":{"code":-10,"message":"Bitcoin Core is in initial sync and waiting for blocks..."},"id":1}`
	}
	_, err := c.GetBlockTemplate(context.Background(), "", time.Second)
	var rpcErr *Error
	if !errors.As(err, &rpcErr) || rpcErr.Code != CodeInIBD {
		t.Fatalf("error %v, want RPC error -10", err)
	}
	if msg := Explain(err); !strings.Contains(msg, "still downloading") {
		t.Errorf("Explain = %q", msg)
	}

	// A busy node answers 503 with a plain-text body.
	f.answer = func(string, []any) (int, string) { return 503, "Work queue depth exceeded" }
	_, err = c.GetBlockchainInfo(context.Background())
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Status != 503 {
		t.Fatalf("error %v, want HTTP 503", err)
	}

	// Wrong password.
	bad := New(c.url, "u", "wrong", "")
	_, err = bad.GetBlockchainInfo(context.Background())
	if !errors.As(err, &httpErr) || httpErr.Status != 401 || !strings.Contains(Explain(err), "rejected the login") {
		t.Fatalf("error %v, want a login error", err)
	}

	// Nothing listening.
	dead := New("http://127.0.0.1:1", "u", "p", "")
	_, err = dead.GetBlockchainInfo(context.Background())
	if err == nil || !strings.Contains(Explain(err), "connection refused") {
		t.Fatalf("Explain(%v) = %q", err, Explain(err))
	}
}

// The cookie file is read on every call, so a node restart (which writes a
// new cookie) is picked up without restarting.
func TestCookieIsReRead(t *testing.T) {
	f, c := newFake(t)
	f.answer = func(string, []any) (int, string) { return 200, `{"result":{"chain":"regtest"},"error":null,"id":1}` }
	cookie := filepath.Join(t.TempDir(), ".cookie")
	client := New(c.url, "", "", cookie)

	if _, err := client.GetBlockchainInfo(context.Background()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing cookie: %v", err)
	}
	f.user, f.pass = "__cookie__", "first"
	os.WriteFile(cookie, []byte("__cookie__:first\n"), 0o600)
	if _, err := client.GetBlockchainInfo(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.pass = "second"
	os.WriteFile(cookie, []byte("__cookie__:second"), 0o600)
	if _, err := client.GetBlockchainInfo(context.Background()); err != nil {
		t.Fatalf("after cookie rotation: %v", err)
	}
	os.WriteFile(cookie, []byte("garbage"), 0o600)
	if _, err := client.GetBlockchainInfo(context.Background()); err == nil {
		t.Fatal("malformed cookie accepted")
	}
}

// Credentials must never travel through a proxy from the environment.
func TestProxyFromEnvironmentIsIgnored(t *testing.T) {
	f, c := newFake(t)
	f.answer = func(string, []any) (int, string) { return 200, `{"result":{"chain":"main"},"error":null,"id":1}` }
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("http_proxy", "http://127.0.0.1:1")
	fresh := New(strings.Replace(c.url, "127.0.0.1", "localhost", 1), "u", "p", "")
	if _, err := fresh.GetBlockchainInfo(context.Background()); err != nil {
		t.Fatalf("request went to the proxy instead of the node: %v", err)
	}
}
