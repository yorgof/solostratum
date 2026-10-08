// Package rpc is a minimal JSON-RPC client for Bitcoin Core. It only uses
// getblockchaininfo, getblocktemplate and submitblock, so it works with a
// tightly restricted rpcwhitelist.
package rpc

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

// Client talks to one Bitcoin Core node.
type Client struct {
	url        string
	user, pass string
	cookieFile string
	http       *http.Client
	id         atomic.Uint64
}

// New creates a client. If cookieFile is set it is re-read on every call so
// a node restart (which rotates the cookie) is picked up automatically.
func New(url, user, pass, cookieFile string) *Client {
	return &Client{
		url: url, user: user, pass: pass, cookieFile: cookieFile,
		http: &http.Client{Transport: &http.Transport{
			// No proxy, even if one is set in the environment: the node's
			// login and any found block must go to the node directly.
			Proxy:               nil,
			DialContext:         (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConnsPerHost: 4,
			IdleConnTimeout:     90 * time.Second,
		}},
	}
}

// Error is an error returned by the node itself (as opposed to a failure to
// reach it).
type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return fmt.Sprintf("node error %d: %s", e.Code, e.Message) }

// Error codes used by Bitcoin Core that callers care about.
const (
	CodeNotConnected = -9  // node has no peers
	CodeInWarmup     = -28 // node is still starting
	CodeInIBD        = -10 // node is still downloading the chain
)

// HTTPError is a failure at the HTTP level, such as a rejected login.
type HTTPError struct{ Status int }

func (e *HTTPError) Error() string {
	switch e.Status {
	case http.StatusUnauthorized:
		return "the node rejected the login (wrong node_user / node_password, or wrong cookie file)"
	case http.StatusForbidden:
		return "the node refused the connection or the request (check rpcallowip and rpcwhitelist in bitcoin.conf)"
	}
	return fmt.Sprintf("the node answered with HTTP status %d", e.Status)
}

// Explain turns a low-level error into a sentence a person can act on.
func Explain(err error) string {
	var rpcErr *Error
	var httpErr *HTTPError
	var netErr net.Error
	switch {
	case errors.As(err, &rpcErr):
		switch rpcErr.Code {
		case CodeInWarmup:
			return "the node is still starting up (" + rpcErr.Message + ")"
		case CodeInIBD:
			return "the node is still downloading the blockchain"
		case CodeNotConnected:
			return "the node is not connected to any peers"
		}
		return rpcErr.Error()
	case errors.As(err, &httpErr):
		return httpErr.Error()
	case errors.Is(err, os.ErrNotExist):
		return "cookie file not found: " + err.Error()
	case errors.As(err, &netErr) && netErr.Timeout():
		return "the node did not answer in time (is node_url correct and the port reachable?)"
	case errors.Is(err, syscall.ECONNREFUSED) || strings.Contains(err.Error(), "refused"):
		return "connection refused (is the node running, and is node_url correct? The node needs server=1, and rpcbind/rpcallowip for remote access)"
	}
	return err.Error()
}

func (c *Client) credentials() (string, string, error) {
	if c.cookieFile == "" {
		return c.user, c.pass, nil
	}
	raw, err := os.ReadFile(c.cookieFile)
	if err != nil {
		return "", "", err
	}
	user, pass, ok := strings.Cut(strings.TrimSpace(string(raw)), ":")
	if !ok {
		return "", "", fmt.Errorf("cookie file %s is malformed", c.cookieFile)
	}
	return user, pass, nil
}

// call performs one JSON-RPC request and decodes the result into out.
func (c *Client) call(ctx context.Context, timeout time.Duration, method string, params []any, out any) error {
	if params == nil {
		params = []any{}
	}
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "1.0", "id": c.id.Add(1), "method": method, "params": params,
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	user, pass, err := c.credentials()
	if err != nil {
		return err
	}
	req.SetBasicAuth(user, pass)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		io.Copy(io.Discard, resp.Body)
		return &HTTPError{resp.StatusCode}
	}
	// Bitcoin Core reports RPC errors with a JSON body and a 4xx/5xx status.
	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  *Error          `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		if resp.StatusCode != http.StatusOK {
			return &HTTPError{resp.StatusCode}
		}
		return fmt.Errorf("unreadable answer from node: %w", err)
	}
	if envelope.Error != nil {
		return envelope.Error
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(envelope.Result, out)
}

// ChainInfo is the subset of getblockchaininfo the bridge uses.
type ChainInfo struct {
	Chain                string  `json:"chain"`
	Blocks               int64   `json:"blocks"`
	Headers              int64   `json:"headers"`
	BestBlockHash        string  `json:"bestblockhash"`
	Difficulty           float64 `json:"difficulty"`
	VerificationProgress float64 `json:"verificationprogress"`
	InitialBlockDownload bool    `json:"initialblockdownload"`
}

// GetBlockchainInfo returns the node's view of the chain.
func (c *Client) GetBlockchainInfo(ctx context.Context) (ChainInfo, error) {
	var info ChainInfo
	err := c.call(ctx, 15*time.Second, "getblockchaininfo", nil, &info)
	return info, err
}

// BlockTemplate is the answer of getblocktemplate.
type BlockTemplate struct {
	Version           uint32            `json:"version"`
	Rules             []string          `json:"rules"`
	PreviousBlockHash string            `json:"previousblockhash"`
	Transactions      []TemplateTx      `json:"transactions"`
	CoinbaseAux       map[string]string `json:"coinbaseaux"`
	CoinbaseValue     int64             `json:"coinbasevalue"`
	LongPollID        string            `json:"longpollid"`
	Target            string            `json:"target"`
	MinTime           int64             `json:"mintime"`
	CurTime           int64             `json:"curtime"`
	Bits              string            `json:"bits"`
	Height            int64             `json:"height"`
	WitnessCommitment string            `json:"default_witness_commitment"`
}

// TemplateTx is one transaction of a block template.
type TemplateTx struct {
	Data string `json:"data"`
	TxID string `json:"txid"`
	Hash string `json:"hash"` // includes witness data (wtxid)
	Fee  int64  `json:"fee"`
}

// segwitRules tells the node which consensus rules this client understands.
var segwitRules = []string{"segwit"}

// GetBlockTemplate fetches a template. If longPollID is not empty the call
// blocks until the node has a new template (new tip, or new transactions
// after about a minute), up to wait.
func (c *Client) GetBlockTemplate(ctx context.Context, longPollID string, wait time.Duration) (*BlockTemplate, error) {
	req := map[string]any{"rules": segwitRules}
	timeout := 30 * time.Second
	if longPollID != "" {
		req["longpollid"] = longPollID
		timeout = wait
	}
	var t BlockTemplate
	if err := c.call(ctx, timeout, "getblocktemplate", []any{req}, &t); err != nil {
		return nil, err
	}
	return &t, nil
}

// ProposeBlock asks the node to validate a block without requiring proof of
// work and without storing or relaying it. It returns "" if the node finds
// the block valid, otherwise the node's rejection reason.
func (c *Client) ProposeBlock(ctx context.Context, block []byte) (string, error) {
	req := map[string]any{"mode": "proposal", "rules": segwitRules, "data": hex.EncodeToString(block)}
	var result *string
	if err := c.call(ctx, 60*time.Second, "getblocktemplate", []any{req}, &result); err != nil {
		return "", err
	}
	if result == nil {
		return "", nil
	}
	return *result, nil
}

// SubmitBlock sends a solved block to the node. It returns "" if the node
// accepted it, otherwise the node's rejection reason.
func (c *Client) SubmitBlock(ctx context.Context, block []byte) (string, error) {
	var result *string
	if err := c.call(ctx, 60*time.Second, "submitblock", []any{hex.EncodeToString(block)}, &result); err != nil {
		return "", err
	}
	if result == nil {
		return "", nil
	}
	return *result, nil
}
