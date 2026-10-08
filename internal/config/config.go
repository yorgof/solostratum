// Package config reads the plain-text settings file.
package config

import (
	"bufio"
	_ "embed"
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Example is the commented settings file written when none exists.
//
//go:embed solostratum.conf.example
var Example string

// FileName is the settings file looked for next to the executable.
const FileName = "solostratum.conf"

// Config holds every setting. Zero values are replaced by Defaults.
type Config struct {
	NodeURL        string
	NodeUser       string
	NodePassword   string
	NodeCookieFile string

	PayoutAddress string

	StratumListen string
	StatusListen  string // empty disables the status page

	CoinbaseTag     string
	StartDifficulty float64
	MinDifficulty   float64
	BlocksDir       string
	StatsDir        string // empty turns the statistics off

	// Dir is the directory of the settings file; relative paths resolve
	// against it.
	Dir string
}

// Defaults returns the settings used when a key is absent.
func Defaults() Config {
	return Config{
		NodeURL:         "http://127.0.0.1:8332",
		StratumListen:   "0.0.0.0:3333",
		StatusListen:    "0.0.0.0:3334",
		CoinbaseTag:     "/solostratum/",
		StartDifficulty: 1024,
		MinDifficulty:   0.001,
		BlocksDir:       "blocks",
		StatsDir:        "stats",
	}
}

// DefaultPath returns the settings file path next to the running executable.
func DefaultPath() string {
	exe, err := os.Executable()
	if err != nil {
		return FileName
	}
	if resolved, err := filepath.EvalSymlinks(exe); err == nil {
		exe = resolved
	}
	return filepath.Join(filepath.Dir(exe), FileName)
}

// Load reads and validates the settings file at path.
func Load(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer f.Close()

	cfg := Defaults()
	if abs, err := filepath.Abs(path); err == nil {
		cfg.Dir = filepath.Dir(abs)
	}
	seen := map[string]int{}
	sc := bufio.NewScanner(f)
	for line := 1; sc.Scan(); line++ {
		text := strings.TrimSpace(strings.TrimPrefix(sc.Text(), "\xef\xbb\xbf"))
		if text == "" || strings.HasPrefix(text, "#") || strings.HasPrefix(text, ";") {
			continue
		}
		key, value, ok := strings.Cut(text, "=")
		if !ok {
			return cfg, fmt.Errorf("line %d: expected \"name = value\", got %q", line, text)
		}
		key = strings.ToLower(strings.TrimSpace(key))
		value = unquote(strings.TrimSpace(value))
		if prev, dup := seen[key]; dup {
			return cfg, fmt.Errorf("line %d: %q is already set on line %d", line, key, prev)
		}
		seen[key] = line
		if err := cfg.set(key, value); err != nil {
			return cfg, fmt.Errorf("line %d: %w", line, err)
		}
	}
	if err := sc.Err(); err != nil {
		return cfg, err
	}
	return cfg, cfg.validate()
}

func unquote(v string) string {
	if len(v) >= 2 && (v[0] == '"' || v[0] == '\'') && v[len(v)-1] == v[0] {
		return v[1 : len(v)-1]
	}
	return v
}

func (c *Config) set(key, value string) error {
	switch key {
	case "node_url":
		c.NodeURL = value
	case "node_user":
		c.NodeUser = value
	case "node_password":
		c.NodePassword = value
	case "node_cookie_file":
		c.NodeCookieFile = value
	case "payout_address":
		c.PayoutAddress = value
	case "stratum_listen":
		c.StratumListen = value
	case "status_listen":
		c.StatusListen = value
	case "coinbase_tag":
		c.CoinbaseTag = value
	case "blocks_dir":
		c.BlocksDir = value
	case "stats_dir":
		c.StatsDir = value
	case "start_difficulty":
		return parsePositive(key, value, &c.StartDifficulty)
	case "min_difficulty":
		return parsePositive(key, value, &c.MinDifficulty)
	default:
		return fmt.Errorf("unknown setting %q", key)
	}
	return nil
}

func parsePositive(key, value string, dst *float64) error {
	f, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(f) || f <= 0 || f > 1e18 {
		return fmt.Errorf("%s must be a positive number, got %q", key, value)
	}
	*dst = f
	return nil
}

func (c *Config) validate() error {
	u, err := url.Parse(c.NodeURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("node_url must look like http://host:port, got %q", c.NodeURL)
	}
	if u.User != nil {
		return errors.New("node_url must not contain a user or password; use node_user and node_password")
	}
	hasPassword := c.NodeUser != "" || c.NodePassword != ""
	if hasPassword && c.NodeCookieFile != "" {
		return errors.New("set either node_user/node_password or node_cookie_file, not both")
	}
	if !hasPassword && c.NodeCookieFile == "" {
		return errors.New("node login is missing: set node_user and node_password, or node_cookie_file")
	}
	if hasPassword && (c.NodeUser == "" || c.NodePassword == "") {
		return errors.New("node_user and node_password must both be set")
	}
	if c.PayoutAddress == "" {
		return errors.New("payout_address is missing: set the Bitcoin address that should receive block rewards")
	}
	if _, _, err := net.SplitHostPort(c.StratumListen); err != nil {
		return fmt.Errorf("stratum_listen must look like 0.0.0.0:3333, got %q", c.StratumListen)
	}
	if c.StatusListen != "" {
		if _, _, err := net.SplitHostPort(c.StatusListen); err != nil {
			return fmt.Errorf("status_listen must look like 0.0.0.0:3334 (or be empty to disable), got %q", c.StatusListen)
		}
	}
	if len(c.CoinbaseTag) > 32 {
		return errors.New("coinbase_tag must be at most 32 characters")
	}
	for _, r := range c.CoinbaseTag {
		if r < 0x20 || r > 0x7e {
			return errors.New("coinbase_tag may only contain plain ASCII characters")
		}
	}
	if c.MinDifficulty > c.StartDifficulty {
		return errors.New("min_difficulty must not be larger than start_difficulty")
	}
	if c.BlocksDir == "" {
		return errors.New("blocks_dir must not be empty")
	}
	c.BlocksDir = c.resolve(c.BlocksDir)
	if c.StatsDir != "" {
		c.StatsDir = c.resolve(c.StatsDir)
	}
	if c.NodeCookieFile != "" {
		c.NodeCookieFile = c.resolve(c.NodeCookieFile)
	}
	return nil
}

func (c *Config) resolve(p string) string {
	if filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(c.Dir, p)
}
