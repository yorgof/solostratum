package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func write(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), FileName)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

const minimal = `
node_url = http://192.0.2.50:8332
node_user = miner
node_password = s3cret=with=equals
payout_address = bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4
`

func TestLoadMinimal(t *testing.T) {
	path := write(t, minimal)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.NodeURL != "http://192.0.2.50:8332" || cfg.NodeUser != "miner" || cfg.NodePassword != "s3cret=with=equals" {
		t.Errorf("node settings: %+v", cfg)
	}
	d := Defaults()
	if cfg.StratumListen != d.StratumListen || cfg.StatusListen != d.StatusListen || cfg.StartDifficulty != d.StartDifficulty {
		t.Errorf("defaults not applied: %+v", cfg)
	}
	if want := filepath.Join(filepath.Dir(path), "blocks"); cfg.BlocksDir != want {
		t.Errorf("blocks dir %q, want %q (next to the settings file)", cfg.BlocksDir, want)
	}
}

func TestLoadAllSettings(t *testing.T) {
	cfg, err := Load(write(t, "\xef\xbb\xbf# comment\r\n"+
		"; another comment\r\n"+
		"NODE_URL = https://node.example.com/rpc \r\n"+
		"node_cookie_file = \"/var/lib/bitcoin/.cookie\"\r\n"+
		"payout_address='bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4'\r\n"+
		"stratum_listen = 127.0.0.1:4000\r\n"+
		"status_listen =\r\n"+
		"coinbase_tag = /my rig/\r\n"+
		"start_difficulty = 512\r\n"+
		"min_difficulty = 0.5\r\n"+
		"blocks_dir = /srv/found\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.NodeURL != "https://node.example.com/rpc" || cfg.NodeCookieFile != "/var/lib/bitcoin/.cookie" {
		t.Errorf("node: %+v", cfg)
	}
	if cfg.PayoutAddress != "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4" || cfg.StratumListen != "127.0.0.1:4000" ||
		cfg.StatusListen != "" || cfg.CoinbaseTag != "/my rig/" || cfg.StartDifficulty != 512 ||
		cfg.MinDifficulty != 0.5 || cfg.BlocksDir != "/srv/found" {
		t.Errorf("settings: %+v", cfg)
	}
}

func TestLoadErrors(t *testing.T) {
	cases := []struct{ name, content, want string }{
		{"unknown key", minimal + "paiout_address = x\n", `unknown setting "paiout_address"`},
		{"line number", "node_url = http://a:1\nbogus line\n", "line 2"},
		{"duplicate", minimal + "node_user = again\n", "already set on line"},
		{"no address", "node_url = http://a:1\nnode_user = u\nnode_password = p\n", "payout_address is missing"},
		{"no login", "node_url = http://a:1\npayout_address = x\n", "node login is missing"},
		{"half login", "node_user = u\npayout_address = x\n", "must both be set"},
		{"both logins", minimal + "node_cookie_file = c\n", "not both"},
		{"bad url", strings.Replace(minimal, "http://192.0.2.50:8332", "192.0.2.50:8332", 1), "node_url must look like"},
		{"credentials in url", strings.Replace(minimal, "http://", "http://u:p@", 1), "must not contain a user"},
		{"bad listen", minimal + "stratum_listen = 3333\n", "stratum_listen must look like"},
		{"bad status listen", minimal + "status_listen = nope\n", "status_listen must look like"},
		{"bad number", minimal + "start_difficulty = fast\n", "must be a positive number"},
		{"negative number", minimal + "min_difficulty = -1\n", "must be a positive number"},
		{"min above start", minimal + "min_difficulty = 5000\n", "must not be larger"},
		{"long tag", minimal + "coinbase_tag = " + strings.Repeat("x", 33) + "\n", "at most 32"},
		{"non-ascii tag", minimal + "coinbase_tag = /₿/\n", "plain ASCII"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Load(write(t, c.content))
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("error %v, want it to mention %q", err, c.want)
			}
		})
	}
}

// The file written on first run must parse, and must fail only because the
// required values are still empty.
func TestExampleFile(t *testing.T) {
	_, err := Load(write(t, Example))
	if err == nil || !strings.Contains(err.Error(), "node login is missing") {
		t.Fatalf("example file: %v", err)
	}
	filled := strings.Replace(Example, "node_user =", "node_user = u", 1)
	filled = strings.Replace(filled, "node_password =", "node_password = p", 1)
	filled = strings.Replace(filled, "payout_address =", "payout_address = bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4", 1)
	cfg, err := Load(write(t, filled))
	if err != nil {
		t.Fatal(err)
	}
	// Every commented-out example value must equal the built-in default,
	// so the file documents the real behaviour.
	uncommented := filled
	for _, key := range []string{"stratum_listen", "status_listen", "coinbase_tag", "start_difficulty", "min_difficulty", "blocks_dir"} {
		if !strings.Contains(uncommented, "# "+key+" =") {
			t.Fatalf("example file does not document %s", key)
		}
		uncommented = strings.Replace(uncommented, "# "+key+" =", key+" =", 1)
	}
	explicit, err := Load(write(t, uncommented))
	if err != nil {
		t.Fatal(err)
	}
	explicit.Dir, cfg.Dir = "", ""
	explicit.BlocksDir, cfg.BlocksDir = filepath.Base(explicit.BlocksDir), filepath.Base(cfg.BlocksDir)
	if explicit != cfg {
		t.Errorf("documented defaults differ from real defaults:\n documented %+v\n real       %+v", explicit, cfg)
	}
}

func TestMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.conf")); !os.IsNotExist(err) {
		t.Fatalf("error %v, want not-exist", err)
	}
}
