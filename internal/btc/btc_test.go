package btc

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"math"
	"math/big"
	"os"
	"strconv"
	"testing"
)

type mainnetBlock struct {
	Hash     string   `json:"hash"`
	Height   int64    `json:"height"`
	Version  uint32   `json:"version"`
	Prev     string   `json:"previousblockhash"`
	Merkle   string   `json:"merkleroot"`
	Time     uint32   `json:"time"`
	Bits     string   `json:"bits"`
	Nonce    uint32   `json:"nonce"`
	TxIDs    []string `json:"tx"`
	bitsUint uint32
}

// loadMainnetBlocks returns real mainnet blocks (header fields and txids)
// used as ground truth.
func loadMainnetBlocks(t *testing.T) []mainnetBlock {
	t.Helper()
	raw, err := os.ReadFile("testdata/mainnet_blocks.json")
	if err != nil {
		t.Fatal(err)
	}
	var blocks []mainnetBlock
	if err := json.Unmarshal(raw, &blocks); err != nil {
		t.Fatal(err)
	}
	for i := range blocks {
		b, err := strconv.ParseUint(blocks[i].Bits, 16, 32)
		if err != nil {
			t.Fatal(err)
		}
		blocks[i].bitsUint = uint32(b)
	}
	return blocks
}

func mustHash(t *testing.T, s string) Hash {
	t.Helper()
	h, err := HashFromDisplayHex(s)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestHeaderHashMatchesMainnet(t *testing.T) {
	for _, b := range loadMainnetBlocks(t) {
		hdr := Header(b.Version, mustHash(t, b.Prev), mustHash(t, b.Merkle), b.Time, b.bitsUint, b.Nonce)
		got := SHA256d(hdr[:])
		if got.String() != b.Hash {
			t.Errorf("block %d: header hash %s, want %s", b.Height, got, b.Hash)
		}
		if HashToInt(got).Cmp(CompactToTarget(b.bitsUint)) > 0 {
			t.Errorf("block %d: real block hash does not meet its own target", b.Height)
		}
	}
}

func TestMerkleBranchesMatchMainnet(t *testing.T) {
	for _, b := range loadMainnetBlocks(t) {
		ids := make([]Hash, len(b.TxIDs))
		for i, s := range b.TxIDs {
			ids[i] = mustHash(t, s)
		}
		want := mustHash(t, b.Merkle)
		if got := MerkleRoot(ids); got != want {
			t.Errorf("block %d: MerkleRoot = %s, want %s", b.Height, got, want)
		}
		branches := MerkleBranches(ids[1:])
		if got := MerkleRootFromBranches(ids[0], branches); got != want {
			t.Errorf("block %d (%d txs): root from branches = %s, want %s", b.Height, len(ids), got, want)
		}
	}
}

func TestMerkleBranchesAllSizes(t *testing.T) {
	for n := 1; n <= 70; n++ {
		ids := make([]Hash, n)
		for i := range ids {
			ids[i] = SHA256d([]byte{byte(n), byte(i)})
		}
		branches := MerkleBranches(ids[1:])
		if got, want := MerkleRootFromBranches(ids[0], branches), MerkleRoot(ids); got != want {
			t.Fatalf("%d transactions: branches give %s, full tree gives %s", n, got, want)
		}
		wantLen := 0
		for 1<<wantLen < n {
			wantLen++
		}
		if len(branches) != wantLen {
			t.Fatalf("%d transactions: %d branches, want %d", n, len(branches), wantLen)
		}
	}
}

func TestCompactToTarget(t *testing.T) {
	cases := []struct {
		bits uint32
		hex  string
	}{
		{0x1d00ffff, "00000000ffff0000000000000000000000000000000000000000000000000000"},
		{0x1b0404cb, "00000000000404cb000000000000000000000000000000000000000000000000"},
		{0x207fffff, "7fffff0000000000000000000000000000000000000000000000000000000000"},
		{0x1701d936, "01d9360000000000000000000000000000000000000000"},
		{0x03123456, "123456"},
		{0x02123456, "1234"},
		{0x01123456, "12"},
		{0x04923456, "0"}, // sign bit set
	}
	for _, c := range cases {
		want, _ := new(big.Int).SetString(c.hex, 16)
		if got := CompactToTarget(c.bits); got.Cmp(want) != 0 {
			t.Errorf("CompactToTarget(%08x) = %x, want %x", c.bits, got, want)
		}
	}
}

func TestDifficultyConversions(t *testing.T) {
	if d := DifficultyFromTarget(CompactToTarget(0x1d00ffff)); d != 1 {
		t.Errorf("difficulty of 1d00ffff = %v, want 1", d)
	}
	// Mainnet block 125552 had difficulty 244112.487774.
	if d := DifficultyFromTarget(CompactToTarget(0x1a44b9f2)); math.Abs(d-244112.487774) > 0.001 {
		t.Errorf("difficulty of 1a44b9f2 = %v", d)
	}
	for _, d := range []float64{1e-9, 0.001, 0.5, 1, 2, 1000, 16384, 1.3e14} {
		back := DifficultyFromTarget(TargetFromDifficulty(d))
		if math.Abs(back-d)/d > 1e-9 {
			t.Errorf("difficulty %v round-trips to %v", d, back)
		}
	}
	if TargetFromDifficulty(1e-80).BitLen() > 256 {
		t.Error("target for a tiny difficulty exceeds 256 bits")
	}
	if TargetFromDifficulty(0).BitLen() != 256 {
		t.Error("zero difficulty should give the maximum target")
	}
}

func TestVarInt(t *testing.T) {
	cases := map[uint64]string{
		0: "00", 0xfc: "fc", 0xfd: "fdfd00", 0xffff: "fdffff",
		0x10000: "fe00000100", 0xffffffff: "feffffffff", 0x100000000: "ff0000000001000000",
	}
	for n, want := range cases {
		if got := hex.EncodeToString(AppendVarInt(nil, n)); got != want {
			t.Errorf("AppendVarInt(%d) = %s, want %s", n, got, want)
		}
	}
}

func TestHeightPush(t *testing.T) {
	cases := map[int64]string{
		0: "00", 1: "51", 16: "60", 17: "0111", 127: "017f", 128: "028000",
		255: "02ff00", 256: "020001", 32767: "02ff7f", 32768: "03008000",
		500000: "0320a107", 840000: "0340d10c", 8388607: "03ffff7f", 8388608: "0400008000",
	}
	for h, want := range cases {
		if got := hex.EncodeToString(AppendHeightPush(nil, h)); got != want {
			t.Errorf("AppendHeightPush(%d) = %s, want %s", h, got, want)
		}
	}
}

func TestSwapWords(t *testing.T) {
	in, _ := hex.DecodeString("0011223344556677")
	if got := hex.EncodeToString(SwapWords(in)); got != "3322110077665544" {
		t.Errorf("SwapWords = %s", got)
	}
}

func TestAddressToScript(t *testing.T) {
	main, _ := NetworkByChain("main")
	test, _ := NetworkByChain("testnet4")
	reg, _ := NetworkByChain("regtest")
	valid := []struct {
		net    Network
		addr   string
		script string
	}{
		// Legacy
		{main, "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", "76a91462e907b15cbf27d5425399ebf6f0fb50ebb88f1888ac"},
		{main, "3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy", "a914b472a266d0bd89c13706a4132ccfb16f7c3b9fcb87"},
		// BIP173 / BIP350 test vectors
		{main, "BC1QW508D6QEJXTDG4Y5R3ZARVARY0C5XW7KV8F3T4", "0014751e76e8199196d454941c45d1b3a323f1433bd6"},
		{test, "tb1qrp33g0q5c5txsp9arysrx4k6zdkfs4nce4xj0gdcccefvpysxf3q0sl5k7", "00201863143c14c5166804bd19203356da136c985678cd4d27a1b8c6329604903262"},
		{main, "bc1pw508d6qejxtdg4y5r3zarvary0c5xw7kw508d6qejxtdg4y5r3zarvary0c5xw7kt5nd6y", "5128751e76e8199196d454941c45d1b3a323f1433bd6751e76e8199196d454941c45d1b3a323f1433bd6"},
		{main, "BC1SW50QGDZ25J", "6002751e"},
		{main, "bc1zw508d6qejxtdg4y5r3zarvaryvaxxpcs", "5210751e76e8199196d454941c45d1b3a323"},
		{test, "tb1qqqqqp399et2xygdj5xreqhjjvcmzhxw4aywxecjdzew6hylgvsesrxh6hy", "0020000000c4a5cad46221b2a187905e5266362b99d5e91c6ce24d165dab93e86433"},
		{test, "tb1pqqqqp399et2xygdj5xreqhjjvcmzhxw4aywxecjdzew6hylgvsesf3hn0c", "5120000000c4a5cad46221b2a187905e5266362b99d5e91c6ce24d165dab93e86433"},
		{main, "bc1p0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7vqzk5jj0", "512079be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"},
	}
	for _, c := range valid {
		got, err := AddressToScript(c.addr, c.net)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", c.addr, err)
			continue
		}
		want, _ := hex.DecodeString(c.script)
		if !bytes.Equal(got, want) {
			t.Errorf("%s: script %x, want %x", c.addr, got, want)
		}
	}

	invalid := []struct {
		net  Network
		addr string
		why  string
	}{
		{main, "", "empty"},
		{main, "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNb", "bad base58 checksum"},
		{main, "1A1zP1eP5QGefi2DMPTfTL5SLmv7Divf0a", "invalid base58 character"},
		{test, "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", "mainnet legacy address on testnet"},
		{test, "BC1QW508D6QEJXTDG4Y5R3ZARVARY0C5XW7KV8F3T4", "mainnet segwit address on testnet"},
		{main, "tb1qrp33g0q5c5txsp9arysrx4k6zdkfs4nce4xj0gdcccefvpysxf3q0sl5k7", "testnet address on mainnet"},
		{reg, "tb1qrp33g0q5c5txsp9arysrx4k6zdkfs4nce4xj0gdcccefvpysxf3q0sl5k7", "testnet address on regtest"},
		{main, "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t5", "bad bech32 checksum"},
		{main, "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3T4", "mixed case"},
		// BIP350 invalid vectors
		{main, "tc1qw508d6qejxtdg4y5r3zarvary0c5xw7kg3g4ty", "unknown prefix"},
		{main, "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kemeawh", "v0 with bech32m checksum"},
		{main, "bc1p0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7vqh2y7hd", "v1 with bech32 checksum"},
		{main, "BC130XLXVLHEMJA6C4DQV22UAPCTQUPFHLXM9H8Z3K2E72Q4K9HCZ7VQ7ZWS8R", "witness version 17"},
		{main, "bc1pw5dgrnzv", "program too short"},
		{main, "bc1p0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7v8n0nx0muaewav253zgeav", "program too long"},
		{main, "BC1QR508D6QEJXTDG4Y5R3ZARVARYV98GJ9P", "v0 program of 16 bytes"},
		{main, "bc1p0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7v07qwwzcrf", "zero padding of more than 4 bits"},
		{main, "bc1gmk9yu", "empty data"},
		{main, "not-an-address", "garbage"},
	}
	for _, c := range invalid {
		if s, err := AddressToScript(c.addr, c.net); err == nil {
			t.Errorf("%q (%s): accepted with script %x", c.addr, c.why, s)
		}
	}
}

// Changing any single character of a valid address must make it invalid:
// a typo must never silently pay somewhere else.
func TestAddressTyposAreRejected(t *testing.T) {
	main, _ := NetworkByChain("main")
	for _, addr := range []string{
		"1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
		"bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4",
		"bc1p0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7vqzk5jj0",
	} {
		original, err := AddressToScript(addr, main)
		if err != nil {
			t.Fatal(err)
		}
		alphabet := bech32Charset
		if addr[0] == '1' {
			alphabet = base58Alphabet
		}
		for i := 4; i < len(addr); i++ {
			for _, c := range alphabet {
				if byte(c) == addr[i] {
					continue
				}
				mutated := addr[:i] + string(c) + addr[i+1:]
				if s, err := AddressToScript(mutated, main); err == nil && !bytes.Equal(s, original) {
					t.Fatalf("typo %q of %q accepted", mutated, addr)
				}
			}
		}
	}
}
