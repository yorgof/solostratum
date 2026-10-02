package btc

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"strings"
)

// Network describes the address formats of one Bitcoin chain.
type Network struct {
	Name         string // as reported by getblockchaininfo ("main", "testnet4", ...)
	Bech32HRP    string
	P2PKHVersion byte
	P2SHVersion  byte
}

var networks = map[string]Network{
	"main":     {"main", "bc", 0x00, 0x05},
	"test":     {"test", "tb", 0x6f, 0xc4},
	"testnet4": {"testnet4", "tb", 0x6f, 0xc4},
	"signet":   {"signet", "tb", 0x6f, 0xc4},
	"regtest":  {"regtest", "bcrt", 0x6f, 0xc4},
}

// NetworkByChain returns the network for the "chain" value Bitcoin Core
// reports in getblockchaininfo.
func NetworkByChain(chain string) (Network, bool) {
	n, ok := networks[chain]
	return n, ok
}

// AddressToScript validates addr for the given network and returns the
// output script that pays to it. Every checksum is verified; an address for
// a different network is rejected.
func AddressToScript(addr string, net Network) ([]byte, error) {
	if addr == "" {
		return nil, errors.New("address is empty")
	}
	if hrp, _, ok := splitBech32(addr); ok && looksLikeSegwitHRP(hrp) {
		return segwitScript(addr, net)
	}
	return base58Script(addr, net)
}

func looksLikeSegwitHRP(hrp string) bool {
	switch strings.ToLower(hrp) {
	case "bc", "tb", "bcrt":
		return true
	}
	return false
}

// ---- Base58Check (legacy P2PKH and P2SH) ----

const base58Alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

func base58Script(addr string, net Network) ([]byte, error) {
	payload, err := base58CheckDecode(addr)
	if err != nil {
		return nil, err
	}
	if len(payload) != 21 {
		return nil, fmt.Errorf("unexpected address length")
	}
	version, hash := payload[0], payload[1:]
	switch version {
	case net.P2PKHVersion:
		// OP_DUP OP_HASH160 <20> OP_EQUALVERIFY OP_CHECKSIG
		s := []byte{0x76, 0xa9, 0x14}
		s = append(s, hash...)
		return append(s, 0x88, 0xac), nil
	case net.P2SHVersion:
		// OP_HASH160 <20> OP_EQUAL
		s := []byte{0xa9, 0x14}
		s = append(s, hash...)
		return append(s, 0x87), nil
	}
	return nil, fmt.Errorf("address is not for the %s network", net.Name)
}

func base58CheckDecode(s string) ([]byte, error) {
	n := new(big.Int)
	radix := big.NewInt(58)
	for _, c := range s {
		i := strings.IndexRune(base58Alphabet, c)
		if i < 0 {
			return nil, fmt.Errorf("invalid character %q", c)
		}
		n.Mul(n, radix)
		n.Add(n, big.NewInt(int64(i)))
	}
	zeros := 0
	for zeros < len(s) && s[zeros] == '1' {
		zeros++
	}
	decoded := append(make([]byte, zeros), n.Bytes()...)
	if len(decoded) < 5 {
		return nil, errors.New("address too short")
	}
	payload, checksum := decoded[:len(decoded)-4], decoded[len(decoded)-4:]
	sum := SHA256d(payload)
	if !bytes.Equal(sum[:4], checksum) {
		return nil, errors.New("checksum mismatch (mistyped address?)")
	}
	return payload, nil
}

// ---- Bech32 / Bech32m (BIP173, BIP350) ----

const bech32Charset = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"

const (
	bech32Const  = 1
	bech32mConst = 0x2bc830a3
)

func bech32Polymod(values []byte) uint32 {
	gen := [5]uint32{0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3}
	chk := uint32(1)
	for _, v := range values {
		top := chk >> 25
		chk = (chk&0x1ffffff)<<5 ^ uint32(v)
		for i := 0; i < 5; i++ {
			if (top>>uint(i))&1 == 1 {
				chk ^= gen[i]
			}
		}
	}
	return chk
}

// splitBech32 finds the separator of a bech32 string without validating it.
func splitBech32(s string) (hrp, data string, ok bool) {
	i := strings.LastIndexByte(s, '1')
	if i < 1 || i+7 > len(s) {
		return "", "", false
	}
	return s[:i], s[i+1:], true
}

// bech32Decode returns the human-readable part, the 5-bit data values
// (without checksum) and the checksum constant that matched.
func bech32Decode(s string) (hrp string, data []byte, constant uint32, err error) {
	if len(s) > 90 {
		return "", nil, 0, errors.New("address too long")
	}
	lower, upper := strings.ToLower(s), strings.ToUpper(s)
	if s != lower && s != upper {
		return "", nil, 0, errors.New("mixed upper and lower case")
	}
	s = lower
	hrp, dataPart, ok := splitBech32(s)
	if !ok {
		return "", nil, 0, errors.New("malformed address")
	}
	values := make([]byte, 0, len(hrp)*2+1+len(dataPart))
	for i := 0; i < len(hrp); i++ {
		if hrp[i] < 33 || hrp[i] > 126 {
			return "", nil, 0, errors.New("invalid character in prefix")
		}
		values = append(values, hrp[i]>>5)
	}
	values = append(values, 0)
	for i := 0; i < len(hrp); i++ {
		values = append(values, hrp[i]&31)
	}
	for i := 0; i < len(dataPart); i++ {
		v := strings.IndexByte(bech32Charset, dataPart[i])
		if v < 0 {
			return "", nil, 0, fmt.Errorf("invalid character %q", dataPart[i])
		}
		values = append(values, byte(v))
	}
	constant = bech32Polymod(values)
	if constant != bech32Const && constant != bech32mConst {
		return "", nil, 0, errors.New("checksum mismatch (mistyped address?)")
	}
	data = values[len(hrp)*2+1 : len(values)-6]
	return hrp, data, constant, nil
}

func segwitScript(addr string, net Network) ([]byte, error) {
	hrp, data, constant, err := bech32Decode(addr)
	if err != nil {
		return nil, err
	}
	if hrp != net.Bech32HRP {
		return nil, fmt.Errorf("address is not for the %s network", net.Name)
	}
	if len(data) == 0 {
		return nil, errors.New("malformed address")
	}
	version := data[0]
	if version > 16 {
		return nil, errors.New("invalid witness version")
	}
	program, err := convert5to8(data[1:])
	if err != nil {
		return nil, err
	}
	if len(program) < 2 || len(program) > 40 {
		return nil, errors.New("invalid witness program length")
	}
	if version == 0 {
		if constant != bech32Const {
			return nil, errors.New("checksum mismatch (mistyped address?)")
		}
		if len(program) != 20 && len(program) != 32 {
			return nil, errors.New("invalid witness program length")
		}
	} else if constant != bech32mConst {
		return nil, errors.New("checksum mismatch (mistyped address?)")
	}
	op := byte(0x00)
	if version > 0 {
		op = 0x50 + version
	}
	script := []byte{op, byte(len(program))}
	return append(script, program...), nil
}

// convert5to8 regroups 5-bit values into bytes, rejecting non-zero or
// over-long padding as BIP173 requires.
func convert5to8(in []byte) ([]byte, error) {
	var acc uint32
	var bits uint
	out := make([]byte, 0, len(in)*5/8)
	for _, v := range in {
		acc = acc<<5 | uint32(v)
		bits += 5
		for bits >= 8 {
			bits -= 8
			out = append(out, byte(acc>>bits))
		}
	}
	if bits >= 5 || acc&(1<<bits-1) != 0 {
		return nil, errors.New("invalid padding")
	}
	return out, nil
}
