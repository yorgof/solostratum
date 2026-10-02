// Package btc holds the small set of Bitcoin primitives the bridge needs:
// hashing, serialization helpers, proof-of-work targets, merkle branches and
// address decoding. It has no dependencies outside the standard library.
package btc

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
)

// Hash is a 32-byte hash in internal byte order, i.e. the order in which it
// is serialized on the wire. Block explorers and Bitcoin Core's RPC show
// hashes with the bytes reversed.
type Hash [32]byte

// SHA256d returns SHA256(SHA256(data)).
func SHA256d(data []byte) Hash {
	first := sha256.Sum256(data)
	return sha256.Sum256(first[:])
}

// String returns the hash the way Bitcoin Core displays it (byte-reversed hex).
func (h Hash) String() string {
	return hex.EncodeToString(Reverse(h[:]))
}

// HashFromDisplayHex parses a hash as shown by Bitcoin Core's RPC.
func HashFromDisplayHex(s string) (Hash, error) {
	var h Hash
	b, err := hex.DecodeString(s)
	if err != nil {
		return h, fmt.Errorf("invalid hash %q: %w", s, err)
	}
	if len(b) != 32 {
		return h, fmt.Errorf("invalid hash %q: want 32 bytes, got %d", s, len(b))
	}
	copy(h[:], Reverse(b))
	return h, nil
}

// Reverse returns a reversed copy of b.
func Reverse(b []byte) []byte {
	out := make([]byte, len(b))
	for i, v := range b {
		out[len(b)-1-i] = v
	}
	return out
}

// SwapWords returns a copy of b with the bytes of every 4-byte word reversed.
// Stratum v1 transmits the previous block hash in this form.
func SwapWords(b []byte) []byte {
	out := make([]byte, len(b))
	for i := 0; i+4 <= len(b); i += 4 {
		out[i], out[i+1], out[i+2], out[i+3] = b[i+3], b[i+2], b[i+1], b[i]
	}
	return out
}

// AppendVarInt appends n in Bitcoin's variable-length integer encoding.
func AppendVarInt(b []byte, n uint64) []byte {
	switch {
	case n < 0xfd:
		return append(b, byte(n))
	case n <= 0xffff:
		return binary.LittleEndian.AppendUint16(append(b, 0xfd), uint16(n))
	case n <= 0xffffffff:
		return binary.LittleEndian.AppendUint32(append(b, 0xfe), uint32(n))
	default:
		return binary.LittleEndian.AppendUint64(append(b, 0xff), n)
	}
}

// AppendPush appends a script push of data using the smallest direct push
// opcode. It supports pushes of up to 255 bytes, which is more than a
// coinbase script can hold.
func AppendPush(b, data []byte) []byte {
	switch {
	case len(data) < 0x4c:
		b = append(b, byte(len(data)))
	case len(data) <= 0xff:
		b = append(b, 0x4c, byte(len(data)))
	default:
		panic("btc: push too large")
	}
	return append(b, data...)
}

// AppendHeightPush appends the block height the way BIP34 requires it at the
// start of the coinbase script: exactly what Bitcoin Core's
// "CScript() << height" produces.
func AppendHeightPush(b []byte, height int64) []byte {
	switch {
	case height == 0:
		return append(b, 0x00) // OP_0
	case height >= 1 && height <= 16:
		return append(b, 0x50+byte(height)) // OP_1 .. OP_16
	}
	// Minimal little-endian script number; a zero byte is added when the top
	// bit is set so the value is not read as negative.
	var num []byte
	for v := height; v > 0; v >>= 8 {
		num = append(num, byte(v))
	}
	if num[len(num)-1]&0x80 != 0 {
		num = append(num, 0x00)
	}
	return AppendPush(b, num)
}

// diff1 is the target for difficulty 1 (compact form 0x1d00ffff).
var diff1 = new(big.Int).Lsh(big.NewInt(0xffff), 208)

// maxTarget is the largest value a 256-bit hash can take.
var maxTarget = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))

// CompactToTarget expands the compact "bits" encoding of a block header into
// the full 256-bit target.
func CompactToTarget(bits uint32) *big.Int {
	exponent := bits >> 24
	mantissa := big.NewInt(int64(bits & 0x007fffff))
	if bits&0x00800000 != 0 { // negative targets are invalid; treat as zero
		return new(big.Int)
	}
	if exponent <= 3 {
		return mantissa.Rsh(mantissa, uint(8*(3-exponent)))
	}
	return mantissa.Lsh(mantissa, uint(8*(exponent-3)))
}

// HashToInt interprets a hash as the 256-bit number that is compared against
// a target.
func HashToInt(h Hash) *big.Int {
	return new(big.Int).SetBytes(Reverse(h[:]))
}

// DifficultyFromTarget converts a target to pool difficulty (difficulty 1 is
// the 0x1d00ffff target).
func DifficultyFromTarget(target *big.Int) float64 {
	if target.Sign() <= 0 {
		return math.Inf(1)
	}
	q := new(big.Float).Quo(new(big.Float).SetInt(diff1), new(big.Float).SetInt(target))
	f, _ := q.Float64()
	return f
}

// TargetFromDifficulty converts a pool difficulty to the target a share hash
// must not exceed.
func TargetFromDifficulty(difficulty float64) *big.Int {
	if difficulty <= 0 || math.IsNaN(difficulty) {
		return new(big.Int).Set(maxTarget)
	}
	q := new(big.Float).SetPrec(300).Quo(new(big.Float).SetPrec(300).SetInt(diff1), big.NewFloat(difficulty))
	t, _ := q.Int(nil)
	if t.Cmp(maxTarget) > 0 {
		return new(big.Int).Set(maxTarget)
	}
	return t
}

// MerkleBranches returns the hashes a miner needs to turn its coinbase
// transaction id into the block's merkle root. txids are the ids of every
// other transaction in the block, in block order.
func MerkleBranches(txids []Hash) []Hash {
	var branches []Hash
	// level holds the nodes to the right of the coinbase path.
	level := txids
	for len(level) > 0 {
		branches = append(branches, level[0])
		rest := level[1:]
		if len(rest) == 0 {
			break
		}
		// The full row (coinbase path + level) is duplicated at the end when
		// its length is odd, which is exactly when rest has odd length.
		next := make([]Hash, 0, (len(rest)+1)/2)
		for i := 0; i < len(rest); i += 2 {
			right := rest[i]
			if i+1 < len(rest) {
				right = rest[i+1]
			}
			next = append(next, hashPair(rest[i], right))
		}
		level = next
	}
	return branches
}

// MerkleRootFromBranches folds a coinbase transaction id with its branches.
func MerkleRootFromBranches(coinbaseTxID Hash, branches []Hash) Hash {
	root := coinbaseTxID
	for _, b := range branches {
		root = hashPair(root, b)
	}
	return root
}

// MerkleRoot computes the merkle root of a full list of transaction ids. It
// is only used to cross-check MerkleBranches.
func MerkleRoot(txids []Hash) Hash {
	if len(txids) == 0 {
		return Hash{}
	}
	level := append([]Hash(nil), txids...)
	for len(level) > 1 {
		if len(level)%2 == 1 {
			level = append(level, level[len(level)-1])
		}
		next := make([]Hash, 0, len(level)/2)
		for i := 0; i < len(level); i += 2 {
			next = append(next, hashPair(level[i], level[i+1]))
		}
		level = next
	}
	return level[0]
}

func hashPair(a, b Hash) Hash {
	var buf [64]byte
	copy(buf[:32], a[:])
	copy(buf[32:], b[:])
	return SHA256d(buf[:])
}

// Header serializes an 80-byte block header.
func Header(version uint32, prev, merkleRoot Hash, ntime, bits, nonce uint32) [80]byte {
	var h [80]byte
	binary.LittleEndian.PutUint32(h[0:], version)
	copy(h[4:], prev[:])
	copy(h[36:], merkleRoot[:])
	binary.LittleEndian.PutUint32(h[68:], ntime)
	binary.LittleEndian.PutUint32(h[72:], bits)
	binary.LittleEndian.PutUint32(h[76:], nonce)
	return h
}
