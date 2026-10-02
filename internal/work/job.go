// Package work turns Bitcoin Core block templates into Stratum jobs and
// assembles complete blocks from solved shares.
package work

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"sync"
	"time"

	"github.com/yorgof/solostratum/internal/btc"
	"github.com/yorgof/solostratum/internal/rpc"
)

const (
	// Extranonce1Size is the per-connection part of the extranonce.
	Extranonce1Size = 4
	// Extranonce2Size is the part the miner iterates.
	Extranonce2Size = 4

	coinbaseTxVersion = 2
	// coinbaseSequence is not final, so the lock time below is enforced.
	coinbaseSequence = 0xfffffffe
)

// Job is one unit of work derived from a block template. Everything that is
// shared between miners lives here; the payout output differs per miner and
// is supplied when the coinbase is built.
type Job struct {
	ID        string
	Seq       uint64 // increases with every job; later jobs supersede earlier ones
	Height    int64
	Version   uint32
	PrevHash  btc.Hash
	Bits      uint32
	Target    *big.Int // network target
	NetDiff   float64  // network difficulty
	CurTime   uint32
	MinTime   uint32
	Value     int64 // subsidy plus fees, in satoshi
	Branches  []btc.Hash
	TxCount   int // transactions excluding the coinbase
	Created   time.Time
	CleanJobs bool // true when this job is for a new chain tip

	coinbase1         []byte // up to and including the extranonce push opcode
	scriptTail        []byte // rest of the coinbase script, after the extranonce
	witnessCommitment []byte // output script, nil before segwit activation
	txData            [][]byte

	mu     sync.Mutex
	shares map[shareKey]struct{}
}

type shareKey struct {
	extranonce1 [Extranonce1Size]byte
	extranonce2 [Extranonce2Size]byte
	ntime       uint32
	nonce       uint32
	version     uint32
}

// NewJob builds a job from a block template.
func NewJob(id string, t *rpc.BlockTemplate, tag string, now time.Time) (*Job, error) {
	job, _, err := newJob(id, t, tag, now, nil)
	return job, err
}

// newJob builds a job, taking transaction bytes from cache where it already
// holds them. Successive templates share almost all of their transactions,
// so reusing the bytes lets many jobs for the same tip stay in memory at the
// cost of little more than one. It returns the cache for the next call.
func newJob(id string, t *rpc.BlockTemplate, tag string, now time.Time, cache map[btc.Hash][]byte) (*Job, map[btc.Hash][]byte, error) {
	job, txids, err := buildJob(id, t, tag, now, cache)
	if err != nil {
		return nil, cache, err
	}
	next := make(map[btc.Hash][]byte, len(txids))
	for i, txid := range txids {
		next[txid] = job.txData[i]
	}
	return job, next, nil
}

func buildJob(id string, t *rpc.BlockTemplate, tag string, now time.Time, cache map[btc.Hash][]byte) (*Job, []btc.Hash, error) {
	for _, rule := range t.Rules {
		// A leading "!" marks a rule the client must understand to mine.
		if len(rule) > 0 && rule[0] == '!' && rule != "!segwit" {
			return nil, nil, fmt.Errorf("the node requires consensus rule %q, which this program does not support", rule[1:])
		}
	}
	prev, err := btc.HashFromDisplayHex(t.PreviousBlockHash)
	if err != nil {
		return nil, nil, fmt.Errorf("template previousblockhash: %w", err)
	}
	bits, err := strconv.ParseUint(t.Bits, 16, 32)
	if err != nil {
		return nil, nil, fmt.Errorf("template bits %q: %w", t.Bits, err)
	}
	if t.Height < 0 || t.CoinbaseValue < 0 || t.CurTime <= 0 || t.CurTime > 0xffffffff {
		return nil, nil, errors.New("template has out-of-range values")
	}
	target := btc.CompactToTarget(uint32(bits))
	if target.Sign() <= 0 {
		return nil, nil, fmt.Errorf("template bits %q give an invalid target", t.Bits)
	}

	j := &Job{
		ID:        id,
		Height:    t.Height,
		Version:   t.Version,
		PrevHash:  prev,
		Bits:      uint32(bits),
		Target:    target,
		NetDiff:   btc.DifficultyFromTarget(target),
		CurTime:   uint32(t.CurTime),
		MinTime:   uint32(max(t.MinTime, 0)),
		Value:     t.CoinbaseValue,
		TxCount:   len(t.Transactions),
		Created:   now,
		shares:    make(map[shareKey]struct{}),
		txData:    make([][]byte, len(t.Transactions)),
		CleanJobs: false,
	}

	txids := make([]btc.Hash, len(t.Transactions))
	for i, tx := range t.Transactions {
		if txids[i], err = btc.HashFromDisplayHex(tx.TxID); err != nil {
			return nil, nil, fmt.Errorf("template transaction %d: %w", i, err)
		}
		// The same txid can carry different witness data, so the cached
		// bytes are only reused when their length matches.
		if data, ok := cache[txids[i]]; ok && len(data)*2 == len(tx.Data) {
			j.txData[i] = data
			continue
		}
		if j.txData[i], err = hex.DecodeString(tx.Data); err != nil {
			return nil, nil, fmt.Errorf("template transaction %d: %w", i, err)
		}
	}
	j.Branches = btc.MerkleBranches(txids)

	if t.WitnessCommitment != "" {
		if j.witnessCommitment, err = hex.DecodeString(t.WitnessCommitment); err != nil {
			return nil, nil, fmt.Errorf("template witness commitment: %w", err)
		}
	}

	// Coinbase transaction, in the serialization without witness data: that
	// is the form whose hash is the transaction id, and so the form miners
	// must hash.
	//
	//   version | 1 input | null outpoint | script length |
	//   script: <height> <extranonce1 extranonce2> <tag> |
	//   sequence | outputs | lock time
	script := btc.AppendHeightPush(nil, t.Height)
	script = append(script, Extranonce1Size+Extranonce2Size) // push opcode for the extranonce
	extranonceOffset := len(script)
	j.scriptTail = btc.AppendPush(nil, []byte(tag))
	scriptLen := extranonceOffset + Extranonce1Size + Extranonce2Size + len(j.scriptTail)
	if scriptLen > 100 {
		return nil, nil, errors.New("coinbase script too long")
	}

	cb := binary.LittleEndian.AppendUint32(nil, coinbaseTxVersion)
	cb = append(cb, 0x01)                // input count
	cb = append(cb, make([]byte, 32)...) // null previous output hash
	cb = append(cb, 0xff, 0xff, 0xff, 0xff)
	cb = btc.AppendVarInt(cb, uint64(scriptLen))
	j.coinbase1 = append(cb, script...)
	return j, txids, nil
}

// Coinbase1 returns the part of the coinbase transaction before the
// extranonce.
func (j *Job) Coinbase1() []byte { return j.coinbase1 }

// Coinbase2 returns the part of the coinbase transaction after the
// extranonce, paying the block reward to payoutScript.
func (j *Job) Coinbase2(payoutScript []byte) []byte {
	b := append([]byte(nil), j.scriptTail...)
	b = binary.LittleEndian.AppendUint32(b, coinbaseSequence)

	outputs := uint64(1)
	if j.witnessCommitment != nil {
		outputs++
	}
	b = btc.AppendVarInt(b, outputs)
	b = binary.LittleEndian.AppendUint64(b, uint64(j.Value))
	b = btc.AppendVarInt(b, uint64(len(payoutScript)))
	b = append(b, payoutScript...)
	if j.witnessCommitment != nil {
		b = binary.LittleEndian.AppendUint64(b, 0)
		b = btc.AppendVarInt(b, uint64(len(j.witnessCommitment)))
		b = append(b, j.witnessCommitment...)
	}
	// Lock time height-1 makes the coinbase unique per height by
	// construction and is valid in a block at this height.
	return binary.LittleEndian.AppendUint32(b, uint32(max(j.Height-1, 0)))
}

// Coinbase returns the complete coinbase transaction without witness data.
func (j *Job) Coinbase(payoutScript, extranonce1, extranonce2 []byte) []byte {
	cb2 := j.Coinbase2(payoutScript)
	b := make([]byte, 0, len(j.coinbase1)+len(extranonce1)+len(extranonce2)+len(cb2))
	b = append(b, j.coinbase1...)
	b = append(b, extranonce1...)
	b = append(b, extranonce2...)
	return append(b, cb2...)
}

// Header builds the block header for a share.
func (j *Job) Header(coinbase []byte, version, ntime, nonce uint32) [80]byte {
	root := btc.MerkleRootFromBranches(btc.SHA256d(coinbase), j.Branches)
	return btc.Header(version, j.PrevHash, root, ntime, j.Bits, nonce)
}

// Block serializes the full block for a solved header. coinbase is the
// transaction without witness data, as returned by Coinbase.
func (j *Job) Block(header [80]byte, coinbase []byte) []byte {
	size := 80 + 9 + len(coinbase) + 40
	for _, d := range j.txData {
		size += len(d)
	}
	b := make([]byte, 0, size)
	b = append(b, header[:]...)
	b = btc.AppendVarInt(b, uint64(len(j.txData)+1))
	if j.witnessCommitment == nil {
		b = append(b, coinbase...)
	} else {
		// A block that commits to witness data needs the coinbase in the
		// witness serialization, carrying the 32-byte reserved value.
		body, lockTime := coinbase[4:len(coinbase)-4], coinbase[len(coinbase)-4:]
		b = append(b, coinbase[:4]...)
		b = append(b, 0x00, 0x01) // segwit marker and flag
		b = append(b, body...)
		b = append(b, 0x01, 0x20) // one witness item of 32 bytes
		b = append(b, make([]byte, 32)...)
		b = append(b, lockTime...)
	}
	for _, d := range j.txData {
		b = append(b, d...)
	}
	return b
}

// MarkShare records a share and reports whether it was new. Miners must not
// be credited twice for the same work.
func (j *Job) MarkShare(extranonce1, extranonce2 []byte, ntime, nonce, version uint32) bool {
	k := shareKey{ntime: ntime, nonce: nonce, version: version}
	copy(k.extranonce1[:], extranonce1)
	copy(k.extranonce2[:], extranonce2)
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, dup := j.shares[k]; dup {
		return false
	}
	j.shares[k] = struct{}{}
	return true
}

// StratumPrevHash returns the previous block hash in the word-swapped form
// Stratum v1 uses.
func (j *Job) StratumPrevHash() string {
	return hex.EncodeToString(btc.SwapWords(j.PrevHash[:]))
}
