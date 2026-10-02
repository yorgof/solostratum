# End-to-end tests

These tests answer one question: **does Bitcoin Core accept the blocks that
come out of solostratum?** They run real Bitcoin Core nodes in Docker, start
the real `solostratum` program against them, mine through its Stratum port,
and check the result by asking Core.

## Running

You need Docker and Go.

```sh
make e2e
```

It takes a few minutes. Everything runs on private `regtest` chains inside
containers that are removed afterwards; nothing touches a real network or
any node you run yourself.

By default the suite runs against Bitcoin Core 31 and 29. To choose other
versions:

```sh
make e2e E2E_CORE_IMAGES="bitcoin/bitcoin:30 bitcoin/bitcoin:28"
```

To run one scenario, or see the details:

```sh
go test -tags e2e -count=1 -v -run 'TestE2E/.*/FirmwareCode' ./e2e/
```

If a run is interrupted, `make e2e-clean` removes leftover containers.

## What is tested

Each scenario runs once per Bitcoin Core version.

| Scenario | What it proves |
|----------|----------------|
| `PayoutAddressTypes` | Blocks are accepted and pay the right amount to legacy, P2SH-segwit, bech32 and taproot addresses, with empty and small blocks. |
| `UsernameOverride` | A miner's username can redirect its reward; invalid, mistyped or wrong-network addresses fall back to the default. |
| `ManyTransactions` | A block with 300 transactions (deep merkle tree, multi-byte transaction count) is accepted. |
| `ConsecutiveBlocksAndFiles` | Several blocks in a row are accepted, each saved to its own file whose content is byte-for-byte the block Core stored. |
| `NewBlockFromNetwork` | When someone else finds a block, miners get new work within seconds and work on the old tip is refused. |
| `FirmwareCode` | A header assembled by the Bitaxe firmware's own C code yields an accepted block. |
| `Cpuminer` | An unrelated, long-established Stratum miner produces accepted blocks. |
| `NodeOutage` | A block found while the node is down is kept and delivered when it returns; during a long outage miners are disconnected and can reconnect afterwards. |
| `StartupChecks` | Wrong address, wrong network, wrong password and unreachable node are refused with a clear message; the status page is self-contained and read-only. |

## The three miners

A server and a test client written by the same person can share the same
misunderstanding, for example about byte order, and happily agree with each
other while producing invalid blocks. Bitcoin Core is the final judge in
every scenario, and three unrelated clients guard against that:

- **Simulated Bitaxe** (`internal/axesim`): sends the same messages in the
  same order as the Bitaxe firmware and shares no code with the server.
- **Firmware code** (`firmware/`): a tiny program that links the actual
  job-construction code of the open-source Bitaxe firmware
  ([ESP-Miner](https://github.com/bitaxeorg/ESP-Miner)), fetched at build
  time. The functions that decide which bytes get hashed on a real device
  are the ones that run here. The firmware is licensed under the
  GPL-3.0; it is downloaded when the test image is built and none of it is
  stored in or distributed with this repository.
- **cpuminer** (`cpuminer/`): pooler's `minerd`, built from its public
  source.

The cpuminer scenario runs its container with host networking, which works
out of the box on Linux. On Docker Desktop, enable host networking in the
settings or skip it with `-skip Cpuminer`.

## A note on regtest

On regtest almost every hash is a valid block, so these tests exercise
block construction and submission thoroughly but cannot exercise realistic
share difficulty. Share validation, difficulty adjustment, duplicate and
stale handling are covered by the unit tests in `internal/stratum`, which
use a harder target.
