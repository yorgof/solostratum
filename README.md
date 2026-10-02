# solostratum

Solo-mine Bitcoin with your own node. solostratum connects to your Bitcoin
Core node and hands out work to your miners (Bitaxe and other Stratum v1
devices). If one of them finds a block, the whole reward goes straight to
your address. No pool, no account, no fees.

- One small program and one text file of settings.
- Works with a node on the same computer, elsewhere on your network, or
  remote.
- Checks your settings and your node before it starts, and tells you in
  plain words what is wrong.
- Every block found is saved to disk before it is sent to the node.
- A simple status page shows your miners, hashrate and best shares.
- No external dependencies: Go standard library only.

> **Project status: new.** solostratum has been tested with real miners:
> on regtest, Bitcoin Core accepted the blocks found by a Bitaxe and a
> NerdQAxe++, and on testnet4 those two and an Avalon Nano 3 mined for an
> hour without a rejected share. Its block construction is validated by
> Bitcoin Core against live mainnet templates at every start. It has not yet
> found a block on mainnet; at home-miner hashrates nobody can wait for that
> to happen. Keep a public solo pool configured as your miner's fallback.

> Solo mining is a lottery. A single Bitaxe will, on average, find a block
> once in thousands of years. The status page shows the odds for your
> hashrate. Mine solo for fun and for the network, not for income.

## What you need

- A fully synced **Bitcoin Core** node whose RPC port you can reach. Any
  recent version works; versions 29 and 31 are tested automatically.
- A miner that speaks Stratum v1, such as a Bitaxe.
- A Bitcoin address of your own for the reward.

## Quick start

1. **Get the program.** Download the archive for your system from the
   [releases page](https://github.com/yorgof/solostratum/releases) and
   unpack it. Or build it yourself (needs [Go](https://go.dev/dl/) 1.23 or
   newer):

   ```sh
   git clone https://github.com/yorgof/solostratum.git
   cd solostratum
   make build
   ```

2. **Run it once.** It creates `solostratum.conf` next to itself and stops:

   ```sh
   ./solostratum
   ```

3. **Fill in the settings.** Open `solostratum.conf` in a text editor and
   set the four required values:

   ```ini
   node_url       = http://127.0.0.1:8332
   node_user      = your-rpc-user
   node_password  = your-rpc-password
   payout_address = your-bitcoin-address
   ```

4. **Run it again.** When you see `Ready`, it is serving miners:

   ```
   Node http://127.0.0.1:8332 is on mainnet at block 900000. Rewards go to bc1q...
   Self-test passed: the node accepts the blocks this program builds.
   Status page: http://192.168.0.20:3334
   Ready. Point your miners at stratum+tcp://192.168.0.20:3333
   ```

5. **Point your miner at it.** In the Bitaxe web interface set:

   | Setting      | Value                                         |
   |--------------|-----------------------------------------------|
   | Stratum host | the IP address shown after `stratum+tcp://`   |
   | Stratum port | `3333`                                        |
   | Stratum user | any name, for example `bitaxe1`               |
   | Password     | anything                                      |

   Keep a public solo pool as the miner's *fallback* pool. If your node
   goes down, solostratum disconnects the miners so they switch to the
   fallback, and they come back by themselves afterwards.

To only verify your settings and node without starting, run
`./solostratum -check`.

## Setting up the node

solostratum talks to Bitcoin Core's RPC interface. In `bitcoin.conf`:

```ini
server=1

# Login for solostratum. Generate the line with Bitcoin Core's
# share/rpcauth/rpcauth.py script, or use rpcuser= and rpcpassword=.
rpcauth=solostratum:<salt>$<hash>

# Only needed when solostratum runs on a different computer than the node:
rpcbind=0.0.0.0
rpcallowip=192.168.0.0/24
```

Restart the node after changing the file.

If solostratum runs on the same computer as the node you can skip the login
and use the node's cookie file instead: leave `node_user` and
`node_password` empty and set `node_cookie_file` to the `.cookie` file in
the node's data folder.

solostratum only ever calls three RPC methods, so you can lock its login
down if you like:

```ini
rpcwhitelist=solostratum:getblockchaininfo,getblocktemplate,submitblock
```

A pruned node works. The node must be fully synced and connected to peers;
solostratum waits until it is.

For a node reached over the internet, put it behind a VPN or an HTTPS
reverse proxy and use an `https://` address. Never expose the RPC port
directly.

## Settings

All settings live in `solostratum.conf`, next to the program. Use
`-config /path/to/file` to keep it somewhere else.

| Setting            | Default           | Meaning |
|--------------------|-------------------|---------|
| `node_url`         | `http://127.0.0.1:8332` | Address of the node's RPC port. |
| `node_user`, `node_password` | –       | RPC login. |
| `node_cookie_file` | –                 | Alternative to the login for a local node. |
| `payout_address`   | –                 | Where block rewards go. Any address type. |
| `stratum_listen`   | `0.0.0.0:3333`    | Where miners connect. |
| `status_listen`    | `0.0.0.0:3334`    | Status page. Empty turns it off; `127.0.0.1:3334` makes it local only. |
| `coinbase_tag`     | `/solostratum/`   | Short text written into blocks you find. |
| `start_difficulty` | `1024`            | Share difficulty for a new miner. Adjusts automatically. |
| `min_difficulty`   | `0.001`           | Lowest share difficulty. |
| `blocks_dir`       | `blocks`          | Folder for found blocks. |

A typo in a setting name is reported with its line number instead of being
silently ignored.

### A different payout address per miner

A miner can send its reward to its own address by using that address as its
Stratum username: `bc1q...` or `bc1q....workername`. The address is fully
validated, including its checksum. A username that is not a valid address
for the node's network is treated as a plain name and the reward goes to
`payout_address`. The status page shows where each miner pays, so you can
confirm it.

## Status page

Open `http://<computer running solostratum>:3334` in a browser. It shows
the node, the block being worked on, and every miner with its hashrate,
difficulty, shares and best share. It is read-only and loads nothing from
the internet. The same data is available as JSON at `/api/status`.

The page has no login. Anyone on your network can view it; nobody can
change anything through it.

## When you find a block

1. The complete block is written to the `blocks` folder, in its own file
   named after its height, the time and its hash, for example
   `block-900000-20260102T030405.000000006Z-0000...beef.hex`. Files are
   never overwritten.
2. Only then is it sent to your node. If the node cannot be reached, it is
   retried every two seconds for thirty minutes.
3. The node's answer is written next to it in a `.result` file and shown on
   the status page.

If everything else fails you can submit the block by hand:

```sh
bitcoin-cli submitblock "$(cat blocks/block-....hex)"
```

## How it stays correct

- **At every start** solostratum builds a complete block from the node's
  current template and asks the node to validate it (everything except the
  proof of work). If the node objects, mining does not start.
- **Unit tests** check the Bitcoin building blocks against real mainnet
  blocks and the official address test vectors.
- **End-to-end tests** start real Bitcoin Core nodes in Docker, mine
  through the Stratum port, and assert that Core accepts the blocks and
  that the coinbase pays the right address. They use three independent
  miners: a simulated Bitaxe, the Bitaxe firmware's own header-building
  code compiled for the test, and cpuminer. See [e2e/README.md](e2e/README.md).

```sh
make test   # fast, no Docker
make e2e    # needs Docker, takes a few minutes
```

To try your own setup without mining hardware there is a CPU miner that
behaves like a Bitaxe on the wire:

```sh
go run ./cmd/axesim -addr 127.0.0.1:3333 -user test
```

## Running with Docker

```sh
docker build -t solostratum .
mkdir data && cp internal/config/solostratum.conf.example data/solostratum.conf
# edit data/solostratum.conf, then:
docker run -d --name solostratum --restart unless-stopped \
    -p 3333:3333 -p 3334:3334 -v "$PWD/data:/data" solostratum
```

Inside a container `127.0.0.1` is the container itself, so `node_url` must
use the node's real address. The image has a health check: Docker reports
the container as unhealthy when it has no current work from the node.

## Good to know

- **Keep it on your own network.** The Stratum port has no password and is
  meant for your own miners. Do not forward it from the internet.
- **Networks:** mainnet, testnet4, testnet3 and regtest. Signet cannot be
  mined this way, because its blocks must be signed by its operators.
- **Protocol:** Stratum v1 with version rolling (BIP310 / BIP320), which is
  what Bitaxe uses. Stratum v2 is not supported.
- **No payout splitting.** Each block pays one address. This is a personal
  solo-mining server, not a pool.
- Work is refreshed every 30 seconds to pick up new transactions, and
  immediately when a new block arrives.

## License

[MIT](LICENSE)
