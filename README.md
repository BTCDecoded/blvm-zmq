# blvm-zmq

First-party **blvm-node** module that binds **ZeroMQ PUB** sockets and publishes Bitcoin-compatible notifications:

| Topic      | When |
|------------|------|
| `hashblock` / `rawblock` | Connected block (`NewBlock` or `BlockMined`; `rawblock` fetches the block over IPC) |
| `hashtx` / `rawtx` / `sequence` | Mempool add (`MempoolTransactionAdded`) |
| `sequence` (removal) | Mempool remove (`MempoolTransactionRemoved`) |

Former in-process `zmq` support was **removed from `blvm-node`**; use this module instead.

Use a module build whose IPC dependencies match the BLVM node version.
For BLVM v0.3.0-beta.6 (node 0.1.104), use a v0.1.7 build rather than the
older v0.1.6 release binary. The publisher does not open a database.
Its manifest grants read-only blockchain and
mempool access plus event subscriptions.

Each notification has three frames: topic, payload, and a 32-bit
little-endian sequence counter for that socket. Topics on one address share
the socket and the counter.

## Configure

In the module data directory (or node overrides under `[modules.blvm-zmq]`):

```toml
endpoint = "tcp://127.0.0.1:28332"
```

`[config_defaults]` in `module.toml` is what a load uses when the operator
sets nothing. The node copies that table and does not name the keys.
`endpoint` binds one PUB socket for every topic. `hashblock`, `hashtx`,
`rawblock`, `rawtx`, and `sequence` are the subscribe topics. A topic key
binds that topic on its own socket. Set a topic to `""` to turn it off, which
also keeps it off when `endpoint` is set. Requires **libzmq** on the host
(`libzmq3-dev` on Debian/Ubuntu, `zeromq` on Arch).

```toml
[modules.blvm-zmq]
endpoint = "tcp://127.0.0.1:28332"
rawblock = ""
hashtx = ""
rawtx = ""
sequence = ""
```

## Build

```bash
cargo build --release --locked
```

## Install

- `loadmodule blvm-zmq` (with marketplace / registry), or
- copy the release binary + `module.toml` into `modules/blvm-zmq/`.

See `blvm-node` docs: `docs/ZMQ_NOTIFICATIONS.md` (updated for the module workflow).

## Releases and `[downloads]`

`module.toml` **`[downloads]`** is owned by **release CI** (URLs + SHA-256 after a successful finalize). Empty placeholders on `main` are normal until then—treat as **non-bootstrap** until filled. Policy: [Module registry](../../blvm/README.md#module-registry) in the **`blvm`** crate README.
