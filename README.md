# blvm-zmq

First-party **blvm-node** module that binds **ZeroMQ PUB** sockets and publishes Bitcoin-compatible notifications:

| Topic      | When |
|------------|------|
| `hashblock` / `rawblock` | Connected block (`NewBlock` or `BlockMined`; `rawblock` fetches the block over IPC) |
| `hashtx` / `rawtx` / `sequence` | Mempool add (`MempoolTransactionAdded`) |
| `sequence` (removal) | Mempool remove (`MempoolTransactionRemoved`) |

Former in-process `zmq` support was **removed from `blvm-node`**; use this module instead.

This checkout targets BLVM v0.3.0-beta.6 (node 0.1.104). Use the rebuilt
module with that node, not the older v0.1.6 release binary. Module IPC
message types come from the pinned node and SDK dependencies. The publisher
does not open a database. Its manifest grants read-only blockchain and
mempool access plus event subscriptions.

Each notification has three frames: topic, payload, and a 32-bit
little-endian sequence counter for that publisher.

## Configure

In the module data directory (or node overrides under `[modules.blvm-zmq]`):

```toml
hashblock = "tcp://127.0.0.1:28332"
hashtx = "tcp://127.0.0.1:28333"
rawblock = "tcp://127.0.0.1:28334"
rawtx = "tcp://127.0.0.1:28335"
sequence = "tcp://127.0.0.1:28336"
```

Set a topic to `""` under `[modules.blvm-zmq]` to disable it, including topics
the node enables by default. On beta.6, use `hashblock` without raw topics:
the node can reject `rawtx` reads despite the manifest grant, and the shared
IPC client does not reliably route `rawblock` read responses while events are
arriving. Requires **libzmq** on the host (`libzmq3-dev` on Debian/Ubuntu,
`zeromq` on Arch).

```toml
[modules.blvm-zmq]
hashblock = "tcp://127.0.0.1:28332"
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
