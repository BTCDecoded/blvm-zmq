# blvm-zmq

First-party **blvm-node** module that binds **ZeroMQ PUB** sockets and publishes Bitcoin-compatible notifications:

| Topic      | When |
|------------|------|
| `hashblock` | New block connected (`NewBlock` → `get_block` for `rawblock`) |
| `hashtx` / `rawtx` / `sequence` | Mempool add (`MempoolTransactionAdded`) |
| `sequence` (removal) | Mempool remove (`MempoolTransactionRemoved`) |

Former in-process `zmq` support was **removed from `blvm-node`**; use this module instead.

## Configure

In the module data directory (or node overrides under `[modules.blvm-zmq]`):

```toml
hashblock = "tcp://127.0.0.1:28332"
hashtx = "tcp://127.0.0.1:28333"
rawblock = "tcp://127.0.0.1:28334"
rawtx = "tcp://127.0.0.1:28335"
sequence = "tcp://127.0.0.1:28336"
```

Omit any topic you do not need. Requires **libzmq** on the host (`libzmq3-dev` on Debian/Ubuntu, `zeromq` on Arch).

## Build (monorepo)

```bash
cd blvm-zmq && cargo build
```

Patches in `Cargo.toml` point at sibling `blvm-*` crates; strip them for crates.io-only CI.

## Install

- `loadmodule blvm-zmq` (with marketplace / registry), or
- copy the release binary + `module.toml` into `modules/blvm-zmq/`.

See `blvm-node` docs: `docs/ZMQ_NOTIFICATIONS.md` (updated for the module workflow).
