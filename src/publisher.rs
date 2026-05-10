//! ZeroMQ PUB publisher (Bitcoin-style notification topics).

use anyhow::{Context, Result};
use blvm_protocol::wire::{serialize_block, serialize_tx};
use blvm_protocol::{Block, Hash, Transaction};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;
use tracing::{debug, info, warn};
use zeromq::{Context as ZmqContext, Socket, PUB};

use crate::config::ZmqConfig;

struct ZmqInner {
    _context: ZmqContext,
    hashblock_socket: Option<Socket>,
    hashtx_socket: Option<Socket>,
    rawblock_socket: Option<Socket>,
    rawtx_socket: Option<Socket>,
    sequence_socket: Option<Socket>,
}

/// ZMQ notification publisher (binds PUB sockets per configured endpoint).
///
/// All `send` calls are synchronized with a mutex so handlers stay `Send` under the Tokio
/// multithreaded runtime (libzmq PUB sockets are safe to use from one issuing thread at a time;
/// we serialize here).
pub struct ZmqPublisher {
    inner: Mutex<ZmqInner>,
    sequence: AtomicU32,
}

impl ZmqPublisher {
    pub fn new(config: &ZmqConfig) -> Result<Self> {
        let context = ZmqContext::new();

        let hashblock_socket = if let Some(ref endpoint) = config.hashblock {
            Some(Self::create_socket(&context, endpoint, "hashblock")?)
        } else {
            None
        };

        let hashtx_socket = if let Some(ref endpoint) = config.hashtx {
            Some(Self::create_socket(&context, endpoint, "hashtx")?)
        } else {
            None
        };

        let rawblock_socket = if let Some(ref endpoint) = config.rawblock {
            Some(Self::create_socket(&context, endpoint, "rawblock")?)
        } else {
            None
        };

        let rawtx_socket = if let Some(ref endpoint) = config.rawtx {
            Some(Self::create_socket(&context, endpoint, "rawtx")?)
        } else {
            None
        };

        let sequence_socket = if let Some(ref endpoint) = config.sequence {
            Some(Self::create_socket(&context, endpoint, "sequence")?)
        } else {
            None
        };

        Ok(Self {
            inner: Mutex::new(ZmqInner {
                _context: context,
                hashblock_socket,
                hashtx_socket,
                rawblock_socket,
                rawtx_socket,
                sequence_socket,
            }),
            sequence: AtomicU32::new(0),
        })
    }

    fn create_socket(context: &ZmqContext, endpoint: &str, topic: &str) -> Result<Socket> {
        let socket = context.socket(PUB)?;
        socket
            .bind(endpoint)
            .with_context(|| format!("Failed to bind ZMQ socket for {topic} to {endpoint}"))?;
        info!("ZMQ {} socket bound to {}", topic, endpoint);
        Ok(socket)
    }

    pub fn publish_hashblock(&self, block_hash: &Hash) -> Result<()> {
        let g = self.inner.lock().expect("zmq publisher mutex poisoned");
        if let Some(ref socket) = g.hashblock_socket {
            socket.send("hashblock", zeromq::SNDMORE)?;
            socket.send(block_hash.as_slice(), 0)?;
            debug!("Published hashblock notification: {:?}", block_hash);
        }
        Ok(())
    }

    pub fn publish_hashtx(&self, tx_hash: &Hash) -> Result<()> {
        let g = self.inner.lock().expect("zmq publisher mutex poisoned");
        if let Some(ref socket) = g.hashtx_socket {
            socket.send("hashtx", zeromq::SNDMORE)?;
            socket.send(tx_hash.as_slice(), 0)?;
            debug!("Published hashtx notification: {:?}", tx_hash);
        }
        Ok(())
    }

    pub fn publish_rawblock(&self, block: &Block) -> Result<()> {
        let g = self.inner.lock().expect("zmq publisher mutex poisoned");
        if let Some(ref socket) = g.rawblock_socket {
            let block_data = serialize_block(block).map_err(|e| anyhow::anyhow!("{e}"))?;
            socket.send("rawblock", zeromq::SNDMORE)?;
            socket.send(&block_data, 0)?;
            debug!(
                "Published rawblock notification: {} bytes",
                block_data.len()
            );
        }
        Ok(())
    }

    pub fn publish_rawtx(&self, tx: &Transaction) -> Result<()> {
        let g = self.inner.lock().expect("zmq publisher mutex poisoned");
        if let Some(ref socket) = g.rawtx_socket {
            let tx_data = serialize_tx(tx).map_err(|e| anyhow::anyhow!("{e}"))?;
            socket.send("rawtx", zeromq::SNDMORE)?;
            socket.send(&tx_data, 0)?;
            debug!("Published rawtx notification: {} bytes", tx_data.len());
        }
        Ok(())
    }

    pub fn publish_sequence(&self, tx_hash: &Hash, is_mempool_entry: bool) -> Result<()> {
        let g = self.inner.lock().expect("zmq publisher mutex poisoned");
        if let Some(ref socket) = g.sequence_socket {
            let sequence_num = self
                .sequence
                .fetch_add(1, Ordering::Relaxed)
                .wrapping_add(1);

            let mut data = Vec::with_capacity(33);
            data.push(if is_mempool_entry { 0x01 } else { 0x02 });
            data.extend_from_slice(tx_hash.as_slice());

            socket.send("sequence", zeromq::SNDMORE)?;
            socket.send(&data, 0)?;
            debug!(
                "Published sequence notification: seq={}, tx={:?}, entry={}",
                sequence_num, tx_hash, is_mempool_entry
            );
        }
        Ok(())
    }

    pub fn publish_block(&self, block: &Block, block_hash: &Hash) -> Result<()> {
        if let Err(e) = self.publish_hashblock(block_hash) {
            warn!("Failed to publish hashblock notification: {}", e);
        }
        if let Err(e) = self.publish_rawblock(block) {
            warn!("Failed to publish rawblock notification: {}", e);
        }
        Ok(())
    }

    pub fn publish_transaction(
        &self,
        tx: &Transaction,
        tx_hash: &Hash,
        is_mempool_entry: bool,
    ) -> Result<()> {
        if let Err(e) = self.publish_hashtx(tx_hash) {
            warn!("Failed to publish hashtx notification: {}", e);
        }
        if let Err(e) = self.publish_rawtx(tx) {
            warn!("Failed to publish rawtx notification: {}", e);
        }
        if let Err(e) = self.publish_sequence(tx_hash, is_mempool_entry) {
            warn!("Failed to publish sequence notification: {}", e);
        }
        Ok(())
    }
}
