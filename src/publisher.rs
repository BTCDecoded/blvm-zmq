//! ZeroMQ PUB publisher (Bitcoin-style notification topics).

use anyhow::{Context, Result};
use blvm_protocol::wire::{serialize_block_witnesses, serialize_tx};
use blvm_protocol::{Block, Hash, Transaction};
use std::cell::Cell;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};
use tracing::{debug, info, warn};
use zeromq::{Context as ZmqContext, PUB, Socket};

use crate::config::ZmqConfig;

struct ZmqInner {
    _context: ZmqContext,
    hashblock_socket: Option<TopicPublisher>,
    hashtx_socket: Option<TopicPublisher>,
    rawblock_socket: Option<TopicPublisher>,
    rawtx_socket: Option<TopicPublisher>,
    sequence_socket: Option<TopicPublisher>,
}

struct TopicPublisher {
    socket: Socket,
    sequence: Cell<u32>,
}

impl TopicPublisher {
    fn send(&self, topic: &str, payload: &[u8]) -> Result<()> {
        let sequence = self.sequence.get();
        self.socket
            .send_multipart([topic.as_bytes(), payload, &sequence.to_le_bytes()], 0)?;
        self.sequence.set(sequence.wrapping_add(1));
        Ok(())
    }
}

/// ZMQ notification publisher (binds PUB sockets per configured endpoint).
///
/// All `send` calls are synchronized so the SDK can share the publisher across handlers.
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

    fn create_socket(context: &ZmqContext, endpoint: &str, topic: &str) -> Result<TopicPublisher> {
        let socket = context.socket(PUB)?;
        socket
            .bind(endpoint)
            .with_context(|| format!("Failed to bind ZMQ socket for {topic} to {endpoint}"))?;
        info!("ZMQ {} socket bound to {}", topic, endpoint);
        Ok(TopicPublisher {
            socket,
            sequence: Cell::new(0),
        })
    }

    pub fn publish_hashblock(&self, block_hash: &Hash) -> Result<()> {
        let g = self.inner.lock().expect("zmq publisher mutex poisoned");
        if let Some(ref socket) = g.hashblock_socket {
            socket.send("hashblock", block_hash.as_slice())?;
            debug!("Published hashblock notification: {:?}", block_hash);
        }
        Ok(())
    }

    pub(crate) fn has_rawtx(&self) -> bool {
        self.inner
            .lock()
            .expect("zmq publisher mutex poisoned")
            .rawtx_socket
            .is_some()
    }

    pub(crate) fn has_rawblock(&self) -> bool {
        self.inner
            .lock()
            .expect("zmq publisher mutex poisoned")
            .rawblock_socket
            .is_some()
    }

    pub fn publish_hashtx(&self, tx_hash: &Hash) -> Result<()> {
        let g = self.inner.lock().expect("zmq publisher mutex poisoned");
        if let Some(ref socket) = g.hashtx_socket {
            socket.send("hashtx", tx_hash.as_slice())?;
            debug!("Published hashtx notification: {:?}", tx_hash);
        }
        Ok(())
    }

    pub fn publish_rawblock(&self, block: &Block) -> Result<()> {
        let g = self.inner.lock().expect("zmq publisher mutex poisoned");
        if let Some(ref socket) = g.rawblock_socket {
            let block_data =
                serialize_block_witnesses(block, &[]).map_err(|e| anyhow::anyhow!("{e}"))?;
            socket.send("rawblock", &block_data)?;
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
            socket.send("rawtx", &tx_data)?;
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

            socket.send("sequence", &data)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn publishes_hashblock_on_loopback() -> Result<()> {
        let publisher = ZmqPublisher::new(&ZmqConfig {
            hashblock: Some("tcp://127.0.0.1:*".into()),
            ..ZmqConfig::default()
        })?;
        let endpoint = publisher
            .inner
            .lock()
            .unwrap()
            .hashblock_socket
            .as_ref()
            .unwrap()
            .socket
            .get_last_endpoint()?
            .expect("loopback endpoint is UTF-8");
        let context = ZmqContext::new();
        let subscriber = context.socket(zeromq::SUB)?;
        subscriber.set_subscribe(b"hashblock")?;
        subscriber.set_rcvtimeo(50)?;
        subscriber.connect(&endpoint)?;

        let hash = [42; 32];
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            publisher.publish_hashblock(&hash)?;
            match subscriber.recv_multipart(0) {
                Ok(frames) => {
                    assert_eq!(frames.len(), 3);
                    assert_eq!(frames[0], b"hashblock");
                    assert_eq!(frames[1], hash);
                    let sequence = u32::from_le_bytes(frames[2].as_slice().try_into()?);
                    publisher.publish_hashblock(&hash)?;
                    assert_eq!(
                        subscriber.recv_multipart(0)?[2],
                        sequence.wrapping_add(1).to_le_bytes()
                    );
                    return Ok(());
                }
                Err(zeromq::Error::EAGAIN) if Instant::now() < deadline => continue,
                Err(error) => return Err(error).context("receiving hashblock notification"),
            }
        }
    }
}
