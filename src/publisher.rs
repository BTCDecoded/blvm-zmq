//! ZeroMQ PUB publisher (Bitcoin-style notification topics).

use anyhow::{Context, Result};
use blvm_protocol::wire::{serialize_block_witnesses, serialize_tx};
use blvm_protocol::{Block, Hash, Transaction};
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};
use tracing::{debug, info, warn};
use zeromq::{Context as ZmqContext, PUB, Socket};

use crate::config::ZmqConfig;

struct ZmqInner {
    _context: ZmqContext,
    /// One PUB socket per distinct address. Topics that name the same address share it.
    sockets: HashMap<String, TopicPublisher>,
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

/// ZMQ notification publisher.
///
/// Each distinct address is bound once. Topics that use the same address share that
/// PUB socket, its sequence counter, and are separated by the topic frame.
///
/// All `send` calls are synchronized so the SDK can share the publisher across handlers.
pub struct ZmqPublisher {
    inner: Mutex<ZmqInner>,
    hashblock: Option<String>,
    hashtx: Option<String>,
    rawblock: Option<String>,
    rawtx: Option<String>,
    sequence_endpoint: Option<String>,
    sequence: AtomicU32,
}

impl ZmqPublisher {
    pub fn new(config: &ZmqConfig) -> Result<Self> {
        let context = ZmqContext::new();
        let hashblock = config.topic_bind(&config.hashblock).map(str::to_string);
        let hashtx = config.topic_bind(&config.hashtx).map(str::to_string);
        let rawblock = config.topic_bind(&config.rawblock).map(str::to_string);
        let rawtx = config.topic_bind(&config.rawtx).map(str::to_string);
        let sequence_endpoint = config.topic_bind(&config.sequence).map(str::to_string);
        let mut sockets = HashMap::new();
        for endpoint in [
            hashblock.as_deref(),
            hashtx.as_deref(),
            rawblock.as_deref(),
            rawtx.as_deref(),
            sequence_endpoint.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            if sockets.contains_key(endpoint) {
                debug!("ZMQ reusing socket for {endpoint}");
                continue;
            }
            sockets.insert(
                endpoint.to_string(),
                Self::create_socket(&context, endpoint)?,
            );
        }

        Ok(Self {
            inner: Mutex::new(ZmqInner {
                _context: context,
                sockets,
            }),
            hashblock,
            hashtx,
            rawblock,
            rawtx,
            sequence_endpoint,
            sequence: AtomicU32::new(0),
        })
    }

    fn create_socket(context: &ZmqContext, endpoint: &str) -> Result<TopicPublisher> {
        let socket = context.socket(PUB)?;
        socket
            .bind(endpoint)
            .with_context(|| format!("Failed to bind ZMQ socket to {endpoint}"))?;
        info!("ZMQ socket bound to {}", endpoint);
        Ok(TopicPublisher {
            socket,
            sequence: Cell::new(0),
        })
    }

    pub fn publish_hashblock(&self, block_hash: &Hash) -> Result<()> {
        let g = self.inner.lock().expect("zmq publisher mutex poisoned");
        if let Some(socket) = self.hashblock.as_ref().and_then(|ep| g.sockets.get(ep)) {
            socket.send("hashblock", block_hash.as_slice())?;
            debug!("Published hashblock notification: {:?}", block_hash);
        }
        Ok(())
    }

    pub(crate) fn has_rawtx(&self) -> bool {
        self.rawtx.is_some()
    }

    pub(crate) fn has_rawblock(&self) -> bool {
        self.rawblock.is_some()
    }

    pub fn publish_hashtx(&self, tx_hash: &Hash) -> Result<()> {
        let g = self.inner.lock().expect("zmq publisher mutex poisoned");
        if let Some(socket) = self.hashtx.as_ref().and_then(|ep| g.sockets.get(ep)) {
            socket.send("hashtx", tx_hash.as_slice())?;
            debug!("Published hashtx notification: {:?}", tx_hash);
        }
        Ok(())
    }

    pub fn publish_rawblock(&self, block: &Block) -> Result<()> {
        let g = self.inner.lock().expect("zmq publisher mutex poisoned");
        if let Some(socket) = self.rawblock.as_ref().and_then(|ep| g.sockets.get(ep)) {
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
        if let Some(socket) = self.rawtx.as_ref().and_then(|ep| g.sockets.get(ep)) {
            let tx_data = serialize_tx(tx).map_err(|e| anyhow::anyhow!("{e}"))?;
            socket.send("rawtx", &tx_data)?;
            debug!("Published rawtx notification: {} bytes", tx_data.len());
        }
        Ok(())
    }

    pub fn publish_sequence(&self, tx_hash: &Hash, is_mempool_entry: bool) -> Result<()> {
        let g = self.inner.lock().expect("zmq publisher mutex poisoned");
        if let Some(socket) = self
            .sequence_endpoint
            .as_ref()
            .and_then(|ep| g.sockets.get(ep))
        {
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
            .sockets
            .get(publisher.hashblock.as_deref().unwrap())
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

    #[test]
    fn shared_endpoint_binds_once() {
        let endpoint = format!("ipc:///tmp/bzmq-{}-one", std::process::id());
        let publisher = ZmqPublisher::new(&ZmqConfig {
            hashblock: Some(endpoint.clone()),
            hashtx: Some(endpoint.clone()),
            rawblock: Some(endpoint.clone()),
            rawtx: Some(endpoint),
            sequence: Some(format!("ipc:///tmp/bzmq-{}-one", std::process::id())),
            ..ZmqConfig::default()
        })
        .expect("one bind");
        assert_eq!(publisher.inner.lock().expect("lock").sockets.len(), 1);
    }

    #[test]
    fn distinct_endpoints_bind_separately() {
        let pid = std::process::id();
        let publisher = ZmqPublisher::new(&ZmqConfig {
            hashblock: Some(format!("ipc:///tmp/bzmq-{pid}-a")),
            hashtx: Some(format!("ipc:///tmp/bzmq-{pid}-b")),
            ..ZmqConfig::default()
        })
        .expect("two binds");
        assert_eq!(publisher.inner.lock().expect("lock").sockets.len(), 2);
    }

    #[test]
    fn endpoint_covers_every_topic() {
        let endpoint = format!("ipc:///tmp/bzmq-{}-all", std::process::id());
        let publisher = ZmqPublisher::new(&ZmqConfig {
            endpoint: Some(endpoint.clone()),
            ..ZmqConfig::default()
        })
        .expect("shared bind");
        assert_eq!(publisher.inner.lock().expect("lock").sockets.len(), 1);
        assert_eq!(publisher.hashblock.as_deref(), Some(endpoint.as_str()));
        assert_eq!(publisher.hashtx.as_deref(), Some(endpoint.as_str()));
        assert!(publisher.has_rawblock());
        assert!(publisher.has_rawtx());
        assert_eq!(
            publisher.sequence_endpoint.as_deref(),
            Some(endpoint.as_str())
        );
    }

    #[test]
    fn topic_key_overrides_endpoint() {
        let pid = std::process::id();
        let shared = format!("ipc:///tmp/bzmq-{pid}-s");
        let only = format!("ipc:///tmp/bzmq-{pid}-h");
        let publisher = ZmqPublisher::new(&ZmqConfig {
            endpoint: Some(shared.clone()),
            hashblock: Some(only.clone()),
            ..ZmqConfig::default()
        })
        .expect("override bind");
        assert_eq!(publisher.inner.lock().expect("lock").sockets.len(), 2);
        assert_eq!(publisher.hashblock.as_deref(), Some(only.as_str()));
        assert_eq!(publisher.hashtx.as_deref(), Some(shared.as_str()));
    }

    #[test]
    fn empty_topic_does_not_use_endpoint() {
        let endpoint = format!("ipc:///tmp/bzmq-{}-off", std::process::id());
        let publisher = ZmqPublisher::new(&ZmqConfig {
            endpoint: Some(endpoint.clone()),
            rawtx: Some(String::new()),
            sequence: Some(" ".into()),
            ..ZmqConfig::default()
        })
        .expect("bind");
        assert_eq!(publisher.inner.lock().expect("lock").sockets.len(), 1);
        assert!(publisher.has_rawblock());
        assert!(!publisher.has_rawtx());
        assert!(publisher.sequence_endpoint.is_none());
        assert_eq!(publisher.hashblock.as_deref(), Some(endpoint.as_str()));
    }
}
