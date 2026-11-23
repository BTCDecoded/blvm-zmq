//! Event handlers → ZMQ PUB (subscribe_events + read_blockchain for fetch).

use blvm_node::module::ipc::protocol::{EventMessage, EventPayload};
use blvm_protocol::Hash;
use blvm_sdk::module::prelude::*;
use blvm_sdk_macros::module;
use std::sync::{Arc, Mutex};
use tracing::warn;

use crate::publisher::ZmqPublisher;

#[derive(Clone)]
pub struct ZmqModule {
    publisher: Arc<ZmqPublisher>,
    last_block: Arc<Mutex<Option<Hash>>>,
}

impl ZmqModule {
    pub fn new(publisher: Arc<ZmqPublisher>) -> Self {
        Self {
            publisher,
            last_block: Arc::new(Mutex::new(None)),
        }
    }
}

#[module]
impl ZmqModule {
    #[on_event(NewBlock, BlockMined)]
    async fn on_new_block(
        &self,
        event: &EventMessage,
        ctx: &InvocationContext,
    ) -> Result<(), ModuleError> {
        let api = ctx
            .node_api()
            .ok_or_else(|| ModuleError::Other("blvm-zmq: node_api required".to_string()))?;
        let block_hash = match event.payload {
            EventPayload::NewBlock { block_hash, .. }
            | EventPayload::BlockMined { block_hash, .. } => block_hash,
            _ => return Ok(()),
        };
        {
            // A locally mined block can emit both events for the same tip.
            let mut last = self.last_block.lock().expect("last block mutex poisoned");
            if *last == Some(block_hash) {
                return Ok(());
            }
            *last = Some(block_hash);
        }
        if let Err(e) = self.publisher.publish_hashblock(&block_hash) {
            warn!("blvm-zmq publish_hashblock: {}", e);
        }
        if self.publisher.has_rawblock() {
            match api.get_block(&block_hash).await {
                Ok(Some(block)) => {
                    if let Err(e) = self.publisher.publish_rawblock(&block) {
                        warn!("blvm-zmq publish_rawblock: {}", e);
                    }
                }
                Ok(None) => warn!("blvm-zmq block not available: {:?}", block_hash),
                Err(e) => warn!("blvm-zmq get_block {:?}: {}", block_hash, e),
            }
        }
        Ok(())
    }

    /// Mempool add: hashtx, rawtx, sequence(entry) — matches former in-process ZMQ path.
    #[on_event(MempoolTransactionAdded)]
    async fn on_mempool_added(
        &self,
        event: &EventMessage,
        ctx: &InvocationContext,
    ) -> Result<(), ModuleError> {
        let api = ctx
            .node_api()
            .ok_or_else(|| ModuleError::Other("blvm-zmq: node_api required".to_string()))?;
        if let EventPayload::MempoolTransactionAdded { tx_hash, .. } = event.payload {
            if !self.publisher.has_rawtx() {
                if let Err(e) = self.publisher.publish_hashtx(&tx_hash) {
                    warn!("blvm-zmq hashtx: {}", e);
                }
                if let Err(e) = self.publisher.publish_sequence(&tx_hash, true) {
                    warn!("blvm-zmq sequence: {}", e);
                }
                return Ok(());
            }
            match api.get_mempool_transaction(&tx_hash).await {
                Ok(Some(tx)) => {
                    if let Err(e) = self.publisher.publish_transaction(&tx, &tx_hash, true) {
                        warn!("blvm-zmq publish_transaction: {}", e);
                    }
                }
                Ok(None) => {
                    if let Err(e) = self.publisher.publish_hashtx(&tx_hash) {
                        warn!("blvm-zmq hashtx (tx gone): {}", e);
                    }
                    if let Err(e) = self.publisher.publish_sequence(&tx_hash, true) {
                        warn!("blvm-zmq sequence: {}", e);
                    }
                }
                Err(e) => warn!("blvm-zmq get_mempool_transaction: {}", e),
            }
        }
        Ok(())
    }

    #[on_event(MempoolTransactionRemoved)]
    async fn on_mempool_removed(
        &self,
        event: &EventMessage,
        _ctx: &InvocationContext,
    ) -> Result<(), ModuleError> {
        if let EventPayload::MempoolTransactionRemoved { tx_hash, .. } = event.payload
            && let Err(e) = self.publisher.publish_sequence(&tx_hash, false)
        {
            warn!("blvm-zmq sequence removal: {}", e);
        }
        Ok(())
    }

    #[command]
    fn help(&self, _ctx: &InvocationContext) -> Result<String, ModuleError> {
        Ok(
            "blvm-zmq — ZeroMQ PUB notifications for blvm-node.\n\
             Set endpoint in config.toml. A topic key binds that topic alone; \"\" turns it off.\n\
             Former node `[zmq]` section → module data dir config (or [modules.blvm-zmq] overrides)."
                .to_string(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blvm_node::module::EventType;

    #[test]
    fn subscribes_to_peer_and_mined_block_events() {
        let events = ZmqModule::event_types();
        assert!(events.contains(&EventType::NewBlock));
        assert!(events.contains(&EventType::BlockMined));
    }
}
