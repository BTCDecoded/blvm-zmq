//! Event handlers → ZMQ PUB (subscribe_events + read_blockchain for fetch).

use blvm_node::module::ipc::protocol::{EventMessage, EventPayload};
use blvm_sdk::module::prelude::*;
use blvm_sdk_macros::module;
use std::sync::Arc;
use tracing::warn;

use crate::publisher::ZmqPublisher;

#[derive(Clone)]
pub struct ZmqModule {
    publisher: Arc<ZmqPublisher>,
}

impl ZmqModule {
    pub fn new(publisher: Arc<ZmqPublisher>) -> Self {
        Self { publisher }
    }
}

#[module]
impl ZmqModule {
    #[on_event(NewBlock)]
    async fn on_new_block(
        &self,
        event: &EventMessage,
        ctx: &InvocationContext,
    ) -> Result<(), ModuleError> {
        let api = ctx
            .node_api()
            .ok_or_else(|| ModuleError::Other("blvm-zmq: node_api required".to_string()))?;
        if let EventPayload::NewBlock { block_hash, .. } = event.payload {
            match api.get_block(&block_hash).await {
                Ok(Some(block)) => {
                    if let Err(e) = self.publisher.publish_block(&block, &block_hash) {
                        warn!("blvm-zmq publish_block: {}", e);
                    }
                }
                Ok(None) => {
                    if let Err(e) = self.publisher.publish_hashblock(&block_hash) {
                        warn!("blvm-zmq publish_hashblock (no block): {}", e);
                    }
                }
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
        if let EventPayload::MempoolTransactionRemoved { tx_hash, .. } = event.payload {
            if let Err(e) = self.publisher.publish_sequence(&tx_hash, false) {
                warn!("blvm-zmq sequence removal: {}", e);
            }
        }
        Ok(())
    }

    #[command]
    fn help(&self, _ctx: &InvocationContext) -> Result<String, ModuleError> {
        Ok(
            "blvm-zmq — ZeroMQ PUB notifications for blvm-node.\n\
             Configure hashblock/hashtx/rawblock/rawtx/sequence endpoints in config.toml.\n\
             Former node `[zmq]` section → module data dir config (or [modules.blvm-zmq] overrides)."
                .to_string(),
        )
    }
}
