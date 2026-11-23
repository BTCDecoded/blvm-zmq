//! blvm-zmq — ZeroMQ PUB notifications (bind) for blockchain and mempool events.
//!
//! Replaces the in-process `zmq` feature on blvm-node. Configure endpoints in this module's
//! `config.toml`. `endpoint` binds every topic. A topic key overrides that topic.
//! An empty topic value turns that topic off.

use anyhow::{Result, bail};
use blvm_node::storage::database::{Database, Tree};
use blvm_sdk::module::ModuleBootstrap;
use blvm_zmq::{ZmqConfig, ZmqModule, ZmqPublisher};
use std::any::Any;
use std::sync::Arc;
use tracing::{info, warn};

const MODULE_NAME: &str = "blvm-zmq";

// The SDK runner needs a database handle, but this module never persists data.
struct StatelessDatabase;

impl Database for StatelessDatabase {
    fn open_tree(&self, _name: &str) -> Result<Box<dyn Tree>> {
        bail!("blvm-zmq does not use a database")
    }

    fn flush(&self) -> Result<()> {
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// The SDK dispatches events serially; avoid reserving a worker stack per host CPU.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let bootstrap = ModuleBootstrap::init_module(MODULE_NAME);

    let setup = |node_api: Arc<dyn blvm_node::module::traits::NodeAPI>,
                 _db: Arc<dyn blvm_node::storage::database::Database>,
                 data_dir: &std::path::Path| {
        let bootstrap = bootstrap.clone();
        let data_dir = data_dir.to_path_buf();
        async move {
            let (_ctx, config) = bootstrap.context_with_config::<ZmqConfig>(&data_dir);
            if !config.is_enabled() {
                warn!("blvm-zmq: no endpoints configured; module loaded but will not bind sockets");
            }
            let publisher = Arc::new(
                ZmqPublisher::new(&config)
                    .map_err(|e| blvm_node::module::traits::ModuleError::Other(e.to_string()))?,
            );
            if config.is_enabled() {
                info!("blvm-zmq: publisher bound (see logs above for endpoints)");
            }
            let module = ZmqModule::new(Arc::clone(&publisher));
            let _ = node_api;
            Ok((module.clone(), module))
        }
    };

    blvm_sdk::run_module! {
        bootstrap: &bootstrap,
        module_name: MODULE_NAME,
        module_type: ZmqModule,
        cli_type: ZmqModule,
        db: Arc::new(StatelessDatabase) as Arc<dyn Database>,
        setup: setup,
        event_types: ZmqModule::event_types(),
    }?;

    warn!("blvm-zmq shutting down");
    Ok(())
}
