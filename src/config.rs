//! Module configuration: ZMQ bind endpoints (`config.toml` in module data dir).

use blvm_sdk_macros::config;
use serde::{Deserialize, Serialize};

/// ZeroMQ PUB endpoints (same semantics as former blvm-node `[zmq]` config).
///
/// Node overrides: `[modules.blvm-zmq]` in the main config.
#[config(name = "blvm-zmq")]
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct ZmqConfig {
    /// `hashblock` topic — 32-byte block hash
    #[serde(default)]
    #[config_env]
    pub hashblock: Option<String>,
    /// `hashtx` topic — 32-byte tx hash
    #[serde(default)]
    #[config_env]
    pub hashtx: Option<String>,
    /// `rawblock` topic — full block wire serialization
    #[serde(default)]
    #[config_env]
    pub rawblock: Option<String>,
    /// `rawtx` topic — full transaction wire serialization
    #[serde(default)]
    #[config_env]
    pub rawtx: Option<String>,
    /// `sequence` topic — mempool add/remove (33-byte frame)
    #[serde(default)]
    #[config_env]
    pub sequence: Option<String>,
}

blvm_sdk::impl_module_config!(ZmqConfig);

impl ZmqConfig {
    /// Key-value map for `ModuleContext` (diagnostics / tooling).
    pub fn to_context_map(&self) -> std::collections::HashMap<String, String> {
        let mut m = std::collections::HashMap::new();
        if let Some(ref v) = self.hashblock {
            m.insert("blvm-zmq.hashblock".to_string(), v.clone());
        }
        if let Some(ref v) = self.hashtx {
            m.insert("blvm-zmq.hashtx".to_string(), v.clone());
        }
        if let Some(ref v) = self.rawblock {
            m.insert("blvm-zmq.rawblock".to_string(), v.clone());
        }
        if let Some(ref v) = self.rawtx {
            m.insert("blvm-zmq.rawtx".to_string(), v.clone());
        }
        if let Some(ref v) = self.sequence {
            m.insert("blvm-zmq.sequence".to_string(), v.clone());
        }
        m
    }

    /// True if at least one endpoint is set.
    pub fn is_enabled(&self) -> bool {
        self.hashblock.is_some()
            || self.hashtx.is_some()
            || self.rawblock.is_some()
            || self.rawtx.is_some()
            || self.sequence.is_some()
    }
}
