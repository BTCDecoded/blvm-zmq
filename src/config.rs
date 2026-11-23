//! Module configuration: ZMQ bind endpoints (`config.toml` in module data dir).

use blvm_sdk_macros::config;
use serde::{Deserialize, Serialize};

/// ZeroMQ PUB bind addresses.
///
/// `endpoint` covers every topic that is not set. A topic key binds that topic
/// on its own address. An empty topic value turns that topic off.
/// Node overrides: `[modules.blvm-zmq]` in the main config.
#[config(name = "blvm-zmq")]
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct ZmqConfig {
    /// Bind every topic that does not set its own address.
    #[serde(default)]
    #[config_env]
    pub endpoint: Option<String>,
    /// `hashblock` topic — 32-byte block hash. Overrides `endpoint` for this topic.
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
    /// Treat explicit empty values as disabled, including node-injected overrides.
    pub fn disable_empty_endpoints(&mut self) {
        for endpoint in [
            &mut self.endpoint,
            &mut self.hashblock,
            &mut self.hashtx,
            &mut self.rawblock,
            &mut self.rawtx,
            &mut self.sequence,
        ] {
            if endpoint
                .as_deref()
                .is_some_and(|value| value.trim().is_empty())
            {
                *endpoint = None;
            }
        }
    }

    /// Key-value map for `ModuleContext` (diagnostics / tooling).
    pub fn to_context_map(&self) -> std::collections::HashMap<String, String> {
        let mut m = std::collections::HashMap::new();
        if let Some(ref v) = self.endpoint {
            m.insert("blvm-zmq.endpoint".to_string(), v.clone());
        }
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

    /// True if at least one non-empty address is set.
    pub fn is_enabled(&self) -> bool {
        [
            &self.endpoint,
            &self.hashblock,
            &self.hashtx,
            &self.rawblock,
            &self.rawtx,
            &self.sequence,
        ]
        .into_iter()
        .any(|value| present(value).is_some())
    }

    /// Address for one topic.
    ///
    /// A non-empty topic key wins. An empty topic key turns that topic off.
    /// An unset topic key uses `endpoint`.
    pub fn topic_bind<'a>(&'a self, specific: &'a Option<String>) -> Option<&'a str> {
        match specific.as_deref().map(str::trim) {
            Some("") => None,
            Some(value) => Some(value),
            None => present(&self.endpoint),
        }
    }
}

fn present(value: &Option<String>) -> Option<&str> {
    value.as_deref().map(str::trim).filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_endpoints_override_defaults_without_disabling_blocks() {
        let mut config = ZmqConfig {
            hashblock: Some("tcp://127.0.0.1:28332".into()),
            rawtx: Some("".into()),
            sequence: Some(" ".into()),
            ..ZmqConfig::default()
        };
        config.disable_empty_endpoints();
        assert!(config.is_enabled());
        assert!(config.rawtx.is_none());
        assert!(config.sequence.is_none());
    }
}
