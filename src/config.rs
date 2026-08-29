use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::ops::Deref;
use std::path::Path;
#[cfg(feature = "compression-zstd")]
use std::path::PathBuf;
use tokio::fs;
use tracing::warn;
use url::Url;

use crate::protocol;
use crate::transport::{DEFAULT_KEEPALIVE_INTERVAL, DEFAULT_KEEPALIVE_SECS, DEFAULT_NODELAY};

/// Application-layer heartbeat interval in secs
const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 30;
const DEFAULT_HEARTBEAT_TIMEOUT_SECS: u64 = 40;

/// Client
const DEFAULT_CLIENT_RETRY_INTERVAL_SECS: u64 = 1;

/// Idle data channels pre-opened per TCP/UDP service
const DEFAULT_TCP_POOL_SIZE: usize = 8;
const DEFAULT_UDP_POOL_SIZE: usize = 2;

const DEFAULT_COMPRESSION_SAMPLE_WINDOW: u64 = 128 * 1024 * 1024;
const DEFAULT_COMPRESSION_DICTIONARY_MAX_SIZE: u64 = 110 * 1024;
// TODO: reconcile with compression::MIN_TRAIN_FACTOR once todo 1 lands
const MIN_TRAIN_FACTOR: usize = 100;

/// String with Debug implementation that emits "MASKED"
/// Used to mask sensitive strings when logging
#[derive(Serialize, Deserialize, Default, PartialEq, Eq, Clone)]
pub struct MaskedString(String);

impl Debug for MaskedString {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.write_str("MASKED")
    }
}

impl Deref for MaskedString {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&str> for MaskedString {
    fn from(s: &str) -> MaskedString {
        MaskedString(String::from(s))
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Eq, Clone)]
pub struct MaskedBytes(Vec<u8>);

impl Debug for MaskedBytes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "MASKED({} bytes)", self.0.len())
    }
}

impl Deref for MaskedBytes {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LoadedDictionary {
    pub bytes: MaskedBytes,
    pub digest: protocol::Digest,
}

impl LoadedDictionary {
    pub(crate) fn from_bytes(bytes: Vec<u8>) -> Self {
        let digest = protocol::digest(&bytes);
        Self {
            bytes: MaskedBytes(bytes),
            digest,
        }
    }
}

/// One or more server addresses. Accepts a string or an array of strings in TOML.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RemoteAddrList(Vec<String>);

impl RemoteAddrList {
    pub fn as_slice(&self) -> &[String] {
        &self.0
    }

    fn normalize(&mut self) -> Result<()> {
        let mut seen = HashSet::new();
        let mut normalized = Vec::with_capacity(self.0.len());
        for addr in self.0.drain(..) {
            if addr.is_empty() {
                bail!("`client.remote_addr` contains an empty address");
            }
            if seen.insert(addr.clone()) {
                normalized.push(addr);
            }
        }
        if normalized.is_empty() {
            bail!("`client.remote_addr` must not be empty");
        }
        self.0 = normalized;
        Ok(())
    }
}

impl Deref for RemoteAddrList {
    type Target = [String];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&str> for RemoteAddrList {
    fn from(s: &str) -> Self {
        RemoteAddrList(vec![s.to_string()])
    }
}

impl<'de> Deserialize<'de> for RemoteAddrList {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum OneOrMany {
            One(String),
            Many(Vec<String>),
        }

        Ok(match OneOrMany::deserialize(deserializer)? {
            OneOrMany::One(s) => RemoteAddrList(vec![s]),
            OneOrMany::Many(v) => RemoteAddrList(v),
        })
    }
}

impl Serialize for RemoteAddrList {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0.as_slice() {
            [single] => serializer.serialize_str(single),
            rest => rest.serialize(serializer),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq, Eq, Default)]
pub enum TransportType {
    #[default]
    #[serde(rename = "tcp")]
    Tcp,
    #[serde(rename = "tls")]
    Tls,
    #[serde(rename = "noise")]
    Noise,
    #[serde(rename = "websocket")]
    Websocket,
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq, Eq)]
pub enum CompressionType {
    #[serde(rename = "zstd")]
    Zstd,
}

/// Per service config
/// All Option are optional in configuration but must be Some value in runtime
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct ClientServiceConfig {
    #[serde(rename = "type", default = "default_service_type")]
    pub service_type: ServiceType,
    #[serde(skip)]
    pub name: String,
    pub local_addr: String,
    #[serde(default)] // Default to false
    pub prefer_ipv6: bool,
    pub token: Option<MaskedString>,
    pub nodelay: Option<bool>,
    pub retry_interval: Option<u64>,
    pub compression_dictionary: Option<String>,
    #[serde(skip)]
    pub compression_dictionary_loaded: Option<LoadedDictionary>,
}

impl ClientServiceConfig {
    pub fn with_name(name: &str) -> ClientServiceConfig {
        ClientServiceConfig {
            name: name.to_string(),
            ..Default::default()
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
pub enum ServiceType {
    #[serde(rename = "tcp")]
    #[default]
    Tcp,
    #[serde(rename = "udp")]
    Udp,
}

fn default_service_type() -> ServiceType {
    Default::default()
}

/// Per service config
/// All Option are optional in configuration but must be Some value in runtime
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct ServerServiceConfig {
    #[serde(rename = "type", default = "default_service_type")]
    pub service_type: ServiceType,
    #[serde(skip)]
    pub name: String,
    pub bind_addr: String,
    pub token: Option<MaskedString>,
    pub nodelay: Option<bool>,
    pub compression: Option<CompressionType>,
    pub compression_dictionary: Option<String>,
    pub compression_auto_dictionary: Option<bool>,
    pub compression_sample_window: Option<u64>,
    pub compression_dictionary_max_size: Option<u64>,
    #[serde(skip)]
    pub compression_dictionary_loaded: Option<LoadedDictionary>,
}

impl ServerServiceConfig {
    pub fn with_name(name: &str) -> ServerServiceConfig {
        ServerServiceConfig {
            name: name.to_string(),
            ..Default::default()
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    pub hostname: Option<String>,
    pub trusted_root: Option<String>,
    pub pkcs12: Option<String>,
    pub pkcs12_password: Option<MaskedString>,
}

fn default_noise_pattern() -> String {
    String::from("Noise_NK_25519_ChaChaPoly_BLAKE2s")
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NoiseConfig {
    #[serde(default = "default_noise_pattern")]
    pub pattern: String,
    pub local_private_key: Option<MaskedString>,
    pub remote_public_key: Option<String>,
    // TODO: Maybe psk can be added
}

fn default_websocket_path() -> String {
    "/".into()
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WebsocketConfig {
    pub tls: bool,
    #[serde(default = "default_websocket_path")]
    pub path: String,
}

fn default_nodelay() -> bool {
    DEFAULT_NODELAY
}

fn default_keepalive_secs() -> u64 {
    DEFAULT_KEEPALIVE_SECS
}

fn default_keepalive_interval() -> u64 {
    DEFAULT_KEEPALIVE_INTERVAL
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TcpConfig {
    #[serde(default = "default_nodelay")]
    pub nodelay: bool,
    #[serde(default = "default_keepalive_secs")]
    pub keepalive_secs: u64,
    #[serde(default = "default_keepalive_interval")]
    pub keepalive_interval: u64,
    pub proxy: Option<Url>,
}

impl Default for TcpConfig {
    fn default() -> Self {
        Self {
            nodelay: default_nodelay(),
            keepalive_secs: default_keepalive_secs(),
            keepalive_interval: default_keepalive_interval(),
            proxy: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct TransportConfig {
    #[serde(rename = "type")]
    pub transport_type: TransportType,
    #[serde(default)]
    pub tcp: TcpConfig,
    pub tls: Option<TlsConfig>,
    pub noise: Option<NoiseConfig>,
    pub websocket: Option<WebsocketConfig>,
}

fn default_heartbeat_timeout() -> u64 {
    DEFAULT_HEARTBEAT_TIMEOUT_SECS
}

fn default_client_retry_interval() -> u64 {
    DEFAULT_CLIENT_RETRY_INTERVAL_SECS
}

#[derive(Debug, Serialize, Deserialize, Default, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields)]
pub struct ClientConfig {
    pub remote_addr: RemoteAddrList,
    pub default_token: Option<MaskedString>,
    pub prefer_ipv6: Option<bool>,
    pub services: HashMap<String, ClientServiceConfig>,
    #[serde(default)]
    pub transport: TransportConfig,
    #[serde(default = "default_heartbeat_timeout")]
    pub heartbeat_timeout: u64,
    #[serde(default = "default_client_retry_interval")]
    pub retry_interval: u64,
}

fn default_heartbeat_interval() -> u64 {
    DEFAULT_HEARTBEAT_INTERVAL_SECS
}

fn default_tcp_pool_size() -> usize {
    DEFAULT_TCP_POOL_SIZE
}

fn default_udp_pool_size() -> usize {
    DEFAULT_UDP_POOL_SIZE
}

#[derive(Debug, Serialize, Deserialize, Default, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub bind_addr: String,
    pub default_token: Option<MaskedString>,
    pub services: HashMap<String, ServerServiceConfig>,
    #[serde(default)]
    pub transport: TransportConfig,
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval: u64,
    #[serde(default = "default_tcp_pool_size")]
    pub tcp_pool_size: usize,
    #[serde(default = "default_udp_pool_size")]
    pub udp_pool_size: usize,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub server: Option<ServerConfig>,
    pub client: Option<ClientConfig>,
}

impl Config {
    #[cfg_attr(not(test), allow(dead_code))]
    fn from_str(s: &str) -> Result<Config> {
        Config::from_str_with_base(s, None)
    }

    fn from_str_with_base(s: &str, base: Option<&Path>) -> Result<Config> {
        let mut config: Config = toml::from_str(s).with_context(|| "Failed to parse the config")?;

        if let Some(server) = config.server.as_mut() {
            Config::validate_server_config(server, base)?;
        }

        if let Some(client) = config.client.as_mut() {
            Config::validate_client_config(client, base)?;
        }

        if config.server.is_none() && config.client.is_none() {
            Err(anyhow!("Neither of `[server]` or `[client]` is defined"))
        } else {
            Ok(config)
        }
    }

    fn validate_server_config(server: &mut ServerConfig, base: Option<&Path>) -> Result<()> {
        // Validate services
        for (name, s) in &mut server.services {
            s.name = name.clone();
            if s.token.is_none() {
                s.token = server.default_token.clone();
                if s.token.is_none() {
                    bail!("The token of service {} is not set", name);
                }
            }
            if s.compression_auto_dictionary.is_some() && s.compression.is_none() {
                bail!(
                    "Service {}: `compression_auto_dictionary` requires `compression = \"zstd\"` to be set",
                    name
                );
            }
            if s.compression_sample_window.is_some() && s.compression.is_none() {
                bail!(
                    "Service {}: `compression_sample_window` requires `compression = \"zstd\"` to be set",
                    name
                );
            }
            if s.compression_dictionary_max_size.is_some() && s.compression.is_none() {
                bail!(
                    "Service {}: `compression_dictionary_max_size` requires `compression = \"zstd\"` to be set",
                    name
                );
            }
            if s.compression_dictionary.is_some() && s.compression.is_none() {
                bail!(
                    "Service {}: `compression_dictionary` requires `compression = \"zstd\"` to be set",
                    name
                );
            }

            #[cfg(not(feature = "compression-zstd"))]
            if s.compression.is_some()
                || s.compression_dictionary.is_some()
                || s.compression_auto_dictionary.is_some()
                || s.compression_sample_window.is_some()
                || s.compression_dictionary_max_size.is_some()
            {
                bail!(
                    "Service {}: `compression` requires compression support; recompile with compression-zstd",
                    name
                );
            }

            if s.compression_dictionary_max_size
                .is_some_and(|max_size| max_size > protocol::MAX_DICT_PUSH_BYTES)
            {
                bail!(
                    "Service {}: `compression_dictionary_max_size` must not exceed {} bytes",
                    name,
                    protocol::MAX_DICT_PUSH_BYTES
                );
            }

            if s.compression.is_some() {
                let sample_window = s
                    .compression_sample_window
                    .unwrap_or(DEFAULT_COMPRESSION_SAMPLE_WINDOW);
                let dictionary_max_size = s
                    .compression_dictionary_max_size
                    .unwrap_or(DEFAULT_COMPRESSION_DICTIONARY_MAX_SIZE);
                let minimum_sample_window =
                    u128::from(dictionary_max_size) * MIN_TRAIN_FACTOR as u128;
                if u128::from(sample_window) < minimum_sample_window {
                    bail!(
                        "Service {}: `compression_sample_window` must be at least {} times `compression_dictionary_max_size`",
                        name,
                        MIN_TRAIN_FACTOR
                    );
                }
            }

            if let Some(dictionary_path) = s.compression_dictionary.as_deref() {
                s.compression_dictionary_loaded =
                    Some(Config::load_compression_dictionary(dictionary_path, base)?);
            } else if s.compression.is_some() {
                #[cfg(not(feature = "compression-zstd"))]
                bail!(
                    "Service {}: `compression` requires compression support; recompile with compression-zstd",
                    name
                );
            }

            if s.compression.is_some() {
                if s.compression_dictionary.is_some() {
                    if s.compression_auto_dictionary == Some(true) {
                        warn!(
                            "Service {}: static `compression_dictionary` takes precedence; disabling `compression_auto_dictionary`",
                            name
                        );
                    }
                    s.compression_auto_dictionary = Some(false);
                } else if s.compression_auto_dictionary.is_none() {
                    s.compression_auto_dictionary = Some(true);
                }
                if s.compression_sample_window.is_none() {
                    s.compression_sample_window = Some(DEFAULT_COMPRESSION_SAMPLE_WINDOW);
                }
                if s.compression_dictionary_max_size.is_none() {
                    s.compression_dictionary_max_size =
                        Some(DEFAULT_COMPRESSION_DICTIONARY_MAX_SIZE);
                }
            }
        }

        Config::validate_transport_config(&server.transport, true)?;

        Ok(())
    }

    fn validate_client_config(client: &mut ClientConfig, base: Option<&Path>) -> Result<()> {
        client.remote_addr.normalize()?;

        // Validate services
        for (name, s) in &mut client.services {
            s.name = name.clone();
            if s.token.is_none() {
                s.token = client.default_token.clone();
                if s.token.is_none() {
                    bail!("The token of service {} is not set", name);
                }
            }
            if s.retry_interval.is_none() {
                s.retry_interval = Some(client.retry_interval);
            }
            if let Some(dictionary_path) = s.compression_dictionary.as_deref() {
                s.compression_dictionary_loaded =
                    Some(Config::load_compression_dictionary(dictionary_path, base)?);
            }
        }

        Config::validate_transport_config(&client.transport, false)?;

        Ok(())
    }

    #[cfg(feature = "compression-zstd")]
    fn resolve_compression_dictionary_path(path: &str, base: Option<&Path>) -> PathBuf {
        let path = Path::new(path);
        if path.is_absolute() {
            path.to_path_buf()
        } else if let Some(base) = base {
            base.join(path)
        } else {
            path.to_path_buf()
        }
    }

    fn load_compression_dictionary(path: &str, base: Option<&Path>) -> Result<LoadedDictionary> {
        #[cfg(not(feature = "compression-zstd"))]
        {
            let _ = (path, base);
            bail!(
                "`compression_dictionary` requires compression support; recompile with compression-zstd"
            );
        }

        #[cfg(feature = "compression-zstd")]
        {
            let resolved = Config::resolve_compression_dictionary_path(path, base);
            let bytes = std::fs::read(&resolved).with_context(|| {
                format!(
                    "Failed to read compression dictionary at {}",
                    resolved.display()
                )
            })?;
            Ok(LoadedDictionary::from_bytes(bytes))
        }
    }

    fn validate_transport_config(config: &TransportConfig, is_server: bool) -> Result<()> {
        config
            .tcp
            .proxy
            .as_ref()
            .map_or(Ok(()), |u| match u.scheme() {
                "socks5" => Ok(()),
                "http" => Ok(()),
                _ => Err(anyhow!(format!("Unknown proxy scheme: {}", u.scheme()))),
            })?;
        match config.transport_type {
            TransportType::Tcp => Ok(()),
            TransportType::Tls => {
                let tls_config = config
                    .tls
                    .as_ref()
                    .ok_or_else(|| anyhow!("Missing TLS configuration"))?;
                if is_server {
                    tls_config
                        .pkcs12
                        .as_ref()
                        .and(tls_config.pkcs12_password.as_ref())
                        .ok_or_else(|| anyhow!("Missing `pkcs12` or `pkcs12_password`"))?;
                }
                Ok(())
            }
            TransportType::Noise => {
                // The check is done in transport
                Ok(())
            }
            TransportType::Websocket => {
                let ws_config = config
                    .websocket
                    .as_ref()
                    .ok_or_else(|| anyhow!("Missing websocket configuration"))?;
                if ws_config.tls && is_server {
                    let tls_config = config
                        .tls
                        .as_ref()
                        .ok_or_else(|| anyhow!("Missing TLS configuration"))?;
                    tls_config
                        .pkcs12
                        .as_ref()
                        .and(tls_config.pkcs12_password.as_ref())
                        .ok_or_else(|| anyhow!("Missing `pkcs12` or `pkcs12_password`"))?;
                }
                Ok(())
            }
        }
    }

    pub async fn from_file(path: &Path) -> Result<Config> {
        let s: String = fs::read_to_string(path)
            .await
            .with_context(|| format!("Failed to read the config {:?}", path))?;
        Config::from_str_with_base(&s, path.parent()).with_context(|| {
            "Configuration is invalid. Please refer to the configuration specification."
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, path::PathBuf};

    #[cfg(feature = "compression-zstd")]
    use std::sync::atomic::{AtomicU64, Ordering};

    use anyhow::Result;

    #[cfg(feature = "compression-zstd")]
    static TEST_DIRECTORY_ID: AtomicU64 = AtomicU64::new(0);

    #[cfg(feature = "compression-zstd")]
    fn create_test_directory(name: &str) -> Result<PathBuf> {
        let id = TEST_DIRECTORY_ID.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("rathole-config-{name}-{}-{id}", std::process::id()));
        fs::create_dir_all(&path)?;
        Ok(path)
    }

    fn list_config_files<T: AsRef<Path>>(root: T) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                files.push(path);
            } else if path.is_dir() {
                files.append(&mut list_config_files(path)?);
            }
        }
        Ok(files)
    }

    fn get_all_example_config() -> Result<Vec<PathBuf>> {
        Ok(list_config_files("./examples")?
            .into_iter()
            .filter(|path| {
                path.extension().and_then(|extension| extension.to_str()) == Some("toml")
            })
            .collect())
    }

    #[test]
    fn test_example_config() -> Result<()> {
        let paths = get_all_example_config()?;
        assert!(!paths.is_empty());
        for p in paths {
            // `examples/compression.toml` needs `compression-zstd` and a
            // `service.dict` that is not shipped; `from_str` cannot load it in
            // any feature combination. Keep the scanner feature-agnostic.
            if p.ends_with("compression.toml") {
                continue;
            }
            let s = fs::read_to_string(p)?;
            Config::from_str(&s)?;
        }
        Ok(())
    }

    #[test]
    fn test_valid_config() -> Result<()> {
        let paths = list_config_files("tests/config_test/valid_config")?;
        for p in paths {
            let s = fs::read_to_string(p)?;
            Config::from_str(&s)?;
        }
        Ok(())
    }

    #[cfg(feature = "compression-zstd")]
    #[test]
    fn test_valid_config_compression_zstd() -> Result<()> {
        let paths = list_config_files("tests/config_test/valid_config_compression_zstd")?;
        for p in paths {
            let s = fs::read_to_string(p)?;
            Config::from_str(&s)?;
        }
        Ok(())
    }

    #[test]
    fn test_invalid_config() -> Result<()> {
        let paths = list_config_files("tests/config_test/invalid_config")?;
        for p in paths {
            let s = fs::read_to_string(&p)?;
            let error = Config::from_str(&s).expect_err("invalid config must be rejected");
            eprintln!("Rejected invalid config {}: {error:#}", p.display());
        }
        Ok(())
    }

    #[test]
    fn test_validate_server_config() -> Result<()> {
        let mut cfg = ServerConfig::default();

        cfg.services.insert(
            "foo1".into(),
            ServerServiceConfig {
                service_type: ServiceType::Tcp,
                name: "foo1".into(),
                bind_addr: "127.0.0.1:80".into(),
                token: None,
                ..Default::default()
            },
        );

        // Missing the token
        assert!(Config::validate_server_config(&mut cfg, None).is_err());

        // Use the default token
        cfg.default_token = Some("123".into());
        assert!(Config::validate_server_config(&mut cfg, None).is_ok());
        assert_eq!(
            cfg.services
                .get("foo1")
                .as_ref()
                .unwrap()
                .token
                .as_ref()
                .unwrap()
                .0,
            "123"
        );

        // The default token won't override the service token
        cfg.services.get_mut("foo1").unwrap().token = Some("4".into());
        assert!(Config::validate_server_config(&mut cfg, None).is_ok());
        assert_eq!(
            cfg.services
                .get("foo1")
                .as_ref()
                .unwrap()
                .token
                .as_ref()
                .unwrap()
                .0,
            "4"
        );
        Ok(())
    }

    #[test]
    fn test_validate_client_config() -> Result<()> {
        let mut cfg = ClientConfig {
            remote_addr: "127.0.0.1:2333".into(),
            ..Default::default()
        };

        cfg.services.insert(
            "foo1".into(),
            ClientServiceConfig {
                service_type: ServiceType::Tcp,
                name: "foo1".into(),
                local_addr: "127.0.0.1:80".into(),
                token: None,
                ..Default::default()
            },
        );

        // Missing the token
        assert!(Config::validate_client_config(&mut cfg, None).is_err());

        // Use the default token
        cfg.default_token = Some("123".into());
        assert!(Config::validate_client_config(&mut cfg, None).is_ok());
        assert_eq!(
            cfg.services
                .get("foo1")
                .as_ref()
                .unwrap()
                .token
                .as_ref()
                .unwrap()
                .0,
            "123"
        );

        // The default token won't override the service token
        cfg.services.get_mut("foo1").unwrap().token = Some("4".into());
        assert!(Config::validate_client_config(&mut cfg, None).is_ok());
        assert_eq!(
            cfg.services
                .get("foo1")
                .as_ref()
                .unwrap()
                .token
                .as_ref()
                .unwrap()
                .0,
            "4"
        );
        Ok(())
    }

    #[test]
    fn test_remote_addr_one_or_many() -> Result<()> {
        let single = Config::from_str(
            r#"
[client]
remote_addr = "example.com:2333"
default_token = "t"
[client.services.foo]
local_addr = "127.0.0.1:80"
"#,
        )?;
        assert_eq!(
            single.client.unwrap().remote_addr.as_slice(),
            &["example.com:2333".to_string()]
        );

        let many = Config::from_str(
            r#"
[client]
remote_addr = ["a:1", "b:2", "a:1"]
default_token = "t"
[client.services.foo]
local_addr = "127.0.0.1:80"
"#,
        )?;
        assert_eq!(
            many.client.unwrap().remote_addr.as_slice(),
            &["a:1".to_string(), "b:2".to_string()]
        );

        assert!(Config::from_str(
            r#"
[client]
remote_addr = []
default_token = "t"
[client.services.foo]
local_addr = "127.0.0.1:80"
"#,
        )
        .is_err());

        Ok(())
    }

    #[test]
    fn test_server_pool_size() -> Result<()> {
        let defaulted = Config::from_str(
            r#"
[server]
bind_addr = "0.0.0.0:2333"
default_token = "t"
[server.services.foo]
bind_addr = "0.0.0.0:8081"
"#,
        )?;
        let s = defaulted.server.unwrap();
        assert_eq!(s.tcp_pool_size, DEFAULT_TCP_POOL_SIZE);
        assert_eq!(s.udp_pool_size, DEFAULT_UDP_POOL_SIZE);

        let custom = Config::from_str(
            r#"
[server]
bind_addr = "0.0.0.0:2333"
default_token = "t"
tcp_pool_size = 16
udp_pool_size = 4
[server.services.foo]
bind_addr = "0.0.0.0:8081"
"#,
        )?;
        let s = custom.server.unwrap();
        assert_eq!(s.tcp_pool_size, 16);
        assert_eq!(s.udp_pool_size, 4);

        let zero = Config::from_str(
            r#"
[server]
bind_addr = "0.0.0.0:2333"
default_token = "t"
tcp_pool_size = 0
udp_pool_size = 0
[server.services.foo]
bind_addr = "0.0.0.0:8081"
"#,
        )?;
        let s = zero.server.unwrap();
        assert_eq!(s.tcp_pool_size, 0);
        assert_eq!(s.udp_pool_size, 0);

        Ok(())
    }

    #[test]
    fn test_server_dictionary_requires_compression() {
        // Given
        let config = r#"
[server]
bind_addr = "0.0.0.0:2333"
default_token = "t"
[server.services.foo]
bind_addr = "0.0.0.0:8081"
compression_dictionary = "dictionary.bin"
"#;

        // When
        let error = Config::from_str(config).unwrap_err();

        // Then
        let message = error.to_string();
        assert!(message.contains("foo"));
        assert!(message.contains("requires `compression = \"zstd\"`"));
    }

    #[cfg(not(feature = "compression-zstd"))]
    #[test]
    fn test_server_compression_rejected_without_feature() {
        // Given
        let config = r#"
[server]
bind_addr = "0.0.0.0:2333"
default_token = "t"
[server.services.foo]
bind_addr = "0.0.0.0:8081"
compression = "zstd"
"#;

        // When
        let error = Config::from_str(config).unwrap_err();

        // Then
        assert!(error
            .to_string()
            .contains("recompile with compression-zstd"));
    }

    #[cfg(not(feature = "compression-zstd"))]
    #[test]
    fn test_client_dictionary_rejected_without_feature() {
        // Given
        let config = r#"
[client]
remote_addr = "example.com:2333"
default_token = "t"
[client.services.foo]
local_addr = "127.0.0.1:80"
compression_dictionary = "dictionary.bin"
"#;

        // When
        let error = Config::from_str(config).unwrap_err();

        // Then
        assert!(error
            .to_string()
            .contains("recompile with compression-zstd"));
    }

    #[cfg(feature = "compression-zstd")]
    #[test]
    fn test_unreadable_dictionary_error_contains_resolved_path() -> Result<()> {
        // Given
        let base = create_test_directory("missing-dictionary")?;
        let resolved = base.join("missing.dict");
        let config = r#"
[server]
bind_addr = "0.0.0.0:2333"
default_token = "t"
[server.services.foo]
bind_addr = "0.0.0.0:8081"
compression = "zstd"
compression_dictionary = "missing.dict"
"#;

        // When
        let error = Config::from_str_with_base(config, Some(&base)).unwrap_err();

        // Then
        assert!(error.to_string().contains(&resolved.display().to_string()));
        fs::remove_dir_all(base)?;
        Ok(())
    }

    #[cfg(feature = "compression-zstd")]
    #[test]
    fn test_relative_client_dictionary_resolves_from_base() -> Result<()> {
        // Given
        let base = create_test_directory("relative-dictionary")?;
        let expected_bytes = b"client dictionary";
        fs::write(base.join("dictionary.bin"), expected_bytes)?;
        let config = r#"
[client]
remote_addr = "example.com:2333"
default_token = "t"
[client.services.foo]
local_addr = "127.0.0.1:80"
compression_dictionary = "dictionary.bin"
"#;

        // When
        let parsed = Config::from_str_with_base(config, Some(&base))?;

        // Then
        let loaded = parsed
            .client
            .unwrap()
            .services
            .get("foo")
            .unwrap()
            .compression_dictionary_loaded
            .clone()
            .unwrap();
        assert_eq!(&*loaded.bytes, expected_bytes);
        fs::remove_dir_all(base)?;
        Ok(())
    }

    #[cfg(feature = "compression-zstd")]
    #[test]
    fn test_valid_server_dictionary_loads_bytes_and_digest() -> Result<()> {
        // Given
        let base = create_test_directory("valid-server-dictionary")?;
        let dictionary_path = base.join("dictionary.bin");
        let expected_bytes = b"server dictionary";
        fs::write(&dictionary_path, expected_bytes)?;
        let config = format!(
            r#"
[server]
bind_addr = "0.0.0.0:2333"
default_token = "t"
[server.services.foo]
bind_addr = "0.0.0.0:8081"
compression = "zstd"
compression_dictionary = "{}"
compression_auto_dictionary = true
"#,
            dictionary_path.display()
        );

        // When
        let parsed = Config::from_str_with_base(&config, Some(Path::new("ignored-base")))?;

        // Then
        let server = parsed.server.unwrap();
        let service = server.services.get("foo").unwrap();
        let loaded = service.compression_dictionary_loaded.clone().unwrap();
        assert_eq!(&*loaded.bytes, expected_bytes);
        assert_eq!(loaded.digest, crate::protocol::digest(expected_bytes));
        assert_eq!(service.compression_auto_dictionary, Some(false));
        fs::remove_dir_all(base)?;
        Ok(())
    }

    #[test]
    fn test_client_compression_key_is_rejected() {
        // Given
        let config = r#"
[client]
remote_addr = "example.com:2333"
default_token = "t"
[client.services.foo]
local_addr = "127.0.0.1:80"
compression = "zstd"
"#;

        // When
        let error = Config::from_str(config).unwrap_err();

        // Then
        assert!(format!("{error:#}").contains("unknown field `compression`"));
    }

    #[cfg(feature = "compression-zstd")]
    #[test]
    fn test_server_auto_dictionary_defaults_and_explicit_values() -> Result<()> {
        // Given
        let config = r#"
[server]
bind_addr = "0.0.0.0:2333"
default_token = "t"

[server.services.defaults]
bind_addr = "0.0.0.0:8081"
compression = "zstd"

[server.services.explicit]
bind_addr = "0.0.0.0:8082"
compression = "zstd"
compression_auto_dictionary = false
compression_sample_window = 22528000
compression_dictionary_max_size = 225280
"#;

        // When
        let parsed = Config::from_str(config)?;

        // Then
        let services = &parsed.server.as_ref().unwrap().services;
        let defaults = services.get("defaults").unwrap();
        assert_eq!(defaults.compression_auto_dictionary, Some(true));
        assert_eq!(defaults.compression_sample_window, Some(134_217_728));
        assert_eq!(defaults.compression_dictionary_max_size, Some(112_640));

        let explicit = services.get("explicit").unwrap();
        assert_eq!(explicit.compression_auto_dictionary, Some(false));
        assert_eq!(explicit.compression_sample_window, Some(22_528_000));
        assert_eq!(explicit.compression_dictionary_max_size, Some(225_280));
        Ok(())
    }

    #[test]
    fn test_masked_bytes_debug_includes_length() {
        // Given
        let bytes = MaskedBytes(vec![1, 2, 3]);

        // When
        let debug = format!("{bytes:?}");

        // Then
        assert_eq!(debug, "MASKED(3 bytes)");
    }
}
