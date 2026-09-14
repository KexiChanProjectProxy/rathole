use crate::config::{
    CompressionType, Config, LoadedDictionary, ServerConfig, ServerServiceConfig, ServiceType,
    TransportType,
};
use crate::config_watcher::{ConfigChange, ServerServiceChange};
use crate::constants::{listen_backoff, UDP_BUFFER_SIZE};
use crate::helper::{retry_notify_with_deadline, write_and_flush};
use crate::multi_map::MultiMap;
use crate::protocol::Hello::{ControlChannelHello, DataChannelHello};
use crate::protocol::{
    self, read_auth, read_hello, Ack, ControlChannelCmd, DataChannelCmd, Hello, UdpTraffic,
    HASH_WIDTH_IN_BYTES,
};
use crate::transport::{SocketOpts, TcpTransport, Transport};
use anyhow::{anyhow, bail, Context, Result};
use backoff::backoff::Backoff;
use backoff::ExponentialBackoff;

use rand::Rng;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{self, copy_bidirectional, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{broadcast, mpsc, watch, Notify, RwLock};
use tokio::time;
use tracing::{debug, error, info, info_span, instrument, warn, Instrument, Span};

#[cfg(feature = "noise")]
use crate::transport::NoiseTransport;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use crate::transport::TlsTransport;
#[cfg(any(feature = "websocket-native-tls", feature = "websocket-rustls"))]
use crate::transport::WebsocketTransport;

type ServiceDigest = protocol::Digest; // SHA256 of a service name
type Nonce = protocol::Digest; // Also called `session_key`
type ServiceCompressionStateMap = Arc<RwLock<HashMap<ServiceDigest, Arc<ServiceCompressionState>>>>;

mod sampler;
use sampler::{SampleBuffer, SamplingStream};
mod observe;
#[cfg(feature = "compression-zstd")]
mod training;
use observe::{maybe_count, ActiveSession, ByteKind, ServerStats, ServiceStats};

const CHAN_SIZE: usize = 2048; // The capacity of various chans
const HANDSHAKE_TIMEOUT: u64 = 5; // Timeout for transport handshake

#[derive(Debug)]
pub enum SamplerState {
    Sampling(SampleBuffer),
    Trained,
    Failed,
}

#[derive(Debug)]
pub struct Generation {
    pub digest: protocol::Digest,
    pub dictionary: LoadedDictionary,
    pub level: i32,
    pub tcp_cmd_bytes: Vec<u8>,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "todo 6 consumes cached UDP command bytes")
    )]
    pub udp_cmd_bytes: Vec<u8>,
}

impl Generation {
    #[cfg(test)]
    fn new(dictionary: LoadedDictionary) -> bincode::Result<Self> {
        Self::with_level(dictionary, crate::constants::DEFAULT_ZSTD_LEVEL)
    }

    fn with_level(dictionary: LoadedDictionary, level: i32) -> bincode::Result<Self> {
        let digest = dictionary.digest;
        Ok(Self {
            digest,
            dictionary,
            level,
            tcp_cmd_bytes: bincode::serialize(&DataChannelCmd::StartForwardTcpZstd {
                dict_digest: digest,
            })?,
            udp_cmd_bytes: bincode::serialize(&DataChannelCmd::StartForwardUdpZstd {
                dict_digest: digest,
            })?,
        })
    }
}

#[derive(Debug)]
pub struct ServiceCompressionState {
    pub sampler: Mutex<SamplerState>,
    sampling_active: AtomicBool,
    sampling_ready: AtomicBool,
    sampling_notify: Arc<Notify>,
    #[cfg(test)]
    sample_lock_count: AtomicUsize,
    pub generation_tx: watch::Sender<Option<Arc<Generation>>>,
    pub generation_rx: watch::Receiver<Option<Arc<Generation>>>,
    pub level: i32,
}

impl ServiceCompressionState {
    #[cfg(test)]
    fn new(sample_window: u64) -> Self {
        Self::with_level(sample_window, crate::constants::DEFAULT_ZSTD_LEVEL)
    }

    fn with_level(sample_window: u64, level: i32) -> Self {
        let (generation_tx, generation_rx) = watch::channel(None);
        let sampling_active = sample_window > 0;
        Self {
            sampler: Mutex::new(SamplerState::Sampling(SampleBuffer::new(sample_window))),
            sampling_active: AtomicBool::new(sampling_active),
            sampling_ready: AtomicBool::new(sample_window == 0),
            sampling_notify: Arc::new(Notify::new()),
            #[cfg(test)]
            sample_lock_count: AtomicUsize::new(0),
            generation_tx,
            generation_rx,
            level,
        }
    }
}

impl Drop for ServiceCompressionState {
    fn drop(&mut self) {
        self.sampling_notify.notify_waiters();
    }
}

fn tcp_generation_snapshot(
    generation_rx: &watch::Receiver<Option<Arc<Generation>>>,
) -> Option<Arc<Generation>> {
    generation_rx.borrow().clone()
}

fn tcp_sampling_state(
    compression_state: Option<&Arc<ServiceCompressionState>>,
) -> Option<Arc<ServiceCompressionState>> {
    let state = compression_state?;
    if !state.is_sampling_active() {
        return None;
    }

    let sampler = state
        .sampler
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match &*sampler {
        SamplerState::Sampling(_) => Some(Arc::clone(state)),
        SamplerState::Trained | SamplerState::Failed => {
            state.sampling_active.store(false, Ordering::Relaxed);
            None
        }
    }
}

async fn get_or_create_service_compression_state(
    states: &ServiceCompressionStateMap,
    service_digest: ServiceDigest,
    service: &ServerServiceConfig,
) -> Option<Arc<ServiceCompressionState>> {
    let is_auto_dictionary_service = matches!(
        (
            service.service_type,
            service.compression,
            service.compression_dictionary.as_ref(),
            service.compression_auto_dictionary,
        ),
        (
            ServiceType::Tcp,
            Some(CompressionType::Zstd),
            None,
            Some(true)
        )
    );
    if !is_auto_dictionary_service {
        return None;
    }

    let mut states = states.write().await;
    Some(match states.entry(service_digest) {
        Entry::Occupied(entry) => Arc::clone(entry.get()),
        Entry::Vacant(entry) => {
            let state = Arc::new(ServiceCompressionState::with_level(
                service.compression_sample_window.unwrap_or_default(),
                service
                    .compression_level
                    .unwrap_or(crate::constants::DEFAULT_ZSTD_LEVEL),
            ));
            #[cfg(feature = "compression-zstd")]
            training::spawn_dictionary_training(
                &state,
                service.name.clone(),
                service.compression_dictionary_max_size.unwrap_or_default(),
            );
            entry.insert(Arc::clone(&state));
            state
        }
    })
}

struct CompressionCtx {
    dict: Option<LoadedDictionary>,
    level: i32,
}

impl CompressionCtx {
    fn digest(&self) -> protocol::Digest {
        self.dict
            .as_ref()
            .map(|dictionary| dictionary.digest)
            .unwrap_or([0; HASH_WIDTH_IN_BYTES])
    }
}

fn tcp_cmd(compression: &Option<Arc<CompressionCtx>>) -> DataChannelCmd {
    match compression {
        None => DataChannelCmd::StartForwardTcp,
        Some(ctx) => DataChannelCmd::StartForwardTcpZstd {
            dict_digest: ctx.digest(),
        },
    }
}

fn udp_cmd(compression: &Option<Arc<CompressionCtx>>) -> DataChannelCmd {
    match compression {
        None => DataChannelCmd::StartForwardUdp,
        Some(ctx) => DataChannelCmd::StartForwardUdpZstd {
            dict_digest: ctx.digest(),
        },
    }
}

#[cfg(feature = "compression-zstd")]
fn wrap_stream<S>(
    stream: S,
    compression_enabled: bool,
    dictionary: Option<&LoadedDictionary>,
    level: i32,
) -> std::io::Result<crate::compression::MaybeCompressed<S>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    use crate::compression::{MaybeCompressed, ZstdStream};

    Ok(if compression_enabled {
        match dictionary {
            Some(dictionary) => MaybeCompressed::Zstd(ZstdStream::with_dict_and_level(
                stream,
                &dictionary.bytes,
                level,
            )?),
            None => MaybeCompressed::Zstd(ZstdStream::with_level(stream, level)?),
        }
    } else {
        MaybeCompressed::Plain(stream)
    })
}

// The entrypoint of running a server
pub async fn run_server(
    config: Config,
    shutdown_rx: broadcast::Receiver<bool>,
    update_rx: mpsc::Receiver<ConfigChange>,
) -> Result<()> {
    let config = match config.server {
            Some(config) => config,
            None => {
                return Err(anyhow!("Try to run as a server, but the configuration is missing. Please add the `[server]` block"))
            }
        };

    match config.transport.transport_type {
        TransportType::Tcp => {
            let mut server = Server::<TcpTransport>::from(config).await?;
            server.run(shutdown_rx, update_rx).await?;
        }
        TransportType::Tls => {
            #[cfg(any(feature = "native-tls", feature = "rustls"))]
            {
                let mut server = Server::<TlsTransport>::from(config).await?;
                server.run(shutdown_rx, update_rx).await?;
            }
            #[cfg(not(any(feature = "native-tls", feature = "rustls")))]
            crate::helper::feature_neither_compile("native-tls", "rustls")
        }
        TransportType::Noise => {
            #[cfg(feature = "noise")]
            {
                let mut server = Server::<NoiseTransport>::from(config).await?;
                server.run(shutdown_rx, update_rx).await?;
            }
            #[cfg(not(feature = "noise"))]
            crate::helper::feature_not_compile("noise")
        }
        TransportType::Websocket => {
            #[cfg(any(feature = "websocket-native-tls", feature = "websocket-rustls"))]
            {
                let mut server = Server::<WebsocketTransport>::from(config).await?;
                server.run(shutdown_rx, update_rx).await?;
            }
            #[cfg(not(any(feature = "websocket-native-tls", feature = "websocket-rustls")))]
            crate::helper::feature_neither_compile("websocket-native-tls", "websocket-rustls")
        }
    }

    Ok(())
}

// A hash map of ControlChannelHandles, indexed by ServiceDigest or Nonce
// See also MultiMap
type ControlChannelMap<T> = MultiMap<ServiceDigest, Nonce, ControlChannelHandle<T>>;

// Server holds all states of running a server
struct Server<T: Transport> {
    // `[server]` config
    config: Arc<ServerConfig>,

    // `[server.services]` config, indexed by ServiceDigest
    services: Arc<RwLock<HashMap<ServiceDigest, ServerServiceConfig>>>,
    // Collection of contorl channels
    control_channels: Arc<RwLock<ControlChannelMap<T>>>,
    service_compression_states: ServiceCompressionStateMap,
    stats: Option<Arc<ServerStats>>,
    // Wrapper around the transport layer
    transport: Arc<T>,
}

// Generate a hash map of services which is indexed by ServiceDigest
fn generate_service_hashmap(
    server_config: &ServerConfig,
) -> HashMap<ServiceDigest, ServerServiceConfig> {
    let mut ret = HashMap::new();
    for u in &server_config.services {
        ret.insert(protocol::digest(u.0.as_bytes()), (*u.1).clone());
    }
    ret
}

impl<T: 'static + Transport> Server<T> {
    // Create a server from `[server]`
    pub async fn from(config: ServerConfig) -> Result<Server<T>> {
        let config = Arc::new(config);
        let services = Arc::new(RwLock::new(generate_service_hashmap(&config)));
        let control_channels = Arc::new(RwLock::new(ControlChannelMap::new()));
        let service_compression_states = Arc::new(RwLock::new(HashMap::new()));
        let transport = Arc::new(T::new(&config.transport)?);
        let stats = config
            .observe_addr
            .as_ref()
            .map(|_| Arc::new(ServerStats::from_services(&config.services)));
        Ok(Server {
            config,
            services,
            control_channels,
            service_compression_states,
            transport,
            stats,
        })
    }

    // The entry point of Server
    pub async fn run(
        &mut self,
        mut shutdown_rx: broadcast::Receiver<bool>,
        mut update_rx: mpsc::Receiver<ConfigChange>,
    ) -> Result<()> {
        // Listen at `server.bind_addr`
        let l = self
            .transport
            .bind(&self.config.bind_addr)
            .await
            .with_context(|| "Failed to listen at `server.bind_addr`")?;
        info!("Listening at {}", self.config.bind_addr);

        if let (Some(addr), Some(stats)) = (self.config.observe_addr.clone(), self.stats.clone()) {
            let listener = TcpListener::bind(&addr)
                .await
                .with_context(|| "Failed to listen at `server.observe_addr`")?;
            match listener.local_addr() {
                Ok(bound) => {
                    info!("Observation endpoint listening at {}", bound);
                    if !bound.ip().is_loopback() {
                        warn!(
                            %bound,
                            "observe_addr is not loopback; the endpoint is unauthenticated"
                        );
                    }
                }
                Err(_) => info!("Observation endpoint listening at {}", addr),
            }
            let observe_shutdown = shutdown_rx.resubscribe();
            tokio::spawn(async move {
                observe::serve(listener, stats, observe_shutdown).await;
            });
        }

        // Retry at least every 100ms
        let mut backoff = ExponentialBackoff {
            max_interval: Duration::from_millis(100),
            max_elapsed_time: None,
            ..Default::default()
        };

        // Wait for connections and shutdown signals
        loop {
            tokio::select! {
                // Wait for incoming control and data channels
                ret = self.transport.accept(&l) => {
                    match ret {
                        Err(err) => {
                            // Detects whether it's an IO error
                            if let Some(err) = err.downcast_ref::<io::Error>() {
                                // If it is an IO error, then it's possibly an
                                // EMFILE. So sleep for a while and retry
                                // TODO: Only sleep for EMFILE, ENFILE, ENOMEM, ENOBUFS
                                if let Some(d) = backoff.next_backoff() {
                                    error!("Failed to accept: {:#}. Retry in {:?}...", err, d);
                                    time::sleep(d).await;
                                } else {
                                    // This branch will never be executed according to the current retry policy
                                    error!("Too many retries. Aborting...");
                                    break;
                                }
                            }
                            // If it's not an IO error, then it comes from
                            // the transport layer, so just ignore it
                        }
                        Ok((conn, addr)) => {
                            backoff.reset();

                            // Do transport handshake with a timeout
                            match time::timeout(Duration::from_secs(HANDSHAKE_TIMEOUT), self.transport.handshake(conn)).await {
                                Ok(conn) => {
                                    match conn.with_context(|| "Failed to do transport handshake") {
                                        Ok(conn) => {
                                            let services = self.services.clone();
                                            let control_channels = self.control_channels.clone();
                                            let service_compression_states = self.service_compression_states.clone();
                                            let server_config = self.config.clone();
                                            let stats = self.stats.clone();
                                            tokio::spawn(async move {
                                                if let Err(err) = handle_connection(conn, services, control_channels, service_compression_states, server_config, stats).await {
                                                    error!("{:#}", err);
                                                }
                                            }.instrument(info_span!("connection", %addr)));
                                        }, Err(e) => {
                                            error!("{:#}", e);
                                        }
                                    }
                                },
                                Err(e) => {
                                    error!("Transport handshake timeout: {}", e);
                                }
                            }
                        }
                    }
                },
                // Wait for the shutdown signal
                _ = shutdown_rx.recv() => {
                    info!("Shuting down gracefully...");
                    break;
                },
                e = update_rx.recv() => {
                    if let Some(e) = e {
                        self.handle_hot_reload(e).await;
                    }
                }
            }
        }

        info!("Shutdown");

        Ok(())
    }

    async fn handle_hot_reload(&mut self, e: ConfigChange) {
        match e {
            ConfigChange::ServerChange(server_change) => match server_change {
                ServerServiceChange::Add(cfg) => {
                    if let Some(stats) = &self.stats {
                        stats.upsert(&cfg);
                    }
                    let hash = protocol::digest(cfg.name.as_bytes());
                    let mut wg = self.services.write().await;
                    let _ = wg.insert(hash, cfg);

                    let mut wg = self.control_channels.write().await;
                    let _ = wg.remove1(&hash);
                    let _ = self.service_compression_states.write().await.remove(&hash);
                }
                ServerServiceChange::Delete(s) => {
                    if let Some(stats) = &self.stats {
                        stats.remove(&s);
                    }
                    let hash = protocol::digest(s.as_bytes());
                    let _ = self.services.write().await.remove(&hash);

                    let mut wg = self.control_channels.write().await;
                    let _ = wg.remove1(&hash);
                    let _ = self.service_compression_states.write().await.remove(&hash);
                }
            },
            ignored => warn!("Ignored {:?} since running as a server", ignored),
        }
    }
}

// Handle connections to `server.bind_addr`
async fn handle_connection<T: 'static + Transport>(
    mut conn: T::Stream,
    services: Arc<RwLock<HashMap<ServiceDigest, ServerServiceConfig>>>,
    control_channels: Arc<RwLock<ControlChannelMap<T>>>,
    service_compression_states: ServiceCompressionStateMap,
    server_config: Arc<ServerConfig>,
    stats: Option<Arc<ServerStats>>,
) -> Result<()> {
    // Read hello
    let hello = read_hello(&mut conn).await?;
    match hello {
        ControlChannelHello(_, service_digest) => {
            do_control_channel_handshake(
                conn,
                services,
                control_channels,
                service_compression_states,
                service_digest,
                server_config,
                stats,
            )
            .await?;
        }
        DataChannelHello(_, nonce) => {
            do_data_channel_handshake(conn, control_channels, nonce).await?;
        }
    }
    Ok(())
}

async fn do_control_channel_handshake<T: 'static + Transport>(
    mut conn: T::Stream,
    services: Arc<RwLock<HashMap<ServiceDigest, ServerServiceConfig>>>,
    control_channels: Arc<RwLock<ControlChannelMap<T>>>,
    service_compression_states: ServiceCompressionStateMap,
    service_digest: ServiceDigest,
    server_config: Arc<ServerConfig>,
    stats: Option<Arc<ServerStats>>,
) -> Result<()> {
    info!("Try to handshake a control channel");

    T::hint(&conn, SocketOpts::for_control_channel());

    // Generate a nonce
    let mut nonce = vec![0u8; HASH_WIDTH_IN_BYTES];
    rand::rng().fill_bytes(&mut nonce);

    // Send hello
    let hello_send = Hello::ControlChannelHello(
        protocol::CURRENT_PROTO_VERSION,
        nonce.clone().try_into().unwrap(),
    );
    conn.write_all(&bincode::serialize(&hello_send).unwrap())
        .await?;
    conn.flush().await?;

    // Lookup the service
    let service_config = match services.read().await.get(&service_digest) {
        Some(v) => v,
        None => {
            conn.write_all(&bincode::serialize(&Ack::ServiceNotExist).unwrap())
                .await?;
            bail!("No such a service {}", hex::encode(service_digest));
        }
    }
    .to_owned();

    let service_name = &service_config.name;

    // Calculate the checksum
    let mut concat = Vec::from(service_config.token.as_ref().unwrap().as_bytes());
    concat.append(&mut nonce);

    // Read auth
    let protocol::Auth(d) = read_auth(&mut conn).await?;

    // Validate
    let session_key = protocol::digest(&concat);
    if session_key != d {
        conn.write_all(&bincode::serialize(&Ack::AuthFailed).unwrap())
            .await?;
        debug!(
            "Expect {}, but got {}",
            hex::encode(session_key),
            hex::encode(d)
        );
        bail!("Service {} failed the authentication", service_name);
    } else {
        let current_services = services.read().await;
        let compression_state = if current_services.get(&service_digest) == Some(&service_config) {
            get_or_create_service_compression_state(
                &service_compression_states,
                service_digest,
                &service_config,
            )
            .await
        } else {
            None
        };
        let mut h = control_channels.write().await;

        // If there's already a control channel for the service, then drop the old one.
        // Because a control channel doesn't report back when it's dead,
        // the handle in the map could be stall, dropping the old handle enables
        // the client to reconnect.
        if h.remove1(&service_digest).is_some() {
            warn!(
                "Dropping previous control channel for service {}",
                service_name
            );
        }

        // Send ack
        conn.write_all(&bincode::serialize(&Ack::Ok).unwrap())
            .await?;
        conn.flush().await?;

        info!(service = %service_config.name, "Control channel established");
        let service_stats = stats
            .as_ref()
            .and_then(|stats| stats.service(&service_config.name));
        let handle = ControlChannelHandle::new(
            conn,
            service_config,
            &server_config,
            compression_state,
            service_stats,
        );

        // Insert the new handle
        let _ = h.insert(service_digest, session_key, handle);
    }

    Ok(())
}

async fn do_data_channel_handshake<T: 'static + Transport>(
    conn: T::Stream,
    control_channels: Arc<RwLock<ControlChannelMap<T>>>,
    nonce: Nonce,
) -> Result<()> {
    debug!("Try to handshake a data channel");

    // Validate
    let control_channels_guard = control_channels.read().await;
    match control_channels_guard.get2(&nonce) {
        Some(handle) => {
            T::hint(&conn, SocketOpts::from_server_cfg(&handle.service));

            // Send the data channel to the corresponding control channel
            handle
                .data_ch_tx
                .send(conn)
                .await
                .with_context(|| "Data channel for a stale control channel")?;
        }
        None => {
            warn!("Data channel has incorrect nonce");
        }
    }
    Ok(())
}

pub struct ControlChannelHandle<T: Transport> {
    // Shutdown the control channel by dropping it
    _shutdown_tx: broadcast::Sender<bool>,
    data_ch_tx: mpsc::Sender<T::Stream>,
    service: ServerServiceConfig,
    _compression_state: Option<Arc<ServiceCompressionState>>,
    stats: Option<Arc<ServiceStats>>,
}

impl<T> ControlChannelHandle<T>
where
    T: 'static + Transport,
{
    // Create a control channel handle, where the control channel handling task
    // and the connection pool task are created.
    #[instrument(name = "handle", skip_all, fields(service = %service.name))]
    fn new(
        conn: T::Stream,
        service: ServerServiceConfig,
        server_config: &ServerConfig,
        compression_state: Option<Arc<ServiceCompressionState>>,
        stats: Option<Arc<ServiceStats>>,
    ) -> ControlChannelHandle<T> {
        // Create a shutdown channel
        let (shutdown_tx, shutdown_rx) = broadcast::channel::<bool>(1);

        // Store data channels
        let (data_ch_tx, data_ch_rx) = mpsc::channel(CHAN_SIZE * 2);

        // Store data channel creation requests
        let (data_ch_req_tx, data_ch_req_rx) = mpsc::unbounded_channel();

        // Cache some data channels for later use
        let pool_size = match service.service_type {
            ServiceType::Tcp => server_config.tcp_pool_size,
            ServiceType::Udp => server_config.udp_pool_size,
        };

        for _i in 0..pool_size {
            if let Err(e) = data_ch_req_tx.send(true) {
                error!("Failed to request data channel {}", e);
            };
        }

        if let Some(stats) = &stats {
            stats.set_control_connected(true);
            stats.attach_auto_dict(compression_state.clone());
        }
        let shutdown_rx_clone = shutdown_tx.subscribe();
        let bind_addr = service.bind_addr.clone();
        let compression_ctx = service.compression.map(|_| {
            Arc::new(CompressionCtx {
                dict: service.compression_dictionary_loaded.clone(),
                level: service
                    .compression_level
                    .unwrap_or(crate::constants::DEFAULT_ZSTD_LEVEL),
            })
        });
        let generation_rx = compression_state
            .as_ref()
            .map(|state| state.generation_rx.clone());
        match service.service_type {
            ServiceType::Tcp => {
                let compression = compression_ctx.clone();
                let generation_rx = generation_rx.clone();
                let compression_state_for_pool = compression_state.clone();
                let stats = stats.clone();
                tokio::spawn(
                    async move {
                        if let Err(e) = run_tcp_connection_pool::<T>(
                            bind_addr,
                            data_ch_rx,
                            data_ch_req_tx,
                            shutdown_rx_clone,
                            compression,
                            generation_rx,
                            compression_state_for_pool,
                            stats,
                        )
                        .await
                        .with_context(|| "Failed to run TCP connection pool")
                        {
                            error!("{:#}", e);
                        }
                    }
                    .instrument(Span::current()),
                )
            }
            ServiceType::Udp => {
                let compression = compression_ctx.clone();
                let stats = stats.clone();
                tokio::spawn(
                    async move {
                        if let Err(e) = run_udp_connection_pool::<T>(
                            bind_addr,
                            data_ch_rx,
                            data_ch_req_tx,
                            shutdown_rx_clone,
                            compression,
                            stats,
                        )
                        .await
                        .with_context(|| "Failed to run TCP connection pool")
                        {
                            error!("{:#}", e);
                        }
                    }
                    .instrument(Span::current()),
                )
            }
        };

        // Create the control channel
        let ch = ControlChannel::<T> {
            conn,
            shutdown_rx,
            data_ch_req_rx,
            heartbeat_interval: server_config.heartbeat_interval,
            generation_rx,
        };

        // Run the control channel
        tokio::spawn(
            async move {
                if let Err(err) = ch.run().await {
                    error!("{:#}", err);
                }
            }
            .instrument(Span::current()),
        );

        ControlChannelHandle {
            _shutdown_tx: shutdown_tx,
            data_ch_tx,
            service,
            _compression_state: compression_state,
            stats,
        }
    }
}

impl<T: Transport> Drop for ControlChannelHandle<T> {
    fn drop(&mut self) {
        if let Some(stats) = &self.stats {
            stats.set_control_connected(false);
        }
    }
}

// Control channel, using T as the transport layer. P is TcpStream or UdpTraffic
struct ControlChannel<T: Transport> {
    conn: T::Stream,                               // The connection of control channel
    shutdown_rx: broadcast::Receiver<bool>,        // Receives the shutdown signal
    data_ch_req_rx: mpsc::UnboundedReceiver<bool>, // Receives visitor connections
    heartbeat_interval: u64,                       // Application-layer heartbeat interval in secs
    generation_rx: Option<watch::Receiver<Option<Arc<Generation>>>>,
}

impl<T: Transport> ControlChannel<T> {
    async fn write_and_flush(&mut self, data: &[u8]) -> Result<()> {
        write_and_flush(&mut self.conn, data)
            .await
            .with_context(|| "Failed to write control cmds")?;
        Ok(())
    }

    async fn push_generation(&mut self, generation: &Generation) -> Result<()> {
        let cmd = bincode::serialize(&ControlChannelCmd::UpdateCompressionDict {
            digest: generation.digest,
            dictionary: generation.dictionary.bytes.to_vec(),
        })?;
        self.write_and_flush(&cmd).await
    }

    // Run a control channel
    #[instrument(skip_all)]
    async fn run(mut self) -> Result<()> {
        let create_ch_cmd = bincode::serialize(&ControlChannelCmd::CreateDataChannel).unwrap();
        let heartbeat = bincode::serialize(&ControlChannelCmd::HeartBeat).unwrap();

        if let Some(generation_rx) = self.generation_rx.as_mut() {
            let generation = generation_rx.borrow_and_update().clone();
            if let Some(generation) = generation {
                self.push_generation(&generation).await?;
            }
        }

        // Wait for data channel requests and the shutdown signal
        loop {
            tokio::select! {
                val = self.data_ch_req_rx.recv() => {
                    match val {
                        Some(_) => {
                            if let Err(e) = self.write_and_flush(&create_ch_cmd).await {
                                error!("{:#}", e);
                                break;
                            }
                        }
                        None => {
                            break;
                        }
                    }
                },
                _ = time::sleep(Duration::from_secs(self.heartbeat_interval)), if self.heartbeat_interval != 0 => {
                            if let Err(e) = self.write_and_flush(&heartbeat).await {
                                error!("{:#}", e);
                                break;
                            }
                }
                generation_changed = async {
                    match self.generation_rx.as_mut() {
                        Some(generation_rx) => generation_rx.changed().await.is_ok(),
                        None => std::future::pending::<bool>().await,
                    }
                }, if self.generation_rx.is_some() => {
                    if generation_changed {
                        let generation = self
                            .generation_rx
                            .as_mut()
                            .and_then(|generation_rx| generation_rx.borrow_and_update().clone());
                        if let Some(generation) = generation {
                            if let Err(e) = self.push_generation(&generation).await {
                                error!("{:#}", e);
                                break;
                            }
                        }
                    } else {
                        self.generation_rx = None;
                    }
                }
                // Wait for the shutdown signal
                _ = self.shutdown_rx.recv() => {
                    break;
                }
            }
        }

        info!("Control channel shutdown");

        Ok(())
    }
}

fn tcp_listen_and_send(
    addr: String,
    data_ch_req_tx: mpsc::UnboundedSender<bool>,
    mut shutdown_rx: broadcast::Receiver<bool>,
) -> mpsc::Receiver<TcpStream> {
    let (tx, rx) = mpsc::channel(CHAN_SIZE);

    tokio::spawn(async move {
        let l = retry_notify_with_deadline(listen_backoff(),  || async {
            Ok(TcpListener::bind(&addr).await?)
        }, |e, duration| {
            error!("{:#}. Retry in {:?}", e, duration);
        }, &mut shutdown_rx).await
        .with_context(|| "Failed to listen for the service");

        let l: TcpListener = match l {
            Ok(v) => v,
            Err(e) => {
                error!("{:#}", e);
                return;
            }
        };

        info!("Listening at {}", &addr);

        // Retry at least every 1s
        let mut backoff = ExponentialBackoff {
            max_interval: Duration::from_secs(1),
            max_elapsed_time: None,
            ..Default::default()
        };

        // Wait for visitors and the shutdown signal
        loop {
            tokio::select! {
                val = l.accept() => {
                    match val {
                        Err(e) => {
                            // `l` is a TCP listener so this must be a IO error
                            // Possibly a EMFILE. So sleep for a while
                            error!("{}. Sleep for a while", e);
                            if let Some(d) = backoff.next_backoff() {
                                time::sleep(d).await;
                            } else {
                                // This branch will never be reached for current backoff policy
                                error!("Too many retries. Aborting...");
                                break;
                            }
                        }
                        Ok((incoming, addr)) => {
                            // For every visitor, request to create a data channel
                            if data_ch_req_tx.send(true).with_context(|| "Failed to send data chan create request").is_err() {
                                // An error indicates the control channel is broken
                                // So break the loop
                                break;
                            }

                            backoff.reset();

                            debug!("New visitor from {}", addr);

                            // Send the visitor to the connection pool
                            let _ = tx.send(incoming).await;
                        }
                    }
                },
                _ = shutdown_rx.recv() => {
                    break;
                }
            }
        }

        info!("TCPListener shutdown");
    }.instrument(Span::current()));

    rx
}

#[instrument(skip_all)]
#[allow(clippy::too_many_arguments)]
async fn run_tcp_connection_pool<T: Transport>(
    bind_addr: String,
    mut data_ch_rx: mpsc::Receiver<T::Stream>,
    data_ch_req_tx: mpsc::UnboundedSender<bool>,
    shutdown_rx: broadcast::Receiver<bool>,
    compression: Option<Arc<CompressionCtx>>,
    generation_rx: Option<watch::Receiver<Option<Arc<Generation>>>>,
    compression_state: Option<Arc<ServiceCompressionState>>,
    stats: Option<Arc<ServiceStats>>,
) -> Result<()> {
    let mut visitor_rx = tcp_listen_and_send(bind_addr, data_ch_req_tx.clone(), shutdown_rx);
    let cmd = bincode::serialize(&tcp_cmd(&compression)).unwrap();

    'pool: while let Some(visitor) = visitor_rx.recv().await {
        let generation = generation_rx.as_ref().map(tcp_generation_snapshot);
        let sampling_state = tcp_sampling_state(compression_state.as_ref());
        let visitor_cmd = match generation.as_ref() {
            Some(Some(generation)) => &generation.tcp_cmd_bytes,
            Some(None) | None => &cmd,
        };
        loop {
            if let Some(mut ch) = data_ch_rx.recv().await {
                if write_and_flush(&mut ch, visitor_cmd).await.is_ok() {
                    #[cfg(feature = "compression-zstd")]
                    let compression = compression.clone();
                    #[cfg(feature = "compression-zstd")]
                    let generation = generation.clone();
                    let stats = stats.clone();
                    tokio::spawn(async move {
                        let visitor = maybe_count(visitor, stats.as_ref(), ByteKind::Visitor);
                        let ch = maybe_count(ch, stats.as_ref(), ByteKind::Wire);
                        #[cfg(feature = "compression-zstd")]
                        let (compression_enabled, dictionary, level) = match generation.as_ref() {
                            Some(generation) => (
                                true,
                                generation.as_ref().map(|generation| &generation.dictionary),
                                generation
                                    .as_ref()
                                    .map(|generation| generation.level)
                                    .unwrap_or(
                                        compression
                                            .as_ref()
                                            .map(|ctx| ctx.level)
                                            .unwrap_or(crate::constants::DEFAULT_ZSTD_LEVEL),
                                    ),
                            ),
                            None => (
                                compression.is_some(),
                                compression.as_ref().and_then(|ctx| ctx.dict.as_ref()),
                                compression
                                    .as_ref()
                                    .map(|ctx| ctx.level)
                                    .unwrap_or(crate::constants::DEFAULT_ZSTD_LEVEL),
                            ),
                        };
                        #[cfg(feature = "compression-zstd")]
                        match wrap_stream(ch, compression_enabled, dictionary, level) {
                            Ok(mut wrapped) => {
                                let _session = stats.as_ref().map(|s| {
                                    s.on_session_start();
                                    ActiveSession(Arc::clone(s))
                                });
                                match sampling_state {
                                    Some(state) => {
                                        let mut visitor = SamplingStream::new(visitor, state);
                                        let _ =
                                            copy_bidirectional(&mut wrapped, &mut visitor).await;
                                    }
                                    None => {
                                        let mut visitor = visitor;
                                        let _ =
                                            copy_bidirectional(&mut wrapped, &mut visitor).await;
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to wrap data channel with compression: {:#}", e);
                            }
                        }

                        #[cfg(not(feature = "compression-zstd"))]
                        {
                            let _session = stats.as_ref().map(|s| {
                                s.on_session_start();
                                ActiveSession(Arc::clone(s))
                            });
                            match sampling_state {
                                Some(state) => {
                                    let mut visitor = SamplingStream::new(visitor, state);
                                    let mut ch = ch;
                                    let _ = copy_bidirectional(&mut ch, &mut visitor).await;
                                }
                                None => {
                                    let mut visitor = visitor;
                                    let mut ch = ch;
                                    let _ = copy_bidirectional(&mut ch, &mut visitor).await;
                                }
                            }
                        }
                    });
                    break;
                } else {
                    // Current data channel is broken. Request for a new one
                    if data_ch_req_tx.send(true).is_err() {
                        break 'pool;
                    }
                }
            } else {
                break 'pool;
            }
        }
    }

    info!("Shutdown");
    Ok(())
}

#[instrument(skip_all)]
async fn run_udp_connection_pool<T: Transport>(
    bind_addr: String,
    mut data_ch_rx: mpsc::Receiver<T::Stream>,
    _data_ch_req_tx: mpsc::UnboundedSender<bool>,
    mut shutdown_rx: broadcast::Receiver<bool>,
    compression: Option<Arc<CompressionCtx>>,
    stats: Option<Arc<ServiceStats>>,
) -> Result<()> {
    // TODO: Load balance

    let l = retry_notify_with_deadline(
        listen_backoff(),
        || async { Ok(UdpSocket::bind(&bind_addr).await?) },
        |e, duration| {
            warn!("{:#}. Retry in {:?}", e, duration);
        },
        &mut shutdown_rx,
    )
    .await
    .with_context(|| "Failed to listen for the service")?;

    info!("Listening at {}", &bind_addr);

    let cmd = bincode::serialize(&udp_cmd(&compression)).unwrap();

    // Receive one data channel
    let mut conn = data_ch_rx
        .recv()
        .await
        .ok_or_else(|| anyhow!("No available data channels"))?;
    write_and_flush(&mut conn, &cmd).await?;

    let conn = maybe_count(conn, stats.as_ref(), ByteKind::Wire);
    #[cfg(feature = "compression-zstd")]
    let mut conn = wrap_stream(
        conn,
        compression.is_some(),
        compression.as_ref().and_then(|ctx| ctx.dict.as_ref()),
        compression
            .as_ref()
            .map(|ctx| ctx.level)
            .unwrap_or(crate::constants::DEFAULT_ZSTD_LEVEL),
    )?;
    #[cfg(not(feature = "compression-zstd"))]
    let mut conn = conn;

    let mut buf = [0u8; UDP_BUFFER_SIZE];
    loop {
        tokio::select! {
            // Forward inbound traffic to the client
            val = l.recv_from(&mut buf) => {
                let (n, from) = val?;
                if let Some(stats) = &stats {
                    stats.add_datagram_in(n as u64);
                }
                UdpTraffic::write_slice(&mut conn, from, &buf[..n]).await?;
                conn.flush().await?;
            },

            // Forward outbound traffic from the client to the visitor
            hdr_len = conn.read_u8() => {
                let t = UdpTraffic::read(&mut conn, hdr_len?).await?;
                if let Some(stats) = &stats {
                    stats.add_datagram_out(t.data.len() as u64);
                }
                l.send_to(&t.data, t.from).await?;
            }

            _ = shutdown_rx.recv() => {
                break;
            }
        }
    }

    debug!("UDP pool dropped");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::AddrMaybeCached;
    use std::net::SocketAddr;
    use tokio::io::DuplexStream;
    use tokio::net::ToSocketAddrs;

    #[derive(Debug)]
    struct DuplexTransport;

    #[async_trait::async_trait]
    impl Transport for DuplexTransport {
        type Acceptor = ();
        type RawStream = DuplexStream;
        type Stream = DuplexStream;

        fn new(_config: &crate::config::TransportConfig) -> Result<Self> {
            unreachable!("DuplexTransport is only used with preconstructed streams")
        }

        fn hint(_conn: &Self::Stream, _opts: SocketOpts) {}

        async fn bind<A: ToSocketAddrs + Send + Sync>(&self, _addr: A) -> Result<Self::Acceptor> {
            unreachable!("DuplexTransport does not bind listeners")
        }

        async fn accept(
            &self,
            _acceptor: &Self::Acceptor,
        ) -> Result<(Self::RawStream, SocketAddr)> {
            unreachable!("DuplexTransport does not accept connections")
        }

        async fn handshake(&self, _conn: Self::RawStream) -> Result<Self::Stream> {
            unreachable!("DuplexTransport does not perform handshakes")
        }

        async fn connect(&self, _addr: &AddrMaybeCached) -> Result<Self::Stream> {
            unreachable!("DuplexTransport does not connect")
        }
    }

    fn control_channel(
        conn: DuplexStream,
        generation_rx: Option<watch::Receiver<Option<Arc<Generation>>>>,
        heartbeat_interval: u64,
    ) -> (
        ControlChannel<DuplexTransport>,
        mpsc::UnboundedSender<bool>,
        broadcast::Sender<bool>,
    ) {
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let (data_ch_req_tx, data_ch_req_rx) = mpsc::unbounded_channel();
        (
            ControlChannel {
                conn,
                shutdown_rx,
                data_ch_req_rx,
                heartbeat_interval,
                generation_rx,
            },
            data_ch_req_tx,
            shutdown_tx,
        )
    }

    fn compression_service(name: &str, service_type: ServiceType) -> ServerServiceConfig {
        ServerServiceConfig {
            name: name.to_string(),
            service_type,
            compression: Some(crate::config::CompressionType::Zstd),
            compression_auto_dictionary: Some(true),
            ..Default::default()
        }
    }

    async fn server_for_hot_reload_tests() -> Server<TcpTransport> {
        Server::from(ServerConfig::default()).await.unwrap()
    }

    fn generation_with_digest(digest: protocol::Digest) -> Arc<Generation> {
        Arc::new(
            Generation::new(LoadedDictionary {
                digest,
                ..Default::default()
            })
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn control_channel_pushes_existing_generation_before_queued_data_channel_request() {
        // Given
        let digest = [7; HASH_WIDTH_IN_BYTES];
        let (_generation_tx, generation_rx) = watch::channel(Some(generation_with_digest(digest)));
        let (server, mut client) = tokio::io::duplex(1024);
        let (control_channel, data_ch_req_tx, shutdown_tx) =
            control_channel(server, Some(generation_rx), 0);
        data_ch_req_tx.send(true).unwrap();

        // When
        let task = tokio::spawn(control_channel.run());
        let first = protocol::read_control_cmd(&mut client).await.unwrap();
        let second = protocol::read_control_cmd(&mut client).await.unwrap();

        // Then
        assert_eq!(
            first,
            ControlChannelCmd::UpdateCompressionDict {
                digest,
                dictionary: Vec::new(),
            }
        );
        assert_eq!(second, ControlChannelCmd::CreateDataChannel);
        shutdown_tx.send(true).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn control_channel_without_generation_receiver_only_sends_existing_commands() {
        // Given
        let (server, mut client) = tokio::io::duplex(1024);
        let (control_channel, data_ch_req_tx, shutdown_tx) = control_channel(server, None, 0);
        data_ch_req_tx.send(true).unwrap();

        // When
        let task = tokio::spawn(control_channel.run());
        let command = protocol::read_control_cmd(&mut client).await.unwrap();

        // Then
        assert_eq!(command, ControlChannelCmd::CreateDataChannel);
        shutdown_tx.send(true).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn control_channel_pushes_live_generation_once() {
        // Given
        let digest = [9; HASH_WIDTH_IN_BYTES];
        let (generation_tx, generation_rx) = watch::channel(None);
        let (server, mut client) = tokio::io::duplex(1024);
        let (control_channel, data_ch_req_tx, shutdown_tx) =
            control_channel(server, Some(generation_rx), 0);
        let task = tokio::spawn(control_channel.run());

        // When
        generation_tx.send_replace(Some(generation_with_digest(digest)));
        let first = protocol::read_control_cmd(&mut client).await.unwrap();
        data_ch_req_tx.send(true).unwrap();
        let second = protocol::read_control_cmd(&mut client).await.unwrap();

        // Then
        assert_eq!(
            first,
            ControlChannelCmd::UpdateCompressionDict {
                digest,
                dictionary: Vec::new(),
            }
        );
        assert_eq!(second, ControlChannelCmd::CreateDataChannel);
        shutdown_tx.send(true).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn control_channel_heartbeat_is_unaffected_by_generation_receiver() {
        // Given
        let (_generation_tx, generation_rx) = watch::channel(None);
        let (server, mut client) = tokio::io::duplex(1024);
        let (control_channel, _data_ch_req_tx, shutdown_tx) =
            control_channel(server, Some(generation_rx), 1);

        // When
        let task = tokio::spawn(control_channel.run());
        let command = time::timeout(
            Duration::from_secs(2),
            protocol::read_control_cmd(&mut client),
        )
        .await
        .unwrap()
        .unwrap();

        // Then
        assert_eq!(command, ControlChannelCmd::HeartBeat);
        shutdown_tx.send(true).unwrap();
        task.await.unwrap().unwrap();
    }

    #[test]
    fn tcp_generation_snapshot_keeps_command_and_dictionary_on_the_same_generation() {
        // Given: two independent receiver reads could race a generation replacement and pair A's
        // command with B's dictionary. The visitor snapshot must perform only one receiver read.
        let state = ServiceCompressionState::new(1024);
        let digest = [7; HASH_WIDTH_IN_BYTES];
        state
            .generation_tx
            .send_replace(Some(generation_with_digest(digest)));

        // When
        let snapshot = tcp_generation_snapshot(&state.generation_rx).unwrap();

        // Then
        let cmd: DataChannelCmd = bincode::deserialize(&snapshot.tcp_cmd_bytes).unwrap();
        let udp_cmd: DataChannelCmd = bincode::deserialize(&snapshot.udp_cmd_bytes).unwrap();
        assert_eq!(
            cmd,
            DataChannelCmd::StartForwardTcpZstd {
                dict_digest: snapshot.dictionary.digest
            }
        );
        assert_eq!(
            udp_cmd,
            DataChannelCmd::StartForwardUdpZstd {
                dict_digest: snapshot.dictionary.digest
            }
        );
    }

    #[test]
    fn tcp_generation_swap_does_not_change_an_existing_visitor_snapshot() {
        // Given
        let state = ServiceCompressionState::new(1024);
        let old_digest = [3; HASH_WIDTH_IN_BYTES];
        let new_digest = [5; HASH_WIDTH_IN_BYTES];
        state
            .generation_tx
            .send_replace(Some(generation_with_digest(old_digest)));
        let visitor_one = tcp_generation_snapshot(&state.generation_rx).unwrap();

        // When
        state
            .generation_tx
            .send_replace(Some(generation_with_digest(new_digest)));
        let visitor_two = tcp_generation_snapshot(&state.generation_rx).unwrap();

        // Then
        assert_eq!(visitor_one.digest, old_digest);
        assert_eq!(visitor_one.dictionary.digest, old_digest);
        assert_eq!(visitor_two.digest, new_digest);
        assert_eq!(visitor_two.dictionary.digest, new_digest);
    }

    #[tokio::test]
    async fn hot_reload_add_evicts_existing_service_compression_state() {
        // Given
        let service = compression_service("reloaded", ServiceType::Tcp);
        let service_digest = protocol::digest(service.name.as_bytes());
        let state = Arc::new(ServiceCompressionState::new(1024));
        state
            .generation_tx
            .send_replace(Some(generation_with_digest([7; HASH_WIDTH_IN_BYTES])));
        let mut server = server_for_hot_reload_tests().await;
        server
            .service_compression_states
            .write()
            .await
            .insert(service_digest, state);

        // When
        server
            .handle_hot_reload(ConfigChange::ServerChange(ServerServiceChange::Add(
                service,
            )))
            .await;

        // Then
        assert!(!server
            .service_compression_states
            .read()
            .await
            .contains_key(&service_digest));
    }

    #[tokio::test]
    async fn hot_reload_delete_evicts_existing_service_compression_state() {
        // Given
        let service_name = "deleted";
        let service_digest = protocol::digest(service_name.as_bytes());
        let state = Arc::new(ServiceCompressionState::new(1024));
        let mut server = server_for_hot_reload_tests().await;
        server
            .service_compression_states
            .write()
            .await
            .insert(service_digest, state);

        // When
        server
            .handle_hot_reload(ConfigChange::ServerChange(ServerServiceChange::Delete(
                service_name.to_string(),
            )))
            .await;

        // Then
        assert!(!server
            .service_compression_states
            .read()
            .await
            .contains_key(&service_digest));
    }

    #[tokio::test]
    async fn hot_reload_updates_observe_stats() {
        let mut config = ServerConfig::default();
        config.observe_addr = Some("127.0.0.1:0".into());
        let mut server = Server::<TcpTransport>::from(config).await.unwrap();
        assert!(server.stats.as_ref().unwrap().service("echo").is_none());

        server
            .handle_hot_reload(ConfigChange::ServerChange(ServerServiceChange::Add(
                ServerServiceConfig {
                    name: "echo".into(),
                    bind_addr: "127.0.0.1:9".into(),
                    ..Default::default()
                },
            )))
            .await;
        assert!(server.stats.as_ref().unwrap().service("echo").is_some());

        server
            .handle_hot_reload(ConfigChange::ServerChange(ServerServiceChange::Delete(
                "echo".into(),
            )))
            .await;
        assert!(server.stats.as_ref().unwrap().service("echo").is_none());
    }

    #[tokio::test]
    async fn udp_auto_dictionary_service_does_not_create_compression_state() {
        // Given
        let service = compression_service("udp", ServiceType::Udp);
        let service_digest = protocol::digest(service.name.as_bytes());
        let states = Arc::new(RwLock::new(HashMap::new()));

        // When
        let state =
            get_or_create_service_compression_state(&states, service_digest, &service).await;

        // Then
        assert!(state.is_none());
        assert!(states.read().await.is_empty());
    }

    #[tokio::test]
    async fn static_dictionary_service_does_not_create_auto_compression_state() {
        // Given
        let mut service = compression_service("static", ServiceType::Tcp);
        service.compression_dictionary = Some("dictionary.bin".to_string());
        service.compression_auto_dictionary = Some(false);
        let service_digest = protocol::digest(service.name.as_bytes());
        let states = Arc::new(RwLock::new(HashMap::new()));

        // When
        let state =
            get_or_create_service_compression_state(&states, service_digest, &service).await;

        // Then
        assert!(state.is_none());
        assert!(states.read().await.is_empty());
    }

    #[test]
    fn data_channel_commands_are_plain_when_compression_is_disabled() {
        // Given
        let compression = None;

        // When
        let tcp = tcp_cmd(&compression);
        let udp = udp_cmd(&compression);

        // Then
        assert_eq!(tcp, DataChannelCmd::StartForwardTcp);
        assert_eq!(udp, DataChannelCmd::StartForwardUdp);
        assert_eq!(
            bincode::serialize(&tcp).unwrap(),
            bincode::serialize(&DataChannelCmd::StartForwardTcp).unwrap()
        );
        assert_eq!(
            bincode::serialize(&udp).unwrap(),
            bincode::serialize(&DataChannelCmd::StartForwardUdp).unwrap()
        );
        #[cfg(feature = "compression-zstd")]
        assert!(matches!(
            wrap_stream(
                tokio::io::duplex(64).0,
                false,
                None,
                crate::constants::DEFAULT_ZSTD_LEVEL
            )
            .unwrap(),
            crate::compression::MaybeCompressed::Plain(_)
        ));
    }

    #[test]
    fn data_channel_commands_include_dictionary_digest_when_compression_is_enabled() {
        // Given
        let digest = [7; HASH_WIDTH_IN_BYTES];
        let compression = Some(Arc::new(CompressionCtx {
            dict: Some(LoadedDictionary {
                digest,
                ..Default::default()
            }),
            level: crate::constants::DEFAULT_ZSTD_LEVEL,
        }));

        // When
        let tcp = tcp_cmd(&compression);
        let udp = udp_cmd(&compression);

        // Then
        assert_eq!(
            tcp,
            DataChannelCmd::StartForwardTcpZstd {
                dict_digest: digest
            }
        );
        assert_eq!(
            udp,
            DataChannelCmd::StartForwardUdpZstd {
                dict_digest: digest
            }
        );
        assert_eq!(
            bincode::serialize(&tcp).unwrap(),
            bincode::serialize(&DataChannelCmd::StartForwardTcpZstd {
                dict_digest: digest
            })
            .unwrap()
        );
        assert_eq!(
            bincode::serialize(&udp).unwrap(),
            bincode::serialize(&DataChannelCmd::StartForwardUdpZstd {
                dict_digest: digest
            })
            .unwrap()
        );
        #[cfg(feature = "compression-zstd")]
        assert!(matches!(
            wrap_stream(
                tokio::io::duplex(64).0,
                true,
                compression.as_ref().and_then(|ctx| ctx.dict.as_ref()),
                crate::constants::DEFAULT_ZSTD_LEVEL,
            )
            .unwrap(),
            crate::compression::MaybeCompressed::Zstd(_)
        ));
    }

    #[test]
    fn data_channel_commands_use_zero_digest_for_dictionary_free_compression() {
        // Given
        let compression = Some(Arc::new(CompressionCtx {
            dict: None,
            level: crate::constants::DEFAULT_ZSTD_LEVEL,
        }));

        // When
        let tcp = tcp_cmd(&compression);
        let udp = udp_cmd(&compression);

        // Then
        let digest = [0; HASH_WIDTH_IN_BYTES];
        assert_eq!(
            tcp,
            DataChannelCmd::StartForwardTcpZstd {
                dict_digest: digest
            }
        );
        assert_eq!(
            udp,
            DataChannelCmd::StartForwardUdpZstd {
                dict_digest: digest
            }
        );
    }

    #[test]
    fn data_channel_commands_omit_level_even_when_not_default() {
        let digest = [7; HASH_WIDTH_IN_BYTES];
        let compression = Some(Arc::new(CompressionCtx {
            dict: Some(LoadedDictionary {
                digest,
                ..Default::default()
            }),
            level: 19,
        }));

        assert_eq!(
            tcp_cmd(&compression),
            DataChannelCmd::StartForwardTcpZstd {
                dict_digest: digest
            }
        );
        assert_eq!(
            udp_cmd(&compression),
            DataChannelCmd::StartForwardUdpZstd {
                dict_digest: digest
            }
        );
    }
}
