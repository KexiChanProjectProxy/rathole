use crate::config::{ClientConfig, ClientServiceConfig, Config, ServiceType, TransportType};
use crate::config_watcher::{ClientServiceChange, ConfigChange};
use crate::helper::udp_connect;
use crate::protocol::Hello::{self, *};
use crate::protocol::{
    self, read_ack, read_control_cmd, read_data_cmd, read_hello, Ack, Auth, ControlChannelCmd,
    DataChannelCmd, UdpTraffic, CURRENT_PROTO_VERSION, HASH_WIDTH_IN_BYTES,
};
use crate::transport::{AddrMaybeCached, SocketOpts, TcpTransport, Transport};
use anyhow::{anyhow, bail, Context, Result};
use backoff::backoff::Backoff;
use backoff::future::retry_notify;
use backoff::ExponentialBackoff;
use bytes::{Bytes, BytesMut};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{self, copy_bidirectional, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::{broadcast, mpsc, oneshot, watch, Mutex, RwLock};
use tokio::time::{self, Duration, Instant};
use tracing::{debug, error, info, instrument, trace, warn, Instrument, Span};

#[cfg(feature = "noise")]
use crate::transport::NoiseTransport;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use crate::transport::TlsTransport;
#[cfg(any(feature = "websocket-native-tls", feature = "websocket-rustls"))]
use crate::transport::WebsocketTransport;

use crate::constants::{run_control_chan_backoff, UDP_BUFFER_SIZE, UDP_SENDQ_SIZE, UDP_TIMEOUT};

// The entrypoint of running a client
pub async fn run_client(
    config: Config,
    shutdown_rx: broadcast::Receiver<bool>,
    update_rx: mpsc::Receiver<ConfigChange>,
) -> Result<()> {
    let config = config.client.ok_or_else(|| {
        anyhow!(
        "Try to run as a client, but the configuration is missing. Please add the `[client]` block"
    )
    })?;

    match config.transport.transport_type {
        TransportType::Tcp => {
            let mut client = Client::<TcpTransport>::from(config).await?;
            client.run(shutdown_rx, update_rx).await
        }
        TransportType::Tls => {
            #[cfg(any(feature = "native-tls", feature = "rustls"))]
            {
                let mut client = Client::<TlsTransport>::from(config).await?;
                client.run(shutdown_rx, update_rx).await
            }
            #[cfg(not(any(feature = "native-tls", feature = "rustls")))]
            crate::helper::feature_neither_compile("native-tls", "rustls")
        }
        TransportType::Noise => {
            #[cfg(feature = "noise")]
            {
                let mut client = Client::<NoiseTransport>::from(config).await?;
                client.run(shutdown_rx, update_rx).await
            }
            #[cfg(not(feature = "noise"))]
            crate::helper::feature_not_compile("noise")
        }
        TransportType::Websocket => {
            #[cfg(any(feature = "websocket-native-tls", feature = "websocket-rustls"))]
            {
                let mut client = Client::<WebsocketTransport>::from(config).await?;
                client.run(shutdown_rx, update_rx).await
            }
            #[cfg(not(any(feature = "websocket-native-tls", feature = "websocket-rustls")))]
            crate::helper::feature_neither_compile("websocket-native-tls", "websocket-rustls")
        }
    }
}

type ServiceDigest = protocol::Digest;
type Nonce = protocol::Digest;
type DictSnapshot = Arc<HashMap<protocol::Digest, Arc<Vec<u8>>>>;

const DICT_CACHE_CAPACITY: usize = 4;

struct DictCache {
    dictionaries_tx: watch::Sender<DictSnapshot>,
    insertion_order: Mutex<VecDeque<protocol::Digest>>,
}

impl DictCache {
    fn new() -> Self {
        let (dictionaries_tx, _) = watch::channel(Arc::new(HashMap::new()));
        Self {
            dictionaries_tx,
            insertion_order: Mutex::new(VecDeque::with_capacity(DICT_CACHE_CAPACITY)),
        }
    }

    async fn insert(&self, digest: protocol::Digest, dictionary: Arc<Vec<u8>>) {
        let mut insertion_order = self.insertion_order.lock().await;
        let current = Arc::clone(&self.dictionaries_tx.borrow());
        if current.contains_key(&digest) {
            return;
        }

        let mut dictionaries = (*current).clone();
        if insertion_order.len() == DICT_CACHE_CAPACITY {
            if let Some(oldest_digest) = insertion_order.pop_front() {
                dictionaries.remove(&oldest_digest);
            }
        }
        insertion_order.push_back(digest);
        dictionaries.insert(digest, dictionary);
        self.dictionaries_tx.send_replace(Arc::new(dictionaries));
    }

    fn lookup(&self, digest: &protocol::Digest) -> Option<Arc<Vec<u8>>> {
        self.dictionaries_tx.borrow().get(digest).cloned()
    }

    fn subscribe(&self) -> watch::Receiver<DictSnapshot> {
        self.dictionaries_tx.subscribe()
    }
}

async fn handle_compression_dict_update(
    cache: &DictCache,
    digest: protocol::Digest,
    dictionary: Vec<u8>,
) {
    #[cfg(feature = "compression-zstd")]
    {
        let actual_digest = protocol::digest(&dictionary);
        if actual_digest != digest {
            warn!(
                claimed_digest = %hex::encode(digest),
                actual_digest = %hex::encode(actual_digest),
                "discarding compression dictionary with mismatched digest"
            );
            return;
        }
        cache.insert(digest, Arc::new(dictionary)).await;
    }

    #[cfg(not(feature = "compression-zstd"))]
    {
        let _ = (cache, digest, dictionary);
        warn!(
            "server pushed a compression dictionary but this binary lacks feature compression-zstd"
        );
    }
}

// Holds the state of a client
struct Client<T: Transport> {
    config: ClientConfig,
    service_handles: HashMap<String, Vec<ControlChannelHandle>>,
    transport: Arc<T>,
}

impl<T: 'static + Transport> Client<T> {
    // Create a Client from `[client]` config block
    async fn from(config: ClientConfig) -> Result<Client<T>> {
        let transport =
            Arc::new(T::new(&config.transport).with_context(|| "Failed to create the transport")?);
        Ok(Client {
            config,
            service_handles: HashMap::new(),
            transport,
        })
    }

    // The entrypoint of Client
    async fn run(
        &mut self,
        mut shutdown_rx: broadcast::Receiver<bool>,
        mut update_rx: mpsc::Receiver<ConfigChange>,
    ) -> Result<()> {
        info!(n_servers = self.config.remote_addr.len(), "Client starting");

        for (name, config) in &self.config.services {
            // Create a control channel to every remote server for each service
            let handles = self.spawn_service_handles((*config).clone());
            self.service_handles.insert(name.clone(), handles);
        }

        // Wait for the shutdown signal
        loop {
            tokio::select! {
                val = shutdown_rx.recv() => {
                    match val {
                        Ok(_) => {}
                        Err(err) => {
                            error!("Unable to listen for shutdown signal: {}", err);
                        }
                    }
                    break;
                },
                e = update_rx.recv() => {
                    if let Some(e) = e {
                        self.handle_hot_reload(e).await;
                    }
                }
            }
        }

        // Shutdown all services
        for (_, handles) in self.service_handles.drain() {
            for handle in handles {
                handle.shutdown();
            }
        }

        Ok(())
    }

    fn spawn_service_handles(&self, config: ClientServiceConfig) -> Vec<ControlChannelHandle> {
        let dict_cache = Arc::new(DictCache::new());
        self.config
            .remote_addr
            .iter()
            .map(|remote_addr| {
                ControlChannelHandle::new(
                    config.clone(),
                    remote_addr.clone(),
                    self.transport.clone(),
                    self.config.heartbeat_timeout,
                    Arc::clone(&dict_cache),
                )
            })
            .collect()
    }

    async fn handle_hot_reload(&mut self, e: ConfigChange) {
        match e {
            ConfigChange::ClientChange(client_change) => match client_change {
                ClientServiceChange::Add(cfg) => {
                    let name = cfg.name.clone();
                    let handles = self.spawn_service_handles(cfg);
                    let _ = self.service_handles.insert(name, handles);
                }
                ClientServiceChange::Delete(s) => {
                    if let Some(handles) = self.service_handles.remove(&s) {
                        for handle in handles {
                            handle.shutdown();
                        }
                    }
                }
            },
            ignored => warn!("Ignored {:?} since running as a client", ignored),
        }
    }
}

struct RunDataChannelArgs<T: Transport> {
    session_key: Nonce,
    remote_addr: AddrMaybeCached,
    connector: Arc<T>,
    socket_opts: SocketOpts,
    service: ClientServiceConfig,
    dict_cache: Arc<DictCache>,
}

async fn do_data_channel_handshake<T: Transport>(
    args: Arc<RunDataChannelArgs<T>>,
) -> Result<T::Stream> {
    // Retry at least every 100ms, at most for 10 seconds
    let backoff = ExponentialBackoff {
        max_interval: Duration::from_millis(100),
        max_elapsed_time: Some(Duration::from_secs(10)),
        ..Default::default()
    };

    // Connect to remote_addr
    let mut conn: T::Stream = retry_notify(
        backoff,
        || async {
            args.connector
                .connect(&args.remote_addr)
                .await
                .with_context(|| format!("Failed to connect to {}", args.remote_addr))
                .map_err(backoff::Error::transient)
        },
        |e, duration| {
            warn!("{:#}. Retry in {:?}", e, duration);
        },
    )
    .await?;

    T::hint(&conn, args.socket_opts);

    // Send nonce
    let v: &[u8; HASH_WIDTH_IN_BYTES] = args.session_key[..].try_into().unwrap();
    let hello = Hello::DataChannelHello(CURRENT_PROTO_VERSION, v.to_owned());
    conn.write_all(&bincode::serialize(&hello).unwrap()).await?;
    conn.flush().await?;

    Ok(conn)
}

async fn run_data_channel<T: Transport>(args: Arc<RunDataChannelArgs<T>>) -> Result<()> {
    // Do the handshake
    let mut conn = do_data_channel_handshake(args.clone()).await?;

    // Forward
    match read_data_cmd(&mut conn).await? {
        DataChannelCmd::StartForwardTcp => {
            if args.service.service_type != ServiceType::Tcp {
                bail!("Expect TCP traffic. Please check the configuration.")
            }
            run_data_channel_for_tcp(conn, &args.service.local_addr).await?;
        }
        DataChannelCmd::StartForwardUdp => {
            if args.service.service_type != ServiceType::Udp {
                bail!("Expect UDP traffic. Please check the configuration.")
            }
            run_data_channel_for_udp(conn, &args.service.local_addr, args.service.prefer_ipv6)
                .await?;
        }
        DataChannelCmd::StartForwardTcpZstd { dict_digest } => {
            run_client_tcp_zstd(conn, dict_digest, &args).await?;
        }
        DataChannelCmd::StartForwardUdpZstd { dict_digest } => {
            run_client_udp_zstd(conn, dict_digest, &args).await?;
        }
    }
    Ok(())
}

async fn run_client_tcp_zstd<T: Transport>(
    conn: T::Stream,
    dict_digest: protocol::Digest,
    args: &Arc<RunDataChannelArgs<T>>,
) -> Result<()> {
    if args.service.service_type != ServiceType::Tcp {
        bail!("Expect TCP traffic. Please check the configuration.")
    }
    #[cfg(not(feature = "compression-zstd"))]
    {
        let _ = dict_digest;
        bail!(
            "Service {}: server requires zstd compression but this binary lacks feature compression-zstd",
            args.service.name
        );
    }
    #[cfg(feature = "compression-zstd")]
    {
        let wrapped = crate::compression::MaybeCompressed::Zstd(
            build_client_zstd_stream(conn, dict_digest, &args.service, &args.dict_cache).await?,
        );
        run_data_channel_for_tcp(wrapped, &args.service.local_addr).await?;
        Ok(())
    }
}

async fn run_client_udp_zstd<T: Transport>(
    conn: T::Stream,
    dict_digest: protocol::Digest,
    args: &Arc<RunDataChannelArgs<T>>,
) -> Result<()> {
    if args.service.service_type != ServiceType::Udp {
        bail!("Expect UDP traffic. Please check the configuration.")
    }
    #[cfg(not(feature = "compression-zstd"))]
    {
        let _ = dict_digest;
        bail!(
            "Service {}: server requires zstd compression but this binary lacks feature compression-zstd",
            args.service.name
        );
    }
    #[cfg(feature = "compression-zstd")]
    {
        let zstd =
            build_client_zstd_stream(conn, dict_digest, &args.service, &args.dict_cache).await?;
        let (rd, wr) = zstd.into_split();
        run_udp_forwarding_loop(rd, wr, &args.service.local_addr, args.service.prefer_ipv6).await?;
        Ok(())
    }
}

#[cfg(feature = "compression-zstd")]
async fn resolve_client_dict(
    dict_digest: protocol::Digest,
    service: &ClientServiceConfig,
    cache: &DictCache,
) -> Result<Option<Arc<Vec<u8>>>> {
    const ZERO_DIGEST: protocol::Digest = [0; HASH_WIDTH_IN_BYTES];

    if dict_digest == ZERO_DIGEST {
        if service.compression_dictionary_loaded.is_some() {
            warn!(
                service = %service.name,
                "compression_dictionary configured but server did not request a dictionary"
            );
        }
        return Ok(None);
    }

    if let Some(dictionary) = cache.lookup(&dict_digest) {
        return Ok(Some(dictionary));
    }

    if let Some(dictionary) = service
        .compression_dictionary_loaded
        .as_ref()
        .filter(|dictionary| dictionary.digest == dict_digest)
    {
        return Ok(Some(Arc::new(dictionary.bytes.to_vec())));
    }

    let client_digest = service
        .compression_dictionary_loaded
        .as_ref()
        .map(|dictionary| dictionary.digest)
        .unwrap_or(ZERO_DIGEST);
    let mut updates = cache.subscribe();
    let wait_for_dictionary = async {
        loop {
            let dictionary = updates.borrow().get(&dict_digest).cloned();
            if let Some(dictionary) = dictionary {
                return Ok(dictionary);
            }
            updates
                .changed()
                .await
                .context("Compression dictionary cache closed while waiting for an update")?;
        }
    };

    match time::timeout(Duration::from_secs(5), wait_for_dictionary).await {
        Ok(dictionary) => dictionary.map(Some),
        Err(_) => {
            bail!(
                "Service {}: timed out waiting for pushed compression dictionary — server expects digest {}, client has {}",
                service.name,
                hex::encode(dict_digest),
                hex::encode(client_digest)
            )
        }
    }
}

#[cfg(feature = "compression-zstd")]
async fn build_client_zstd_stream<S>(
    conn: S,
    dict_digest: protocol::Digest,
    service: &ClientServiceConfig,
    cache: &DictCache,
) -> Result<crate::compression::ZstdStream<S>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    match resolve_client_dict(dict_digest, service, cache).await? {
        Some(dictionary) => Ok(crate::compression::ZstdStream::with_dict_and_level(
            conn,
            dictionary.as_slice(),
            crate::constants::DEFAULT_ZSTD_LEVEL,
        )?),
        None => Ok(crate::compression::ZstdStream::with_level(
            conn,
            crate::constants::DEFAULT_ZSTD_LEVEL,
        )),
    }
}

// Simply copying back and forth for TCP
#[instrument(skip(conn))]
async fn run_data_channel_for_tcp<S>(mut conn: S, local_addr: &str) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    debug!("New data channel starts forwarding");

    let mut local = TcpStream::connect(local_addr)
        .await
        .with_context(|| format!("Failed to connect to {}", local_addr))?;
    let _ = copy_bidirectional(&mut conn, &mut local).await;
    Ok(())
}

// Things get a little tricker when it gets to UDP because it's connection-less.
// A UdpPortMap must be maintained for recent seen incoming address, giving them
// each a local port, which is associated with a socket. So just the sender
// to the socket will work fine for the map's value.
type UdpPortMap = Arc<RwLock<HashMap<SocketAddr, mpsc::Sender<Bytes>>>>;

#[instrument(skip(conn))]
async fn run_data_channel_for_udp<S>(conn: S, local_addr: &str, prefer_ipv6: bool) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // FIXME: https://github.com/tokio-rs/tls/issues/40
    // Maybe this is our concern
    let (rd, wr) = io::split(conn);
    run_udp_forwarding_loop(rd, wr, local_addr, prefer_ipv6).await
}

async fn run_udp_forwarding_loop<R, W>(
    mut rd: R,
    mut wr: W,
    local_addr: &str,
    prefer_ipv6: bool,
) -> Result<()>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    debug!("New data channel starts forwarding");

    let port_map: UdpPortMap = Arc::new(RwLock::new(HashMap::new()));

    // The channel stores UdpTraffic that needs to be sent to the server
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<UdpTraffic>(UDP_SENDQ_SIZE);

    // Keep sending items from the outbound channel to the server
    tokio::spawn(async move {
        while let Some(t) = outbound_rx.recv().await {
            trace!("outbound {:?}", t);
            if let Err(e) = t
                .write(&mut wr)
                .await
                .with_context(|| "Failed to forward UDP traffic to the server")
            {
                debug!("{:?}", e);
                break;
            }
            if let Err(e) = wr.flush().await {
                debug!("{:?}", e);
                break;
            }
        }
    });

    loop {
        // Read a packet from the server
        let hdr_len = rd.read_u8().await?;
        let packet = UdpTraffic::read(&mut rd, hdr_len)
            .await
            .with_context(|| "Failed to read UDPTraffic from the server")?;
        let m = port_map.read().await;

        if m.get(&packet.from).is_none() {
            // This packet is from a address we don't see for a while,
            // which is not in the UdpPortMap.
            // So set up a mapping (and a forwarder) for it

            // Drop the reader lock
            drop(m);

            // Grab the writer lock
            // This is the only thread that will try to grab the writer lock
            // So no need to worry about some other thread has already set up
            // the mapping between the gap of dropping the reader lock and
            // grabbing the writer lock
            let mut m = port_map.write().await;

            match udp_connect(local_addr, prefer_ipv6).await {
                Ok(s) => {
                    let (inbound_tx, inbound_rx) = mpsc::channel(UDP_SENDQ_SIZE);
                    m.insert(packet.from, inbound_tx);
                    tokio::spawn(run_udp_forwarder(
                        s,
                        inbound_rx,
                        outbound_tx.clone(),
                        packet.from,
                        port_map.clone(),
                    ));
                }
                Err(e) => {
                    error!("{:#}", e);
                }
            }
        }

        // Now there should be a udp forwarder that can receive the packet
        let m = port_map.read().await;
        if let Some(tx) = m.get(&packet.from) {
            let _ = tx.send(packet.data).await;
        }
    }
}

// Run a UdpSocket for the visitor `from`
#[instrument(skip_all, fields(from))]
async fn run_udp_forwarder(
    s: UdpSocket,
    mut inbound_rx: mpsc::Receiver<Bytes>,
    outbount_tx: mpsc::Sender<UdpTraffic>,
    from: SocketAddr,
    port_map: UdpPortMap,
) -> Result<()> {
    debug!("Forwarder created");
    let mut buf = BytesMut::new();
    buf.resize(UDP_BUFFER_SIZE, 0);

    loop {
        tokio::select! {
            // Receive from the server
            data = inbound_rx.recv() => {
                if let Some(data) = data {
                    s.send(&data).await?;
                } else {
                    break;
                }
            },

            // Receive from the service
            val = s.recv(&mut buf) => {
                let len = match val {
                    Ok(v) => v,
                    Err(_) => break
                };

                let t = UdpTraffic{
                    from,
                    data: Bytes::copy_from_slice(&buf[..len])
                };

                outbount_tx.send(t).await?;
            },

            // No traffic for the duration of UDP_TIMEOUT, clean up the state
            _ = time::sleep(Duration::from_secs(UDP_TIMEOUT)) => {
                break;
            }
        }
    }

    let mut port_map = port_map.write().await;
    port_map.remove(&from);

    debug!("Forwarder dropped");
    Ok(())
}

// Control channel, using T as the transport layer
struct ControlChannel<T: Transport> {
    digest: ServiceDigest,              // SHA256 of the service name
    service: ClientServiceConfig,       // `[client.services.foo]` config block
    shutdown_rx: oneshot::Receiver<u8>, // Receives the shutdown signal
    remote_addr: String,                // `client.remote_addr`
    transport: Arc<T>,                  // Wrapper around the transport layer
    heartbeat_timeout: u64,             // Application layer heartbeat timeout in secs
    dict_cache: Arc<DictCache>,
}

// Handle of a control channel
// Dropping it will also drop the actual control channel
struct ControlChannelHandle {
    shutdown_tx: oneshot::Sender<u8>,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "retains the service cache across control-channel sessions"
        )
    )]
    dict_cache: Arc<DictCache>,
}

impl<T: 'static + Transport> ControlChannel<T> {
    #[instrument(skip_all)]
    async fn run(&mut self) -> Result<()> {
        let mut remote_addr = AddrMaybeCached::new(&self.remote_addr);
        remote_addr.resolve().await?;

        let mut conn = self
            .transport
            .connect(&remote_addr)
            .await
            .with_context(|| format!("Failed to connect to {}", self.remote_addr))?;
        T::hint(&conn, SocketOpts::for_control_channel());

        // Send hello
        debug!("Sending hello");
        let hello_send =
            Hello::ControlChannelHello(CURRENT_PROTO_VERSION, self.digest[..].try_into().unwrap());
        conn.write_all(&bincode::serialize(&hello_send).unwrap())
            .await?;
        conn.flush().await?;

        // Read hello
        debug!("Reading hello");
        let nonce = match read_hello(&mut conn).await? {
            ControlChannelHello(_, d) => d,
            _ => {
                bail!("Unexpected type of hello");
            }
        };

        // Send auth
        debug!("Sending auth");
        let mut concat = Vec::from(self.service.token.as_ref().unwrap().as_bytes());
        concat.extend_from_slice(&nonce);

        let session_key = protocol::digest(&concat);
        let auth = Auth(session_key);
        conn.write_all(&bincode::serialize(&auth).unwrap()).await?;
        conn.flush().await?;

        // Read ack
        debug!("Reading ack");
        match read_ack(&mut conn).await? {
            Ack::Ok => {}
            v => {
                return Err(anyhow!("{}", v))
                    .with_context(|| format!("Authentication failed: {}", self.service.name));
            }
        }

        // Channel ready
        info!("Control channel established");

        // Socket options for the data channel
        let socket_opts = SocketOpts::from_client_cfg(&self.service);
        let data_ch_args = Arc::new(RunDataChannelArgs {
            session_key,
            remote_addr,
            connector: self.transport.clone(),
            socket_opts,
            service: self.service.clone(),
            dict_cache: Arc::clone(&self.dict_cache),
        });

        loop {
            tokio::select! {
                val = read_control_cmd(&mut conn) => {
                    let val = val?;
                    debug!( "Received {:?}", val);
                    match val {
                        ControlChannelCmd::CreateDataChannel => {
                            let args = data_ch_args.clone();
                            tokio::spawn(async move {
                                if let Err(e) = run_data_channel(args).await.with_context(|| "Failed to run the data channel") {
                                    warn!("{:#}", e);
                                }
                            }.instrument(Span::current()));
                        },
                        ControlChannelCmd::HeartBeat => (),
                        ControlChannelCmd::UpdateCompressionDict { digest, dictionary } => {
                            handle_compression_dict_update(&self.dict_cache, digest, dictionary).await;
                        }
                    }
                },
                _ = time::sleep(Duration::from_secs(self.heartbeat_timeout)), if self.heartbeat_timeout != 0 => {
                    return Err(anyhow!("Heartbeat timed out"))
                }
                _ = &mut self.shutdown_rx => {
                    break;
                }
            }
        }

        info!("Control channel shutdown");
        Ok(())
    }
}

impl ControlChannelHandle {
    #[instrument(name="handle", skip_all, fields(service = %service.name, remote = %remote_addr))]
    fn new<T: 'static + Transport>(
        service: ClientServiceConfig,
        remote_addr: String,
        transport: Arc<T>,
        heartbeat_timeout: u64,
        dict_cache: Arc<DictCache>,
    ) -> ControlChannelHandle {
        let digest = protocol::digest(service.name.as_bytes());

        info!("Starting {}", hex::encode(digest));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let mut retry_backoff = run_control_chan_backoff(service.retry_interval.unwrap());

        let mut s = ControlChannel {
            digest,
            service,
            shutdown_rx,
            remote_addr,
            transport,
            heartbeat_timeout,
            dict_cache: Arc::clone(&dict_cache),
        };

        tokio::spawn(
            async move {
                let mut start = Instant::now();

                while let Err(err) = s
                    .run()
                    .await
                    .with_context(|| "Failed to run the control channel")
                {
                    if s.shutdown_rx.try_recv() != Err(oneshot::error::TryRecvError::Empty) {
                        break;
                    }

                    if start.elapsed() > Duration::from_secs(3) {
                        // The client runs for at least 3 secs and then disconnects
                        retry_backoff.reset();
                    }

                    if let Some(duration) = retry_backoff.next_backoff() {
                        error!("{:#}. Retry in {:?}...", err, duration);
                        time::sleep(duration).await;
                    } else {
                        // Should never reach
                        panic!("{:#}. Break", err);
                    }

                    start = Instant::now();
                }
            }
            .instrument(Span::current()),
        );

        ControlChannelHandle {
            shutdown_tx,
            dict_cache,
        }
    }

    fn shutdown(self) {
        // A send failure shows that the actor has already shutdown.
        let _ = self.shutdown_tx.send(0u8);
    }
}

#[cfg(all(test, feature = "compression-zstd"))]
mod tests {
    use super::{resolve_client_dict, DictCache};
    use crate::config::{ClientServiceConfig, LoadedDictionary};
    use crate::protocol::{Digest, HASH_WIDTH_IN_BYTES};
    use std::sync::Arc;
    use tokio::time::{self, Duration, Instant};

    const ZERO_DIGEST: Digest = [0; HASH_WIDTH_IN_BYTES];
    const SERVER_DIGEST: Digest = [1; HASH_WIDTH_IN_BYTES];
    const CLIENT_DIGEST: Digest = [2; HASH_WIDTH_IN_BYTES];

    fn service_with_dictionary(digest: Digest) -> ClientServiceConfig {
        ClientServiceConfig {
            name: "test-service".into(),
            compression_dictionary_loaded: Some(LoadedDictionary {
                digest,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn resolves_plain_zstd_when_server_and_client_have_no_dictionary() {
        // Given
        let service = ClientServiceConfig::with_name("test-service");
        let cache = DictCache::new();

        // When
        let dictionary = resolve_client_dict(ZERO_DIGEST, &service, &cache)
            .await
            .unwrap();

        // Then
        assert_eq!(dictionary, None);
    }

    #[tokio::test]
    async fn ignores_client_dictionary_when_server_requests_plain_zstd() {
        // Given
        let service = service_with_dictionary(CLIENT_DIGEST);
        let cache = DictCache::new();

        // When
        let dictionary = resolve_client_dict(ZERO_DIGEST, &service, &cache)
            .await
            .unwrap();

        // Then
        assert_eq!(dictionary, None);
    }

    #[tokio::test]
    async fn resolves_client_dictionary_when_digest_matches_server() {
        // Given
        let service = service_with_dictionary(SERVER_DIGEST);
        let cache = DictCache::new();

        // When
        let dictionary = resolve_client_dict(SERVER_DIGEST, &service, &cache)
            .await
            .unwrap();

        // Then
        assert_eq!(
            dictionary.as_ref().map(|bytes| bytes.as_slice()),
            Some(&[][..])
        );
    }

    #[tokio::test(start_paused = true)]
    async fn times_out_when_server_dictionary_never_arrives() {
        // Given
        let service = ClientServiceConfig::with_name("test-service");
        let cache = Arc::new(DictCache::new());
        let resolver_cache = Arc::clone(&cache);

        // When
        let resolver = tokio::spawn(async move {
            resolve_client_dict(SERVER_DIGEST, &service, &resolver_cache).await
        });
        tokio::task::yield_now().await;
        time::advance(Duration::from_secs(5) + Duration::from_millis(1)).await;
        let error = resolver.await.unwrap().unwrap_err();

        // Then
        let message = error.to_string();
        assert!(message.contains("test-service"));
        assert!(message.contains(&hex::encode(SERVER_DIGEST)));
        assert!(message.contains(&hex::encode(ZERO_DIGEST)));
        assert!(message.contains("timed out"));
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_reports_mismatched_static_dictionary_digest() {
        // Given
        let service = service_with_dictionary(CLIENT_DIGEST);
        let cache = Arc::new(DictCache::new());
        let resolver_cache = Arc::clone(&cache);

        // When
        let resolver = tokio::spawn(async move {
            resolve_client_dict(SERVER_DIGEST, &service, &resolver_cache).await
        });
        tokio::task::yield_now().await;
        time::advance(Duration::from_secs(5) + Duration::from_millis(1)).await;
        let error = resolver.await.unwrap().unwrap_err();

        // Then
        let message = error.to_string();
        assert!(message.contains("test-service"));
        assert!(message.contains(&hex::encode(SERVER_DIGEST)));
        assert!(message.contains(&hex::encode(CLIENT_DIGEST)));
        assert!(message.contains("timed out"));
    }

    #[tokio::test(start_paused = true)]
    async fn resolves_cached_dictionary_without_waiting() {
        // Given
        let service = ClientServiceConfig::with_name("test-service");
        let cache = Arc::new(DictCache::new());
        let dictionary = Arc::new(vec![1, 2, 3]);
        let digest = crate::protocol::digest(&dictionary);
        cache.insert(digest, Arc::clone(&dictionary)).await;
        let resolver_cache = Arc::clone(&cache);

        // When
        let resolver =
            tokio::spawn(
                async move { resolve_client_dict(digest, &service, &resolver_cache).await },
            );
        tokio::task::yield_now().await;

        // Then
        assert!(resolver.is_finished());
        assert_eq!(
            resolver.await.unwrap().unwrap().as_deref(),
            Some(&*dictionary)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn resolves_dictionary_promptly_when_push_arrives_while_waiting() {
        // Given
        let service = ClientServiceConfig::with_name("test-service");
        let cache = Arc::new(DictCache::new());
        let dictionary = Arc::new(vec![4, 5, 6]);
        let digest = crate::protocol::digest(&dictionary);
        let resolver_cache = Arc::clone(&cache);
        let started_at = Instant::now();
        let resolver =
            tokio::spawn(
                async move { resolve_client_dict(digest, &service, &resolver_cache).await },
            );
        tokio::task::yield_now().await;
        assert!(!resolver.is_finished());

        // When
        cache.insert(digest, Arc::clone(&dictionary)).await;
        tokio::task::yield_now().await;

        // Then
        assert!(resolver.is_finished());
        assert!(started_at.elapsed() < Duration::from_secs(5));
        assert_eq!(
            resolver.await.unwrap().unwrap().as_deref(),
            Some(&*dictionary)
        );
    }
}

#[cfg(test)]
mod dict_cache_tests {
    use super::{handle_compression_dict_update, ControlChannelHandle, DictCache};
    #[cfg(feature = "compression-zstd")]
    use crate::protocol::HASH_WIDTH_IN_BYTES;
    use crate::protocol::{self, Digest};
    use std::sync::Arc;

    fn dictionary(index: u8) -> Vec<u8> {
        vec![index; usize::from(index) + 1]
    }

    #[tokio::test]
    async fn returns_dictionary_after_push() {
        // Given
        let cache = DictCache::new();
        let dictionary = dictionary(1);
        let digest = protocol::digest(&dictionary);

        // When
        cache.insert(digest, Arc::new(dictionary.clone())).await;

        // Then
        assert_eq!(cache.lookup(&digest).as_deref(), Some(&dictionary));
    }

    #[cfg(feature = "compression-zstd")]
    #[tokio::test]
    async fn rejects_pushed_dictionary_when_digest_mismatches() {
        // Given
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_test_writer()
            .try_init();
        let cache = DictCache::new();
        let dictionary = dictionary(2);
        let claimed_digest = [9; HASH_WIDTH_IN_BYTES];

        // When
        handle_compression_dict_update(&cache, claimed_digest, dictionary).await;

        // Then
        assert_eq!(cache.lookup(&claimed_digest), None);
    }

    #[tokio::test]
    async fn evicts_oldest_dictionary_after_fifth_generation() {
        // Given
        let cache = DictCache::new();
        let generations: Vec<(Digest, Vec<u8>)> = (1..=5)
            .map(|index| {
                let dictionary = dictionary(index);
                (protocol::digest(&dictionary), dictionary)
            })
            .collect();

        // When
        for (digest, dictionary) in &generations {
            cache.insert(*digest, Arc::new(dictionary.clone())).await;
        }

        // Then
        assert_eq!(cache.lookup(&generations[0].0), None);
        for (digest, dictionary) in &generations[1..] {
            assert_eq!(cache.lookup(digest).as_deref(), Some(dictionary));
        }
    }

    #[tokio::test]
    async fn shares_dictionary_between_handles_for_same_service() {
        // Given
        let service_cache = Arc::new(DictCache::new());
        let (first_shutdown_tx, _) = tokio::sync::oneshot::channel();
        let first = ControlChannelHandle {
            shutdown_tx: first_shutdown_tx,
            dict_cache: Arc::clone(&service_cache),
        };
        let (second_shutdown_tx, _) = tokio::sync::oneshot::channel();
        let second = ControlChannelHandle {
            shutdown_tx: second_shutdown_tx,
            dict_cache: Arc::clone(&service_cache),
        };
        let dictionary = dictionary(3);
        let digest = protocol::digest(&dictionary);

        // When
        first
            .dict_cache
            .insert(digest, Arc::new(dictionary.clone()))
            .await;

        // Then
        assert!(Arc::ptr_eq(&first.dict_cache, &second.dict_cache));
        assert_eq!(
            second.dict_cache.lookup(&digest).as_deref(),
            Some(&dictionary)
        );
    }

    #[tokio::test]
    async fn subscriber_observes_inserted_generation() {
        // Given
        let cache = DictCache::new();
        let mut updates = cache.subscribe();
        let dictionary = dictionary(4);
        let digest = protocol::digest(&dictionary);

        // When
        cache.insert(digest, Arc::new(dictionary.clone())).await;
        updates.changed().await.unwrap();

        // Then
        assert_eq!(
            updates.borrow().get(&digest).map(Arc::as_ref),
            Some(&dictionary)
        );
    }

    #[cfg(not(feature = "compression-zstd"))]
    #[tokio::test]
    async fn drops_pushed_dictionary_without_compression_feature() {
        // Given
        let cache = DictCache::new();
        let dictionary = dictionary(5);
        let digest = protocol::digest(&dictionary);

        // When
        handle_compression_dict_update(&cache, digest, dictionary).await;

        // Then
        assert_eq!(cache.lookup(&digest), None);
    }
}
