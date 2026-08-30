use super::{SamplerState, ServiceCompressionState};
use crate::config::{CompressionType, ServerServiceConfig, ServiceType};
use anyhow::{Context, Result};
use serde::Serialize;
use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::task::{Context as TaskContext, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::time;
use tracing::{debug, error, info, warn};

const OBSERVE_REQUEST_LIMIT: usize = 8192;
const OBSERVE_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy)]
pub(super) enum ByteKind {
    Visitor,
    Wire,
}

pub(super) struct ServerStats {
    started: Instant,
    services: RwLock<HashMap<String, Arc<ServiceStats>>>,
}

pub(super) struct ServiceStats {
    name: String,
    service_type: ServiceType,
    bind_addr: String,
    compression: Option<CompressionType>,
    static_digest: Option<[u8; 32]>,
    auto_dict: Mutex<Option<Arc<ServiceCompressionState>>>,
    control_connected: AtomicBool,
    connections_active: AtomicU64,
    connections_total: AtomicU64,
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
    wire_bytes_in: AtomicU64,
    wire_bytes_out: AtomicU64,
    datagrams_in: AtomicU64,
    datagrams_out: AtomicU64,
}

pub(super) struct ActiveSession(pub Arc<ServiceStats>);

impl Drop for ActiveSession {
    fn drop(&mut self) {
        self.0.on_session_end();
    }
}

#[derive(Serialize, Debug, PartialEq)]
pub struct StatsSnapshot {
    pub uptime_secs: u64,
    pub services: Vec<ServiceSnapshot>,
}

#[derive(Serialize, Debug, PartialEq)]
pub struct ServiceSnapshot {
    pub name: String,
    #[serde(rename = "type")]
    pub service_type: &'static str,
    pub bind_addr: String,
    pub control_connected: bool,
    pub connections_active: u64,
    pub connections_total: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub wire_bytes_in: u64,
    pub wire_bytes_out: u64,
    #[serde(skip_serializing_if = "is_zero")]
    pub datagrams_in: u64,
    #[serde(skip_serializing_if = "is_zero")]
    pub datagrams_out: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression_ratio: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dictionary: Option<DictionarySnapshot>,
}

#[derive(Serialize, Debug, PartialEq, Eq)]
pub struct DictionarySnapshot {
    pub mode: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
}

fn is_zero(v: &u64) -> bool {
    *v == 0
}

fn service_type_str(service_type: ServiceType) -> &'static str {
    match service_type {
        ServiceType::Tcp => "tcp",
        ServiceType::Udp => "udp",
    }
}

fn compression_ratio(plain: u64, wire: u64) -> Option<f64> {
    if wire == 0 {
        None
    } else {
        Some(plain as f64 / wire as f64)
    }
}

impl ServerStats {
    pub(super) fn from_services(services: &HashMap<String, ServerServiceConfig>) -> Self {
        let mut map = HashMap::with_capacity(services.len());
        for cfg in services.values() {
            map.insert(cfg.name.clone(), Arc::new(ServiceStats::from_config(cfg)));
        }
        Self {
            started: Instant::now(),
            services: RwLock::new(map),
        }
    }

    pub(super) fn upsert(&self, cfg: &ServerServiceConfig) {
        let mut services = self
            .services
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        services.insert(cfg.name.clone(), Arc::new(ServiceStats::from_config(cfg)));
    }

    pub(super) fn remove(&self, name: &str) {
        self.services
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(name);
    }

    pub(super) fn service(&self, name: &str) -> Option<Arc<ServiceStats>> {
        self.services
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .cloned()
    }

    pub fn snapshot(&self) -> StatsSnapshot {
        let services = self.services.read().unwrap_or_else(PoisonError::into_inner);
        let mut list: Vec<ServiceSnapshot> = services.values().map(|s| s.snapshot()).collect();
        list.sort_by(|a, b| a.name.cmp(&b.name));
        StatsSnapshot {
            uptime_secs: self.started.elapsed().as_secs(),
            services: list,
        }
    }
}

impl ServiceStats {
    pub(super) fn from_config(cfg: &ServerServiceConfig) -> Self {
        Self {
            name: cfg.name.clone(),
            service_type: cfg.service_type,
            bind_addr: cfg.bind_addr.clone(),
            compression: cfg.compression,
            static_digest: cfg
                .compression_dictionary_loaded
                .as_ref()
                .map(|dictionary| dictionary.digest),
            auto_dict: Mutex::new(None),
            control_connected: AtomicBool::new(false),
            connections_active: AtomicU64::new(0),
            connections_total: AtomicU64::new(0),
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            wire_bytes_in: AtomicU64::new(0),
            wire_bytes_out: AtomicU64::new(0),
            datagrams_in: AtomicU64::new(0),
            datagrams_out: AtomicU64::new(0),
        }
    }

    pub(super) fn set_control_connected(&self, connected: bool) {
        self.control_connected.store(connected, Ordering::Relaxed);
    }

    pub(super) fn attach_auto_dict(&self, state: Option<Arc<ServiceCompressionState>>) {
        *self
            .auto_dict
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = state;
    }

    pub(super) fn on_session_start(&self) {
        self.connections_total.fetch_add(1, Ordering::Relaxed);
        self.connections_active.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn on_session_end(&self) {
        self.connections_active.fetch_sub(1, Ordering::Relaxed);
    }

    pub(super) fn add_datagram_in(&self, n: u64) {
        self.datagrams_in.fetch_add(1, Ordering::Relaxed);
        self.bytes_in.fetch_add(n, Ordering::Relaxed);
    }

    pub(super) fn add_datagram_out(&self, n: u64) {
        self.datagrams_out.fetch_add(1, Ordering::Relaxed);
        self.bytes_out.fetch_add(n, Ordering::Relaxed);
    }

    fn add_read(&self, n: u64, kind: ByteKind) {
        match kind {
            ByteKind::Visitor => {
                self.bytes_in.fetch_add(n, Ordering::Relaxed);
            }
            ByteKind::Wire => {
                self.wire_bytes_in.fetch_add(n, Ordering::Relaxed);
            }
        }
    }

    fn add_write(&self, n: u64, kind: ByteKind) {
        match kind {
            ByteKind::Visitor => {
                self.bytes_out.fetch_add(n, Ordering::Relaxed);
            }
            ByteKind::Wire => {
                self.wire_bytes_out.fetch_add(n, Ordering::Relaxed);
            }
        }
    }

    fn snapshot(&self) -> ServiceSnapshot {
        let bytes_in = self.bytes_in.load(Ordering::Relaxed);
        let bytes_out = self.bytes_out.load(Ordering::Relaxed);
        let wire_bytes_in = self.wire_bytes_in.load(Ordering::Relaxed);
        let wire_bytes_out = self.wire_bytes_out.load(Ordering::Relaxed);
        let compression = self.compression.map(|_| "zstd");
        let compression_ratio = if compression.is_some() {
            compression_ratio(
                bytes_in.saturating_add(bytes_out),
                wire_bytes_in.saturating_add(wire_bytes_out),
            )
        } else {
            None
        };
        ServiceSnapshot {
            name: self.name.clone(),
            service_type: service_type_str(self.service_type),
            bind_addr: self.bind_addr.clone(),
            control_connected: self.control_connected.load(Ordering::Relaxed),
            connections_active: self.connections_active.load(Ordering::Relaxed),
            connections_total: self.connections_total.load(Ordering::Relaxed),
            bytes_in,
            bytes_out,
            wire_bytes_in,
            wire_bytes_out,
            datagrams_in: self.datagrams_in.load(Ordering::Relaxed),
            datagrams_out: self.datagrams_out.load(Ordering::Relaxed),
            compression,
            compression_ratio,
            dictionary: self.dictionary_snapshot(),
        }
    }

    fn dictionary_snapshot(&self) -> Option<DictionarySnapshot> {
        self.compression?;
        if let Some(digest) = self.static_digest {
            return Some(DictionarySnapshot {
                mode: "static",
                state: None,
                digest: Some(hex::encode(digest)),
            });
        }
        let auto = self
            .auto_dict
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        match auto {
            Some(state) => {
                let sampler = state.sampler.lock().unwrap_or_else(PoisonError::into_inner);
                let dict_state = match &*sampler {
                    SamplerState::Sampling(_) => "sampling",
                    SamplerState::Trained => "trained",
                    SamplerState::Failed => "failed",
                };
                drop(sampler);
                let digest = state
                    .generation_rx
                    .borrow()
                    .as_ref()
                    .map(|generation| hex::encode(generation.digest));
                Some(DictionarySnapshot {
                    mode: "auto",
                    state: Some(dict_state),
                    digest,
                })
            }
            None => Some(DictionarySnapshot {
                mode: "none",
                state: None,
                digest: None,
            }),
        }
    }
}

pub(super) enum MaybeCounted<S> {
    Raw(S),
    Counted(CountingStream<S>),
}

pub(super) struct CountingStream<S> {
    inner: S,
    stats: Arc<ServiceStats>,
    kind: ByteKind,
}

pub(super) fn maybe_count<S>(
    inner: S,
    stats: Option<&Arc<ServiceStats>>,
    kind: ByteKind,
) -> MaybeCounted<S> {
    match stats {
        Some(stats) => MaybeCounted::Counted(CountingStream {
            inner,
            stats: Arc::clone(stats),
            kind,
        }),
        None => MaybeCounted::Raw(inner),
    }
}

impl<S> AsyncRead for MaybeCounted<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Raw(inner) => Pin::new(inner).poll_read(cx, buf),
            Self::Counted(inner) => Pin::new(inner).poll_read(cx, buf),
        }
    }
}

impl<S> AsyncWrite for MaybeCounted<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Raw(inner) => Pin::new(inner).poll_write(cx, buf),
            Self::Counted(inner) => Pin::new(inner).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Raw(inner) => Pin::new(inner).poll_flush(cx),
            Self::Counted(inner) => Pin::new(inner).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Raw(inner) => Pin::new(inner).poll_shutdown(cx),
            Self::Counted(inner) => Pin::new(inner).poll_shutdown(cx),
        }
    }
}

impl<S> AsyncRead for CountingStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let filled_before = buf.filled().len();
        match Pin::new(&mut this.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let n = buf.filled().len() - filled_before;
                if n > 0 {
                    this.stats.add_read(n as u64, this.kind);
                }
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl<S> AsyncWrite for CountingStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(written)) => {
                if written > 0 {
                    this.stats.add_write(written as u64, this.kind);
                }
                Poll::Ready(Ok(written))
            }
            other => other,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

pub(super) async fn serve(
    listener: TcpListener,
    stats: Arc<ServerStats>,
    mut shutdown_rx: broadcast::Receiver<bool>,
) {
    info!("Observation endpoint ready");
    loop {
        tokio::select! {
            ret = listener.accept() => {
                match ret {
                    Ok((stream, _)) => {
                        let stats = Arc::clone(&stats);
                        tokio::spawn(async move {
                            if let Err(e) = handle_client(stream, stats).await {
                                debug!("observe request failed: {:#}", e);
                            }
                        });
                    }
                    Err(e) => {
                        error!("observe accept failed: {e}");
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                break;
            }
        }
    }
}

async fn handle_client(mut stream: TcpStream, stats: Arc<ServerStats>) -> Result<()> {
    let mut buf = vec![0u8; OBSERVE_REQUEST_LIMIT];
    let mut n = 0usize;
    loop {
        if n == buf.len() {
            write_response(
                &mut stream,
                431,
                "text/plain; charset=utf-8",
                b"request too large\n",
            )
            .await?;
            return Ok(());
        }
        let read = time::timeout(OBSERVE_REQUEST_TIMEOUT, stream.read(&mut buf[n..]))
            .await
            .context("observe request timed out")??;
        if read == 0 {
            return Ok(());
        }
        n += read;
        if buf[..n].windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }

    let req = std::str::from_utf8(&buf[..n]).unwrap_or("");
    let first = req.lines().next().unwrap_or("");
    let mut parts = first.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("/").split('?').next().unwrap_or("/");

    if method != "GET" {
        write_response(
            &mut stream,
            405,
            "text/plain; charset=utf-8",
            b"method not allowed\n",
        )
        .await?;
        return Ok(());
    }

    match path {
        "/" | "/stats" => match serde_json::to_vec(&stats.snapshot()) {
            Ok(body) => {
                write_response(&mut stream, 200, "application/json", &body).await?;
            }
            Err(e) => {
                warn!("failed to serialize observe stats: {e}");
                write_response(
                    &mut stream,
                    500,
                    "text/plain; charset=utf-8",
                    b"internal error\n",
                )
                .await?;
            }
        },
        "/metrics" => {
            let body = prometheus_text(&stats.snapshot());
            write_response(
                &mut stream,
                200,
                "text/plain; version=0.0.4; charset=utf-8",
                body.as_bytes(),
            )
            .await?;
        }
        "/health" => {
            write_response(&mut stream, 200, "text/plain; charset=utf-8", b"ok\n").await?;
        }
        _ => {
            write_response(
                &mut stream,
                404,
                "text/plain; charset=utf-8",
                b"not found\n",
            )
            .await?;
        }
    }
    Ok(())
}

async fn write_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> io::Result<()> {
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        _ => "Error",
    };
    let header = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await
}

fn escape_label(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
    out
}

fn prometheus_text(snapshot: &StatsSnapshot) -> String {
    let mut out = String::new();
    out.push_str("# HELP rathole_uptime_seconds Seconds since the observation endpoint started.\n");
    out.push_str("# TYPE rathole_uptime_seconds gauge\n");
    out.push_str(&format!(
        "rathole_uptime_seconds {}\n",
        snapshot.uptime_secs
    ));

    fn series(out: &mut String, metric: &str, service: &ServiceSnapshot, value: u64) {
        out.push_str(&format!(
            "{metric}{{service=\"{}\",type=\"{}\"}} {value}\n",
            escape_label(&service.name),
            service.service_type
        ));
    }

    out.push_str(
        "# HELP rathole_control_connected 1 if the service control channel is connected.\n",
    );
    out.push_str("# TYPE rathole_control_connected gauge\n");
    for service in &snapshot.services {
        series(
            &mut out,
            "rathole_control_connected",
            service,
            u64::from(service.control_connected),
        );
    }

    out.push_str("# HELP rathole_connections_active Currently forwarding TCP visitor sessions.\n");
    out.push_str("# TYPE rathole_connections_active gauge\n");
    for service in &snapshot.services {
        series(
            &mut out,
            "rathole_connections_active",
            service,
            service.connections_active,
        );
    }

    out.push_str("# HELP rathole_connections_total Accepted TCP visitor sessions.\n");
    out.push_str("# TYPE rathole_connections_total counter\n");
    for service in &snapshot.services {
        series(
            &mut out,
            "rathole_connections_total",
            service,
            service.connections_total,
        );
    }

    out.push_str(
        "# HELP rathole_bytes_in_total Visitor-facing bytes received (public to client).\n",
    );
    out.push_str("# TYPE rathole_bytes_in_total counter\n");
    for service in &snapshot.services {
        series(
            &mut out,
            "rathole_bytes_in_total",
            service,
            service.bytes_in,
        );
    }

    out.push_str("# HELP rathole_bytes_out_total Visitor-facing bytes sent (client to public).\n");
    out.push_str("# TYPE rathole_bytes_out_total counter\n");
    for service in &snapshot.services {
        series(
            &mut out,
            "rathole_bytes_out_total",
            service,
            service.bytes_out,
        );
    }

    out.push_str("# HELP rathole_wire_bytes_in_total Data-channel bytes received after optional compression.\n");
    out.push_str("# TYPE rathole_wire_bytes_in_total counter\n");
    for service in &snapshot.services {
        series(
            &mut out,
            "rathole_wire_bytes_in_total",
            service,
            service.wire_bytes_in,
        );
    }

    out.push_str(
        "# HELP rathole_wire_bytes_out_total Data-channel bytes sent after optional compression.\n",
    );
    out.push_str("# TYPE rathole_wire_bytes_out_total counter\n");
    for service in &snapshot.services {
        series(
            &mut out,
            "rathole_wire_bytes_out_total",
            service,
            service.wire_bytes_out,
        );
    }

    out.push_str("# HELP rathole_datagrams_in_total UDP datagrams received from visitors.\n");
    out.push_str("# TYPE rathole_datagrams_in_total counter\n");
    for service in &snapshot.services {
        series(
            &mut out,
            "rathole_datagrams_in_total",
            service,
            service.datagrams_in,
        );
    }

    out.push_str("# HELP rathole_datagrams_out_total UDP datagrams sent to visitors.\n");
    out.push_str("# TYPE rathole_datagrams_out_total counter\n");
    for service in &snapshot.services {
        series(
            &mut out,
            "rathole_datagrams_out_total",
            service,
            service.datagrams_out,
        );
    }

    out.push_str("# HELP rathole_compression_ratio Visitor bytes divided by data-channel bytes. Only set when compression is enabled.\n");
    out.push_str("# TYPE rathole_compression_ratio gauge\n");
    for service in &snapshot.services {
        if let Some(ratio) = service.compression_ratio {
            out.push_str(&format!(
                "rathole_compression_ratio{{service=\"{}\",type=\"{}\"}} {ratio}\n",
                escape_label(&service.name),
                service.service_type
            ));
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LoadedDictionary;
    use crate::protocol::HASH_WIDTH_IN_BYTES;
    use std::net::SocketAddr;

    fn tcp_service(name: &str) -> ServerServiceConfig {
        ServerServiceConfig {
            name: name.to_string(),
            bind_addr: "127.0.0.1:8081".into(),
            service_type: ServiceType::Tcp,
            ..Default::default()
        }
    }

    fn zstd_service(name: &str) -> ServerServiceConfig {
        ServerServiceConfig {
            name: name.to_string(),
            bind_addr: "127.0.0.1:8081".into(),
            service_type: ServiceType::Tcp,
            compression: Some(CompressionType::Zstd),
            ..Default::default()
        }
    }

    #[test]
    fn snapshot_sorts_services_and_omits_idle_udp_fields() {
        let mut services = HashMap::new();
        services.insert("b".into(), tcp_service("b"));
        services.insert("a".into(), tcp_service("a"));
        let stats = ServerStats::from_services(&services);
        let snap = stats.snapshot();
        assert_eq!(
            snap.services
                .iter()
                .map(|s| s.name.as_str())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert!(snap.services[0].compression.is_none());
        assert!(snap.services[0].compression_ratio.is_none());
        assert!(snap.services[0].dictionary.is_none());
    }

    #[test]
    fn compression_ratio_is_plain_over_wire() {
        assert_eq!(compression_ratio(0, 0), None);
        assert_eq!(compression_ratio(10, 0), None);
        assert_eq!(compression_ratio(10, 5), Some(2.0));
    }

    #[test]
    fn zstd_service_reports_ratio_and_none_dictionary() {
        let mut services = HashMap::new();
        let cfg = zstd_service("echo");
        services.insert("echo".into(), cfg);
        let stats = ServerStats::from_services(&services);
        let service = stats.service("echo").unwrap();
        service.bytes_in.store(100, Ordering::Relaxed);
        service.bytes_out.store(50, Ordering::Relaxed);
        service.wire_bytes_in.store(20, Ordering::Relaxed);
        service.wire_bytes_out.store(10, Ordering::Relaxed);
        let snap = service.snapshot();
        assert_eq!(snap.compression, Some("zstd"));
        assert_eq!(snap.compression_ratio, Some(5.0));
        assert_eq!(
            snap.dictionary,
            Some(DictionarySnapshot {
                mode: "none",
                state: None,
                digest: None,
            })
        );
    }

    #[test]
    fn static_dictionary_digest_is_hex() {
        let digest = [0xab; HASH_WIDTH_IN_BYTES];
        let mut cfg = zstd_service("dict");
        cfg.compression_dictionary_loaded = Some(LoadedDictionary {
            digest,
            ..Default::default()
        });
        let stats = ServiceStats::from_config(&cfg);
        assert_eq!(
            stats.dictionary_snapshot(),
            Some(DictionarySnapshot {
                mode: "static",
                state: None,
                digest: Some("ab".repeat(32)),
            })
        );
    }

    #[test]
    fn auto_dictionary_reports_sampling_state() {
        let cfg = zstd_service("auto");
        let stats = ServiceStats::from_config(&cfg);
        stats.attach_auto_dict(Some(Arc::new(ServiceCompressionState::new(1024))));
        assert_eq!(
            stats.dictionary_snapshot(),
            Some(DictionarySnapshot {
                mode: "auto",
                state: Some("sampling"),
                digest: None,
            })
        );
    }

    #[test]
    fn prometheus_escapes_label_values() {
        let service = ServiceSnapshot {
            name: r#"a"b\c"#.into(),
            service_type: "tcp",
            bind_addr: "127.0.0.1:1".into(),
            control_connected: true,
            connections_active: 0,
            connections_total: 1,
            bytes_in: 2,
            bytes_out: 3,
            wire_bytes_in: 4,
            wire_bytes_out: 5,
            datagrams_in: 0,
            datagrams_out: 0,
            compression: Some("zstd"),
            compression_ratio: Some(2.0),
            dictionary: None,
        };
        let text = prometheus_text(&StatsSnapshot {
            uptime_secs: 9,
            services: vec![service],
        });
        assert!(text.contains("rathole_uptime_seconds 9"));
        assert!(text.contains(r#"rathole_control_connected{service="a\"b\\c",type="tcp"} 1"#));
        assert!(text.contains("rathole_compression_ratio"));
        assert!(text.contains("2"));
    }

    #[test]
    fn upsert_replaces_and_remove_drops_service() {
        let stats = ServerStats::from_services(&HashMap::new());
        stats.upsert(&tcp_service("echo"));
        assert!(stats.service("echo").is_some());
        stats.remove("echo");
        assert!(stats.service("echo").is_none());
    }

    #[tokio::test]
    async fn counting_stream_counts_visitor_bytes() {
        let stats = Arc::new(ServiceStats::from_config(&tcp_service("echo")));
        let (a, mut b) = tokio::io::duplex(64);
        let mut counted = maybe_count(a, Some(&stats), ByteKind::Visitor);
        b.write_all(b"hello").await.unwrap();
        let mut buf = [0u8; 5];
        counted.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");
        counted.write_all(b"world").await.unwrap();
        counted.flush().await.unwrap();
        b.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"world");
        assert_eq!(stats.bytes_in.load(Ordering::Relaxed), 5);
        assert_eq!(stats.bytes_out.load(Ordering::Relaxed), 5);
        assert_eq!(stats.wire_bytes_in.load(Ordering::Relaxed), 0);
        assert_eq!(stats.wire_bytes_out.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn session_guard_tracks_active_connections() {
        let stats = Arc::new(ServiceStats::from_config(&tcp_service("echo")));
        {
            stats.on_session_start();
            let _guard = ActiveSession(Arc::clone(&stats));
            assert_eq!(stats.connections_active.load(Ordering::Relaxed), 1);
            assert_eq!(stats.connections_total.load(Ordering::Relaxed), 1);
        }
        assert_eq!(stats.connections_active.load(Ordering::Relaxed), 0);
        assert_eq!(stats.connections_total.load(Ordering::Relaxed), 1);
    }

    async fn http_get(addr: SocketAddr, path: &str) -> (u16, String) {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let text = String::from_utf8(buf).unwrap();
        let status = text
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse::<u16>()
            .unwrap();
        let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
        (status, body)
    }

    #[tokio::test]
    async fn observe_http_routes() {
        let mut services = HashMap::new();
        services.insert("echo".into(), tcp_service("echo"));
        let stats = Arc::new(ServerStats::from_services(&services));
        stats.service("echo").unwrap().set_control_connected(true);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let task = tokio::spawn(serve(listener, Arc::clone(&stats), shutdown_rx));

        let (status, body) = http_get(addr, "/health").await;
        assert_eq!(status, 200);
        assert_eq!(body, "ok\n");

        let (status, body) = http_get(addr, "/stats").await;
        assert_eq!(status, 200);
        let snap: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(snap["services"].as_array().unwrap().len(), 1);
        assert_eq!(snap["services"][0]["name"], "echo");
        assert!(snap["services"][0]["control_connected"].as_bool().unwrap());

        let (status, body) = http_get(addr, "/").await;
        assert_eq!(status, 200);
        assert!(body.contains("\"name\":\"echo\""));

        let (status, body) = http_get(addr, "/metrics").await;
        assert_eq!(status, 200);
        assert!(body.contains("rathole_control_connected"));
        assert!(body.contains(r#"service="echo""#));

        let (status, _) = http_get(addr, "/nope").await;
        assert_eq!(status, 404);

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"POST /stats HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(text.starts_with("HTTP/1.1 405"));

        shutdown_tx.send(true).unwrap();
        task.await.unwrap();
    }
}
