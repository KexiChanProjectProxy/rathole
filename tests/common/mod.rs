use std::{
    fmt::Debug,
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use anyhow::{anyhow, Result};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, ToSocketAddrs},
    sync::{broadcast, Notify},
};
use tracing::{field::Field, Event, Subscriber};
use tracing_subscriber::{
    layer::{Context, SubscriberExt},
    registry::LookupSpan,
    EnvFilter, Layer,
};

pub const PING: &str = "ping";
pub const PONG: &str = "pong";

#[derive(Clone, Default)]
pub struct LogCapture {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
    notify: Arc<Notify>,
}

#[derive(Default)]
struct CapturedEvent {
    message: String,
    fields: Vec<(String, String)>,
}

impl LogCapture {
    pub fn init(level: &str) -> &'static Self {
        static CAPTURE: OnceLock<LogCapture> = OnceLock::new();
        CAPTURE.get_or_init(|| {
            let capture = LogCapture::default();
            let subscriber = tracing_subscriber::registry()
                .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::from(level)))
                .with(capture.clone())
                .with(tracing_subscriber::fmt::layer());
            let _ = tracing::subscriber::set_global_default(subscriber);
            capture
        })
    }

    pub fn clear(&self) {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    pub async fn wait_for(&self, message: &str, service: &str, timeout: Duration) -> Result<()> {
        tokio::time::timeout(timeout, async {
            loop {
                let notified = self.notify.notified();
                if self.contains(message, service) {
                    return;
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| anyhow!("timed out waiting for log `{message}` for service `{service}`"))?;
        Ok(())
    }

    fn contains(&self, message: &str, service: &str) -> bool {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .any(|event| {
                event.message == message
                    && event
                        .fields
                        .iter()
                        .any(|(name, value)| name == "service" && value == service)
            })
    }
}

impl<S> Layer<S> for LogCapture
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        let mut captured = CapturedEvent::default();
        event.record(&mut captured);
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(captured);
        self.notify.notify_waiters();
    }
}

impl tracing::field::Visit for CapturedEvent {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}").trim_matches('"').to_string();
        } else {
            self.fields
                .push((field.name().to_string(), format!("{value:?}")));
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        } else {
            self.fields
                .push((field.name().to_string(), value.to_string()));
        }
    }
}

pub async fn run_rathole_server(
    config_path: &str,
    shutdown_rx: broadcast::Receiver<bool>,
) -> Result<()> {
    let cli = rathole::Cli {
        config_path: Some(PathBuf::from(config_path)),
        server: true,
        client: false,
        ..Default::default()
    };
    rathole::run(cli, shutdown_rx).await
}

pub async fn run_rathole_client(
    config_path: &str,
    shutdown_rx: broadcast::Receiver<bool>,
) -> Result<()> {
    let cli = rathole::Cli {
        config_path: Some(PathBuf::from(config_path)),
        server: false,
        client: true,
        ..Default::default()
    };
    rathole::run(cli, shutdown_rx).await
}

pub mod tcp {
    use super::*;

    pub async fn echo_server<A: ToSocketAddrs>(addr: A) -> Result<()> {
        let l = TcpListener::bind(addr).await?;

        loop {
            let (conn, _addr) = l.accept().await?;
            tokio::spawn(async move {
                let _ = echo(conn).await;
            });
        }
    }

    pub async fn pingpong_server<A: ToSocketAddrs>(addr: A) -> Result<()> {
        let l = TcpListener::bind(addr).await?;

        loop {
            let (conn, _addr) = l.accept().await?;
            tokio::spawn(async move {
                let _ = pingpong(conn).await;
            });
        }
    }

    async fn echo(conn: TcpStream) -> Result<()> {
        let (mut rd, mut wr) = conn.into_split();
        io::copy(&mut rd, &mut wr).await?;

        Ok(())
    }

    async fn pingpong(mut conn: TcpStream) -> Result<()> {
        let mut buf = [0u8; PING.len()];

        while conn.read_exact(&mut buf).await? != 0 {
            assert_eq!(buf, PING.as_bytes());
            conn.write_all(PONG.as_bytes()).await?;
        }

        Ok(())
    }
}

pub mod udp {
    use rathole::UDP_BUFFER_SIZE;
    use tokio::net::UdpSocket;
    use tracing::debug;

    use super::*;

    pub async fn echo_server<A: ToSocketAddrs>(addr: A) -> Result<()> {
        let l = UdpSocket::bind(addr).await?;
        debug!("UDP echo server listening");

        let mut buf = [0u8; UDP_BUFFER_SIZE];
        loop {
            let (n, addr) = l.recv_from(&mut buf).await?;
            debug!("Get {:?} from {}", &buf[..n], addr);
            l.send_to(&buf[..n], addr).await?;
        }
    }

    pub async fn pingpong_server<A: ToSocketAddrs>(addr: A) -> Result<()> {
        let l = UdpSocket::bind(addr).await?;

        let mut buf = [0u8; UDP_BUFFER_SIZE];
        loop {
            let (n, addr) = l.recv_from(&mut buf).await?;
            assert_eq!(&buf[..n], PING.as_bytes());
            l.send_to(PONG.as_bytes(), addr).await?;
        }
    }
}
