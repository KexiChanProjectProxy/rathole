use core::result::Result;
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use super::{AddrMaybeCached, SocketOpts, TcpTransport, TlsTransport, Transport};
use crate::config::{TlsConfig, TransportConfig};
use anyhow::{anyhow, Context as _};
use async_trait::async_trait;
use bytes::Bytes;
use futures_core::stream::Stream;
use futures_sink::Sink;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};

#[cfg(any(feature = "native-tls", feature = "rustls"))]
use super::tls::get_tcpstream;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use super::tls::TlsStream;

use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use tokio_tungstenite::{accept_hdr_async_with_config, client_async_with_config, WebSocketStream};
use tokio_util::io::StreamReader;

#[derive(Debug)]
enum TransportStream {
    Insecure(TcpStream),
    Secure(TlsStream<TcpStream>),
}

impl TransportStream {
    fn get_tcpstream(&self) -> &TcpStream {
        match self {
            TransportStream::Insecure(s) => s,
            TransportStream::Secure(s) => get_tcpstream(s),
        }
    }
}

impl AsyncRead for TransportStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            TransportStream::Insecure(s) => Pin::new(s).poll_read(cx, buf),
            TransportStream::Secure(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for TransportStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        match self.get_mut() {
            TransportStream::Insecure(s) => Pin::new(s).poll_write(cx, buf),
            TransportStream::Secure(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            TransportStream::Insecure(s) => Pin::new(s).poll_flush(cx),
            TransportStream::Secure(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            TransportStream::Insecure(s) => Pin::new(s).poll_shutdown(cx),
            TransportStream::Secure(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

#[derive(Debug)]
struct StreamWrapper {
    inner: WebSocketStream<TransportStream>,
}

impl Stream for StreamWrapper {
    type Item = Result<Bytes, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.get_mut().inner).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(err))) => {
                Poll::Ready(Some(Err(Error::new(ErrorKind::Other, err))))
            }
            Poll::Ready(Some(Ok(res))) => {
                if let Message::Binary(b) = res {
                    Poll::Ready(Some(Ok(Bytes::from(b))))
                } else {
                    Poll::Ready(Some(Err(Error::new(
                        ErrorKind::InvalidData,
                        "unexpected frame",
                    ))))
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

#[derive(Debug)]
pub struct WebsocketTunnel {
    inner: StreamReader<StreamWrapper, Bytes>,
}

impl AsyncRead for WebsocketTunnel {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl AsyncBufRead for WebsocketTunnel {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<&[u8]>> {
        Pin::new(&mut self.get_mut().inner).poll_fill_buf(cx)
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        Pin::new(&mut self.get_mut().inner).consume(amt)
    }
}

impl AsyncWrite for WebsocketTunnel {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let sw = self.get_mut().inner.get_mut();
        ready!(Pin::new(&mut sw.inner)
            .poll_ready(cx)
            .map_err(|err| Error::new(ErrorKind::Other, err)))?;

        match Pin::new(&mut sw.inner).start_send(Message::binary(buf.to_vec())) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(e) => Poll::Ready(Err(Error::new(ErrorKind::Other, e))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.get_mut().inner.get_mut().inner)
            .poll_flush(cx)
            .map_err(|err| Error::new(ErrorKind::Other, err))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.get_mut().inner.get_mut().inner)
            .poll_close(cx)
            .map_err(|err| Error::new(ErrorKind::Other, err))
    }
}

#[derive(Debug)]
enum SubTransport {
    Secure(TlsTransport),
    Insecure(TcpTransport),
}

#[derive(Debug)]
pub struct WebsocketTransport {
    sub: SubTransport,
    conf: WebSocketConfig,
    tls: bool,
    path: String,
    hostname: Option<String>,
}

#[async_trait]
impl Transport for WebsocketTransport {
    type Acceptor = TcpListener;
    type RawStream = TcpStream;
    type Stream = WebsocketTunnel;

    fn new(config: &TransportConfig) -> anyhow::Result<Self> {
        let wsconfig = config
            .websocket
            .as_ref()
            .ok_or_else(|| anyhow!("Missing websocket config"))?;

        let conf = WebSocketConfig::default().write_buffer_size(0);
        let path = normalize_ws_path(&wsconfig.path);
        let hostname = config.tls.as_ref().and_then(|t| t.hostname.clone());
        let tls = wsconfig.tls;
        let sub = match tls {
            true => {
                if config.tls.is_some() {
                    SubTransport::Secure(TlsTransport::new(config)?)
                } else {
                    let mut local = config.clone();
                    local.tls = Some(TlsConfig::default());
                    SubTransport::Secure(TlsTransport::new(&local)?)
                }
            }
            false => SubTransport::Insecure(TcpTransport::new(config)?),
        };
        Ok(WebsocketTransport {
            sub,
            conf,
            tls,
            path,
            hostname,
        })
    }

    fn hint(conn: &Self::Stream, opt: SocketOpts) {
        opt.apply(conn.inner.get_ref().inner.get_ref().get_tcpstream())
    }

    async fn bind<A: ToSocketAddrs + Send + Sync>(
        &self,
        addr: A,
    ) -> anyhow::Result<Self::Acceptor> {
        TcpListener::bind(addr).await.map_err(Into::into)
    }

    async fn accept(&self, a: &Self::Acceptor) -> anyhow::Result<(Self::RawStream, SocketAddr)> {
        let (s, addr) = match &self.sub {
            SubTransport::Insecure(t) => t.accept(a).await?,
            SubTransport::Secure(t) => t.accept(a).await?,
        };
        Ok((s, addr))
    }

    async fn handshake(&self, conn: Self::RawStream) -> anyhow::Result<Self::Stream> {
        let tsream = match &self.sub {
            SubTransport::Insecure(t) => TransportStream::Insecure(t.handshake(conn).await?),
            SubTransport::Secure(t) => TransportStream::Secure(t.handshake(conn).await?),
        };
        let expected = self.path.clone();
        let wsstream = accept_hdr_async_with_config(
            tsream,
            move |req: &Request, response: Response| {
                if req.uri().path() == expected {
                    Ok(response)
                } else {
                    let mut resp = ErrorResponse::new(Some(format!(
                        "WebSocket path mismatch: got {}, expected {}",
                        req.uri().path(),
                        expected
                    )));
                    *resp.status_mut() = StatusCode::NOT_FOUND;
                    Err(resp)
                }
            },
            Some(self.conf),
        )
        .await?;
        let tun = WebsocketTunnel {
            inner: StreamReader::new(StreamWrapper { inner: wsstream }),
        };
        Ok(tun)
    }

    async fn connect(&self, addr: &AddrMaybeCached) -> anyhow::Result<Self::Stream> {
        let u = websocket_handshake_uri(
            self.tls,
            &addr.addr,
            self.hostname.as_deref(),
            &self.path,
        )?;
        let tstream = match &self.sub {
            SubTransport::Insecure(t) => TransportStream::Insecure(t.connect(addr).await?),
            SubTransport::Secure(t) => TransportStream::Secure(t.connect(addr).await?),
        };
        let (wsstream, _) = client_async_with_config(u.as_str(), tstream, Some(self.conf))
            .await
            .with_context(|| format!("Failed to connect websocket to {u}"))?;
        let tun = WebsocketTunnel {
            inner: StreamReader::new(StreamWrapper { inner: wsstream }),
        };
        Ok(tun)
    }
}

fn normalize_ws_path(path: &str) -> String {
    if path.is_empty() {
        "/".into()
    } else if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    }
}

fn websocket_handshake_uri(
    tls: bool,
    addr: &str,
    hostname: Option<&str>,
    path: &str,
) -> anyhow::Result<String> {
    let (addr_host, port) = crate::helper::host_port_pair(addr)?;
    let host = hostname.filter(|h| !h.is_empty()).unwrap_or(addr_host);
    let host = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    };
    let scheme = if tls { "wss" } else { "ws" };
    let default_port = if tls { 443 } else { 80 };
    let authority = if port == default_port {
        host
    } else {
        format!("{host}:{port}")
    };
    Ok(format!("{scheme}://{authority}{path}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{TransportConfig, WebsocketConfig};
    use crate::transport::{AddrMaybeCached, Transport};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn ws_transport(path: &str) -> WebsocketTransport {
        WebsocketTransport::new(&TransportConfig {
            websocket: Some(WebsocketConfig {
                tls: false,
                path: path.into(),
            }),
            ..Default::default()
        })
        .unwrap()
    }

    #[test]
    fn handshake_uri_ws_with_port() {
        assert_eq!(
            websocket_handshake_uri(false, "example.com:2333", None, "/").unwrap(),
            "ws://example.com:2333/"
        );
    }

    #[test]
    fn handshake_uri_wss_omits_443() {
        assert_eq!(
            websocket_handshake_uri(true, "1.2.3.4:443", Some("tunnel.example.com"), "/rathole")
                .unwrap(),
            "wss://tunnel.example.com/rathole"
        );
    }

    #[test]
    fn normalize_adds_leading_slash() {
        assert_eq!(normalize_ws_path("rathole"), "/rathole");
    }

    #[tokio::test]
    async fn websocket_path_match_and_mismatch() {
        let server = ws_transport("/rathole");
        let acceptor = server.bind("127.0.0.1:0").await.unwrap();
        let bound = acceptor.local_addr().unwrap();
        let server_task = tokio::spawn(async move {
            let (raw, _) = server.accept(&acceptor).await?;
            let mut stream = server.handshake(raw).await?;
            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf).await?;
            assert_eq!(&buf, b"ping");
            stream.write_all(b"pong").await?;
            stream.flush().await?;
            anyhow::Ok(())
        });

        let ok_client = ws_transport("/rathole");
        let mut client = ok_client
            .connect(&AddrMaybeCached::new(&bound.to_string()))
            .await
            .expect("path match");
        client.write_all(b"ping").await.unwrap();
        client.flush().await.unwrap();
        let mut buf = [0u8; 4];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"pong");
        server_task.await.unwrap().unwrap();
        drop(client);

        let server = ws_transport("/rathole");
        let acceptor = server.bind("127.0.0.1:0").await.unwrap();
        let bound = acceptor.local_addr().unwrap();
        let server_task = tokio::spawn(async move {
            let (raw, _) = server.accept(&acceptor).await?;
            let _ = server.handshake(raw).await;
            anyhow::Ok(())
        });

        let bad_client = ws_transport("/wrong");
        assert!(
            bad_client
                .connect(&AddrMaybeCached::new(&bound.to_string()))
                .await
                .is_err()
        );
        let _ = server_task.await;
    }
}
