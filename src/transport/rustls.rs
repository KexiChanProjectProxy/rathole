use crate::config::{TlsConfig, TransportConfig};
use crate::helper::host_port_pair;
use crate::transport::{AddrMaybeCached, SocketOpts, TcpTransport, Transport};
use std::fmt::Debug;
use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use p12::PFX;
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};
pub(crate) use tokio_rustls::TlsStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};

pub struct TlsTransport {
    tcp: TcpTransport,
    config: TlsConfig,
    connector: Option<TlsConnector>,
    tls_acceptor: Option<TlsAcceptor>,
}

// workaround for TlsConnector and TlsAcceptor not implementing Debug
impl Debug for TlsTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsTransport")
            .field("tcp", &self.tcp)
            .field("config", &self.config)
            .finish()
    }
}

fn load_server_config(config: &TlsConfig) -> Result<Option<ServerConfig>> {
    if let Some(pkcs12_path) = config.pkcs12.as_ref() {
        let buf = fs::read(pkcs12_path)?;
        let pfx = PFX::parse(buf.as_slice())?;
        let pass = config.pkcs12_password.as_ref().unwrap();

        let certs = pfx.cert_bags(pass)?;
        let keys = pfx.key_bags(pass)?;

        let chain: Vec<CertificateDer> = certs.into_iter().map(CertificateDer::from).collect();
        let key = PrivatePkcs8KeyDer::from(keys.into_iter().next().unwrap());

        Ok(Some(
            ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(chain, key.into())?,
        ))
    } else {
        Ok(None)
    }
}

fn load_client_config(config: &TlsConfig) -> Result<Option<ClientConfig>> {
    let mut root_certs = RootCertStore::empty();
    if let Some(path) = config.trusted_root.as_ref() {
        let mut reader = std::io::BufReader::new(fs::File::open(path)?);
        let certs: Vec<_> = rustls_pemfile::certs(&mut reader).collect::<Result<_, _>>()?;
        if certs.is_empty() {
            return Err(anyhow!("No certificates in `tls.trusted_root`"));
        }
        let (added, _) = root_certs.add_parsable_certificates(certs);
        if added == 0 {
            return Err(anyhow!("No certificates in `tls.trusted_root`"));
        }
    } else {
        let native = rustls_native_certs::load_native_certs();
        if native.certs.is_empty() {
            return Err(anyhow!("Failed to load native certs: {:?}", native.errors));
        }
        let (added, _) = root_certs.add_parsable_certificates(native.certs);
        if added == 0 {
            return Err(anyhow!("Failed to load native certs: {:?}", native.errors));
        }
    }

    Ok(Some(
        ClientConfig::builder()
            .with_root_certificates(root_certs)
            .with_no_client_auth(),
    ))
}

#[async_trait]
impl Transport for TlsTransport {
    type Acceptor = TcpListener;
    type RawStream = TcpStream;
    type Stream = TlsStream<TcpStream>;

    fn new(config: &TransportConfig) -> Result<Self> {
        let tcp = TcpTransport::new(config)?;
        let config = config
            .tls
            .as_ref()
            .ok_or_else(|| anyhow!("Missing tls config"))?;

        let connector = load_client_config(config)?.map(|c| Arc::new(c).into());
        let tls_acceptor = load_server_config(config)?.map(|c| Arc::new(c).into());

        Ok(TlsTransport {
            tcp,
            config: config.clone(),
            connector,
            tls_acceptor,
        })
    }

    fn hint(conn: &Self::Stream, opt: SocketOpts) {
        opt.apply(conn.get_ref().0);
    }

    async fn bind<A: ToSocketAddrs + Send + Sync>(&self, addr: A) -> Result<Self::Acceptor> {
        let l = TcpListener::bind(addr)
            .await
            .with_context(|| "Failed to create tcp listener")?;
        Ok(l)
    }

    async fn accept(&self, a: &Self::Acceptor) -> Result<(Self::RawStream, SocketAddr)> {
        self.tcp
            .accept(a)
            .await
            .with_context(|| "Failed to accept TCP connection")
    }

    async fn handshake(&self, conn: Self::RawStream) -> Result<Self::Stream> {
        let conn = self.tls_acceptor.as_ref().unwrap().accept(conn).await?;
        Ok(tokio_rustls::TlsStream::Server(conn))
    }

    async fn connect(&self, addr: &AddrMaybeCached) -> Result<Self::Stream> {
        let conn = self.tcp.connect(addr).await?;

        let connector = self.connector.as_ref().unwrap();

        let host_name = self
            .config
            .hostname
            .as_deref()
            .unwrap_or(host_port_pair(&addr.addr)?.0);

        Ok(tokio_rustls::TlsStream::Client(
            connector
                .connect(ServerName::try_from(host_name)?.to_owned(), conn)
                .await?,
        ))
    }
}

pub(crate) fn get_tcpstream(s: &TlsStream<TcpStream>) -> &TcpStream {
    &s.get_ref().0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{TlsConfig, TransportConfig};

    #[test]
    fn client_config_without_trusted_root_loads_system_roots() {
        let cfg = TransportConfig {
            tls: Some(TlsConfig {
                hostname: Some("example.com".into()),
                trusted_root: None,
                pkcs12: None,
                pkcs12_password: None,
            }),
            ..Default::default()
        };
        TlsTransport::new(&cfg).expect("system roots");
    }
}
