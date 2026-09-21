use anyhow::{anyhow, Ok, Result};
use common::{run_rathole_client, PING, PONG};
use rand::RngExt;
#[cfg(feature = "compression-zstd")]
use sha2::{Digest as _, Sha256};
use std::future::Future;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, UdpSocket},
    sync::broadcast,
    time,
};
use tracing::{debug, info, instrument};

use crate::common::run_rathole_server;

mod common;

const ECHO_SERVER_ADDR: &str = "127.0.0.1:8080";
const PINGPONG_SERVER_ADDR: &str = "127.0.0.1:8081";
const ECHO_SERVER_ADDR_EXPOSED: &str = "127.0.0.1:2334";
const PINGPONG_SERVER_ADDR_EXPOSED: &str = "127.0.0.1:2335";
const HITTER_NUM: usize = 4;
const MULTI_ECHO_SERVER_ADDR: &str = "127.0.0.1:18080";
const MULTI_PINGPONG_SERVER_ADDR: &str = "127.0.0.1:18081";
const MULTI_A_ECHO_EXPOSED: &str = "127.0.0.1:12334";
const MULTI_A_PINGPONG_EXPOSED: &str = "127.0.0.1:12335";
const MULTI_B_ECHO_EXPOSED: &str = "127.0.0.1:12434";
const MULTI_B_PINGPONG_EXPOSED: &str = "127.0.0.1:12435";
const OBSERVE_ECHO_SERVER_ADDR: &str = "127.0.0.1:28080";
const OBSERVE_ECHO_EXPOSED: &str = "127.0.0.1:22334";
const OBSERVE_HTTP: &str = "127.0.0.1:24077";
#[cfg(feature = "compression-zstd")]
const OBSERVE_ZSTD_ECHO_SERVER_ADDR: &str = "127.0.0.1:28090";
#[cfg(feature = "compression-zstd")]
const OBSERVE_ZSTD_ECHO_EXPOSED: &str = "127.0.0.1:22344";
#[cfg(feature = "compression-zstd")]
const OBSERVE_ZSTD_HTTP: &str = "127.0.0.1:24087";

#[derive(Clone, Copy, Debug)]
enum Type {
    Tcp,
    Udp,
}

fn init() {
    common::LogCapture::init("info");
}

// Some tests use the same ports (8080, 8081, 2333-2335), and the test harness runs tests
// in parallel. They take turns here. The ports are free again only once the runtime is
// gone, together with the echo servers spawned on it, so the lock is released after that.
fn run_on_default_ports(test: impl Future<Output = Result<()>>) -> Result<()> {
    static DEFAULT_PORTS: Mutex<()> = Mutex::new(());
    let _ports = DEFAULT_PORTS.lock().unwrap_or_else(PoisonError::into_inner);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(test)
}

#[test]
fn tcp() -> Result<()> {
    run_on_default_ports(tcp_transports())
}

async fn tcp_transports() -> Result<()> {
    init();

    // Spawn a echo server
    tokio::spawn(async move {
        if let Err(e) = common::tcp::echo_server(ECHO_SERVER_ADDR).await {
            panic!("Failed to run the echo server for testing: {:?}", e);
        }
    });

    // Spawn a pingpong server
    tokio::spawn(async move {
        if let Err(e) = common::tcp::pingpong_server(PINGPONG_SERVER_ADDR).await {
            panic!("Failed to run the pingpong server for testing: {:?}", e);
        }
    });

    test("tests/for_tcp/tcp_transport.toml", Type::Tcp).await?;

    #[cfg(any(
         // FIXME: Self-signed certificate on macOS nativetls requires manual interference.
         all(target_os = "macos", feature = "rustls"),
         // On other OS accept run with either
         all(not(target_os = "macos"), any(feature = "native-tls", feature = "rustls")),
     ))]
    test("tests/for_tcp/tls_transport.toml", Type::Tcp).await?;

    #[cfg(feature = "noise")]
    test("tests/for_tcp/noise_transport.toml", Type::Tcp).await?;

    #[cfg(any(feature = "websocket-native-tls", feature = "websocket-rustls"))]
    test("tests/for_tcp/websocket_transport.toml", Type::Tcp).await?;

    #[cfg(any(feature = "websocket-native-tls", feature = "websocket-rustls"))]
    test("tests/for_tcp/websocket_path_transport.toml", Type::Tcp).await?;

    #[cfg(not(target_os = "macos"))]
    #[cfg(any(feature = "websocket-native-tls", feature = "websocket-rustls"))]
    test("tests/for_tcp/websocket_tls_transport.toml", Type::Tcp).await?;

    #[cfg(feature = "compression-zstd")]
    test("tests/for_tcp/zstd_compression.toml", Type::Tcp).await?;

    #[cfg(feature = "compression-zstd")]
    test("tests/for_tcp/zstd_dict.toml", Type::Tcp).await?;

    Ok(())
}

#[tokio::test]
async fn observe_stats_counts_tcp_connections_and_bytes() -> Result<()> {
    if cfg!(not(all(feature = "client", feature = "server"))) {
        return Ok(());
    }
    init();

    tokio::spawn(async move {
        if let Err(e) = common::tcp::echo_server(OBSERVE_ECHO_SERVER_ADDR).await {
            panic!("Failed to run the echo server for observe testing: {:?}", e);
        }
    });

    let (client_shutdown_tx, client_shutdown_rx) = broadcast::channel(1);
    let (server_shutdown_tx, server_shutdown_rx) = broadcast::channel(1);
    let config = "tests/for_tcp/observe.toml";
    let client = tokio::spawn(async move {
        run_rathole_client(config, client_shutdown_rx)
            .await
            .unwrap();
    });
    let server = tokio::spawn(async move {
        run_rathole_server(config, server_shutdown_rx)
            .await
            .unwrap();
    });

    wait_http_ok(OBSERVE_HTTP, "/health").await;
    wait_control_connected(OBSERVE_HTTP, "echo").await;

    let payload = b"observe-payload-0123456789";
    let echoed = echo_payload(OBSERVE_ECHO_EXPOSED, payload).await?;
    assert_eq!(echoed, payload);

    let echo = wait_service_bytes(OBSERVE_HTTP, "echo", payload.len() as u64).await;
    assert!(echo["control_connected"].as_bool().unwrap());
    assert!(echo["connections_total"].as_u64().unwrap() >= 1);
    assert_eq!(echo["connections_active"].as_u64().unwrap(), 0);
    assert!(echo["bytes_in"].as_u64().unwrap() >= payload.len() as u64);
    assert!(echo["bytes_out"].as_u64().unwrap() >= payload.len() as u64);
    assert!(echo["wire_bytes_out"].as_u64().unwrap() >= payload.len() as u64);
    assert!(echo.get("compression").is_none());

    let metrics = wait_http_ok(OBSERVE_HTTP, "/metrics").await;
    assert!(metrics.contains("rathole_connections_total"));
    assert!(metrics.contains(r#"service="echo""#));

    server_shutdown_tx.send(true)?;
    client_shutdown_tx.send(true)?;
    let _ = tokio::join!(server, client);
    Ok(())
}

#[cfg(feature = "compression-zstd")]
#[tokio::test]
async fn observe_stats_reports_zstd_compression_ratio() -> Result<()> {
    if cfg!(not(all(feature = "client", feature = "server"))) {
        return Ok(());
    }
    init();

    tokio::spawn(async move {
        if let Err(e) = common::tcp::echo_server(OBSERVE_ZSTD_ECHO_SERVER_ADDR).await {
            panic!(
                "Failed to run the echo server for observe zstd testing: {:?}",
                e
            );
        }
    });

    let (client_shutdown_tx, client_shutdown_rx) = broadcast::channel(1);
    let (server_shutdown_tx, server_shutdown_rx) = broadcast::channel(1);
    let config = "tests/for_tcp/observe_zstd.toml";
    let client = tokio::spawn(async move {
        run_rathole_client(config, client_shutdown_rx)
            .await
            .unwrap();
    });
    let server = tokio::spawn(async move {
        run_rathole_server(config, server_shutdown_rx)
            .await
            .unwrap();
    });

    wait_http_ok(OBSERVE_ZSTD_HTTP, "/health").await;
    wait_control_connected(OBSERVE_ZSTD_HTTP, "echo").await;

    let payload = vec![b'A'; 32 * 1024];
    let echoed = echo_payload(OBSERVE_ZSTD_ECHO_EXPOSED, &payload).await?;
    assert_eq!(echoed, payload);

    let echo = wait_service_bytes(OBSERVE_ZSTD_HTTP, "echo", payload.len() as u64).await;
    assert_eq!(echo["compression"], "zstd");
    let bytes_in = echo["bytes_in"].as_u64().unwrap();
    let wire_out = echo["wire_bytes_out"].as_u64().unwrap();
    assert!(bytes_in >= payload.len() as u64);
    assert!(wire_out > 0);
    assert!(
        wire_out < bytes_in,
        "expected zstd to shrink visitor bytes {bytes_in} to wire {wire_out}"
    );
    let ratio = echo["compression_ratio"].as_f64().unwrap();
    assert!(ratio > 1.0, "compression_ratio {ratio}");

    server_shutdown_tx.send(true)?;
    client_shutdown_tx.send(true)?;
    let _ = tokio::join!(server, client);
    Ok(())
}

#[cfg(feature = "compression-zstd")]
#[test]
fn zstd_half_close() -> Result<()> {
    run_on_default_ports(zstd_half_close_over_tunnel())
}

#[cfg(feature = "compression-zstd")]
async fn zstd_half_close_over_tunnel() -> Result<()> {
    if cfg!(not(all(feature = "client", feature = "server"))) {
        return Ok(());
    }

    init();

    // Given
    tokio::spawn(async move {
        if let Err(e) = common::tcp::echo_server(ECHO_SERVER_ADDR).await {
            panic!("Failed to run the echo server for testing: {:?}", e);
        }
    });

    let (client_shutdown_tx, client_shutdown_rx) = broadcast::channel(1);
    let (server_shutdown_tx, server_shutdown_rx) = broadcast::channel(1);
    let client = tokio::spawn(async move {
        run_rathole_client("tests/for_tcp/zstd_compression.toml", client_shutdown_rx)
            .await
            .unwrap();
    });
    time::sleep(Duration::from_secs(1)).await;
    let server = tokio::spawn(async move {
        run_rathole_server("tests/for_tcp/zstd_compression.toml", server_shutdown_rx)
            .await
            .unwrap();
    });
    time::sleep(Duration::from_millis(2500)).await;

    let mut sent = vec![0u8; 64 * 1024];
    rand::rng().fill(&mut sent[..]);
    let mut conn = TcpStream::connect(ECHO_SERVER_ADDR_EXPOSED).await?;
    let (mut rd, mut wr) = conn.split();
    let mut received = Vec::new();

    // When
    let write = async {
        wr.write_all(&sent).await?;
        wr.shutdown().await?;
        Result::<()>::Ok(())
    };
    let read = async {
        rd.read_to_end(&mut received).await?;
        Result::<()>::Ok(())
    };
    let (write_result, read_result) = tokio::join!(write, read);
    write_result?;
    read_result?;

    // Then
    assert_eq!(
        received.len(),
        sent.len(),
        "byte count mismatch after half-close"
    );
    assert_eq!(
        Sha256::digest(&sent),
        Sha256::digest(&received),
        "content hash mismatch after half-close"
    );

    server_shutdown_tx.send(true)?;
    client_shutdown_tx.send(true)?;
    let _ = tokio::join!(server, client);

    Ok(())
}

#[cfg(feature = "compression-zstd")]
#[test]
fn zstd_auto_dictionary_trains_and_swaps_generation() -> Result<()> {
    run_on_default_ports(zstd_auto_dictionary_over_tunnel())
}

#[cfg(feature = "compression-zstd")]
async fn zstd_auto_dictionary_over_tunnel() -> Result<()> {
    if cfg!(not(all(feature = "client", feature = "server"))) {
        return Ok(());
    }

    init();
    let capture = common::LogCapture::init("info");
    capture.clear();

    // Given
    tokio::spawn(async move {
        if let Err(e) = common::tcp::echo_server(ECHO_SERVER_ADDR).await {
            panic!("Failed to run the echo server for testing: {:?}", e);
        }
    });

    let (client_shutdown_tx, client_shutdown_rx) = broadcast::channel(1);
    let (server_shutdown_tx, server_shutdown_rx) = broadcast::channel(1);
    let client = tokio::spawn(async move {
        run_rathole_client("tests/for_tcp/zstd_auto_dict.toml", client_shutdown_rx)
            .await
            .unwrap();
    });
    time::sleep(Duration::from_secs(1)).await;
    let server = tokio::spawn(async move {
        run_rathole_server("tests/for_tcp/zstd_auto_dict.toml", server_shutdown_rx)
            .await
            .unwrap();
    });
    time::sleep(Duration::from_millis(2500)).await;

    let mut held_connection = TcpStream::connect(ECHO_SERVER_ADDR_EXPOSED).await?;

    // When
    for sequence in 0..32 {
        let record = format!(
            "{{\"service\":\"echo\",\"sequence\":{sequence},\"payload\":\"rathole repetitive payload block {}\"}}\n",
            sequence % 7
        );
        let payload: Vec<u8> = record
            .as_bytes()
            .iter()
            .copied()
            .cycle()
            .take(16 * 1024)
            .collect();
        let mut echoed = vec![0; payload.len()];
        held_connection.write_all(&payload).await?;
        held_connection.read_exact(&mut echoed).await?;
        assert_eq!(echoed, payload);
    }

    capture
        .wait_for(
            "trained compression dictionary",
            "echo",
            Duration::from_secs(10),
        )
        .await?;

    let held_payload = b"held visitor remains on its pre-training generation";
    let mut held_echoed = vec![0; held_payload.len()];
    held_connection.write_all(held_payload).await?;
    held_connection.read_exact(&mut held_echoed).await?;

    let mut new_connection = TcpStream::connect(ECHO_SERVER_ADDR_EXPOSED).await?;
    let new_payload = b"{\"service\":\"echo\",\"sequence\":99,\"payload\":\"rathole repetitive payload block 1\"}\n";
    let mut new_echoed = vec![0; new_payload.len()];
    new_connection.write_all(new_payload).await?;
    new_connection.read_exact(&mut new_echoed).await?;

    // Then
    assert_eq!(held_echoed, held_payload);
    assert_eq!(new_echoed, new_payload);

    server_shutdown_tx.send(true)?;
    client_shutdown_tx.send(true)?;
    let _ = tokio::join!(server, client);

    Ok(())
}

#[tokio::test]
async fn tcp_multi_server() -> Result<()> {
    if cfg!(not(all(feature = "client", feature = "server"))) {
        return Ok(());
    }

    init();

    tokio::spawn(async move {
        if let Err(e) = common::tcp::echo_server(MULTI_ECHO_SERVER_ADDR).await {
            panic!("Failed to run the echo server for testing: {:?}", e);
        }
    });

    tokio::spawn(async move {
        if let Err(e) = common::tcp::pingpong_server(MULTI_PINGPONG_SERVER_ADDR).await {
            panic!("Failed to run the pingpong server for testing: {:?}", e);
        }
    });

    let (client_shutdown_tx, client_shutdown_rx) = broadcast::channel(1);
    let (server_a_shutdown_tx, server_a_shutdown_rx) = broadcast::channel(1);
    let (server_b_shutdown_tx, server_b_shutdown_rx) = broadcast::channel(1);

    info!("start server A and server B");
    let server_a = tokio::spawn(async move {
        run_rathole_server("tests/for_tcp/multi_server_a.toml", server_a_shutdown_rx)
            .await
            .unwrap();
    });
    let server_b = tokio::spawn(async move {
        run_rathole_server("tests/for_tcp/multi_server_b.toml", server_b_shutdown_rx)
            .await
            .unwrap();
    });

    info!("start the client");
    let client = tokio::spawn(async move {
        run_rathole_client("tests/for_tcp/multi_server_client.toml", client_shutdown_rx)
            .await
            .unwrap();
    });
    time::sleep(Duration::from_millis(2500)).await;

    info!("hit server A");
    echo_hitter(MULTI_A_ECHO_EXPOSED, Type::Tcp).await.unwrap();
    pingpong_hitter(MULTI_A_PINGPONG_EXPOSED, Type::Tcp)
        .await
        .unwrap();

    info!("hit server B");
    echo_hitter(MULTI_B_ECHO_EXPOSED, Type::Tcp).await.unwrap();
    pingpong_hitter(MULTI_B_PINGPONG_EXPOSED, Type::Tcp)
        .await
        .unwrap();

    info!("concurrent hits on both servers");
    let mut v = Vec::new();
    for _ in 0..HITTER_NUM / 2 {
        v.push(tokio::spawn(async move {
            echo_hitter(MULTI_A_ECHO_EXPOSED, Type::Tcp).await.unwrap();
        }));
        v.push(tokio::spawn(async move {
            echo_hitter(MULTI_B_ECHO_EXPOSED, Type::Tcp).await.unwrap();
        }));
        v.push(tokio::spawn(async move {
            pingpong_hitter(MULTI_A_PINGPONG_EXPOSED, Type::Tcp)
                .await
                .unwrap();
        }));
        v.push(tokio::spawn(async move {
            pingpong_hitter(MULTI_B_PINGPONG_EXPOSED, Type::Tcp)
                .await
                .unwrap();
        }));
    }
    for h in v {
        assert!(tokio::join!(h).0.is_ok());
    }

    info!("shutdown");
    server_a_shutdown_tx.send(true)?;
    server_b_shutdown_tx.send(true)?;
    client_shutdown_tx.send(true)?;
    let _ = tokio::join!(server_a, server_b, client);

    Ok(())
}

#[tokio::test]
async fn udp() -> Result<()> {
    init();

    // Spawn a echo server
    tokio::spawn(async move {
        if let Err(e) = common::udp::echo_server(ECHO_SERVER_ADDR).await {
            panic!("Failed to run the echo server for testing: {:?}", e);
        }
    });

    // Spawn a pingpong server
    tokio::spawn(async move {
        if let Err(e) = common::udp::pingpong_server(PINGPONG_SERVER_ADDR).await {
            panic!("Failed to run the pingpong server for testing: {:?}", e);
        }
    });

    test("tests/for_udp/tcp_transport.toml", Type::Udp).await?;

    #[cfg(any(
         // FIXME: Self-signed certificate on macOS nativetls requires manual interference.
         all(target_os = "macos", feature = "rustls"),
         // On other OS accept run with either
         all(not(target_os = "macos"), any(feature = "native-tls", feature = "rustls")),
     ))]
    test("tests/for_udp/tls_transport.toml", Type::Udp).await?;

    #[cfg(feature = "noise")]
    test("tests/for_udp/noise_transport.toml", Type::Udp).await?;

    #[cfg(any(feature = "websocket-native-tls", feature = "websocket-rustls"))]
    test("tests/for_udp/websocket_transport.toml", Type::Udp).await?;

    #[cfg(not(target_os = "macos"))]
    #[cfg(any(feature = "websocket-native-tls", feature = "websocket-rustls"))]
    test("tests/for_udp/websocket_tls_transport.toml", Type::Udp).await?;

    #[cfg(feature = "compression-zstd")]
    test("tests/for_udp/zstd_compression.toml", Type::Udp).await?;

    Ok(())
}

#[instrument]
async fn test(config_path: &'static str, t: Type) -> Result<()> {
    if cfg!(not(all(feature = "client", feature = "server"))) {
        // Skip the test if the client or the server is not enabled
        return Ok(());
    }

    let (client_shutdown_tx, client_shutdown_rx) = broadcast::channel(1);
    let (server_shutdown_tx, server_shutdown_rx) = broadcast::channel(1);

    // Start the client
    info!("start the client");
    let client = tokio::spawn(async move {
        run_rathole_client(config_path, client_shutdown_rx)
            .await
            .unwrap();
    });

    // Sleep for 1 second. Expect the client keep retrying to reach the server
    time::sleep(Duration::from_secs(1)).await;

    // Start the server
    info!("start the server");
    let server = tokio::spawn(async move {
        run_rathole_server(config_path, server_shutdown_rx)
            .await
            .unwrap();
    });
    time::sleep(Duration::from_millis(2500)).await; // Wait for the client to retry

    info!("echo");
    echo_hitter(ECHO_SERVER_ADDR_EXPOSED, t).await.unwrap();
    info!("pingpong");
    pingpong_hitter(PINGPONG_SERVER_ADDR_EXPOSED, t)
        .await
        .unwrap();

    // Simulate the client crash and restart
    info!("shutdown the client");
    client_shutdown_tx.send(true)?;
    let _ = tokio::join!(client);

    info!("restart the client");
    let client_shutdown_rx = client_shutdown_tx.subscribe();
    let client = tokio::spawn(async move {
        run_rathole_client(config_path, client_shutdown_rx)
            .await
            .unwrap();
    });
    time::sleep(Duration::from_secs(1)).await; // Wait for the client to start

    info!("echo");
    echo_hitter(ECHO_SERVER_ADDR_EXPOSED, t).await.unwrap();
    info!("pingpong");
    pingpong_hitter(PINGPONG_SERVER_ADDR_EXPOSED, t)
        .await
        .unwrap();

    // Simulate the server crash and restart
    info!("shutdown the server");
    server_shutdown_tx.send(true)?;
    let _ = tokio::join!(server);

    info!("restart the server");
    let server_shutdown_rx = server_shutdown_tx.subscribe();
    let server = tokio::spawn(async move {
        run_rathole_server(config_path, server_shutdown_rx)
            .await
            .unwrap();
    });
    time::sleep(Duration::from_millis(2500)).await; // Wait for the client to retry

    // Simulate heavy load
    info!("lots of echo and pingpong");

    let mut v = Vec::new();

    for _ in 0..HITTER_NUM / 2 {
        v.push(tokio::spawn(async move {
            echo_hitter(ECHO_SERVER_ADDR_EXPOSED, t).await.unwrap();
        }));

        v.push(tokio::spawn(async move {
            pingpong_hitter(PINGPONG_SERVER_ADDR_EXPOSED, t)
                .await
                .unwrap();
        }));
    }

    for h in v {
        assert!(tokio::join!(h).0.is_ok());
    }

    // Shutdown
    info!("shutdown the server and the client");
    server_shutdown_tx.send(true)?;
    client_shutdown_tx.send(true)?;

    let _ = tokio::join!(server, client);

    Ok(())
}

async fn echo_hitter(addr: &'static str, t: Type) -> Result<()> {
    match t {
        Type::Tcp => tcp_echo_hitter(addr).await,
        Type::Udp => udp_echo_hitter(addr).await,
    }
}

async fn pingpong_hitter(addr: &'static str, t: Type) -> Result<()> {
    match t {
        Type::Tcp => tcp_pingpong_hitter(addr).await,
        Type::Udp => udp_pingpong_hitter(addr).await,
    }
}

async fn tcp_echo_hitter(addr: &'static str) -> Result<()> {
    let mut conn = TcpStream::connect(addr).await?;

    let mut wr = [0u8; 1024];
    let mut rd = [0u8; 1024];
    for _ in 0..100 {
        rand::rng().fill(&mut wr);
        conn.write_all(&wr).await?;
        conn.read_exact(&mut rd).await?;
        assert_eq!(wr, rd);
    }

    Ok(())
}

async fn udp_echo_hitter(addr: &'static str) -> Result<()> {
    let conn = UdpSocket::bind("127.0.0.1:0").await?;
    conn.connect(addr).await?;

    let mut wr = [0u8; 128];
    let mut rd = [0u8; 128];
    for _ in 0..3 {
        rand::rng().fill(&mut wr);

        conn.send(&wr).await?;
        debug!("send");

        conn.recv(&mut rd).await?;
        debug!("recv");

        assert_eq!(wr, rd);
    }
    Ok(())
}

async fn tcp_pingpong_hitter(addr: &'static str) -> Result<()> {
    let mut conn = TcpStream::connect(addr).await?;

    let wr = PING.as_bytes();
    let mut rd = [0u8; PONG.len()];

    for _ in 0..100 {
        conn.write_all(wr).await?;
        conn.read_exact(&mut rd).await?;
        assert_eq!(rd, PONG.as_bytes());
    }

    Ok(())
}

async fn udp_pingpong_hitter(addr: &'static str) -> Result<()> {
    let conn = UdpSocket::bind("127.0.0.1:0").await?;
    conn.connect(&addr).await?;

    let wr = PING.as_bytes();
    let mut rd = [0u8; PONG.len()];

    for _ in 0..3 {
        conn.send(wr).await?;
        debug!("ping");

        conn.recv(&mut rd).await?;
        debug!("pong");

        assert_eq!(rd, PONG.as_bytes());
    }

    Ok(())
}

async fn http_get(addr: &str, path: &str) -> std::io::Result<(u16, String)> {
    let mut stream = TcpStream::connect(addr).await?;
    let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await?;
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    std::result::Result::Ok((status, body))
}

async fn wait_http_ok(addr: &str, path: &str) -> String {
    for _ in 0..100 {
        if let std::result::Result::Ok((200, body)) = http_get(addr, path).await {
            return body;
        }
        time::sleep(Duration::from_millis(50)).await;
    }
    panic!("observe endpoint {addr}{path} did not become ready");
}

fn service_named<'a>(body: &'a str, name: &str) -> serde_json::Value {
    let snap: serde_json::Value = serde_json::from_str(body).expect("observe JSON");
    snap["services"]
        .as_array()
        .expect("services")
        .iter()
        .find(|service| service["name"] == name)
        .cloned()
        .unwrap_or_else(|| panic!("missing service {name} in {body}"))
}

async fn wait_control_connected(addr: &str, name: &str) {
    for _ in 0..100 {
        let body = wait_http_ok(addr, "/stats").await;
        let service = service_named(&body, name);
        if service["control_connected"].as_bool() == Some(true) {
            return;
        }
        time::sleep(Duration::from_millis(50)).await;
    }
    panic!("service {name} control channel did not connect");
}

async fn wait_service_bytes(addr: &str, name: &str, min_bytes: u64) -> serde_json::Value {
    for _ in 0..100 {
        let body = wait_http_ok(addr, "/stats").await;
        let service = service_named(&body, name);
        if service["bytes_in"].as_u64().unwrap_or(0) >= min_bytes {
            return service;
        }
        time::sleep(Duration::from_millis(50)).await;
    }
    panic!("service {name} did not reach {min_bytes} bytes_in");
}

async fn echo_payload(addr: &str, data: &[u8]) -> Result<Vec<u8>> {
    let mut last_err = None;
    for _ in 0..50 {
        match TcpStream::connect(addr).await {
            std::result::Result::Ok(mut stream) => {
                stream.write_all(data).await?;
                stream.shutdown().await?;
                let mut buf = Vec::new();
                stream.read_to_end(&mut buf).await?;
                return Ok(buf);
            }
            Err(e) => {
                last_err = Some(e);
                time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    Err(anyhow!("connect {addr}: {:?}", last_err))
}
