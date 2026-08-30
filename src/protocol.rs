pub const HASH_WIDTH_IN_BYTES: usize = 32;
pub const MAX_DICT_PUSH_BYTES: u64 = 16 * 1024 * 1024;

use anyhow::{bail, Context, Result};
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::LazyLock;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::trace;

type ProtocolVersion = u8;
const _PROTO_V0: u8 = 0u8;
const PROTO_V1: u8 = 1u8;

pub const CURRENT_PROTO_VERSION: ProtocolVersion = PROTO_V1;

pub type Digest = [u8; HASH_WIDTH_IN_BYTES];

#[derive(Deserialize, Serialize, Debug)]
pub enum Hello {
    ControlChannelHello(ProtocolVersion, Digest), // sha256sum(service name) or a nonce
    DataChannelHello(ProtocolVersion, Digest),    // token provided by CreateDataChannel
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Auth(pub Digest);

#[derive(Deserialize, Serialize, Debug)]
pub enum Ack {
    Ok,
    ServiceNotExist,
    AuthFailed,
}

impl std::fmt::Display for Ack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Ack::Ok => "Ok",
                Ack::ServiceNotExist => "Service not exist",
                Ack::AuthFailed => "Incorrect token",
            }
        )
    }
}

#[derive(Deserialize, Serialize, PartialEq, Eq)]
pub enum ControlChannelCmd {
    CreateDataChannel,
    HeartBeat,
    UpdateCompressionDict { digest: Digest, dictionary: Vec<u8> },
}

impl std::fmt::Debug for ControlChannelCmd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ControlChannelCmd::CreateDataChannel => f.write_str("CreateDataChannel"),
            ControlChannelCmd::HeartBeat => f.write_str("HeartBeat"),
            ControlChannelCmd::UpdateCompressionDict { digest, dictionary } => f
                .debug_struct("UpdateCompressionDict")
                .field("digest", digest)
                .field(
                    "dictionary",
                    &format_args!("MASKED({} bytes)", dictionary.len()),
                )
                .finish(),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq)]
// Shared "StartForward" prefix groups the wire-protocol start-forwarding commands;
// renaming variants would be a breaking protocol change outside this lint's scope.
#[allow(clippy::enum_variant_names)]
pub enum DataChannelCmd {
    StartForwardTcp,
    StartForwardUdp,
    StartForwardTcpZstd { dict_digest: Digest },
    StartForwardUdpZstd { dict_digest: Digest },
}

type UdpPacketLen = u16; // `u16` should be enough for any practical UDP traffic on the Internet
#[derive(Deserialize, Serialize, Debug)]
struct UdpHeader {
    from: SocketAddr,
    len: UdpPacketLen,
}

#[derive(Debug)]
pub struct UdpTraffic {
    pub from: SocketAddr,
    pub data: Bytes,
}

impl UdpTraffic {
    pub async fn write<T: AsyncWrite + Unpin>(&self, writer: &mut T) -> Result<()> {
        let hdr = UdpHeader {
            from: self.from,
            len: self.data.len() as UdpPacketLen,
        };

        let v = bincode::serialize(&hdr).unwrap();

        trace!("Write {:?} of length {}", hdr, v.len());
        writer.write_u8(v.len() as u8).await?;
        writer.write_all(&v).await?;

        writer.write_all(&self.data).await?;

        Ok(())
    }

    #[allow(dead_code)]
    pub async fn write_slice<T: AsyncWrite + Unpin>(
        writer: &mut T,
        from: SocketAddr,
        data: &[u8],
    ) -> Result<()> {
        let hdr = UdpHeader {
            from,
            len: data.len() as UdpPacketLen,
        };

        let v = bincode::serialize(&hdr).unwrap();

        trace!("Write {:?} of length {}", hdr, v.len());
        writer.write_u8(v.len() as u8).await?;
        writer.write_all(&v).await?;

        writer.write_all(data).await?;

        Ok(())
    }

    pub async fn read<T: AsyncRead + Unpin>(reader: &mut T, hdr_len: u8) -> Result<UdpTraffic> {
        let mut buf = vec![0; hdr_len as usize];
        reader
            .read_exact(&mut buf)
            .await
            .with_context(|| "Failed to read udp header")?;

        let hdr: UdpHeader =
            bincode::deserialize(&buf).with_context(|| "Failed to deserialize UdpHeader")?;

        trace!("hdr {:?}", hdr);

        let mut data = BytesMut::new();
        data.resize(hdr.len as usize, 0);
        reader.read_exact(&mut data).await?;

        Ok(UdpTraffic {
            from: hdr.from,
            data: data.freeze(),
        })
    }
}

pub fn digest(data: &[u8]) -> Digest {
    use sha2::{Digest, Sha256};
    Sha256::digest(data).into()
}

struct PacketLength {
    hello: usize,
    ack: usize,
    auth: usize,
    c_cmd: usize,
    d_cmd: usize,
}

impl PacketLength {
    pub fn new() -> PacketLength {
        let username = "default";
        let d = digest(username.as_bytes());
        let hello = bincode::serialized_size(&Hello::ControlChannelHello(CURRENT_PROTO_VERSION, d))
            .unwrap() as usize;
        let c_cmd =
            bincode::serialized_size(&ControlChannelCmd::CreateDataChannel).unwrap() as usize;
        let d_cmd = bincode::serialized_size(&DataChannelCmd::StartForwardTcp).unwrap() as usize;
        let ack = Ack::Ok;
        let ack = bincode::serialized_size(&ack).unwrap() as usize;

        let auth = bincode::serialized_size(&Auth(d)).unwrap() as usize;
        PacketLength {
            hello,
            ack,
            auth,
            c_cmd,
            d_cmd,
        }
    }
}

static PACKET_LEN: LazyLock<PacketLength> = LazyLock::new(PacketLength::new);

pub async fn read_hello<T: AsyncRead + AsyncWrite + Unpin>(conn: &mut T) -> Result<Hello> {
    let mut buf = vec![0u8; PACKET_LEN.hello];
    conn.read_exact(&mut buf)
        .await
        .with_context(|| "Failed to read hello")?;
    let hello = bincode::deserialize(&buf).with_context(|| "Failed to deserialize hello")?;

    match hello {
        Hello::ControlChannelHello(v, _) => {
            if v != CURRENT_PROTO_VERSION {
                bail!(
                    "Protocol version mismatched. Expected {}, got {}. Please update `rathole`.",
                    CURRENT_PROTO_VERSION,
                    v
                );
            }
        }
        Hello::DataChannelHello(v, _) => {
            if v != CURRENT_PROTO_VERSION {
                bail!(
                    "Protocol version mismatched. Expected {}, got {}. Please update `rathole`.",
                    CURRENT_PROTO_VERSION,
                    v
                );
            }
        }
    }

    Ok(hello)
}

pub async fn read_auth<T: AsyncRead + AsyncWrite + Unpin>(conn: &mut T) -> Result<Auth> {
    let mut buf = vec![0u8; PACKET_LEN.auth];
    conn.read_exact(&mut buf)
        .await
        .with_context(|| "Failed to read auth")?;
    bincode::deserialize(&buf).with_context(|| "Failed to deserialize auth")
}

pub async fn read_ack<T: AsyncRead + AsyncWrite + Unpin>(conn: &mut T) -> Result<Ack> {
    let mut bytes = vec![0u8; PACKET_LEN.ack];
    conn.read_exact(&mut bytes)
        .await
        .with_context(|| "Failed to read ack")?;
    bincode::deserialize(&bytes).with_context(|| "Failed to deserialize ack")
}

pub async fn read_control_cmd<T: AsyncRead + AsyncWrite + Unpin>(
    conn: &mut T,
) -> Result<ControlChannelCmd> {
    // Bincode serializes enum discriminants as fixed-width little-endian u32 values.
    let mut tag_bytes = vec![0u8; PACKET_LEN.c_cmd];
    conn.read_exact(&mut tag_bytes)
        .await
        .with_context(|| "Failed to read cmd")?;
    let tag_bytes_array: [u8; 4] = tag_bytes
        .as_slice()
        .try_into()
        .with_context(|| "Invalid ControlChannelCmd tag length")?;
    let tag = u32::from_le_bytes(tag_bytes_array);

    match tag {
        0 | 1 => {
            bincode::deserialize(&tag_bytes).with_context(|| "Failed to deserialize control cmd")
        }
        2 => {
            let mut digest = [0u8; HASH_WIDTH_IN_BYTES];
            conn.read_exact(&mut digest)
                .await
                .with_context(|| "Failed to read control cmd dict digest")?;

            let dictionary_len = conn
                .read_u64_le()
                .await
                .with_context(|| "Failed to read control cmd dictionary length")?;
            if dictionary_len > MAX_DICT_PUSH_BYTES {
                bail!("dictionary too large");
            }
            let dictionary_len = usize::try_from(dictionary_len)
                .with_context(|| "Invalid control cmd dictionary length")?;
            let mut dictionary = vec![0u8; dictionary_len];
            conn.read_exact(&mut dictionary)
                .await
                .with_context(|| "Failed to read control cmd dictionary")?;

            Ok(ControlChannelCmd::UpdateCompressionDict { digest, dictionary })
        }
        _ => bail!("Unknown ControlChannelCmd tag: {}", tag),
    }
}

pub async fn read_data_cmd<T: AsyncRead + AsyncWrite + Unpin>(
    conn: &mut T,
) -> Result<DataChannelCmd> {
    // Bincode serializes enum discriminants as fixed-width little-endian u32 values.
    let mut tag_bytes = vec![0u8; PACKET_LEN.d_cmd];
    conn.read_exact(&mut tag_bytes)
        .await
        .with_context(|| "Failed to read cmd")?;
    let tag_bytes_array: [u8; 4] = tag_bytes
        .as_slice()
        .try_into()
        .with_context(|| "Invalid DataChannelCmd tag length")?;
    let tag = u32::from_le_bytes(tag_bytes_array);

    match tag {
        0 | 1 => bincode::deserialize(&tag_bytes).with_context(|| "Failed to deserialize data cmd"),
        2 | 3 => {
            let mut dict_digest = vec![0u8; HASH_WIDTH_IN_BYTES];
            conn.read_exact(&mut dict_digest)
                .await
                .with_context(|| "Failed to read data cmd dict digest")?;
            tag_bytes.extend_from_slice(&dict_digest);
            bincode::deserialize(&tag_bytes).with_context(|| "Failed to deserialize data cmd")
        }
        _ => bail!("Unknown DataChannelCmd tag: {}", tag),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_existing_control_cmd_wire_encoding() {
        // Given
        let create_data_channel =
            bincode::serialize(&ControlChannelCmd::CreateDataChannel).unwrap();
        let heartbeat = bincode::serialize(&ControlChannelCmd::HeartBeat).unwrap();

        // Then
        assert_eq!(create_data_channel, [0, 0, 0, 0]);
        assert_eq!(heartbeat, [1, 0, 0, 0]);
    }

    #[tokio::test]
    async fn test_update_compression_dict_control_cmd_roundtrip() {
        // Given
        let command = ControlChannelCmd::UpdateCompressionDict {
            digest: [7; HASH_WIDTH_IN_BYTES],
            dictionary: vec![1, 2, 3, 4, 5],
        };
        let bytes = bincode::serialize(&command).unwrap();
        let (mut writer, mut reader) = tokio::io::duplex(1024);
        writer.write_all(&bytes).await.unwrap();

        // When
        let decoded = read_control_cmd(&mut reader).await.unwrap();

        // Then
        assert_eq!(decoded, command);
    }

    #[tokio::test]
    async fn test_update_compression_dict_control_cmd_does_not_overread() {
        // Given
        let update = ControlChannelCmd::UpdateCompressionDict {
            digest: [7; HASH_WIDTH_IN_BYTES],
            dictionary: vec![1, 2, 3, 4, 5],
        };
        let heartbeat = ControlChannelCmd::HeartBeat;
        let mut bytes = bincode::serialize(&update).unwrap();
        bytes.extend_from_slice(&bincode::serialize(&heartbeat).unwrap());
        let (mut writer, mut reader) = tokio::io::duplex(1024);
        writer.write_all(&bytes).await.unwrap();

        // When
        let decoded_update = read_control_cmd(&mut reader).await.unwrap();
        let decoded_heartbeat = read_control_cmd(&mut reader).await.unwrap();

        // Then
        assert_eq!(decoded_update, update);
        assert_eq!(decoded_heartbeat, heartbeat);
    }

    #[tokio::test]
    async fn test_update_compression_dict_rejects_oversized_length() {
        // Given
        let mut bytes = 2u32.to_le_bytes().to_vec();
        bytes.extend_from_slice(&[7; HASH_WIDTH_IN_BYTES]);
        bytes.extend_from_slice(&u64::MAX.to_le_bytes());
        let (mut writer, mut reader) = tokio::io::duplex(1024);
        writer.write_all(&bytes).await.unwrap();

        // When
        let error = read_control_cmd(&mut reader).await.unwrap_err();

        // Then
        assert!(error.to_string().contains("dictionary too large"));
    }

    #[test]
    fn test_update_compression_dict_debug_masks_dictionary() {
        // Given
        let command = ControlChannelCmd::UpdateCompressionDict {
            digest: [7; HASH_WIDTH_IN_BYTES],
            dictionary: vec![0; 4096],
        };

        // When
        let debug = format!("{:?}", command);

        // Then
        assert!(debug.len() < 200);
        assert!(!debug.contains("[0, 0, 0, 0"));
    }

    #[test]
    fn test_existing_data_cmd_wire_encoding() {
        // Given
        let tcp = bincode::serialize(&DataChannelCmd::StartForwardTcp).unwrap();
        let udp = bincode::serialize(&DataChannelCmd::StartForwardUdp).unwrap();

        // Then
        assert_eq!(tcp, [0, 0, 0, 0]);
        assert_eq!(udp, [1, 0, 0, 0]);
    }

    #[tokio::test]
    async fn test_zstd_data_cmd_roundtrip() {
        // Given
        let commands = [
            DataChannelCmd::StartForwardTcpZstd {
                dict_digest: [7; HASH_WIDTH_IN_BYTES],
            },
            DataChannelCmd::StartForwardUdpZstd {
                dict_digest: [9; HASH_WIDTH_IN_BYTES],
            },
        ];

        for command in commands {
            let bytes = bincode::serialize(&command).unwrap();
            let (mut writer, mut reader) = tokio::io::duplex(1024);
            writer.write_all(&bytes).await.unwrap();

            // When
            let decoded = read_data_cmd(&mut reader).await.unwrap();

            // Then
            assert_eq!(decoded, command);
        }
    }

    #[tokio::test]
    async fn test_unit_data_cmd_does_not_overread() {
        // Given
        let sentinel = [0xAA; HASH_WIDTH_IN_BYTES];
        let mut bytes = bincode::serialize(&DataChannelCmd::StartForwardTcp).unwrap();
        bytes.extend_from_slice(&sentinel);
        let (mut writer, mut reader) = tokio::io::duplex(1024);
        writer.write_all(&bytes).await.unwrap();

        // When
        let command = read_data_cmd(&mut reader).await.unwrap();
        let mut remaining = [0; HASH_WIDTH_IN_BYTES];
        reader.read_exact(&mut remaining).await.unwrap();

        // Then
        assert_eq!(command, DataChannelCmd::StartForwardTcp);
        assert_eq!(remaining, sentinel);
    }
}
