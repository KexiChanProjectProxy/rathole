use async_compression::{
    tokio::{bufread::ZstdDecoder, write::ZstdEncoder},
    Level,
};
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, BufReader, ReadBuf, ReadHalf, WriteHalf};

use crate::constants::DEFAULT_ZSTD_LEVEL;

#[cfg(feature = "compression-zstd")]
pub mod train;

pub type ZstdReadHalf<S> = ZstdDecoder<BufReader<ReadHalf<S>>>;
pub type ZstdWriteHalf<S> = ZstdEncoder<WriteHalf<S>>;

fn zstd_quality(level: i32) -> Level {
    Level::Precise(level)
}

pub struct ZstdStream<S> {
    decoder: ZstdReadHalf<S>,
    encoder: ZstdWriteHalf<S>,
}

impl<S> ZstdStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub fn new(stream: S) -> Self {
        Self::with_level(stream, DEFAULT_ZSTD_LEVEL)
    }

    pub fn with_level(stream: S, level: i32) -> Self {
        let (reader, writer) = tokio::io::split(stream);
        Self {
            decoder: ZstdDecoder::new(BufReader::new(reader)),
            encoder: ZstdEncoder::with_quality(writer, zstd_quality(level)),
        }
    }

    pub fn with_dict(stream: S, dictionary: &[u8]) -> io::Result<Self> {
        Self::with_dict_and_level(stream, dictionary, DEFAULT_ZSTD_LEVEL)
    }

    pub fn with_dict_and_level(stream: S, dictionary: &[u8], level: i32) -> io::Result<Self> {
        let (reader, writer) = tokio::io::split(stream);
        Ok(Self {
            decoder: ZstdDecoder::with_dict(BufReader::new(reader), dictionary)?,
            encoder: ZstdEncoder::with_dict(writer, zstd_quality(level), dictionary)?,
        })
    }

    pub fn into_split(self) -> (ZstdReadHalf<S>, ZstdWriteHalf<S>) {
        (self.decoder, self.encoder)
    }
}

impl<S> AsyncRead for ZstdStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().decoder).poll_read(cx, buf)
    }
}

impl<S> AsyncWrite for ZstdStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().encoder).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().encoder).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().encoder).poll_shutdown(cx)
    }
}

pub enum MaybeCompressed<S> {
    Plain(S),
    #[cfg(feature = "compression-zstd")]
    Zstd(ZstdStream<S>),
}

impl<S> AsyncRead for MaybeCompressed<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(feature = "compression-zstd")]
            Self::Zstd(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl<S> AsyncWrite for MaybeCompressed<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(feature = "compression-zstd")]
            Self::Zstd(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "compression-zstd")]
            Self::Zstd(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(feature = "compression-zstd")]
            Self::Zstd(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

#[cfg(all(test, feature = "compression-zstd"))]
mod tests {
    use super::ZstdStream;
    use crate::compression::train::{train_dictionary, MIN_SAMPLE_COUNT};
    use std::io;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const DICT: &[u8] = b"rathole-zstd-dictionary: tcp udp tunnel payload service token";
    const WRONG_DICT: &[u8] = b"different-zstd-dictionary: alpha beta gamma delta epsilon";

    async fn compressed_len(
        payload: &[u8],
        dictionary: Option<&[u8]>,
        level: i32,
    ) -> io::Result<usize> {
        let (stream, mut wire) = tokio::io::duplex(payload.len() + 4096);
        let mut compressor = match dictionary {
            Some(dictionary) => ZstdStream::with_dict_and_level(stream, dictionary, level)?,
            None => ZstdStream::with_level(stream, level),
        };
        let mut compressed = Vec::new();

        let (written, read) = tokio::join!(
            async {
                compressor.write_all(payload).await?;
                compressor.shutdown().await
            },
            wire.read_to_end(&mut compressed),
        );
        written?;
        read?;
        Ok(compressed.len())
    }

    #[tokio::test]
    async fn roundtrips_bytes_over_duplex_stream() -> io::Result<()> {
        // Given
        let (left, right) = tokio::io::duplex(4096);
        let mut sender = ZstdStream::new(left);
        let mut receiver = ZstdStream::new(right);
        let payload = b"repetitive tunnel payload ".repeat(128);
        let mut received = Vec::new();

        // When
        let (sent, read) = tokio::join!(
            async {
                sender.write_all(&payload).await?;
                sender.shutdown().await
            },
            receiver.read_to_end(&mut received),
        );
        sent?;
        read?;

        // Then
        assert_eq!(received, payload);
        Ok(())
    }

    #[tokio::test]
    async fn flush_makes_open_stream_decodable() -> io::Result<()> {
        // Given
        let (left, right) = tokio::io::duplex(4096);
        let mut sender = ZstdStream::new(left);
        let mut receiver = ZstdStream::new(right);
        let payload = b"decodable before frame close";
        let mut received = vec![0; payload.len()];

        // When
        let (sent, read) = tokio::join!(
            async {
                sender.write_all(payload).await?;
                sender.flush().await
            },
            receiver.read_exact(&mut received),
        );
        sent?;
        read?;

        // Then
        assert_eq!(received, payload);
        Ok(())
    }

    #[tokio::test]
    async fn roundtrips_bytes_with_raw_dictionary() -> io::Result<()> {
        // Given
        let (left, right) = tokio::io::duplex(4096);
        let mut sender = ZstdStream::with_dict(left, DICT)?;
        let mut receiver = ZstdStream::with_dict(right, DICT)?;
        let payload = DICT.repeat(32);
        let mut received = Vec::new();

        // When
        let (sent, read) = tokio::join!(
            async {
                sender.write_all(&payload).await?;
                sender.shutdown().await
            },
            receiver.read_to_end(&mut received),
        );
        sent?;
        read?;

        // Then
        assert_eq!(received, payload);
        Ok(())
    }

    #[tokio::test]
    async fn reading_with_wrong_dictionary_returns_error() -> io::Result<()> {
        // Given
        let (left, right) = tokio::io::duplex(4096);
        let mut sender = ZstdStream::with_dict(left, DICT)?;
        let mut receiver = ZstdStream::with_dict(right, WRONG_DICT)?;
        let payload = DICT.repeat(32);
        let mut received = Vec::new();

        // When
        let (sent, read) = tokio::join!(
            async {
                sender.write_all(&payload).await?;
                sender.shutdown().await
            },
            receiver.read_to_end(&mut received),
        );
        sent?;

        // Then
        assert!(read.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn trained_dictionary_reduces_compressed_output_size() -> anyhow::Result<()> {
        // Given
        const WINDOW: usize = 409_600;
        const MAX_DICT_SIZE: usize = 4_096;
        let sample_size = WINDOW / MIN_SAMPLE_COUNT;
        let mut samples = Vec::with_capacity(WINDOW);
        let mut sizes = Vec::with_capacity(MIN_SAMPLE_COUNT);
        for index in 0..MIN_SAMPLE_COUNT {
            let record = format!(
                "{{\"service\":\"tunnel-{index}\",\"route\":{},\"status\":{},\"payload\":\"rathole repetitive payload block {}\"}}\n",
                index % 3,
                index % 2,
                index % 4
            );
            let start = samples.len();
            samples.extend(record.as_bytes().iter().copied().cycle().take(sample_size));
            sizes.push(samples.len() - start);
        }
        let remainder = WINDOW - samples.len();
        samples.extend(std::iter::repeat_n(b' ', remainder));
        let last_size = sizes
            .last_mut()
            .ok_or_else(|| anyhow::anyhow!("training corpus has no samples"))?;
        *last_size += remainder;
        let dictionary = train_dictionary(&samples, &sizes, MAX_DICT_SIZE)?;
        let payload = b"{\"service\":\"tunnel-new\",\"route\":1,\"status\":0,\"payload\":\"rathole repetitive payload block 3\"}\n";

        // When
        let with_dictionary = compressed_len(
            payload,
            Some(&dictionary),
            crate::constants::DEFAULT_ZSTD_LEVEL,
        )
        .await?;
        let without_dictionary =
            compressed_len(payload, None, crate::constants::DEFAULT_ZSTD_LEVEL).await?;

        // Then
        assert!(
            with_dictionary < without_dictionary,
            "trained dictionary must reduce compressed bytes: with={with_dictionary}, without={without_dictionary}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn higher_level_compresses_repetitive_payload_smaller() -> io::Result<()> {
        let payload = b"rathole json {\"status\":200,\"msg\":\"ok\"}\n".repeat(256);
        let fastest = compressed_len(&payload, None, 1).await?;
        let default = compressed_len(&payload, None, crate::constants::DEFAULT_ZSTD_LEVEL).await?;
        let high = compressed_len(&payload, None, 19).await?;
        assert!(
            default <= fastest,
            "default level must not expand vs level 1: default={default}, fastest={fastest}"
        );
        assert!(
            high <= default,
            "level 19 must not expand vs default: high={high}, default={default}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_closes_one_direction_only() -> io::Result<()> {
        // Given
        let (left, right) = tokio::io::duplex(4096);
        let mut left = ZstdStream::new(left);
        let mut right = ZstdStream::new(right);
        let outbound = b"left to right after shutdown";
        let reverse = b"right to left remains open";
        let mut outbound_received = vec![0; outbound.len()];

        // When
        let (sent, read) = tokio::join!(
            async {
                left.write_all(outbound).await?;
                left.shutdown().await
            },
            async {
                right.read_exact(&mut outbound_received).await?;
                let mut eof = [0];
                right.read(&mut eof).await
            },
        );
        sent?;
        let eof_len = read?;

        let mut reverse_received = vec![0; reverse.len()];
        let (sent, read) = tokio::join!(
            async {
                right.write_all(reverse).await?;
                right.flush().await
            },
            left.read_exact(&mut reverse_received),
        );
        sent?;
        read?;

        // Then
        assert_eq!(outbound_received, outbound);
        assert_eq!(eof_len, 0);
        assert_eq!(reverse_received, reverse);
        Ok(())
    }
}
