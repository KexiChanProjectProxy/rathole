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

pub type ZstdReadHalf<S> = ZstdDecoder<BufReader<ReadHalf<S>>>;
pub type ZstdWriteHalf<S> = ZstdEncoder<WriteHalf<S>>;

pub struct ZstdStream<S> {
    decoder: ZstdReadHalf<S>,
    encoder: ZstdWriteHalf<S>,
}

impl<S> ZstdStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub fn new(stream: S) -> Self {
        let (reader, writer) = tokio::io::split(stream);
        Self {
            decoder: ZstdDecoder::new(BufReader::new(reader)),
            encoder: ZstdEncoder::new(writer),
        }
    }

    pub fn with_dict(stream: S, dictionary: &[u8]) -> io::Result<Self> {
        let (reader, writer) = tokio::io::split(stream);
        Ok(Self {
            decoder: ZstdDecoder::with_dict(BufReader::new(reader), dictionary)?,
            encoder: ZstdEncoder::with_dict(writer, Level::Default, dictionary)?,
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
    use std::io;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const DICT: &[u8] = b"rathole-zstd-dictionary: tcp udp tunnel payload service token";
    const WRONG_DICT: &[u8] = b"different-zstd-dictionary: alpha beta gamma delta epsilon";

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
