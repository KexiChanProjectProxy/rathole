use super::{SamplerState, ServiceCompressionState};
use std::io;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

const MIN_SAMPLE_SIZE: usize = 256;
const MAX_SAMPLE_SIZE: usize = 16 * 1024;

#[derive(Debug, Default)]
pub struct SampleBuffer {
    pub data: Vec<u8>,
    pub sizes: Vec<usize>,
    pending_size: usize,
    window: usize,
}

impl SampleBuffer {
    pub fn new(window: u64) -> Self {
        Self {
            data: Vec::new(),
            sizes: Vec::new(),
            pending_size: 0,
            window: usize::try_from(window).unwrap_or(usize::MAX),
        }
    }

    fn append(&mut self, bytes: &[u8]) -> bool {
        let remaining = self.window.saturating_sub(self.data.len());
        let bytes = &bytes[..bytes.len().min(remaining)];
        self.data.extend_from_slice(bytes);
        self.record_boundaries(bytes.len());

        let reached = self.data.len() >= self.window;
        if reached {
            self.finish_pending_sample();
        }
        reached
    }

    fn record_boundaries(&mut self, mut added: usize) {
        if self.pending_size > 0 {
            let merged = added.min(MIN_SAMPLE_SIZE - self.pending_size);
            self.pending_size += merged;
            added -= merged;
            if self.pending_size == MIN_SAMPLE_SIZE {
                self.sizes.push(self.pending_size);
                self.pending_size = 0;
            }
        }

        while added >= MIN_SAMPLE_SIZE {
            let sample_size = added.min(MAX_SAMPLE_SIZE);
            self.sizes.push(sample_size);
            added -= sample_size;
        }
        self.pending_size += added;
    }

    fn finish_pending_sample(&mut self) {
        if self.pending_size == 0 {
            return;
        }

        if let Some(last) = self.sizes.last_mut() {
            if *last + self.pending_size <= MAX_SAMPLE_SIZE {
                *last += self.pending_size;
            } else {
                let moved = MIN_SAMPLE_SIZE - self.pending_size;
                *last -= moved;
                self.sizes.push(MIN_SAMPLE_SIZE);
            }
            self.pending_size = 0;
        }
    }

    fn take(&mut self) -> Self {
        Self {
            data: std::mem::take(&mut self.data),
            sizes: std::mem::take(&mut self.sizes),
            pending_size: std::mem::take(&mut self.pending_size),
            window: self.window,
        }
    }
}

impl ServiceCompressionState {
    pub(super) fn record_plaintext(&self, bytes: &[u8]) {
        if !self.sampling_active.load(Ordering::Relaxed) {
            return;
        }

        #[cfg(test)]
        self.sample_lock_count.fetch_add(1, Ordering::Relaxed);
        let mut sampler = self
            .sampler
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &mut *sampler {
            SamplerState::Sampling(buffer) => {
                if buffer.append(bytes) {
                    self.sampling_active.store(false, Ordering::Relaxed);
                    self.sampling_ready.store(true, Ordering::Release);
                    self.sampling_notify.notify_one();
                }
            }
            SamplerState::Trained | SamplerState::Failed => {
                self.sampling_active.store(false, Ordering::Relaxed);
            }
        }
    }

    pub(super) fn is_sampling_active(&self) -> bool {
        self.sampling_active.load(Ordering::Relaxed)
    }

    pub fn take_ready_samples(&self) -> Option<SampleBuffer> {
        if !self.sampling_ready.swap(false, Ordering::AcqRel) {
            return None;
        }

        let mut sampler = self
            .sampler
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &mut *sampler {
            SamplerState::Sampling(buffer) => Some(buffer.take()),
            SamplerState::Trained | SamplerState::Failed => None,
        }
    }

    #[cfg(test)]
    pub(super) fn sample_lock_count(&self) -> usize {
        self.sample_lock_count.load(Ordering::Relaxed)
    }
}

pub struct SamplingStream<S> {
    inner: S,
    state: Arc<ServiceCompressionState>,
}

impl<S> SamplingStream<S> {
    pub(super) fn new(inner: S, state: Arc<ServiceCompressionState>) -> Self {
        Self { inner, state }
    }
}

impl<S> AsyncRead for SamplingStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let filled_before = buf.filled().len();
        match Pin::new(&mut this.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                this.state.record_plaintext(&buf.filled()[filled_before..]);
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl<S> AsyncWrite for SamplingStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(written)) => {
                this.state.record_plaintext(&buf[..written]);
                Poll::Ready(Ok(written))
            }
            other => other,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests;
