use super::*;
use crate::server::tcp_sampling_state;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn snapshot(state: &ServiceCompressionState) -> (Vec<u8>, Vec<usize>) {
    let sampler = state
        .sampler
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match &*sampler {
        SamplerState::Sampling(buffer) => (buffer.data.clone(), buffer.sizes.clone()),
        SamplerState::Trained | SamplerState::Failed => panic!("sampler is not collecting"),
    }
}

#[tokio::test]
async fn records_successful_reads_and_writes_as_plaintext() -> io::Result<()> {
    // Given
    let state = Arc::new(ServiceCompressionState::new(600));
    let (stream, mut peer) = tokio::io::duplex(2048);
    let mut sampled = SamplingStream::new(stream, Arc::clone(&state));
    let outbound = vec![1; 300];
    let inbound = vec![2; 300];
    let mut received = vec![0; outbound.len()];
    let mut read_back = vec![0; inbound.len()];

    // When
    sampled.write_all(&outbound).await?;
    peer.read_exact(&mut received).await?;
    peer.write_all(&inbound).await?;
    sampled.read_exact(&mut read_back).await?;

    // Then
    let (data, sizes) = snapshot(&state);
    assert_eq!(received, outbound);
    assert_eq!(read_back, inbound);
    assert_eq!(data, [outbound, inbound].concat());
    assert_eq!(sizes, vec![300, 300]);
    Ok(())
}

#[tokio::test]
async fn merges_small_writes_and_splits_large_writes() -> io::Result<()> {
    // Given
    let large = vec![7; 1024 * 1024];
    let window = u64::try_from(300 + large.len()).unwrap();
    let state = Arc::new(ServiceCompressionState::new(window));
    let (stream, _peer) = tokio::io::duplex(2 * 1024 * 1024);
    let mut sampled = SamplingStream::new(stream, Arc::clone(&state));

    // When
    sampled.write_all(&[1; 100]).await?;
    sampled.write_all(&[2; 100]).await?;
    sampled.write_all(&[3; 100]).await?;
    sampled.write_all(&large).await?;

    // Then
    let (data, sizes) = snapshot(&state);
    assert_eq!(data.len(), 300 + large.len());
    assert_eq!(&sizes[..2], &[256, 256]);
    assert_eq!(sizes.len(), 66);
    assert!(sizes[2..65].iter().all(|size| *size == MAX_SAMPLE_SIZE));
    assert_eq!(sizes[65], 16_172);
    assert_eq!(sizes.iter().sum::<usize>(), data.len());
    assert!(sizes.iter().all(|size| (256..=16_384).contains(size)));
    Ok(())
}

#[tokio::test]
async fn completed_window_skips_lock_and_additional_bytes() -> io::Result<()> {
    // Given
    let state = Arc::new(ServiceCompressionState::new(1024));
    let (stream, _peer) = tokio::io::duplex(2 * 1024 * 1024);
    let mut sampled = SamplingStream::new(stream, Arc::clone(&state));

    // When
    sampled.write_all(&vec![4; 2048]).await?;
    let locks_at_cutoff = state.sample_lock_count();
    sampled.write_all(&vec![5; 1024 * 1024]).await?;

    // Then
    let (data, sizes) = snapshot(&state);
    assert!(!state.is_sampling_active());
    assert_eq!(data.len(), 1024);
    assert_eq!(sizes, vec![1024]);
    assert_eq!(state.sample_lock_count(), locks_at_cutoff);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_streams_preserve_all_samples_before_cutoff() -> io::Result<()> {
    // Given
    let state = Arc::new(ServiceCompressionState::new(8192));
    let (left, _left_peer) = tokio::io::duplex(8192);
    let (right, _right_peer) = tokio::io::duplex(8192);
    let left_state = Arc::clone(&state);
    let right_state = Arc::clone(&state);

    // When
    let left_task = tokio::spawn(async move {
        SamplingStream::new(left, left_state)
            .write_all(&vec![6; 4096])
            .await
    });
    let right_task = tokio::spawn(async move {
        SamplingStream::new(right, right_state)
            .write_all(&vec![7; 4096])
            .await
    });
    left_task.await.unwrap()?;
    right_task.await.unwrap()?;

    // Then
    let (data, sizes) = snapshot(&state);
    assert_eq!(data.len(), 8192);
    assert_eq!(sizes.iter().sum::<usize>(), 8192);
    assert_eq!(data.iter().filter(|byte| **byte == 6).count(), 4096);
    assert_eq!(data.iter().filter(|byte| **byte == 7).count(), 4096);
    Ok(())
}

#[test]
fn tcp_sampling_state_wraps_only_active_sampling_services() {
    // Given
    let sampling = Arc::new(ServiceCompressionState::new(1024));
    let trained = Arc::new(ServiceCompressionState::new(1024));
    let failed = Arc::new(ServiceCompressionState::new(1024));
    *trained
        .sampler
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = SamplerState::Trained;
    *failed
        .sampler
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = SamplerState::Failed;

    // When / Then
    assert!(tcp_sampling_state(Some(&sampling)).is_some());
    assert!(tcp_sampling_state(Some(&trained)).is_none());
    assert!(tcp_sampling_state(Some(&failed)).is_none());
    assert!(tcp_sampling_state(None).is_none());
}

#[test]
fn zero_window_is_ready_without_locking_or_collecting() {
    // Given
    let state = ServiceCompressionState::new(0);

    // When
    state.record_plaintext(&vec![8; 1024 * 1024]);
    let samples = state.take_ready_samples().unwrap();

    // Then
    assert!(samples.data.is_empty());
    assert!(samples.sizes.is_empty());
    assert_eq!(state.sample_lock_count(), 0);
    assert!(state.take_ready_samples().is_none());
}
