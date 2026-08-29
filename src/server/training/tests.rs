use super::*;
use crate::compression::train::{MIN_SAMPLE_COUNT, TEST_MAX_DICT, TEST_WINDOW};
use std::fmt::Debug;
use std::sync::Mutex;
use tracing::field::{Field, Visit};
use tracing::instrument::WithSubscriber;
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone, Default)]
struct LogCapture {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

#[derive(Default)]
struct CapturedEvent {
    message: String,
    fields: Vec<(String, String)>,
}

impl LogCapture {
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
    }
}

impl Visit for CapturedEvent {
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

fn waiter(state: &Arc<ServiceCompressionState>, service_name: &str) -> TrainingWaiter {
    TrainingWaiter {
        state: Arc::downgrade(state),
        sampling_notify: Arc::clone(&state.sampling_notify),
        service_name: service_name.to_string(),
        max_size: TEST_MAX_DICT,
    }
}

fn successful_state() -> Arc<ServiceCompressionState> {
    let state = Arc::new(ServiceCompressionState::new(TEST_WINDOW as u64));
    let base_size = TEST_WINDOW / MIN_SAMPLE_COUNT;
    let remainder = TEST_WINDOW % MIN_SAMPLE_COUNT;
    for index in 0..MIN_SAMPLE_COUNT {
        let sample_size = base_size + usize::from(index < remainder);
        let record = format!(
            "{{\"service\":\"tunnel-{index}\",\"payload\":\"rathole repetitive payload block {index}\"}}\n"
        );
        let sample: Vec<u8> = record
            .as_bytes()
            .iter()
            .copied()
            .cycle()
            .take(sample_size)
            .collect();
        state.record_plaintext(&sample);
    }
    state
}

fn failed_state() -> Arc<ServiceCompressionState> {
    let state = Arc::new(ServiceCompressionState::new(50));
    state.record_plaintext(&[b'x'; 50]);
    state
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successful_training_swaps_generation_and_logs_transition() {
    // Given
    let _test_guard = TEST_LOCK.lock().await;
    let state = successful_state();
    let capture = LogCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());

    // When
    wait_and_train_dictionary(waiter(&state, "success-service"))
        .with_subscriber(subscriber)
        .await;

    // Then
    let generation = state
        .generation_rx
        .borrow()
        .clone()
        .expect("successful training publishes a generation");
    assert_eq!(
        generation.digest,
        crate::protocol::digest(&generation.dictionary.bytes)
    );
    assert!(matches!(
        *state
            .sampler
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        SamplerState::Trained
    ));
    assert!(capture.contains("trained compression dictionary", "success-service"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_training_is_terminal_and_does_not_retry() {
    // Given
    let _test_guard = TEST_LOCK.lock().await;
    let state = failed_state();
    let capture = LogCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());

    // When
    wait_and_train_dictionary(waiter(&state, "failure-service"))
        .with_subscriber(subscriber)
        .await;
    let lock_count = state.sample_lock_count();
    state.record_plaintext(&[b'y'; 4096]);

    // Then
    assert!(matches!(
        *state
            .sampler
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        SamplerState::Failed
    ));
    assert!(state.generation_rx.borrow().is_none());
    assert_eq!(state.sample_lock_count(), lock_count);
    assert!(capture.contains(
        "dictionary training failed; service stays dictionary-less",
        "failure-service"
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn global_semaphore_serializes_concurrent_training() {
    // Given
    let _test_guard = TEST_LOCK.lock().await;
    CURRENT_TRAININGS.store(0, Ordering::SeqCst);
    MAX_CONCURRENT_TRAININGS.store(0, Ordering::SeqCst);
    let first = successful_state();
    let second = successful_state();

    // When
    tokio::join!(
        wait_and_train_dictionary(waiter(&first, "first-service")),
        wait_and_train_dictionary(waiter(&second, "second-service")),
    );

    // Then
    assert_eq!(CURRENT_TRAININGS.load(Ordering::SeqCst), 0);
    assert_eq!(MAX_CONCURRENT_TRAININGS.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_training_releases_taken_sample_buffer() {
    // Given
    let _test_guard = TEST_LOCK.lock().await;
    let state = failed_state();

    // When
    wait_and_train_dictionary(waiter(&state, "memory-service")).await;

    // Then
    assert!(state.take_ready_samples().is_none());
    let sampler = state
        .sampler
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(matches!(*sampler, SamplerState::Failed));
    // Terminal variants `Trained` and `Failed` do not carry a `SampleBuffer`.
    // After training completes (success or failure) `state.sampler` must be
    // one of those variants — not `Sampling(_)` with any buffer at all.
    // That is a stronger proof of memory release than `capacity() == 0` on a
    // lingering empty buffer, because the buffer object itself no longer
    // exists in that slot.
    assert!(
        matches!(*sampler, SamplerState::Trained | SamplerState::Failed),
        "sampler still holds Sampling(_) buffer after training completed"
    );
}
