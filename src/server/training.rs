use super::{Generation, SampleBuffer, SamplerState, ServiceCompressionState};
use crate::compression::train::train_dictionary;
use crate::config::LoadedDictionary;
use anyhow::{Context, Result};
use std::sync::{Arc, Weak};
use tokio::sync::Semaphore;
use tracing::{info, warn};

static DICTIONARY_TRAINING_SEMAPHORE: Semaphore = Semaphore::const_new(1);

struct TrainingJob {
    state: Arc<ServiceCompressionState>,
    service_name: String,
    samples: SampleBuffer,
    max_size: usize,
}

struct TrainingWaiter {
    state: Weak<ServiceCompressionState>,
    sampling_notify: Arc<tokio::sync::Notify>,
    service_name: String,
    max_size: usize,
}

pub(super) fn spawn_dictionary_training(
    state: &Arc<ServiceCompressionState>,
    service_name: String,
    max_size: u64,
) {
    let sampling_notify = Arc::clone(&state.sampling_notify);
    let state = Arc::downgrade(state);
    tokio::spawn(wait_and_train_dictionary(TrainingWaiter {
        state,
        sampling_notify,
        service_name,
        max_size: usize::try_from(max_size).unwrap_or(usize::MAX),
    }));
}

async fn wait_and_train_dictionary(waiter: TrainingWaiter) {
    loop {
        let notified = waiter.sampling_notify.notified();
        let Some(state) = waiter.state.upgrade() else {
            return;
        };
        if let Some(samples) = state.take_ready_samples() {
            train_dictionary_job(TrainingJob {
                state,
                service_name: waiter.service_name,
                samples,
                max_size: waiter.max_size,
            })
            .await;
            return;
        }
        drop(state);
        notified.await;
    }
}

async fn train_dictionary_job(job: TrainingJob) {
    let sample_bytes = job.samples.data.len();
    match train_dictionary_blocking(job.samples, job.max_size).await {
        Ok(dictionary_bytes) => {
            let dict_bytes = dictionary_bytes.len();
            let dictionary = LoadedDictionary::from_bytes(dictionary_bytes);
            match Generation::new(dictionary) {
                Ok(generation) => {
                    job.state
                        .generation_tx
                        .send_replace(Some(Arc::new(generation)));
                    set_terminal_state(&job.state, SamplerState::Trained);
                    info!(
                        service = %job.service_name,
                        sample_bytes,
                        dict_bytes,
                        "trained compression dictionary"
                    );
                }
                Err(error) => fail_training(&job.state, &job.service_name, error),
            }
        }
        Err(error) => fail_training(&job.state, &job.service_name, error),
    }
}

async fn train_dictionary_blocking(samples: SampleBuffer, max_size: usize) -> Result<Vec<u8>> {
    let _permit = DICTIONARY_TRAINING_SEMAPHORE
        .acquire()
        .await
        .context("Dictionary training semaphore closed")?;
    tokio::task::spawn_blocking(move || {
        #[cfg(test)]
        let _training_probe = TrainingProbe::enter();
        train_dictionary(&samples.data, &samples.sizes, max_size)
    })
    .await
    .context("Dictionary training task failed")?
}

fn fail_training(
    state: &ServiceCompressionState,
    service_name: &str,
    error: impl std::fmt::Display,
) {
    set_terminal_state(state, SamplerState::Failed);
    warn!(
        service = %service_name,
        error = %error,
        "dictionary training failed; service stays dictionary-less"
    );
}

fn set_terminal_state(state: &ServiceCompressionState, terminal: SamplerState) {
    let mut sampler = state
        .sampler
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *sampler = terminal;
}

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(test)]
static CURRENT_TRAININGS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static MAX_CONCURRENT_TRAININGS: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
struct TrainingProbe;

#[cfg(test)]
impl TrainingProbe {
    fn enter() -> Self {
        let current = CURRENT_TRAININGS.fetch_add(1, Ordering::SeqCst) + 1;
        MAX_CONCURRENT_TRAININGS.fetch_max(current, Ordering::SeqCst);
        std::thread::sleep(std::time::Duration::from_millis(25));
        Self
    }
}

#[cfg(test)]
impl Drop for TrainingProbe {
    fn drop(&mut self) {
        CURRENT_TRAININGS.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests;
