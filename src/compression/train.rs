use anyhow::{Context, Result};

pub const MIN_TRAIN_FACTOR: usize = 1;
pub const MIN_SAMPLE_COUNT: usize = 7;
pub const TEST_MAX_DICT: usize = 4 * 1024;
pub const TEST_WINDOW: usize = MIN_TRAIN_FACTOR * TEST_MAX_DICT;

/// Trains a zstd dictionary from concatenated samples and their individual sizes.
///
/// # Errors
/// Returns an error when zstd rejects the corpus or dictionary size.
pub fn train_dictionary(samples: &[u8], sizes: &[usize], max_size: usize) -> Result<Vec<u8>> {
    zstd::dict::from_continuous(samples, sizes, max_size).context("Failed to train zstd dictionary")
}

#[cfg(test)]
mod tests {
    use super::{train_dictionary, MIN_SAMPLE_COUNT, TEST_MAX_DICT, TEST_WINDOW};
    use anyhow::Result;
    use rand::{rngs::StdRng, RngExt, SeedableRng};

    const SEEDS: [u64; 8] = [7, 19, 41, 73, 101, 149, 211, 307];

    fn corpus(seed: u64, window: usize, sample_count: usize) -> (Vec<u8>, Vec<usize>) {
        let mut rng = StdRng::seed_from_u64(seed);
        let base_size = window / sample_count;
        let remainder = window % sample_count;
        let mut samples = Vec::with_capacity(window);
        let mut sizes = Vec::with_capacity(sample_count);

        for index in 0..sample_count {
            let sample_size = base_size + usize::from(index < remainder);
            let user_id = rng.random_range(0..64);
            let route = rng.random_range(0..8);
            let status = rng.random_range(0..5);
            let record = format!(
                "{{\"service\":\"tunnel-{route}\",\"user\":{user_id},\"status\":{status},\"payload\":\"rathole repetitive payload block {route} {status}\"}}\n"
            );
            let start = samples.len();
            while samples.len() - start < sample_size {
                let remaining = sample_size - (samples.len() - start);
                samples.extend_from_slice(&record.as_bytes()[..remaining.min(record.len())]);
            }
            sizes.push(sample_size);
        }

        (samples, sizes)
    }

    #[test]
    fn training_constants_succeed_for_reproducible_corpora() -> Result<()> {
        for seed in SEEDS {
            let (samples, sizes) = corpus(seed, TEST_WINDOW, MIN_SAMPLE_COUNT);
            let dictionary = train_dictionary(&samples, &sizes, TEST_MAX_DICT)?;

            anyhow::ensure!(!dictionary.is_empty(), "trained dictionary is empty");
            anyhow::ensure!(
                dictionary.len() <= TEST_MAX_DICT,
                "trained dictionary exceeds configured maximum"
            );
        }
        Ok(())
    }

    #[test]
    fn one_fewer_than_minimum_sample_count_is_rejected() -> Result<()> {
        for seed in SEEDS {
            let (samples, sizes) = corpus(seed, TEST_WINDOW, MIN_SAMPLE_COUNT - 1);
            anyhow::ensure!(
                train_dictionary(&samples, &sizes, TEST_MAX_DICT).is_err(),
                "six samples unexpectedly trained for seed {seed}"
            );
        }
        Ok(())
    }

    #[test]
    fn insufficient_samples_return_training_error() -> Result<()> {
        let (samples, sizes) = corpus(7, 300, 3);

        let error = train_dictionary(&samples, &sizes, TEST_MAX_DICT)
            .expect_err("three tiny samples must not train a dictionary");

        let message = format!("{error:#}");
        eprintln!("{message}");
        anyhow::ensure!(
            message.to_ascii_lowercase().contains("train"),
            "training error lacks context: {message}"
        );
        Ok(())
    }
}
