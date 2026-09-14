use backoff::ExponentialBackoff;
use std::time::Duration;

// FIXME: Determine reasonable size
/// UDP MTU. Currently far larger than necessary
pub const UDP_BUFFER_SIZE: usize = 2048;
pub const UDP_SENDQ_SIZE: usize = 1024;
pub const UDP_TIMEOUT: u64 = 60;

/// zstd compression level used when `compression = "zstd"` and
/// `compression_level` is omitted. Also the level every client encoder uses.
pub const DEFAULT_ZSTD_LEVEL: i32 = 9;
/// Lowest accepted `compression_level`.
pub const MIN_ZSTD_LEVEL: i32 = 1;
/// Highest honored `compression_level`. Anything above logs a warning and falls back
/// to `DEFAULT_ZSTD_LEVEL`.
pub const MAX_ZSTD_LEVEL: i32 = 19;

pub fn listen_backoff() -> ExponentialBackoff {
    ExponentialBackoff {
        max_elapsed_time: None,
        max_interval: Duration::from_secs(1),
        ..Default::default()
    }
}

pub fn run_control_chan_backoff(interval: u64) -> ExponentialBackoff {
    ExponentialBackoff {
        randomization_factor: 0.2,
        max_elapsed_time: None,
        multiplier: 3.0,
        max_interval: Duration::from_secs(interval),
        ..Default::default()
    }
}
