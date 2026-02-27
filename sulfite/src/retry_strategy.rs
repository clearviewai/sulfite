//! Retry strategy configuration for S3 operations.
//!
//! The library uses these configs internally with a retry implementation;
//! no external retry crate types are exposed in the public API.

use std::time::Duration;
use tokio_retry::strategy::{jitter, ExponentialBackoff, FibonacciBackoff, FixedInterval};

/// Configuration for how failed operations are retried.
///
/// Only configuration is exposed; the library maps these to an internal
/// delay iterator. Use [`RetryStrategy::default`] for exponential backoff
/// (100 ms initial, factor 2, max 10 s).
#[derive(Clone, Debug)]
pub enum RetryStrategy {
    /// Exponential backoff: delay = min(initial * factor^attempt, max_delay).
    ExponentialBackoff {
        /// Initial delay in milliseconds.
        initial_millis: u64,
        /// Multiplier per attempt.
        factor: u64,
        /// Cap in seconds.
        max_delay_secs: u64,
    },
    /// Fibonacci backoff: delays follow the Fibonacci sequence (each delay is sum of previous two).
    FibonacciBackoff {
        /// Initial delay in milliseconds.
        initial_millis: u64,
        /// Multiplier per attempt.
        factor: u64,
        /// Cap in seconds.
        max_delay_secs: u64,
    },
    /// Fixed delay between every retry.
    FixedInterval {
        /// Delay in milliseconds between attempts.
        interval_millis: u64,
    },
}

impl Default for RetryStrategy {
    fn default() -> Self {
        RetryStrategy::ExponentialBackoff {
            initial_millis: 100, // 100ms before first retry
            factor: 2,           // classic exponential: 100ms, 200ms, 400ms, 800ms, …
            max_delay_secs: 10,
        }
    }
}

impl RetryStrategy {
    /// Build the delay iterator used for retries (jitter + take). Called internally only.
    pub(crate) fn delay_iterator_with_jitter(
        self,
        max_retries: usize,
    ) -> impl Iterator<Item = Duration> + Clone {
        self.into_delay_iterator().map(jitter).take(max_retries)
    }

    fn into_delay_iterator(self) -> RetryDelayIterator {
        match self {
            RetryStrategy::ExponentialBackoff {
                initial_millis,
                factor,
                max_delay_secs,
            } => RetryDelayIterator::ExponentialBackoff(
                ExponentialBackoff::from_millis(initial_millis)
                    .factor(factor)
                    .max_delay(Duration::from_secs(max_delay_secs)),
            ),
            RetryStrategy::FibonacciBackoff {
                initial_millis,
                factor,
                max_delay_secs,
            } => RetryDelayIterator::FibonacciBackoff(
                FibonacciBackoff::from_millis(initial_millis)
                    .factor(factor)
                    .max_delay(Duration::from_secs(max_delay_secs)),
            ),
            RetryStrategy::FixedInterval { interval_millis } => {
                RetryDelayIterator::FixedInterval(FixedInterval::from_millis(interval_millis))
            }
        }
    }
}

/// Internal: wraps tokio_retry strategies so they are not in the public API.
#[derive(Clone)]
enum RetryDelayIterator {
    ExponentialBackoff(ExponentialBackoff),
    FibonacciBackoff(FibonacciBackoff),
    FixedInterval(FixedInterval),
}

impl Iterator for RetryDelayIterator {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            RetryDelayIterator::ExponentialBackoff(s) => s.next(),
            RetryDelayIterator::FibonacciBackoff(s) => s.next(),
            RetryDelayIterator::FixedInterval(s) => s.next(),
        }
    }
}
