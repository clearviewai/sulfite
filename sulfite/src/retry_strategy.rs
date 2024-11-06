use std::time::Duration;
pub use tokio_retry::strategy::{ExponentialBackoff, FibonacciBackoff, FixedInterval};

#[derive(Debug, Clone)]
pub enum RetryStrategy {
    ExponentialBackoff(ExponentialBackoff),
    FibonacciBackoff(FibonacciBackoff),
    FixedInterval(FixedInterval),
}

impl Iterator for RetryStrategy {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            RetryStrategy::ExponentialBackoff(eb) => eb.next(),
            RetryStrategy::FibonacciBackoff(fb) => fb.next(),
            RetryStrategy::FixedInterval(fi) => fi.next(),
        }
    }
}
