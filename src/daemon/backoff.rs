//! Jittered exponential backoff, shared by the reconnect and retry loops.

use std::time::Duration;

/// Exponential backoff with jitter. [`Backoff::next_delay`] returns the delay
/// to wait and advances toward the ceiling; [`Backoff::reset`] returns to the
/// floor once whatever it guards has been stable.
#[derive(Debug)]
pub(crate) struct Backoff {
    min: Duration,
    max: Duration,
    current: Duration,
}

impl Backoff {
    pub(crate) fn new(min: Duration, max: Duration) -> Self {
        Backoff {
            min,
            max,
            current: min,
        }
    }

    /// Returns the next delay (jittered) and advances toward the ceiling.
    pub(crate) fn next_delay(&mut self) -> Duration {
        // Jitter by ±20% so many loops backing off together do not retry in
        // lockstep.
        let delay = self.current.mul_f64(rand::random_range(0.8..1.2));
        self.current = (self.current * 2).min(self.max);
        delay
    }

    /// Resets to the floor delay.
    pub(crate) fn reset(&mut self) {
        self.current = self.min;
    }
}
