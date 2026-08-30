// SPDX-License-Identifier: BUSL-1.1

//! Trigger batch processing configuration.

/// Configuration for trigger batch processing.
pub struct BatchConfig {
    /// Maximum rows per trigger batch (default 1024).
    pub batch_size: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self { batch_size: 1024 }
    }
}
