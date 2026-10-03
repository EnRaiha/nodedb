// SPDX-License-Identifier: BUSL-1.1

//! Poll a predicate with a deadline. Same shape as
//! `nodedb-cluster/tests/common/mod.rs::wait_for`, copied rather than
//! shared because test harnesses cross crate boundaries.

use std::future::Future;
use std::time::{Duration, Instant};

pub async fn wait_for<F: FnMut() -> bool>(
    desc: &str,
    deadline: Duration,
    step: Duration,
    mut pred: F,
) {
    let start = Instant::now();
    while start.elapsed() < deadline {
        if pred() {
            return;
        }
        tokio::time::sleep(step).await;
    }
    panic!("timed out after {:?} waiting for: {}", deadline, desc);
}

/// [`wait_for`] with a check that says why it does not hold yet. The
/// timeout message carries the last reason.
pub async fn wait_for_report<F: FnMut() -> Result<(), String>>(
    desc: &str,
    deadline: Duration,
    step: Duration,
    mut check: F,
) {
    let start = Instant::now();
    let mut last = String::new();
    while start.elapsed() < deadline {
        match check() {
            Ok(()) => return,
            Err(reason) => last = reason,
        }
        tokio::time::sleep(step).await;
    }
    panic!("timed out after {deadline:?} waiting for: {desc}; last check: {last}");
}

/// Async predicate variant of [`wait_for`]. Awaits the future returned
/// by `pred` directly so callers don't need `block_in_place` /
/// `Handle::block_on` gymnastics inside an async context.
pub async fn wait_for_async<F, Fut>(desc: &str, deadline: Duration, step: Duration, mut pred: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = Instant::now();
    while start.elapsed() < deadline {
        if pred().await {
            return;
        }
        tokio::time::sleep(step).await;
    }
    panic!("timed out after {:?} waiting for: {}", deadline, desc);
}
