// SPDX-License-Identifier: BUSL-1.1

//! Resolve-before-propose handlers for governed state-dependent KV writes.

mod apply;
mod atomic_ops;
mod context;
mod dispatch;
mod predicate_ops;
mod transfer_ops;
mod write_ops;

#[cfg(test)]
mod tests;
