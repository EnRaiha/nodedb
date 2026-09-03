// SPDX-License-Identifier: BUSL-1.1

pub mod permit;
pub mod registry;

pub use permit::{ConnectionPermit, ScopedConnectionPermit};
pub use registry::{AdmissionError, AdmissionRegistry};
