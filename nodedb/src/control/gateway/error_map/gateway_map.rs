// SPDX-License-Identifier: BUSL-1.1

//! The listener-facing mapping type. Each protocol surface adds its own
//! `impl` block in its own module.

/// Maps a gateway [`crate::Error`] into each listener's error envelope.
pub struct GatewayErrorMap;
