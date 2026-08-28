// SPDX-License-Identifier: BUSL-1.1

//! Grouped test target for cluster tests that need neither the
//! `cluster_common` nor the `common` shared harness — each case builds its
//! own fixtures directly against `nodedb-cluster` / `nodedb-raft` types.
//!
//! Cargo compiles this whole directory into ONE test binary rather than one
//! per file.

mod cases;
