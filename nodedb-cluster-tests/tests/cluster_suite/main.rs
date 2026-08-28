// SPDX-License-Identifier: BUSL-1.1

//! Grouped test target for cluster tests that bring up nodes via the raw
//! `cluster_common` in-process harness (`TestNode`/`CalvinTestNode`, no
//! pgwire client). Cargo compiles this whole directory into ONE test
//! binary rather than one per file, so the shared dependency closure
//! links once instead of once per case.

mod cases;
