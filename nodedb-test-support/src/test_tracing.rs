// SPDX-License-Identifier: BUSL-1.1

//! Opt-in tracing for in-process test servers.

/// Install a tracing subscriber once per test process, when `RUST_LOG` is set.
///
/// Harness-spawned servers run in the test process, so without a subscriber
/// every server-side line — a stalled applier, a refused conf-change, a raft
/// election that never settles — is discarded, and a failure surfaces only as
/// whatever the test's own assertion happened to print. The harness installs
/// this itself so no test has to remember to; the one test that forgets is the
/// one being debugged.
///
/// A no-op unless `RUST_LOG` is set, so normal runs stay silent and fast.
pub fn init() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("RUST_LOG").is_some() {
            // `try_init` rather than `init`: several harnesses spawn servers in
            // the same process, and a global subscriber may already be set.
            let _ = tracing_subscriber::fmt()
                .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                .with_test_writer()
                .try_init();
        }
    });
}
