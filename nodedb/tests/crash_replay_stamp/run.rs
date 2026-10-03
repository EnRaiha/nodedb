// SPDX-License-Identifier: BUSL-1.1

//! The boot, park, checkpoint, crash and restore sequence every engine case
//! runs.

use std::time::{Duration, Instant};

use crate::crash_harness::log_fields::{boot_section, log_field, same_value};
use crate::crash_harness::{CrashHarness, diagnostics};

use super::case::{Case, LiveApplied, Names, QUIET_CHECKPOINT_INTERVAL_SECS};
use super::truncation::{truncate_while_held, truncation_advances_once_settled, write_filler};

/// How long the test waits for a checkpoint whose stamp names a B, for
/// truncation runs, or for a segment to go: thirty checkpoint cycles at one
/// per second. A is parked until the test releases it, so this bounds only the
/// wait for the checkpoint manager.
pub(super) const CHECKPOINT_DEADLINE: Duration = Duration::from_secs(30);

/// How long the process may take to abort once A is released.
const CRASH_TIMEOUT: Duration = Duration::from_secs(60);

/// The smallest WAL segment target the config accepts, in whole MiB.
const WAL_SEGMENT_TARGET_MB: &str = "1";

pub(super) async fn run(case: Case) {
    let names = Names::of(&case);
    let mut h = CrashHarness::new()
        .with_env(
            "NODEDB_CHECKPOINT_INTERVAL_SECS",
            QUIET_CHECKPOINT_INTERVAL_SECS,
        )
        .with_env("RUST_LOG", &format!("warn,{}", case.log_directives));
    if case.wal_truncation {
        h.set_env("NODEDB_WAL_SEGMENT_TARGET_MB", WAL_SEGMENT_TARGET_MB);
    }
    h.spawn();
    h.wait_ready();
    h.exec(&names.fill(case.create_held)).await;
    h.exec(&names.fill(case.create_applied)).await;
    if case.wal_truncation {
        h.exec(&format!(
            "CREATE COLLECTION {} (k STRING PRIMARY KEY, v STRING) WITH (engine='kv')",
            names.filler
        ))
        .await;
        // Sealed segments below A, so the floor A holds lets truncation run.
        write_filler(&h, &names.filler, "below").await;
    }
    if let Some(seed) = case.seed_held {
        h.exec(&names.fill(seed)).await;
    }

    // Boot 2 arms the gate and the abort, keyed to the held collection. Both
    // match only a request carrying a WAL LSN, so boot itself passes them.
    h.kill_9();
    let release = h.data_dir().join("release-held-write");
    h.set_env(
        "NODEDB_CHECKPOINT_INTERVAL_SECS",
        case.checkpoint_interval_secs,
    );
    h.set_env(
        "NODEDB_FAILPOINTS",
        &format!(
            "funnel::before_dispatch::{held}=wait_file({}),core::after_apply::{held}=abort",
            release.display(),
            held = names.held,
        ),
    );
    h.reopen();

    let conn_str = h.pgwire_conn_str();
    let insert_held = names.fill(case.insert_held);
    let held_task = tokio::spawn(async move {
        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .map_err(|e| e.to_string())?;
        tokio::spawn(connection);
        client
            .simple_query(&insert_held)
            .await
            .map_err(|e| format!("{insert_held}: {e}"))?;
        Ok::<(), String>(())
    });

    // Apply B writes until a checkpoint names one of them above its prefix.
    // They apply in another data group than A's, so A's parked entry holds
    // none of them back.
    let deadline = Instant::now() + CHECKPOINT_DEADLINE;
    let mut applied = 0usize;
    loop {
        h.exec(&(case.insert_applied)(&names.applied, applied))
            .await;
        applied += 1;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let log = boot_section(&h.server_log(), 2);
        if log_field(&log, case.published, "applied_ranges")
            .iter()
            .any(|n| *n > 0)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "no {} named an applied LSN above its prefix within {CHECKPOINT_DEADLINE:?}: \
             write A never parked, or no checkpoint ran while it was.{}\n{}",
            case.published,
            h.keep_data_dir_note(),
            diagnostics::log_tail_section(&h.server_log())
        );
    }
    let held_segment = if case.wal_truncation {
        Some(truncate_while_held(&h, &names.filler).await)
    } else {
        None
    };
    assert!(
        !held_task.is_finished(),
        "write A finished before its release: the gate never parked it"
    );
    // No read reaches A's data group while A is parked (see `LiveApplied`).
    let mut live = match case.live_applied {
        LiveApplied::Read => h.query_col_idx(&names.fill(case.read_applied), 0).await,
        LiveApplied::Computed(rows) => rows(applied),
    };
    live.sort();

    // Release A. It applies, and the process aborts before its response
    // leaves, so no checkpoint written after it can hold it.
    std::fs::write(&release, b"release").expect("create the release file");
    h.await_self_crash(CRASH_TIMEOUT);
    let marker = format!(
        "fail_point aborting process: core::after_apply::{}",
        names.held
    );
    assert!(
        h.server_log().contains(&marker),
        "the process exited, but not after write A applied.{}\n{}",
        h.keep_data_dir_note(),
        diagnostics::log_tail_section(&h.server_log())
    );
    // A's client lost its connection with the process; its result says nothing.
    let _ = held_task.await;

    h.clear_env("NODEDB_FAILPOINTS");
    h.reopen();

    let (restored, field) = case.restored;
    let proof = log_field(&boot_section(&h.server_log(), 3), restored, field);
    assert!(
        proof.iter().any(|n| *n > 0),
        "no {restored} line has {field} above zero, so this run did not reproduce the \
         in-flight write (values: {proof:?}).{}\n{}",
        h.keep_data_dir_note(),
        diagnostics::log_tail_section(&h.server_log())
    );

    assert_restored(&h, &case, &names, &live).await;

    if let Some(segment) = held_segment {
        truncation_advances_once_settled(&mut h, &names.filler, &segment).await;
        h.kill_9();
        h.reopen();
        assert_restored(&h, &case, &names, &live).await;
    }
}

/// A's row is back, and every B write is present once.
async fn assert_restored(h: &CrashHarness, case: &Case, names: &Names, live: &[String]) {
    let held = h.query_col_idx(&names.fill(case.read_held), 0).await;
    assert!(
        held.len() == 1 && same_value(&held[0], case.held_value),
        "read {held:?}, expected [{}]: write A to {} applied after the checkpoint and \
         before the crash; replay must apply it, never skip it as covered by a higher \
         applied LSN",
        case.held_value,
        names.held
    );
    let mut replayed = h.query_col_idx(&names.fill(case.read_applied), 0).await;
    replayed.sort();
    // Two numbers compare by value, so a computed `8` equals a read `8.0`.
    assert!(
        replayed.len() == live.len()
            && replayed
                .iter()
                .zip(live)
                .all(|(read, expected)| same_value(read, expected)),
        "the replayed state of {} must equal the live state: every B write once. \
         Read {replayed:?}, expected {live:?}",
        names.applied
    );
}
