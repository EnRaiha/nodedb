// SPDX-License-Identifier: BUSL-1.1

//! Env-driven crash-harness diagnostics: how much of the server log a
//! failure panic prints, whether the data directory survives a failed test
//! for post-mortem inspection, and marking which boot each log line
//! belongs to.
//!
//! Split out of `mod.rs` because this is a distinct concern from process
//! lifecycle — shaping what a test failure prints and what it leaves
//! behind, not spawning/killing the child process itself.

use std::io::Write as _;
use std::path::Path;
use std::sync::OnceLock;

/// Subdirectory of the data directory the server writes faultbox reports
/// under. Mirrors `nodedb::bootstrap::diagnostics::REPORTS_SUBDIR`, which is
/// private to the binary crate — this is the test harness's own copy of the
/// same layout decision, not a shared constant, since the harness reads the
/// child process's output rather than linking it.
const REPORTS_SUBDIR: &str = "diagnostics";

/// Default tail length when `NODEDB_TEST_LOG_TAIL_LINES` is unset.
///
/// The failure this harness exists to diagnose spans two server boots
/// (crash + `reopen`) in one accumulated log file, and the interesting
/// warning is often emitted well before the fatal line on the second boot.
/// 400 lines is enough to carry it; the old fixed 60-line tail routinely
/// showed nothing but the tail end of `reopen` and dropped the evidence.
const DEFAULT_TAIL_LINES: usize = 400;

fn env_truthy(value: &str) -> bool {
    !matches!(value.trim(), "" | "0" | "false" | "FALSE" | "False")
}

/// Whether `NODEDB_TEST_KEEP_DATA_DIR` is set truthy for this process.
///
/// Read once and cached: the env is fixed for the lifetime of the test
/// binary, and every `CrashHarness` drop needs the answer.
pub fn keep_data_dir_requested() -> bool {
    static KEEP: OnceLock<bool> = OnceLock::new();
    *KEEP.get_or_init(|| {
        std::env::var("NODEDB_TEST_KEEP_DATA_DIR")
            .map(|v| env_truthy(&v))
            .unwrap_or(false)
    })
}

/// Number of trailing log lines a failure panic includes, from
/// `NODEDB_TEST_LOG_TAIL_LINES`, defaulting to [`DEFAULT_TAIL_LINES`].
///
/// Read once and cached, same rationale as [`keep_data_dir_requested`].
pub fn tail_line_count() -> usize {
    static LINES: OnceLock<usize> = OnceLock::new();
    *LINES.get_or_init(|| {
        std::env::var("NODEDB_TEST_LOG_TAIL_LINES")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_TAIL_LINES)
    })
}

/// Format the trailing lines of `log` with the header a panic message
/// expects, e.g. `--- server log (last 400 lines) ---`, sized by
/// [`tail_line_count`].
pub fn log_tail_section(log: &str) -> String {
    let n = tail_line_count();
    let lines: Vec<&str> = log.lines().collect();
    let tail = if lines.len() <= n {
        lines.join("\n")
    } else {
        lines[lines.len() - n..].join("\n")
    };
    format!("--- server log (last {n} lines) ---\n{tail}")
}

/// A note appended to diagnostic panics stating where the data directory is
/// retained, or empty when `NODEDB_TEST_KEEP_DATA_DIR` is unset.
///
/// Put directly in the panic text (not just printed on drop) so the path is
/// visible in the failure a human actually reads, rather than requiring them
/// to scroll past it to whatever `Drop` prints afterward.
pub fn keep_data_dir_note(data_dir: &Path) -> String {
    if keep_data_dir_requested() {
        format!(
            "\ndata dir retained (NODEDB_TEST_KEEP_DATA_DIR): {}\n",
            data_dir.display()
        )
    } else {
        String::new()
    }
}

/// List the faultbox reports the server filed under `<data_dir>/diagnostics`,
/// most recently seen first.
///
/// The server records some failures — a wedged Raft applier, a Calvin
/// completion timeout — as structured reports rather than only a log line,
/// specifically because they are hard to root-cause from the line alone. A
/// crash-harness test failure is exactly the moment that context matters, so
/// tests read it back the same way an operator would: through the public
/// `faultbox::reader` API, not by re-deriving the on-disk layout. An empty
/// or missing reports directory yields an empty list, not an error — most
/// test failures never touch a capture site at all.
pub fn faultbox_reports(data_dir: &Path) -> Vec<faultbox::reader::Group> {
    faultbox::reader::list(data_dir.join(REPORTS_SUBDIR)).unwrap_or_default()
}

/// A compact triage section for a failure panic, listing every faultbox
/// report the server filed, or empty when none were.
///
/// Formatted like [`log_tail_section`] so the two can be concatenated in a
/// panic message: the log tail shows what the server was doing, this shows
/// what it already diagnosed about itself.
pub fn faultbox_report_section(data_dir: &Path) -> String {
    let groups = faultbox_reports(data_dir);
    if groups.is_empty() {
        return String::new();
    }
    let lines: Vec<String> = groups
        .iter()
        .map(faultbox::reader::Group::summary)
        .collect();
    format!(
        "--- faultbox reports ({} occurrence(s) across {} group(s)) ---\n{}\n",
        faultbox::reader::total_occurrences(&groups),
        groups.len(),
        lines.join("\n")
    )
}

/// Append a boot delimiter to the harness's accumulated server log.
///
/// `spawn()` and `reopen()` both write into the same log file across the
/// lifetime of a `CrashHarness`, so a tail spanning a crash + reopen can
/// straddle both boots with no marker showing where one ends and the next
/// begins. Called right after the child is spawned, while its pid is known
/// and before it has had time to write its own first log line.
pub fn mark_boot(log_path: &Path, boot_ordinal: u32, pid: u32) {
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(log_path)
        .expect("open server log for boot marker");
    writeln!(
        f,
        "\n=== crash harness boot {boot_ordinal} (pid {pid}) ===\n"
    )
    .expect("write boot marker to server log");
}
