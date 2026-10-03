// SPDX-License-Identifier: Apache-2.0

//! Emits `NODEDB_WIRE_BUILD_ID`: the short git commit hash, or
//! `CARGO_PKG_VERSION` when git is unavailable (a crates.io build).
//!
//! Commit only — never the dirty-tree state, which would force a rebuild of
//! every dependent crate on every uncommitted edit.

use std::process::Command;

fn main() {
    let build_id =
        git_commit().unwrap_or_else(|| std::env::var("CARGO_PKG_VERSION").unwrap_or_default());
    println!("cargo:rustc-env=NODEDB_WIRE_BUILD_ID={build_id}");

    for path in git_head_ref_paths() {
        println!("cargo:rerun-if-changed={path}");
    }
}

/// Short hash of the current commit, or `None` when git is unavailable.
fn git_commit() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let commit = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    if commit.is_empty() {
        None
    } else {
        Some(commit)
    }
}

/// Paths, resolved via `git`, whose mtime tracks the current commit: the
/// git-dir `HEAD` file, and — when `HEAD` is a symbolic ref — the ref file
/// it points at. Empty when git is unavailable.
fn git_head_ref_paths() -> Vec<String> {
    let Some(git_dir) = git_dir() else {
        return Vec::new();
    };
    let mut paths = vec![format!("{git_dir}/HEAD")];
    if let Some(head_ref) = symbolic_ref() {
        paths.push(format!("{git_dir}/{head_ref}"));
    }
    paths
}

fn git_dir() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--git-dir"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let dir = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    if dir.is_empty() { None } else { Some(dir) }
}

/// The ref `HEAD` points at (e.g. `refs/heads/main`), or `None` on a
/// detached `HEAD`.
fn symbolic_ref() -> Option<String> {
    let output = Command::new("git")
        .args(["symbolic-ref", "-q", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let r = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    if r.is_empty() { None } else { Some(r) }
}
