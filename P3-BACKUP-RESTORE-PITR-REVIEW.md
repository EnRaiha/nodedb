# NodeDB P3 Backup/Restore/PITR — DeepSeek V4 Pro Review

Scope: repo `NodeDB-Lab/nodedb` @ `461c3ad`, review based on cited code digests and file/line evidence. Findings not visible in cited code are marked **UNCONFIRMED**.  

---

## PITR-01 Segment filename parser rejects real WAL filenames — PARTIAL (parser confirmed; writer format unconfirmed) — High

**Evidence**  
`nodedb/src/wal/archiver.rs::parse_segment_filename` requires exactly three parts:

```rust
if parts.len() != 3 || parts[0] != "wal" { return None; }
```

The parser accepts only `wal-{first}-{last}.seg`. The audit claims real files are written as `wal-{lsn}.seg` (single LSN). The parser side is confirmed in code; the writer filename format is not visible in the cited digest but is plausible and must be verified.

If the writer emits `wal-{lsn}.seg`, the archiver cannot parse/restore real WAL segments. That makes PITR inert and prevents archive/restore from working.

**Severity**: **High** — PITR/restore cannot consume real WAL archives.

**Concrete fix design**

File: `nodedb/src/wal/archiver.rs`  
Function: `parse_segment_filename`

1. Accept both legacy three-part and current single-LSN filenames:

```rust
fn parse_segment_filename(name: &str) -> Option<SegmentInfo> {
    let stem = name.strip_suffix(".seg")?;
    let parts: Vec<&str> = stem.split('-').collect();

    match parts.as_slice() {
        // Current writer format: wal-{lsn}.seg
        ["wal", lsn] => {
            let first = parse_hex_lsn(lsn)?;
            Some(SegmentInfo { first_lsn: first, last_lsn: first })
        }
        // Legacy/canonical format: wal-{first}-{last}.seg
        ["wal", first, last] => {
            Some(SegmentInfo {
                first_lsn: parse_hex_lsn(first)?,
                last_lsn:  parse_hex_lsn(last)?,
            })
        }
        _ => None,
    }
}
```

2. Normalize the writer so new files are emitted in a consistent format. The parser above should remain backward-compatible, but new segment writes should use `wal-{first_lsn}-{last_lsn}.seg` so the range is explicit. This avoids ambiguity for segments containing multiple LSNs.

3. Add a helper `parse_hex_lsn` that validates hex encoding and length; fail closed on invalid input.

**Test design**

Unit tests in `archiver.rs`:

- `wal-00000001.seg` → `first_lsn == last_lsn == 1`
- `wal-00000001-00000005.seg` → `first=1, last=5`
- `wal-00000001-,seg`, `wal-`, `foo-00000001.seg`, `wal-xyz.seg` → `None`
- Filename without `.seg` → `None`
- Ensure parse output round-trips with archive listing.

---

## PITR-02 `resolve_pitr` exists but has no production callers; PITR is not wired — PARTIAL (exists confirmed; no caller in cited grep) — High

**Evidence**  
`nodedb/src/storage/snapshot.rs:147-174` defines `resolve_pitr`; `resolve_pitr_utc` at line 229. The code computes a target LSN and a base snapshot. However, the cited callers for `snapshot_executor.rs` are:

- `execute_restore_tenant_snapshot`  
- Handler in `data/executor/handlers/snapshot/restore/tenant_snapshot.rs:20,442`

These are tenant snapshot restore paths, **not** base+archive→LSN WAL replay. No caller of `resolve_pitr` or `resolve_pitr_utc` appears in the cited code.  

If no production path invokes `resolve_pitr`, then PITR is scaffolding only. Worse, even if called, `resolve_pitr` only selects a base snapshot and target LSN; the evidence does not show a WAL replay engine being invoked. Without replay, PITR restore would return only the base state, not the requested point-in-time state.

**Severity**: **High** — PITR feature is inert or incomplete.

**Fix design**

- Add a new executor path, for example `execute_restore_pitr` in `nodedb/src/storage/snapshot_executor.rs`.
- Wire it through a handler in `data/executor/handlers/snapshot/restore/` that:
  1. Resolves `PitrTarget` via `resolve_pitr_utc`.
  2. Restores the selected base snapshot using the existing snapshot restore path.
  3. Lists archived WAL segments between base snapshot LSN and target LSN.
  4. Replays those WAL segments in order using the WAL replay/apply function.
  5. Verifies replay reaches exactly the target LSN and fails otherwise.
- If no WAL replay function exists in the codebase, this must be implemented before PITR can be considered functional. Search for `replay`, `apply_wal`, `wal_iterator` and wire them.
- Add a feature gate or explicit error: if PITR is requested but replay is not available, return an “unsupported” error rather than silently returning a stale base.

**Test design**

Integration test:

1. Write dataset `A` at T0, checkpoint.
2. Write dataset `B` at T1, archive WAL.
3. Write dataset `C` at T2 (not to be included).
4. Request PITR restore to T1.
5. Assert restored database equals dataset `A + B`, not `C`.

Unit tests for `resolve_pitr` / `resolve_pitr_utc`:

- Timestamp before first snapshot → error or earliest base.
- Timestamp between snapshots → correct base and target LSN.
- Timestamp after last WAL → error or clamp policy.

---

## PITR-03 Checkpoint/upload failure only `warn!` leading to WAL truncation and archive holes — PARTIAL (warn sites confirmed; exact truncation flow partly inferred) — High

**Evidence**  
`nodedb/src/control/checkpoint_manager.rs` lines 190, 204, 223, 227, 236 contain `warn!` on upload/checkpoint failures. Line 26 comment states `WalManager::truncate_before()` deletes old WAL segments **after** checkpoint.

The audit claim is that an upload failure is only logged with `warn!`, but the truncation logic still proceeds, deleting WAL segments that were never durably archived. This creates a permanent hole in the archive stream and makes PITR impossible across that boundary.

The `warn!` sites are confirmed in the cited code. The precise control flow should be inspected to confirm that truncation is not gated by successful archive/upload, but the comment and warn-only sites strongly suggest the unsafe ordering.

**Severity**: **High** — data loss for backup/PITR archive.

**Fix design**

File: `nodedb/src/control/checkpoint_manager.rs`

1. Replace `warn!` on upload/archive failure with `error!` and return a `Result` from the upload step.
2. Gate WAL truncation on durable upload completion. For example:

```rust
let upload_result = upload_checkpoint(checkpoint_id, &checkpoint_path)?;
if upload_result.is_err() {
    error!(...);
    return Err(...);  // do NOT truncate
}
// Only after successful upload:
WalManager::truncate_before(checkpoint_lsn)?;
```

3. Maintain explicit state between upload and truncate:
   - `CheckpointUploadState::PendingUpload { checkpoint_lsn }`
   - `CheckpointUploadState::Uploaded { checkpoint_lsn }`
   - `truncate_before` must accept/verify that all WAL segments up to the LSN are uploaded/durable.

4. Update the line 26 comment: truncation must happen **only after successful durable upload**, not merely after checkpoint.

5. Add metrics/alerts for upload failures and retry/backoff.

**Test design**

- Mock upload failure → assert `truncate_before` is not called.
- Assert failed upload leaves WAL segments present.
- Simulate retry after upload success → assert truncate succeeds and removes only the expected segments.
- Kill process between upload and truncate; on restart assert no truncation before upload state is confirmed.

---

## PITR-04 Restore staleness / lock-poisoning bypass in `restore/mod.rs:80-97` — UNCONFIRMED — Medium (if confirmed)

**Evidence**  
Cited as `nodedb/src/control/backup/restore/mod.rs:80-97`, but the code contents are not included in this digest. The audit says lock poisoning is bypassed.

If restore code catches `PoisonError` and proceeds without checking snapshot/state integrity, it may restore from a partially mutated or inconsistent state after a panic. Some poisoning bypasses are deliberate to allow recovery from a failed restore, but they can also mask torn writes.

**Severity**: **Medium** if confirmed; may be intentional with adequate safeguards.

**Fix design**

- If lock poisoning is bypassed, add a pre-restore safety check:
  - Verify snapshot manifest and checksums.
  - Run a consistency check on the target before allowing restore.
  - Require an explicit `force` flag for bypassing poisoning.
- In `restore/mod.rs`, distinguish:
  - `PoisonError` from a read-only snapshot lock → safe after checks.
  - `PoisonError` from a write/checkpoint lock → do not proceed unless integrity is verified.
- Log the original panic and the bypass reason.

**Test design**

- Simulate panic while holding the relevant lock, poison it, then attempt restore.
- Assert restore fails without `force`.
- Assert restore succeeds with `force` after consistency check.
- Assert no silent corruption in the restored state.

---

## MED-01 Hand-rolled UTC math in `snapshot_restore.rs:101-172` — UNCONFIRMED — Medium (if confirmed)

**Evidence**  
Cited file/line not included in this digest. The audit claims hand-rolled UTC arithmetic. Without the code, exact correctness cannot be verified.

If the code manually computes leap years, epoch offsets, or timezone conversions, it is likely to have edge-case bugs around pre-epoch timestamps, leap seconds, and DST boundaries.

**Severity**: **Medium** if present.

**Fix design**

File: `nodedb/src/.../snapshot_restore.rs` around lines 101-172.

- Replace hand-rolled arithmetic with `chrono` or `time`:
  - Use `DateTime::from_timestamp` / `Utc.timestamp_opt`.
  - Validate `LocalResult::Single`; reject ambiguous/nonexistent instants.
  - Avoid manual `86400` seconds/day for date math; use calendar-aware operations.
- Ensure the same UTC conversion is used consistently in snapshot and PITR paths.

**Test design**

- Parametrized tests around Unix epoch boundaries: `1969-12-31T23:59:59Z`, `1970-01-01T00:00:00Z`, pre-epoch negative timestamps.
- Leap-second-adjacent timestamps.
- Timestamps far past year 2038 and before 1901.
- Assert mapping is monotonic and invertible where expected.

---

## MED-02 No restore verification (row-count/checksum reconciliation) — VERIFIED as missing — Medium

**Evidence**  
No code in the cited digest performs row-count checks, table checksums, or reconciliation after restore. Restore paths only appear to copy/apply data. This is a missing verification feature.

**Severity**: **Medium** — restored data may be corrupt or incomplete without detection.

**Fix design**

Add a post-restore verification step, ideally in `nodedb/src/storage/snapshot_executor.rs` or a new `restore_verification` module:

- During snapshot or WAL archive, store per-table metadata: row count, size, checksum (e.g. xxHash or SHA-256) in the manifest.
- After restore, recompute metadata and compare against the manifest.
- If verification fails, return an error and do not mark the restore as successful.
- For PITR, verify tables expected at the target LSN, not just base snapshot.

**Test design**

- Restore a known dataset; verify row count and checksum pass.
- Corrupt a snapshot file or truncate a table; verify restore fails.
- Ensure verification failure produces a clear error and does not silently expose partial data.

---

## MED-03 Backup full-snapshot only; no incremental/scheduling/retention — VERIFIED as missing — Medium (deferrable)

**Evidence**  
No scheduler, retention policy, incremental backup, or backup lifecycle code appears in the cited digest. Only full-snapshot restore paths are present.

**Severity**: **Medium** — acceptable for initial P3 if only full-snapshot restore is scoped, but not production-grade.

**Fix design**

- Add a scheduler in `nodedb/src/control/backup/` with configurable interval and retention policy.
- Implement retention: keep N full snapshots, delete older ones after successful upload.
- If incremental backups are desired, track WAL segments since the last snapshot and include them in the backup manifest as an incremental delta.
- Store backup manifest with `backup_type: Full | Incremental`, `base_snapshot_id`, and `wal_range`.

**Test design**

- Configure scheduler interval; trigger backup; assert snapshot created.
- Advance time; assert retention policy deletes old snapshots beyond N.
- For incremental, assert only new WAL segments are uploaded after full snapshot.
- Assert backup manifest correctly references base and delta.

---

# Cross-cutting findings

1. **PITR is not production-ready.** Three high-severity gaps are present or strongly indicated:
   - Segment filename parser likely rejects real WAL files.
   - `resolve_pitr` has no production caller and no visible WAL replay path.
   - Checkpoint upload failure appears to allow WAL truncation, creating archive holes.

2. **Restore verification is absent.** Even full-snapshot restore has no row-count or checksum confirmation. This should be added before production restore use.

3. **Backup lifecycle is incomplete.** No scheduling, retention, or incremental support. This can be deferred if v0.7 only requires manual full snapshot restore.

4. **Unified WAL naming and durable-upload ordering are required.** These are cross-cutting invariants: archived WAL names must be parseable by all components, and truncation must never outrun durable archival.

---

# P3 Backup/Restore/PITR ship decision

**BLOCKED for v0.7 if PITR is in scope.**

Blocking items:

- **PITR-01** — parser mismatch must be fixed and covered by tests.
- **PITR-02** — PITR must be wired end-to-end, including WAL replay; if WAL replay does not exist, this is a blocker and must be implemented or PITR must be explicitly removed from the v0.7 scope.
- **PITR-03** — upload failure must prevent WAL truncation; truncate only after durable upload.

Deferrable items (can move to v0.8 if scope is strictly full-snapshot restore):

- **MED-02** restore verification — recommend including basic row-count verification in v0.7, but not a hard blocker if PITR is deferred.
- **MED-03** incremental scheduling/retention.
- **MED-01** UTC math — investigate and fix if confirmed.

**UNCONFIRMED** items:

- **PITR-04** lock-poisoning bypass — inspect `restore/mod.rs:80-97`; if it bypasses poisoning without integrity checks, promote to blocking or at least high-priority fix.

---

**Bottom line**: Do not ship P3 with PITR enabled until PITR-01, PITR-02, and PITR-03 are fixed and covered by integration tests. Full-snapshot restore alone may be shippable if PITR is removed from the v0.7 deliverable, but restore verification should be added as a fast-follow.
