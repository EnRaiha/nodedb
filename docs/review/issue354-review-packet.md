# Review packet: startup names the WAL format version it found

Self-contained. A reviewer with no context can read this file, the commits it names, and the logs in
`logs/`, and judge the work.

## The problem

A store written by another build failed every boot with this:

```
StartupError: WAL validation failed — cannot start with corrupted WAL segments
  error=WAL segment '…/wal-00000000000000710217.seg' is non-empty (59932672 bytes) but contains no
        valid WAL records — the segment appears to be corrupted
```

The store was not corrupted. It was written in WAL format version 1 and this build reads version 3.
Nothing in the message said so, and its reader went looking for damage. There is no migration path
either, so the operator's real problem, "this store needs an upgrade", was invisible.

It says this now:

```
StartupError: WAL validation failed — cannot start with these WAL segments
  error=version compatibility: WAL segment '…/wal-00000000000000710217.seg' holds records in WAL
        format version 1; this build requires 3. The store needs a migration, not a repair.
```

The node still refuses to start. Only the diagnosis changed. The store stays unreadable by this build,
which is the separate question of a migration path, and that question is not in this change.

## The root cause, and why the first attempt was wrong

`WalError::UnsupportedVersion { version, supported }` already existed and the record header already
returned it. `nodedb-wal/src/reader.rs` then flattened it, along with every other header validation
failure, into `StopReason::Corruption`. The reader knew the version and threw it away, which is why the
gate saw an empty tail and reported corruption.

The first attempt at this change added a new error variant, `WalFormatUnsupported`, and read the version
from the segment in the gate. That worked, and it cost fifteen classification edits across the error
machinery, growth in a file already over the repository's size limit, and a preflight bypass. A review
found the root cause above, and the change was rebuilt on it.

The final change is three files:

| File | Change |
| --- | --- |
| `nodedb-wal/src/reader.rs` | Return `UnsupportedVersion` instead of flattening it into `StopReason::Corruption` |
| `nodedb/src/wal/manager/replay.rs` | Catch it at the gate and report `Error::VersionCompat` with the segment, the version found, the version required, and the right advice for the direction of the gap |
| `nodedb/src/bootstrap/wal_init.rs` | The wrapper log line above the error is neutral, because it wraps every validation failure |

## Design decisions a reviewer should check

| Decision | Why |
| --- | --- |
| The reader returns the error rather than a stop reason | The reason travels with its values instead of being re-derived from the file afterwards. It also removes the six-byte re-read the first attempt needed, and with it the preamble case that re-read got wrong |
| `Error::VersionCompat` rather than a new variant | It already exists and carries a detail string, so the message keeps the segment path and the direction of the gap without touching the classification machinery |
| Version zero takes the empty-tail path | Zero is uninitialised bytes, not a format any build wrote. A torn write that persists the four magic bytes over a zeroed tail leaves exactly that, and refusing it would refuse a store that used to boot |
| The preallocated-tail tolerance is kept, and the version check no longer sits in front of it | The reader reports the version before the gate looks at `end_offset`, so the tolerance only ever sees a segment whose records this build can read |
| The advice depends on the direction | An older store needs a migration. A newer store needs a newer binary, and telling that operator to migrate would be wrong |

## Evidence

| Arm | Command | Exit | Commit | Log |
| --- | --- | --- | --- | --- |
| red | `cargo nextest run -p nodedb --lib -E 'test(a_store_written_in_an_older_format_says_which_version)'` | **100** | `e01612439` | `logs/red-base.log` |
| green | `cargo nextest run -p nodedb --lib -E 'test(/wal::manager::replay/) or test(/class_parity/)'` | **0** | `afe1c835e` | `logs/green-afe1c835e.log` |
| mutation A | the same filter, with the reader's version error removed | **100** | `afe1c835e` | `logs/mutation-A-reader-surfacing.log` |
| mutation B | the same filter, with the version-zero guard removed | **100** | `afe1c835e` | `logs/mutation-B-zeroed-guard.log` |
| end to end | a real production copy and a real version-3 store, release binary | **0** | `435fb0b42` | `logs/e2e-version-message.log` |
| full suite | `cargo nextest run -p nodedb --lib` | **0** | `d23e47984` | `logs/full-suite.log` |

What each arm proves:

- **Red** fails against the old wording, so the test is about the bug and not about the change.
- **Green** passes the replay module and the error-class parity module.
- **Mutation A** removes the reader's version error, and exactly the version-message tests fail. The four guards keep passing, so they are guards and not proofs, which is what they are for.
- **Mutation B** removes the version-zero guard, and exactly `a_zeroed_version_is_not_a_format_gap` fails.
- **End to end** refuses a real version-1 production copy with the message above, the word "corrupted" appears zero times in its log, and a real version-3 store reaches `ready=1`. This run is what found the wrapper line above the error still saying "cannot start with corrupted WAL segments", which no unit test could see.
- **Full suite** is 8,784 tests, three more than before this work, which is the three tests this change adds.

Mutation A and B were each run with only that mutation installed. An earlier B run still had A applied,
which masked B's effect; it was discarded and re-run.

## The tests

| Test | What it pins |
| --- | --- |
| `a_store_written_in_an_older_format_says_which_version` | A plaintext store in version 1 is refused, naming the version found, the version required, the segment, and not the word "corrupted" |
| `a_store_with_a_preamble_says_which_version` | The same behind an encrypted store's preamble, which is where the first attempt's offset assumption broke |
| `a_zeroed_version_is_not_a_format_gap` | A torn write that persists the magic and leaves the version zeroed is not a format gap, and does not refuse a store that used to boot |
| `a_preallocated_newest_segment_does_not_block_startup` | A record-less newest segment stays allowed, which is the tolerance this repository already had |
| `a_segment_with_no_records_behind_the_newest_still_blocks_startup` | A record-less segment that is not the tail stays fatal |
| `a_segment_that_is_not_a_wal_still_says_corrupted` | Bytes with no WAL framing are still corruption |

## Questions for the reviewer

1. Is surfacing `UnsupportedVersion` from the reader the right layer, given the writer paths that also call recovery?
2. Version zero is treated as uninitialised bytes. Is that the right line, or should a zeroed version be reported as damage?
3. The advice in the message depends on whether the found version is below or above the required one. Is the wording right for a store from a **newer** build?
4. Do the six tests pin behaviour, or the current wording?

## What is not in this change

| Item | Status |
| --- | --- |
| A migration or dump path for an older store | Not attempted. The store stays unreadable, and the message now says so |
| A total boot deadline | Designed, not implemented. `StartupTuning` carries the two bounds this branch makes configurable |
| The redundant per-vshard WAL replay, the measured five-fold win | Separate work, planned in `plan-next-work.md` |
