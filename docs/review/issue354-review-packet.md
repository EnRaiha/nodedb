# Review packet: a store from another build now says which version

Self-contained. A reviewer with no session context can read this file, the five commits it names, and
judge the work.

## What the change is

NodeDB refuses to start when a WAL segment holds records in a format version it cannot read. It used
to say this:

```
StartupError: WAL validation failed — cannot start with corrupted WAL segments
  error=WAL segment '…/wal-00000000000000710217.seg' is non-empty (59932672 bytes) but contains no
        valid WAL records — the segment appears to be corrupted
```

The store was not corrupted. It was written by an older build, in WAL format version 1, and this build
reads version 3. Nothing in the message said so, and the reader went looking for damage. There is no
migration path either, so the operator's real problem, "this store needs an upgrade", was invisible.

It says this now:

```
StartupError: WAL validation failed — cannot start with these WAL segments
  error=WAL segment '…/wal-00000000000000710217.seg' holds records in WAL format version 1;
        this build requires 3. The store needs a migration, not a repair.
```

The behaviour is unchanged: the node still refuses to start. Only the diagnosis changed. The store
remains unreadable by this build, which is the separate question of a migration path, and it is not in
this change.

## The five commits

| Commit | What it does |
| --- | --- |
| `e01612439` | Drops a tolerance that had been added earlier, and adds the tests for what the message should say. This is the red commit: one of its tests fails on it |
| `e531f319d` | Adds `Error::WalFormatUnsupported` and the version read, in sixteen files because the variant has to be classified everywhere `SegmentCorrupted` is |
| `d92244beb` | Restores the tolerance **behind** the version check, and reads the version behind the preamble. Both answers to the first review |
| `faa465fa5` | Moves the version read to the offset where the reader stopped, which is zero in a plaintext segment and sixteen behind an encrypted store's preamble |
| `435fb0b42` | Makes the log line above the error neutral. Found by the end-to-end run, not by a unit test |

## Evidence, with exit codes

| Arm | Command | Exit | What it shows |
| --- | --- | --- | --- |
| base | `cargo nextest run -p nodedb --lib -E 'test(a_store_written_in_an_older_format_says_which_version) or test(a_segment_that_is_not_a_wal_still_says_corrupted)'` | **100** | The version test fails against the old wording, which is the bug |
| fix | `cargo nextest run -p nodedb --lib -E 'test(/wal::manager::replay/) or test(/class_parity/)'` | **0** | All six replay tests and the error-class parity tests pass on `faa465fa5` |
| mutation | the same filter, with the version comparison replaced by `None` | **100** | Exactly two tests fail, both the version-message ones; the four guards keep passing |
| end to end | `e2e-version-message.sh` on a real production copy and a real version-3 store | **0** | The version-1 store is refused with the message above, and the version-3 store boots to serving |

The mutation is recorded with its exact diff:
`evidence-mutations/mutation-version-read-removed.diff`, sha256 `e74a7e6936bda4e4`.

Logs: `/home/maya/.drill/issue354/logs/` — one file per arm, with the exit code in the name.

## Design decisions a reviewer should check

| Decision | Why |
| --- | --- |
| A new error variant rather than a better string in the old one | A format gap is an upgrade and corruption is a repair. The variant carries `found` and `required`, so the message cannot drift from the values |
| The version is read at `info.end_offset` | That is where the reader stopped. A plaintext store stops at 0, an encrypted one behind its 16-byte `WALP` preamble, and a torn tail stops with this build's own version, so a torn tail is not mistaken for a format gap |
| The preallocated-tail tolerance is kept, behind the check | Every boot recreates the newest segment. Refusing a record-less newest segment made a store permanently unbootable, which an earlier review caught. The version check runs first, so the store that motivated this work is still refused |
| `WalFormatUnsupported` is a permanent apply failure | Retrying cannot help until an upgrade, and the arm `SegmentCorrupted` happens to sit in returns `Transient` |
| The wrapper log line is neutral | It wraps every validation failure, and it was the first line an operator read |

## Questions for the reviewer

1. Is the ordering right: version check, then the tail tolerance, then corruption? A store that is both
   the tail and in the wrong format must be refused, and one that is the tail and merely empty must not.
2. `carried_format_version` reads six bytes at the stop offset. A file shorter than six bytes, a file
   that vanishes between discovery and the read, and a file whose bytes at that offset are not a header
   all yield `None`, which falls through to corruption. Is that the right default?
3. The new variant was added to every exhaustive match that mentions `SegmentCorrupted` — sixteen files.
   Check the class each match assigns, especially the retryability ones.
4. Do the five tests pin behaviour or wording? The mutation says only the two version tests fail without
   the code, which is the intended split.
5. Anything in the change that makes the *store* worse rather than the message better.

## What is not in this change

| Item | Status |
| --- | --- |
| A migration or dump path for an older store | Not attempted. The store stays unreadable, and the message now says so |
| The redundant per-vshard WAL replay, the measured five-fold win | Separate unit, planned next |
| The withdrawn tolerance itself | Reverted. It was measured harmful: on the version-1 store it let the gate pass, replayed zero records, and the node died later in the write-group settle |

## Where the wider evidence lives

`Bumi-Hijau/wiki/nodedb/NODEDB-BOOT-BENCH-20261006/` — the boot study, the two-machine hardware
measurement, the flexibility design, and the plan for the work after this.
