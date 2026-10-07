# Plan: the next work, one pull request

Constraints, as decided: **one branch** (`drill/issue354`), **one pull request** (`#412`, currently a
draft), no new pull requests. Each item lands as its own commit with its own evidence, so the pull
request stays reviewable even though it grows.

Every item below starts with a survey against the code graph. The survey is not ceremony: three of the
findings in this pack were wrong until the graph or the artefact contradicted them, and each item names
the query that would do the contradicting.

## Step 0, before anything else: the withdrawn part two

`drill/issue354` still carries the WAL gate tolerance, and it is measured harmful: on a version-1 store
it let the gate pass, replayed **0 records**, and died later in the write-group settle. Adding work on
top of it would bury that.

| Option | What happens |
| --- | --- |
| **Replace it** (recommended) | drop the tolerance, keep the two tests as the record of what was measured, and add a version-aware refusal: the gate reads the format version from the header and says `store written in WAL format version 1; this build requires 3` instead of `corrupted`. Same fail-closed behaviour, accurate message, and it is provable end to end on the production copy |
| Revert it | one commit that removes the tolerance and its tests. Fastest, and leaves the misleading message in place |

Proof for the replacement: boot the production copy and read the message. It currently names
`corrupted`; it must name the version. The store is at `~/bench-prod-data/pristine.tar.zst` and the CT
still has it installed.

## Step 1, the scan fix (O1), the measured five times

| | |
| --- | --- |
| Target | `control/cluster/calvin/scheduler/recovery.rs` `scan_applied` `:76`, `recover_applied` `:159`, `read_applied_recovery` `:63`; caller loop `start_raft_helpers.rs:159` `:201` |
| Survey | `drill_blast symbol=scan_applied` then `drill_blast symbol=recover_applied` — the callers are the blast radius, and the design document claims only one production caller |
| Change | take `&[WalRecord]` instead of `&WalManager`; delete the manager-taking variants so the compiler forbids the read; reach the records that `init_wal` already materialises (`bootstrap/wal_init.rs:47`, carried to the data plane at `bootstrap/data_plane.rs:311`) through a `OnceLock` on `SharedState`, the pattern that struct already uses for `raft_status_fn` (`control/state/fields.rs:21`, `:156`) |
| Proof | the parity test (the new function returns byte-identical results for every vshard of a fixture), the boot-replay-count assertion that would have caught this, and the bench A/B: **83.4 s to 16.8 s** on the 20k store, loop **67 s to 77 ms** |
| Risk | the plumbing is the risk, not the scan. If the records cannot be reached without a wide signature change, stop and report rather than threading them through five layers |

## Step 2, visibility: boot phases and the WAL tail

Two small items that make every later item measurable.

| | Boot phase durations | WAL tail size |
| --- | --- | --- |
| Target | `control/metrics/system/{record,fields,render}.rs`, hooking `control/startup/**` `StartupSequencer` | the WAL manager's append path and `RecoveryInfo::record_count` (`nodedb-wal/src/recovery.rs`) |
| Survey | `drill_search pattern="record_shutdown_phase_duration"` — the shutdown path is the template to copy | `drill_search pattern="record_count"` and the append path in `wal/manager/append*.rs` |
| Change | `record_startup_phase_duration(phase, ms)` beside the shutdown one, rendered as `nodedb_startup_phase_duration_seconds{phase}` | a `nodedb_wal_tail_records` gauge |
| Proof | a boot log naming every phase with a duration, where today 15.8 s produce no line at all | the gauge moves after writes and drops after a checkpoint |
| Why first | without these, "the boot got faster" is an opinion. With them, it is a number in the same place every time |

## Step 3, a total boot deadline

| | |
| --- | --- |
| Target | the boot sequence from `main.rs` through `bootstrap/cluster_ready.rs` to serving |
| Survey | `drill_blast symbol=await_cluster_ready` — the call chain that has to be wrapped |
| Change | `[tuning.startup] boot_deadline_ms`, separate from the two per-gate bounds, covering the whole sequence including the phase after the gates |
| Proof | the deadline fires at a small value and names the phase it was in, using step 2's instrumentation. Measured reason: the loop that dominates the boot sits after both existing gates, and 1 s and 600 s data-group bounds boot the same store |
| Risk | a deadline that fires on a healthy boot is worse than none. Default it generous, and make it log before it fails |

## Step 4, the probe in the tree

| | |
| --- | --- |
| Target | the CLI subcommand table, plus the probe logic from `~/laptop-test-kit/nodedb-scan-probe.rs` (200 lines, no dependencies) |
| Survey | `drill_search pattern="enum Command|Subcommand"` in `nodedb/src/main.rs` and the CLI module |
| Change | `nodedb doctor boot-cost --wal <segment> [--passes N]`, printing microseconds per record per pass and a projected loop time |
| Proof | run it against a real segment and against the synthetic image; the number must match the bench's measured coefficient on the same machine (3.32 µs per record here) |
| Why | it turns a post-incident measurement into a pre-deployment one, and it gives CI a regression number |

## Step 5, the WAL tail policy

| | |
| --- | --- |
| Target | the checkpoint trigger and the WAL retention path, plus the config surface and a CLI verb |
| Survey | `drill_search pattern="checkpoint"` across `nodedb/src/wal` and `nodedb/src/control`, then `drill_blast` on the trigger it finds. The segment roll at `nodedb-wal/src/segmented.rs:221` is **not** the checkpoint path, and confusing the two would build the knob in the wrong place |
| Change | `[wal] checkpoint_after_records` with a default, and `nodedb checkpoint` for an operator to prune before a planned restart |
| Proof | the measured lever: the same store booted in **207 s with a 46,191-record tail and 20 s once a checkpoint pruned it to about 4,130**. A test that writes past the threshold and asserts the tail is bounded, plus the boot time before and after |
| Risk | a checkpoint that runs too eagerly costs write throughput. The default must come from the measurement, not from taste |

## Later, and why not now

| Item | Why it waits |
| --- | --- |
| Work-relative bounds (F3) | needs the progress clock, which is stage two of the existing design document |
| Lazy vshard schedulers (F4) | the largest change, and step 1 removes most of its urgency |
| The WAL format window (F7) | a maintainer decision about upgrades, and it is not ours to take |
| The X250 hardware run | not needed: the two-machine result already answers the question, and hardware differs by 1.19 while the store differs by 10 |

## What done looks like

| Gate | Evidence |
| --- | --- |
| Each step | its own commit, with a red proof or a before-and-after measurement in the drill ledger |
| The whole branch | the repository preflight exit 0 on the final head, and the full `nodedb --lib` suite green (8,781 tests today) |
| The pull request | body regenerated from the ledger, the withdrawn part two described honestly, and the review gate closed by a verdict that names the reviewed commit |
| The claim | every performance statement traceable to a log in `results/` or `hardware-runs/` |
