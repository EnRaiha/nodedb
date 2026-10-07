# Where to design flexibility, and where not to

Every row below comes from a measurement in this pack. A knob is proposed only where the measurement
shows an operator could change an outcome; rows where a knob would buy nothing are in the last section,
so the list does not grow for its own sake.

## The measured facts the design has to answer to

| Fact | Value | Source |
| --- | --- | --- |
| Boot cost is linear in WAL records | 4.5 ms per record, whole boot | `FINDINGS.md`, the fat increment |
| The same store, after one checkpoint | **207 s to 20 s**, same machine, same data | the fat-store section |
| Hardware difference between a 2018 desktop and a 2023 laptop | **1.19 times** | the two-machine section |
| Cores | no effect, 2 cores to one CPU of capacity | the CPU axis |
| Store shape difference | **10 times** | the fat-store section |
| The bounds | do not fire on any store we have, 1 s and 600 s boot the same | the advantage section |
| The dominant loop sits after both gates | no bound covers it | the same section |
| A production store cannot be read by a current build | WAL format version 1 against 3, no migration | the correction section |

The design follows from the third and fourth rows: **hardware is a 1.19, the store is a 10.** Flexibility
should be spent on the store and on visibility, not on machine profiles.

## Where flexibility pays

| # | Area | What it should look like | What it buys | Cost |
| --- | --- | --- | --- | --- |
| F1 | **WAL tail policy** | `[wal] checkpoint_after_records` with a default, plus `nodedb checkpoint` for an operator to prune before a planned restart | boot cost becomes a *chosen* number rather than a function of traffic since the last checkpoint. Measured lever: 207 s to 20 s | medium: the checkpoint path exists, the policy and the command do not |
| F2 | **A total boot deadline** | `[tuning.startup] boot_deadline_ms`, separate from the per-gate bounds | the per-gate bounds do not cover the phase that actually dominates; an operator currently has no knob that speaks to the whole boot | small: one deadline over the existing boot path |
| F3 | **Work-relative bounds** | express the gates in records or in progress rate, not only in seconds: fail when *no* progress for N records applied, or when the applied rate falls below X | survives hardware changes by construction. A 1.19 hardware factor makes a seconds-based bound mostly a hardware statement | medium: needs the progress clock the design document already describes as stage two |
| F4 | **Lazy vshard schedulers** | `[tuning.startup] vshard_start = eager \| lazy`, where lazy starts a vshard's scheduler on first touch | converts the largest boot term into first-touch cost, which is where it belongs for a node that never serves all 1,024 vshards | large: it changes when a vshard's recovery runs, so it needs the same care the snapshot-install path got |
| F5 | **Boot phase visibility** | per-phase durations at boot, the way shutdown already has them, plus `vshard N of 1024` in `/healthz` | an operator can see which phase owns the time. Today 15.8 s of a patched boot produce no log line at all | small: the shutdown path is the pattern to copy |
| F6 | **A tail-size metric** | `nodedb_wal_tail_records` as a gauge, next to the boot metrics | the number that decides the boot is invisible today. An operator can watch it and checkpoint before it becomes a problem | small |
| F7 | **A format compatibility window** | a documented supported-version window, and a `nodedb migrate-wal` or an offline dump/restore that reads an older version | today a store written by production cannot be read by a current build at all, and the gate refuses it correctly but says "corrupted" | large, and it is a maintainer decision first |
| F8 | **The boot-cost probe in the tree** | `nodedb doctor boot-cost --wal <segment>`, the same measurement this pack's probe makes | an operator can predict a boot before an upgrade, on the hardware that will run it. It also gives a regression number for CI | small: the probe is 200 lines and dependency-free |

## Where we measured fluctuation, and the knob each one implies

The design rule this pack earned: **where the system's own numbers move, a fixed value will be wrong
sometimes.** Every row below is a fluctuation we observed, not a guess, and the knob is what absorbs it.

| Where it moved | Measured swing | What absorbs it |
| --- | --- | --- |
| The WAL tail, same store, one boot apart | **46,191 to about 4,130 records**, boot 207 s to 20 s | F1, a tail policy, and F6, the tail as a metric. An 11-fold swing dwarfs every hardware factor |
| The CPU clock on one laptop, between its own two runs | **2,021 to 3,266 MHz**, and the two runs differ by half a second | F3, bounds in work units. A seconds-based bound is a statement about the clock, and the clock is not stable |
| Write throughput under concurrency | **23.6 rows/s with two writers, 34.5 with four** | the admission and lease path: a documented concurrency expectation, and backpressure that reports itself rather than stalling |
| A read under write load | `did not apply through read index 36600 in time; retry` | a read-path tolerance an operator can set, and a metric that counts the retries |
| The patched boot, repeated on one machine | **16.780 to 18.035 s**, seven percent apart | not a knob: margin. A bound with no margin over a seven percent spread will fire on a healthy boot |
| Segment size written by the writer | **59,932,672 against 67,108,864**, so not always the full target | the preallocation and segment-target policy, which is also what sets the tail size F1 governs |
| Time inside a boot that produces no log line | **15.8 s of silence** | F5, phase durations. A fluctuation you cannot see cannot be tuned |
| The WAL format across builds | version 1 against 3, no migration | F7. The most expensive fluctuation, because it is not slow, it is unreadable |

Two of these deserve emphasis because they change priorities:

| Reading | Consequence |
| --- | --- |
| The largest swing is the store, not the machine, and it is **11-fold** | F1 outranks every hardware-shaped idea |
| The clock moves **61 percent** between two runs of the same laptop | any bound expressed only in seconds is partly measuring thermal state, which is why F3 exists and why a machine profile would encode noise |

## Where a knob would buy nothing

| Area | Why not |
| --- | --- |
| The vshard count, 1,024 | the cost is per *record*, not per vshard, and the count is a routing invariant. In a cluster it already divides by the node count |
| The number of engine passes | measured at about a millisecond per boot. A knob here is noise |
| Raising the two bounds alone | measured: 1 s and 600 s boot the same store. The problem is the unit, not the value, which is why F2 and F3 exist |
| A machine profile, fast or slow | hardware is a 1.19 across six years of silicon. A profile would encode a rounding error |
| Making the scan parallel | the loop is serial by design, and the work is redundant rather than large. Removing 1,023 of the 1,024 passes beats distributing them |

## What I would do first, and why

| Order | Item | Reason |
| --- | --- | --- |
| 1 | **F1, WAL tail policy** | it is the only lever measured to move the boot by an order of magnitude, and it needs no new machinery |
| 2 | **F5 and F6, visibility** | cheap, and they are what make F1 usable: an operator can see the tail and the phase that owns the time |
| 3 | **F2, a total boot deadline** | closes the gap that the two existing bounds leave open, the phase after the gates |
| 4 | **F8, the probe in the tree** | gives the number before a deployment instead of after an incident |
| 5 | **F3, work-relative bounds** | the right long-term unit, and it depends on the progress clock, so it follows the others |
| 6 | **F4, lazy vshard schedulers** | largest payoff on paper, largest risk, and the scan fix makes it much less urgent |
| 7 | **F7, the format window** | not ours to decide, but it is the one gap that makes a store unreadable rather than slow |

The scan fix itself stays where it is: a correctness-preserving change that removes a redundant pass, and
it is worth 1,023 of the 1,024 passes on every machine regardless of what any of these knobs do.
