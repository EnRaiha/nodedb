# Findings: why our node cannot start inside 30 s and 60 s

One question, answered from our own node's journal. No simulation, no estimate.

## The answer in one paragraph

The two bounds are not boot budgets. They are tolerances on two gates that sit inside a boot
sequence which, on our store, does about **519 seconds of work** before the node serves. The metadata
gate (30 s, a progress clock) and the data-group gate (60 s, a wall clock) both monitor a gate in the
middle of that work, so both fire on a cold start with a backlog. Raising them to 300 s and 600 s is
what let the node finish, and it works, but it is a tolerance rather than a fix.

## Where the time actually goes

Store 1.4 GB, `data_plane_cores = 2`, one disk. Successful boot, process start 17:45:12, serving
17:53:51. Attribution by module, from the journal of that boot.

| Interval | Module writing lines | Work |
| --- | --- | --- |
| 0 to 6 s | `spawn`, `engine`, `load` | process start, memory governor, data-plane cores pinned |
| 6 to 40 s | nothing at all (34 s silence) | WAL and catalog open, no log lines |
| 40 to 111 s | `scheduler` (815+ lines) | Calvin channels and replay building state |
| 111 to 122 s | `wal::manager::replay: WAL replay complete records=17` | data-plane WAL done. **17 records**, so not the cost |
| 122 to 260 s | `schema_barrier` (84 per 30 s), `unregister_collection` (216 per 30 s), a 40 s silence | schema rehydration and catalog cleanup |
| 260 to 348 s | `apply` (~2,700 lines) | data groups applying entries |
| 348 to 498 s | `catch_up` (~900 lines) | data groups catching up to the commit index |
| 498 to 519 s | `commit_redo` (2,493), `commit_resolve` (1,062) | Calvin commit and redo storm, ending at `HTTP API server listening` |

## Why each bound fires

| Bound | Mechanism | Why it fires here |
| --- | --- | --- |
| metadata, 30 s | progress clock, resets on every `applied_index` advance | the cold boot contains silences of 26 s, 34 s and 40 s before serving. A 30 s window without an advance is normal at this size |
| data group, 60 s | wall clock from the first poll, never resets | catch-up alone occupies about 150 s and apply about 90 s. 60 s is structurally impossible for this store |

The failed attempt is sharper than the table: at 60 s the group had

```
data raft group recovery timeout after 60s: group 2 applied 378 of 379 committed entries
```

That is 99.7 percent complete with one entry left. No bound, 60 s or 3600 s, fixes a wedged entry;
that is the capacity and redo defect (#392 in the upstream tree, absent from our production binary).

## The CPU axis, measured on CT 212

Store: 20,000 rows, 72 MB on disk, cleanly stopped and archived as `pristine-clean.tar.zst`. Page
cache dropped before every run. Bounds set to one hour, so nothing truncates the measurement.
Interleaved A B A B against drift.

| Run | Cores | `data_plane_cores` | Boot seconds | Per-vshard cost |
| --- | --- | --- | --- | --- |
| `cores-2-r1` | 2 | 2 | 83.2 | 66.1 ms |
| `cores-2-r2` | 2 | 2 | 84.4 | 66.1 ms |
| `cores-6-r1` | 6 | 6 | 84.3 | 65.9 ms |
| `cores-6-r2` | 6 | 6 | 83.4 | 65.9 ms |
| `cpulimit-1-r1` | 6, capped at 1 CPU | 6 | 83.9 | 66.5 ms |

**The spread across all five runs is 1.2 s, and the core count changes nothing.** Tripling the cores,
then starving six threads down to one CPU of capacity, produces the same boot time to within 1.4
percent.

## What the 84 seconds actually is

1,024 Calvin vshards are started on every boot, in one serial loop, and each iteration costs about
66 ms:

```
07:08:29.039785  nodedb::wal::manager::replay: WAL replay …
07:08:29.040209  scheduler: calvin scheduler starting vshard_id=787 …
07:08:29.105917  nodedb::wal::manager::replay: WAL replay …     <- next vshard, +65.7 ms
07:08:29.106408  scheduler: calvin scheduler starting vshard_id=788 …
07:08:29.171839  nodedb::wal::manager::replay: WAL replay …     <- +65.4 ms
```

So the shape of a boot is

\[
t_{boot} \approx N_{vshard} \times \left( t_{replay} + t_{start} \right) + t_{fixed},
\qquad N_{vshard} = 1024
\]

with \(t_{replay} + t_{start} \approx 66\ \text{ms}\), and \(t_{fixed}\) the process start, the
catalog open and the tail. The per-vshard step writes no log lines while it runs, and its cost does
not move with CPU capacity, which places it in serialised IO or a fixed wait, not in computation.

That also explains production without a second experiment: the same 1,024 steps, with a larger cost
per step because each vshard holds more data. About 500 s over 1,024 steps is roughly 0.5 s per
vshard, the same loop over a 1.4 GB store.

## Fresh against used, measured on CT 212

| Run | Store | Vshard starts | Boot seconds |
| --- | --- | --- | --- |
| `fresh-6-r1` | brand new, first boot ever | 0 | 0.875 |
| `fresh-6-r2` | empty but initialized | 1,024 | 1.165 |
| `used-6-r1` | 20,000 rows, 72 MB | 1,024 | 83.406 |
| `used-6-r2` | 20,000 rows, 72 MB | 1,024 | 83.354 |

So the per-vshard step is data-dependent, not fixed: about 1 ms when a vshard holds nothing, about
66 ms when it holds data. The count of steps is always 1,024. See `VSHARD-SURVEY.md` for what the
step does and where the cost comes from.

## The scan costs more than the work: a measured A/B

The survey (`VSHARD-SURVEY.md`) shows `scan_applied` replays the entire WAL once per hosted vshard, so
a boot does work proportional to \(V \times R\) to answer \(V\) small questions. To measure what that
repetition costs, branch `exp/calvin-scan-cache` adds one experiment-only helper, 23 lines in
`nodedb/src/control/cluster/calvin/scheduler/recovery.rs`: the scan reuses one replay per WAL
directory per process. It is not a proposed patch, and the comment in the code says so.

| Run | Binary | `WAL replay complete` lines | Scheduler lines | Boot seconds |
| --- | --- | --- | --- | --- |
| `used-6-r1` baseline | `4c37f7e0b` build, `74c0ceb9` | 1,028 | 1,024 | 83.406 |
| `used-6-r2` baseline | same | 1,028 | 1,024 | 83.354 |
| `used-6-r1` patched | experiment build, `a726f465` | **5** | 1,024 | **18.035** |
| `used-6-r2` patched | same | **5** | 1,024 | **16.780** |
| `cpulimit-1-r1` patched | same, six threads on one CPU | 5 | 1,024 | 16.509 |

The prediction before the run was about 17 seconds. It landed at 16.8 to 18.0.

The loop itself collapsed: the 1,024 scheduler starts now span **77 milliseconds**
(`08:40:43.318` to `08:40:43.395`) instead of about 67 seconds. The computation was never the cost:
18.7 million in-memory record visits take 77 ms. What took 67 seconds was 1,023 redundant full reads
of the same WAL from disk, each with decryption.

### What the patched boot still spends

| Segment | Time |
| --- | --- |
| startup, checkpoint loads, engine init | about 5 s, logged |
| **silence** between the first replay and the vshard loop | **15.8 s, no log lines** |
| the whole 1,024-vshard loop | 0.08 s |
| silence before serving (drains, warm, lease) | 2.6 s |

So the next cost centre is 15.8 silent seconds after the first replay, which fits the write-group
settle, the tombstone load and the catalog work. The experiment did not touch it, and it is not
attributed yet.

## The fat increment is linear, and it is measured in WAL records

Three store sizes, two binaries, cold cache, one archived image each. Boot is the time from the
first log line to `HTTP API server listening`.

| Store | Rows | WAL records | Baseline boot | Patched boot | Baseline loop | Model \(3.70\ \text{ms} \times R\) |
| --- | --- | --- | --- | --- | --- | --- |
| fresh | 0 | 0 | 0.875, 1.165 | not run | about 0 | 0 |
| 3k | 3,000 | 15,201 | 68.905, 69.079 | 13.413, 13.710 | about 55.5 s | **56.2 s** |
| 20k | 20,000 | 18,256 | 83.406, 83.354 | 16.780, 18.035 | about 66.0 s | **67.5 s** |

The coefficient comes from the first store and is confirmed by the second, within two percent:

\[
t_{loop} \approx 3.70\ \text{ms} \times R \qquad (R = \text{WAL records at boot})
\]

and the boot itself, on the baseline binary, is

\[
t_{boot} \approx 1.2\ \text{s} + 4.5\ \text{ms} \times R
\]

which predicts the 20k store at 82.6 s against 83.4 s measured. The extrapolation is the point of the
exercise:

| Records | Baseline boot | Patched boot |
| --- | --- | --- |
| 18,256 | 83 s (measured) | 17 s (measured) |
| 50,000 | 3.1 min | 48 s |
| 116,000 | 8.6 min | 2.1 min |
| 1,000,000 | **62 min** | 14.5 min |
| 5,000,000 | **5.1 h** | 72 min |

The 116,000 row is not invented: production booted in 519 s, and the measured coefficient implies a
store with roughly that many WAL records. The model reproduces the production boot from a bench
measurement, which is the strongest check available without booting a production copy.

### The fat is not the user's data

| Store | Rows | WAL records | Records per row |
| --- | --- | --- | --- |
| 3k | 3,000 | 15,201 | 5.1 |
| 20k | 20,000 | 18,256 | 0.9 |

Fifteen times fewer rows bought only 17 percent fewer records, and 17 percent less boot. The record
count is dominated by what the engine writes on its own: epochs, redo, catalog and checkpoint traffic
since the last checkpoint. A nearly empty database already pays the linear cost, and an operator
cannot fix it by deleting data. What moves the number is checkpoint cadence and write volume, not
table size.

## Does the engine count multiply the cost

Fifteen engines exist (`EngineId::ALL`, `nodedb-mem/src/engine.rs:65`): Vector, Graph,
DocumentSchemaless, DocumentStrict, Kv, Columnar, Timeseries, Spatial, Array, Fts, Sparse, Crdt,
Query, Wal, Bridge. The boot log reports `engines=15`.

The data-plane redo replay (`nodedb/src/wal/redo/replay.rs:195`) runs **eleven arms** over the record
set: vector, vector extended, document redo, kv, timeseries, array, crdt, fts, spatial, graph node
label, and graph redo. Ten of them walk the merged, LSN-ordered slice; the graph node label arm walks
the raw records.

Each arm walks **every** record and decodes only its own. From `nodedb/src/data/executor/wal_replay/kv.rs:43`:

```
for record in records {
    if self.replay_halted() { break; }
    let logical_type = record.logical_record_type();
    let record_type = RecordType::from_raw(logical_type);
    …            // the owned branch does the work; everything else falls through
}
```

So the arithmetic is:

| Quantity | Value |
| --- | --- |
| Arms per boot | 11 |
| Records in the 3k store | 15,201 |
| Slice touches from foreign arms | \(11 \times 15201 \approx 167{,}000\) |
| Cost of a type check and a skip | a few nanoseconds, so **about a millisecond in total** |
| Patched boot, of which that is the filter | 13.4 s |

**No. The engine count multiplies a cheap filter, not the fat.** In a store that only ever used the
document path, ten of the eleven arms do nothing per record except look at the type and fall through.
The seconds are spent decoding and applying the records each engine owns, once each, which is real
storage work: about 0.7 ms per record at this size.

One consequence worth stating: an engine with no data still runs its arm, so a store with no vector,
graph, spatial or CRDT records still pays eleven walks. Gating an arm on "this engine has data" would
save milliseconds, so it is not a lever for boot time. The lever is the record count, which is a
function of checkpoint cadence and write volume, not of how many engines are configured.

## Who receives the load

The 3k store measures 59 MB, of which the WAL is 29.5 MB holding 15,201 records, about 1,940 bytes per
record. With those numbers the recipients are identifiable, and only one of them is pathological.

| Load | Recipient | Volume per boot | Is that the right recipient |
| --- | --- | --- | --- |
| The whole WAL, once | `init_wal` (`bootstrap/wal_init.rs:47`), whose result is kept as `Arc<[WalRecord]>` and shared with the data plane | 29.5 MB | yes, and it is already materialised for everyone else |
| The whole WAL again after the write-group settle | the settle path | 29.5 MB | acceptable, it is one extra pass |
| **The whole WAL, once per hosted vshard** | `recover_applied` to `scan_applied` (`control/cluster/calvin/scheduler/recovery.rs:76`), called from `for vshard_id in hosted` (`start_raft_cluster/start_raft_helpers.rs:159`) | **1,024 × 29.5 MB = 30.2 GB** | **no.** Each call keeps about 0.1 percent of what it reads: the applied markers of one vshard |
| Each record, decoded and applied | the engine that owns it, once | 29.5 MB in total | yes |
| Every record, filtered | the 11 redo replay arms | about 167,000 slice touches | yes, and it is about a millisecond |

So the fat has a single recipient: the per-vshard Calvin recovery scan, which reads the entire WAL
30.2 GB worth of times per boot to collect a handful of markers each time. The store it does this for
is 59 MB.

Two consequences that the measurements already showed, now explained:

| Observation | Cause |
| --- | --- |
| 2 cores, 6 cores and one CPU of capacity boot in the same time | the loop runs on the boot thread, serially. The data-plane cores are not involved, so there is nothing for extra cores to do |
| About 55 to 66 ms per vshard, invariant to record content | that is 29.5 MB read, decrypted, parsed into records, scanned and dropped, at roughly 500 MB/s of effective throughput. It is bandwidth-bound, not compute-bound |

The allocation churn is part of the cost: the record set is materialised and dropped 1,024 times, so
the allocator hands out and takes back 30 GB per boot for a 59 MB store.

## What a cluster does to this load

Everything measured so far is a single node, and a single node hosts every vshard: the boot log starts
`vshard_id=0` through `vshard_id=1023` on one machine. A cluster changes the counts, not the shape.

| Mechanism | Source |
| --- | --- |
| vshard to group, round robin: `vshard_to_group[i] = 1 + i % num_groups` | `nodedb-cluster/src/routing.rs:114` |
| group to node, round robin over the node list, `replication_factor` nodes per group | `routing.rs:118` |
| the boot loop starts schedulers only for the vshards this node hosts, and only the new ones | `start_raft_helpers.rs:135`, `:159` |
| each hosted vshard registers its own lanes: a lock manager, a read-result sender, a promotion sender, a verdict sender, and a scheduler task | `start_raft_helpers.rs:249`, `:263`, `:278`, and the verdict registration above them |

So with \(N\) nodes and replication factor \(r\), a node hosts \(r \times 1024 / N\) vshards, and its
WAL holds roughly its share of the records. Since the scan cost is the product of the two, per node:

\[
t_{scan,node} \propto \frac{r \times 1024}{N} \times \frac{r \times R}{N}
= \left(\frac{r}{N}\right)^2 \times 1024 R
\]

| Topology | Hosted vshards per node (r = 1) | Scan cost per node, relative to one node | Cluster total |
| --- | --- | --- | --- |
| 1 node | 1,024 | 1 | 1 |
| 3 nodes | 341 | about 1/9 | about 1/3 |
| 6 nodes | 171 | about 1/36 | about 1/6 |

With \(r = 2\) the per-node figure is \(4/N^2\): about 0.44 of a single node at three nodes, and about
0.11 at six. These rows are arithmetic on the measured coefficient, not measurements. No multi-node
boot has been run.

Three things a cluster does not change:

| Thing | Why it stays |
| --- | --- |
| The shape, \(V \times R\) | only the two counts shrink. A node that hosts many vshards over a large WAL still pays the product |
| Snapshot install paths | a migrated or installed vshard writes a `SnapshotInstalled` record, which the scan must honour because it clears the applied tail for that group. A cluster adds record types and reasons for the scan, not fewer |
| The need for the fix | the same one-line change removes the whole term on any topology. A cluster only hides it behind division |

The practical reading for us: the production node is single-node, so it has the worst case of this
pathology, and a three-node cluster with replication two would roughly halve its per-node scan cost.
That is a workaround by division. The fix is still to read the WAL once.

## Correction: the refused production copy was a WAL format version gap

This supersedes the earlier reading of the same evidence. The refusal we hit was not a preallocated or
torn segment. It was a format version the current build cannot read, and the gate was right.

Every record header is `magic(4) | format_version(2) | record_type(4) | lsn(8) | tenant_id(8) | …`
(`nodedb-wal/src/record/header.rs:22`), and a header whose version is not exactly `WAL_FORMAT_VERSION`
does not open (`:144`).

| Store | First bytes of every segment | Format version |
| --- | --- | --- |
| production copy, `~/bench-prod-data/pristine.tar.zst`, 2 segments | `57 4e 59 53 01 00 …` | **1** |
| our own bench store | `57 4e 59 53 03 00 …` | **3** |

Read it with `od -A n -t u2 -j 4 -N 2 <segment>`: bytes 4 and 5 are the version, little endian.

| Segment of the production copy | Bytes | Version |
| --- | --- | --- |
| `nodedb/wal/wal-00000000000000710217.seg` | **59,932,672** | 1 |
| `nodedb/wal/audit.wal/wal-00000000000000000001.seg` | 71,364,608 | 1 |

The first size is the number in the refusal message, byte for byte, which ties the evidence together:
that file is the one the gate named, and it is version 1.

### A provenance error this audit caught

The first version of this table described a four-segment, 321 MB store as the production copy. It is
not: that archive contains `bench_t`, our own benchmark table, so an older binary had written our
benchmark dataset into it. It is version 1 for the same reason production is, because that older
binary predates the bump, but it is not production's store. The table above now names the artefact
that is actually production's, and the conclusion did not change when the artefact did.

| Ref | Commit | `WAL_FORMAT_VERSION` |
| --- | --- | --- |
| `origin/main`, the base of the pull request | `03d4fda6119d` | **3** |
| the binary production runs | `3b4a96da2d5b` | **1** |

The bump to 3 is `b92a97398` ("fix(cluster): make consensus, recovery, and restore crash-safe"). It is
not in production's build, and it is one of the 122 commits production is behind. No tolerance for an
older version exists anywhere in the reader, and the legacy migration was deleted
(`nodedb-wal/src/segment/migration.rs`, removed in `58e0efd9c`).

Consequences, stated as they are:

| # | Consequence |
| --- | --- |
| 1 | A build from `origin/main` cannot read one record from production's WAL. The four segments all stop at `offset=0 last_lsn=0`, which is what produced every `torn write` warning and then the fatal validation error |
| 2 | `validate_for_startup` refusing the store is **correct** behaviour, not over-strictness |
| 3 | What an upgrade puts at risk is the WAL tail since the last checkpoint, not the whole store. The production copy carries eight engine checkpoint directories (`columnar-ckpt`, `crdt-ckpt`, `graph-label-ckpt`, `kv-ckpt`, `sparse-vector-ckpt`, `spatial-ckpt`, `sync-hwm-ckpt`, `vector-ckpt`) and a 174 MB `system.redb`, so the engines rebuild from those and only the tail is in question |
| 4 | This is why no copy of production could boot against a main-based binary in any of our experiments. The dataset workaround we adopted was necessary for a reason we did not know at the time |

### What this retracts

| Retracted | Why |
| --- | --- |
| "A preallocated newest segment is refused" as the explanation for the observed failure | the observed failure is the version gap. The preallocated case was constructed in a unit test, never observed |
| The earlier answer that the code contradicts itself, warning about a torn tail and then refusing | the warning reports where parsing stopped, correctly. The refusal is the same fact read one layer up, and for a version gap refusing is right |
| The tolerance patch on branch `drill/issue354` | measured on the production store below: it does not fix the case, and it turns a clear refusal into a silent loss of the whole tail |

### What the tolerance does on the production store, measured

The production copy carries exactly **one** data segment, so that segment is also the newest, and the
tolerance applies to it. Booting the copy with each binary:

| Binary | Result |
| --- | --- |
| head, requiring version 3 | `StartupError: WAL validation failed — cannot start with corrupted WAL segments error=segment corrupted: WAL segment '/var/lib/nodedb/wal/wal-00000000000000710217.seg' is non-empty (59932672 bytes) but contains no valid WAL records` |
| with the tolerance applied | the gate passes, `WAL replay complete records=0` is logged **1,029 times**, and the boot then dies in `settling the write groups` with `storage error (sparse): open chain heads table: chain_heads is of type Table<&str, &str>` |

So the patch replaces an early, accurate refusal with a silent empty replay and a misleading storage
error further along. The tail is not reported as lost; it is simply not there. That is why part two is
withdrawn, and the measurement is the reason rather than the argument.

### A second defect this exposed

The `chain_heads is of type Table<&str, &str>` error is not caused by the tolerance. It is a
pre-existing type mismatch in the sparse engine's chain-heads table that only becomes reachable when
the boot gets past the WAL gate with an empty log. Worth its own look, and worth noting because the
gate has been hiding it.

## A fat store, measured: 50,000 rows, 195 MB, and a 10x swing with no data change

The store was grown from 20,000 rows to 50,000 by two concurrent writers, then stopped cleanly.
It is 195 MB with an 80 MB WAL. Then it was booted six times, and the same store behaved two
different ways:

| Boot | WAL records | The 1,024-vshard loop | Whole boot |
| --- | --- | --- | --- |
| the first, right after the ingest | **46,191** | **170.0 s** | **207.0 s** |
| every later boot | about 4,130 | 15 to 16 s | 19 to 20 s |

The first row is the linear model at a third point, on a store three times the size of the one the
coefficient came from:

\[
t_{boot} \approx 1.2\ \text{s} + 4.5\ \text{ms} \times 46{,}191 = 209\ \text{s}
\]

against **207.0 s** measured, one percent apart.

The second row is the operational point. Nothing about the data changed between the two rows: the same
195 MB, the same 50,000 rows. The boot fell from 207 s to 20 s because the first boot checkpointed and
pruned the WAL tail, leaving about 4,130 records behind instead of 46,191. **Boot cost follows the tail
since the last checkpoint, not the size of the store.** A 195 MB database boots in 20 s; a 195 MB
database with a long unpruned tail boots in 207 s.

### What the load did to reads

Under the two-writer load, a read failed with

```
ERROR: raft group 2 cannot serve a linearizable read here: this node did not apply through read
index 36600 in time; retry
```

The error names the reason and says retry, and the write rate under that load is about 20 to 25 rows
per second, the same figure the descriptor-lease work observed. It is recorded here because a fat-load
test that only measures the boot would have missed it.

### A clarification about which build was measured

Both binaries used in the fat-store runs perform the same 1,028 WAL replays and boot in the same time,
because neither of them carries the scan fix: the head build carries only the startup-bound work and
the withdrawn WAL gate tolerance, and the scan fix lives on the experiment branch
`exp/calvin-scan-cache`. The 83 s to 17 s result earlier in this document belongs to that experiment
build, and it remains the measured value of the scan defect.

## Does what we changed give any advantage

Asked directly, and answered with measurements rather than intent. Each row names what was run and
what it showed.

| Change | Advantage | Evidence |
| --- | --- | --- |
| Stale-field rejection | **Yes, demonstrated** | a key at `[server]` fails the boot with `` `[server] raft_ready_timeout_ms` is not read; set `[tuning.startup] raft_ready_timeout_ms` instead `` — our message, naming the replacement |
| Configurable bounds | **wiring proven, effect not reproducible here** | the long-tail store boots in 129.8 s at 28,129 records with the **old** values (30 s and 60 s) and in 129.9 s with the new ones (300 s and 600 s). The waits never block on this store, so the knob's value rests on the production incident: a 519 s boot that died at a fixed bound |
| Typed durations | not measurable, compile-time only | the newtypes stop a transposition at every call site; the pin test holds that |
| The WAL gate tolerance, part two | **no advantage, measured harm** | the crash-recovery store boots identically with and without it, 58.5 s and the same 51,530 rows, and on the version-1 production copy it silently dropped the tail |

### Crash recovery, measured on the fat store

The store was written to by two writers for 60 seconds and then killed with `SIGKILL`, leaving a 19 MB
WAL written by this build.

| Binary | Result | Records replayed | Rows after recovery |
| --- | --- | --- | --- |
| head, with the tolerance | SERVING in 58.5 s | 11,964 | 51,530 |
| head without it, the same store | SERVING in 58.5 s | 11,962 | 51,530 |

Two things follow. The engine's own crash recovery is sound on this shape: every committed row came
back. And the gate never fired, because the segment held a valid committed prefix, which is the
ordinary case. That is why part two has nothing to show here.

### A third confirmation of the linear model

The long-tail boot replayed 28,129 records in 129.8 s:

\[
t_{boot} \approx 1.2\ \text{s} + 4.5\ \text{ms} \times 28{,}129 = 127.8\ \text{s}
\]

against 129.8 s measured, under two percent apart.

## Two real machines, measured: the hardware matters less than the store

The same stick, the same store, the same binary, booted on a 2023 laptop and on this 2018 desktop. The
stick's autorun identified each machine, measured the scan, ran a real boot, and wrote its own files.

| Machine | CPU | Threads | The 1,024-vshard loop | Whole boot | Probe µs per record |
| --- | --- | --- | --- | --- | --- |
| Lenovo Yoga 7 2-in-1 14AHP9 (83DK) | AMD Ryzen 7 8840HS | 16 | **43.51 s**, **44.00 s** | 44.19 s, 45.19 s | 1.781, 1.816 |
| the reference host, native | Intel i5-8400 | 6 | 51.73 s | 53.16 s | 1.921 |
| the reference host under QEMU, 2 vCPU | Intel i5-8400 | 2 | 54.76 s | 56.18 s | 2.030 |

Per record per pass, from the loop itself: **2.80 µs on the Ryzen 7 8840HS** and **3.32 µs on the
i5-8400**.

The answer to "does a newer machine make a difference" is yes, and it is **19 percent**:

\[
\frac{51.73}{43.51} = 1.19
\]

Not the 29 to 59 times that was guessed, and less than the two to three times I expected. Three
reasons, all of them measurable:

| Reason | Evidence |
| --- | --- |
| The loop is serial | 16 threads on the Yoga changed nothing against 6 on the desktop, and the earlier CPU axis was flat from 2 cores to one CPU of capacity |
| The cost is per record, not per byte | the same 3.32 µs per record held across two stores whose records differ threefold in size |
| The laptop's clock moved under load | its own report shows 2,021 MHz on the first run and 3,266 MHz on the second, and the two runs differ by half a second |

The probe tracked the real loop: it reported the Yoga 1.12 times faster and the loop itself 1.19 times
faster, so it is a usable relative instrument even though it under-reports in absolute terms.

What this settles for the fix: removing 1,023 of the 1,024 passes is worth about **19 times more on a
2023 laptop than the difference between that laptop and a 2018 desktop**. The store shape dominates the
hardware, and the earlier measurement shows it plainly: the same machine booted the same store in
**207 s with a long WAL tail and 20 s once a checkpoint pruned it**, a factor of ten, with no hardware
change at all.

The raw files are in `hardware-runs/`, one result and one boot log per run, exactly as the stick wrote
them.

## So why 30 s and 60 s cannot be met

| Bound | Arithmetic |
| --- | --- |
| data group, 60 s | the boot needs about 68 s of per-vshard work on a 72 MB store before the gate can be satisfied. On the production store the same loop needs minutes |
| metadata, 30 s | the applier shares the machine with that loop, so gaps longer than 30 s occur while the loop is elsewhere, even though nothing is wedged |

Raising the bound works because the loop does finish. It is serial, not stuck. The levers that would
shorten it, none of which is a bound:

| Lever | Why |
| --- | --- |
| Start vshards lazily, only those with data | 1,024 starts happen even when the store holds one table |
| Replay several vshards in parallel, bounded by IO | the step is CPU-insensitive, so parallelism helps only if the cost is a wait rather than a saturated queue |
| Batch the per-vshard work into one pass | removes 1,023 loop iterations and their fixed overhead |
| Find out what the 66 ms is | it is the whole cost. If it is one fsync per vshard, batching the fsyncs is the fix |

## Findings, each with its evidence

| # | Finding | Evidence |
| --- | --- | --- |
| F1 | A store copied from a live node cannot boot: the WAL validator rejects a segment that is larger than a preamble but holds no valid records | `nodedb/src/wal/manager/replay.rs:20` `validate_for_startup`, branch `end_offset == 0`. Boot log in `results/failed-prod-copy/run-cores-6.log`: `StartupError: WAL validation failed — cannot start with corrupted WAL segments` |
| F2 | Deleting the offending segment is not enough. Five attempts each failed on a segment the boot recreates, named `wal/audit.wal/wal-…0001.seg`, while that directory holds only `wal-…0001.dwb` at 4.4 MB | leaves the audit WAL with data in the double-write buffer and no records in the segment file |
| F3 | The write path is about 24 rows per second and does not improve with concurrency | 2 writers, 20,000 rows, 848 s. 4 writers, 4,000 rows, 116 s. 16 writers reached 24,037 rows in 20 minutes, with `descriptor lease grant did not apply within 5s (… outcome: TimedOut)` |
| F4 | Our production boot already shows the Calvin capacity family the upstream fix targets | 2,493 `commit_redo` and 1,062 `commit_resolve` lines in the final 30 s, including `calvin: flush/drop response was not Ok while applying an already-committed verdict`. Production runs 122 commits behind main |
| F5 | The metadata bound was right once: it fired on a genuine stall, not on slowness | the swap attempt recorded `metadata group applied no entry for 30s (applied_index stuck at 25170)` |

Measured here: the boot timeline, the three bounds that fired, the write rate, the WAL rejection.
Inferred: that the catch-up and apply intervals are throughput-bound on 2 data-plane cores and one
disk. Not measured: which of those two intervals is CPU-bound rather than IO-bound.

## What follows for the design decision

| Lever | Effect on a 519 s boot |
| --- | --- |
| Raise the bound | none on the work. It buys tolerance, and it hides stuck from slow |
| Progress clock per group plus a cap | distinguishes stuck from slow, which is what an operator needs at 02:00 |
| Fewer entries to replay (checkpoint more often, prune the log) | shrinks the work itself, the only lever that changes 519 s |
| More apply throughput (cores, batching) | unmeasured. This is the experiment the benchmark exists to run |
| The capacity and redo fix in #392 | removes the wedged-entry case, which is what actually killed the boot at 60 s |

## The single-machine plan: variable timing instead of many machines

Comparing many PCs is expensive and confounded. One host with a controlled capacity profile answers
the same question better, because it isolates the factor.

| Profile axis | Instrument | Emulates |
| --- | --- | --- |
| CPU capacity | `pct set --cores N`, `--cpulimit N` | fewer or slower cores |
| Disk latency | `dm-delay` on a dedicated volume for the data dir | a slower disk, including slower fsync |
| Disk throughput | cgroup v2 `io.max` (rbps, wbps, riops, wiops) | a narrower disk |
| Memory | `--memory`, `memory_limit` | a smaller page cache |
| Cache warmth | `drop_caches` before each run | a cold start |

Each profile gets a fingerprint from `fio`, so a result reads "boot took X at 1 ms fsync latency"
instead of "boot took X on machine B". What this cannot emulate: instruction-level differences
between CPU generations, and the internal cache behaviour of a different disk. For the question
"how much capacity does a boot need", it is enough, and it is honest about being an emulation.
