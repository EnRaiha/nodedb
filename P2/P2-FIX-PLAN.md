# P2 — Fix Plans (untuk review GLM 5.3)

> ## ✅ IMPLEMENTED (24-08-2026) — SEMUA 4 PLAN TELAH DILAKSANAKAN
>
> Fix 1 `888684628` (PR #243), Fix 2 `e60a853ae` (PR #244), Fix 3 `4f929593c`
> (PR #245), Fix 4 `16d99164a` (PR #246) — semua hijau (build 0, raft 122,
> cluster 1044, nodedb 6218, clippy 0, maya-gate clean). Fix Plan 5
> (combined integration test) dan rest ini kekal sebagai rekod design.

Base: `origin/main` @ `54fe575c0` (repo `/home/maya/projects/nodedb-rebase`).
Tarikh: 2026-08-24. Semua line numbers + insert points diverifikasi terhadap `git show origin/main:<path>` oleh 4 subagents read-only.

Fail ini mengandungi 4 fix plan untuk isu epic #165 yang BELUM selesai (lihat `P2/P2-UNSOLVED-ISSUES.md` untuk drill penuh). Setiap plan: goal, per-file changes dengan exact insert point + snippet, test plan, verification, refactor suggestion, effort.

> **GLM 5.3 REVIEW (2026-08-24):** Review penuh + 13 resolution points + 4 improved solutions dalam `P2/P2-GLM53-REVIEW-RESOLUTION.md`. Perubahan utama yang dipakai di sini:
>
> - **Sequencing revised:** Fix 1 → Fix 2 → Fix 3 → Fix 4 (Fix 3 mesti SELEPAS Fix 2 — epoch strict + mixed-version cluster)
> - **Fix 1:** F4 (persist incarnation) = primary fix; F1/F2 defense-in-depth; F3 bunuh re-kill race. Self-advertise rate-limited (500ms). Direction: `IncarnationTracker` consolidation
> - **Fix 2:** restart-path `NodeInfo.wire_version` re-stamp = MUST-FIX sebelum 1.0 (bukan optional)
> - **Fix 3:** tambah recovery path (auto-rejoin) + exempt RPC_INSTALL_SNAPSHOT + RPC_PING_REQ (indirect relay)
> - **Fix 4:** direction `LeaseManager` + SWIM Dead → on_node_crash hook
> - **Combined integration test** (rolling upgrade + crash + epoch bump + lease GC) ditambah sebagai seksyen akhir

## Kandungan

1. **Fix Plan 1 — SWIM fast-restart rejoin stick** (F1 echo refutation deterministik, F2 ping=liveness + re-advertise, F3 cancel suspicion timer, F4 persist incarnation via catalog) — ~2–2.75 hari
2. **Fix Plan 2 — Wire version rolling upgrade** (buka window, range check join, ClusterVersionView move ke nodedb-cluster, rejection enrichment) — ~1.5 hari core + 0.5–1 hari optional
3. **Fix Plan 3 — cluster_epoch enforcement** (validate_peer_cluster_epoch dalam parse_frame — satu chokepoint transport, StalePeerEpoch error, exemption JOIN/PING/PONG + snapshot + recovery path) — S
4. **Fix Plan 4 — descriptor-lease crash-wedge GC** (drain filter bukan-ahli + topology hook GC + periodic sweep + SWIM Dead hook) — ~3.5 hari fix, 2–3 hari refactor berasingan
5. **Combined Integration Test** (tambah — GLM R11)

---

# Fix Plan 1 — SWIM fast-restart rejoin stick

Base: `origin/main` @ `54fe575c0` (repo `/home/maya/projects/nodedb-rebase`, read-only drill done via `git show origin/main:<path>` / `git grep origin/main`).
All line numbers refer to `origin/main`.

---

## 1. Goal

Eliminate the permanent membership divergence that follows a fast restart of a SWIM node:

1. Node restarts → `Incarnation::ZERO` (production `SwimConfig::initial_incarnation`), announces `Alive(0)` exactly **once** (bootstrap prime).
2. Peers still hold `Suspect(N)` / `Dead(N)` (N ≫ 0). Merge rule: `Alive(0)` is dominated → `MergeOutcome::Refute` → peer re-gossips its stored record into its dissemination queue.
3. That refutation reaches the restarted node only **probabilistically** (piggyback fanout to random targets). The restarted node has **zero retry** (prime is one-shot, drained at fanout threshold), so it stays self-`Alive(0)` while the cluster keeps `Dead(N)` — forever.
4. Secondary aggravator: `SuspicionTimer::cancel` is never called in production, so a peer that had the restarted node on a suspicion timer promotes it to `Dead` at expiry — building the `Dead` update from `member.incarnation` **at expiry time**, which can clobber an already-applied `Alive(N+1)` refutation with `Dead(N+1)` (same incarnation, higher state precedence).
5. No incarnation persistence: `Incarnation::bump()` is dead API, `initial_incarnation` is test-only.

**Fix strategy (4 changes + plumbing):**

- **F1** Deterministic refutation echo: piggyback-refutations are returned to the _source_ of the datagram instead of only random gossip.
- **F2** Inbound-Ping handling uses `ping.from`/`ping.incarnation` (learn sender liveness) and re-advertises our own self-`Alive` on every Ack (deterministic retry).
- **F3** Cancel the suspicion timer whenever a member transitions back to `Alive` (production wiring of the existing `SuspicionTimer::cancel`).
- **F4** Persist the local incarnation in the redb catalog (`METADATA_TABLE`, same pattern as `cluster_epoch`) and load it on subsystem start with `Incarnation::bump()` so a restart announces `Alive(persisted+1)` that dominates the cluster's last-known value.

Races that remain (timer expiry vs refutation arrival) are healed by F1's echo loop: the restarted node bumps again on learning it is stale.

---

## 2. Per-file changes (exact)

### 2.1 `nodedb-cluster/src/swim/incarnation.rs` — new `IncarnationSink` trait

Insert after the `impl Incarnation` block (after `pub fn bump`, ~line 88, before `impl fmt::Display`):

```rust
/// Persistence hook for the local incarnation. Invoked by the failure
/// detector whenever the local incarnation is bumped (self-refutation or
/// restart adoption) so a restart can resume above the cluster's last-known
/// value. The detector treats failures as best-effort: a lost save only
/// costs one extra refutation round-trip after the next restart.
pub trait IncarnationSink: Send + Sync {
    fn save(&self, incarnation: Incarnation);
}
```

No other change here (`bump()`/`refute()` already tested).

### 2.2 `nodedb-cluster/src/swim/mod.rs` — export the trait

```rust
pub use incarnation::{Incarnation, IncarnationSink};
```

(line ~87: `pub use incarnation::Incarnation;`)

### 2.3 `nodedb-cluster/src/catalog/schema.rs` — new key

After `pub(super) const KEY_CLUSTER_EPOCH: &str = "cluster_epoch";` (line ~41):

```rust
/// Local node's SWIM incarnation (u64 LE) — persisted so a restart can
/// rejoin above the cluster's last-known value instead of zero.
pub(super) const KEY_SWIM_INCARNATION: &str = "swim_incarnation";
```

### 2.4 `nodedb-cluster/src/catalog/core.rs` — save/load (mirror `cluster_epoch`)

Insert immediately after `load_cluster_epoch` (ends ~line 140, before `// ── TLS Certificates ──`):

```rust
/// Persist this node's SWIM incarnation (monotonic, bumped on every
/// self-refutation). Overwrites any prior value.
pub fn save_swim_incarnation(&self, incarnation: u64) -> Result<()> {
    let bytes = incarnation.to_le_bytes();
    let txn = self.db.begin_write().map_err(catalog_err)?;
    {
        let mut table = txn.open_table(METADATA_TABLE).map_err(catalog_err)?;
        table
            .insert(KEY_SWIM_INCARNATION, bytes.as_slice())
            .map_err(catalog_err)?;
    }
    txn.commit().map_err(catalog_err)?;
    Ok(())
}

/// Load the persisted SWIM incarnation. `None` if never written
/// (first boot → callers start from `Incarnation::ZERO`).
pub fn load_swim_incarnation(&self) -> Result<Option<u64>> {
    let txn = self.db.begin_read().map_err(catalog_err)?;
    let table = txn.open_table(METADATA_TABLE).map_err(catalog_err)?;
    match table.get(KEY_SWIM_INCARNATION).map_err(catalog_err)? {
        Some(guard) => {
            let bytes = guard.value();
            if bytes.len() == 8 {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(bytes);
                Ok(Some(u64::from_le_bytes(arr)))
            } else {
                Ok(None)
            }
        }
        None => Ok(None),
    }
}
```

(Exact same shape as `save_cluster_epoch`/`load_cluster_epoch` at core.rs:116-140.)

### 2.5 `nodedb-cluster/src/swim/detector/runner.rs` — F1 + F2 + F3 + F4 hooks

**(a) Struct field + constructor** — add to `FailureDetector` (after `local_incarnation: Mutex<Incarnation>,` ~line 44):

```rust
incarnation_sink: Option<Arc<dyn IncarnationSink>>,
```

Change `with_subscribers` signature (~line 61) to take a new param and store it:

```rust
pub fn with_subscribers(
    cfg: SwimConfig,
    membership: Arc<MembershipList>,
    transport: Arc<dyn Transport>,
    scheduler: ProbeScheduler,
    subscribers: Vec<Arc<dyn MembershipSubscriber>>,
    incarnation_sink: Option<Arc<dyn IncarnationSink>>,
) -> Self
```

`new()` passes `None`; add an accessor for tests if needed. Import `IncarnationSink` from `crate::swim::incarnation`.

**(b) F3 — cancel suspicion timer on Alive transition.** Make `apply_and_notify` (currently sync, ~line 93) async and cancel on Alive:

```rust
async fn apply_and_notify(&self, update: &MemberUpdate) -> MergeOutcome {
    let old_state = self.membership.get(&update.node_id).map(|m| m.state);
    let outcome = apply_and_disseminate(&self.membership, &self.dissemination, update);
    let new_state = self.membership.get(&update.node_id).map(|m| m.state);
    // A member that is Alive must never sit on a suspicion timer: without
    // this, the timer would promote it to Dead at expiry using the
    // *current* incarnation and clobber a fresh Alive refutation
    // (Dead(N+1) beats Alive(N+1) at equal incarnation).
    if matches!(outcome, MergeOutcome::Insert | MergeOutcome::Apply | MergeOutcome::SelfRefute { .. })
        && new_state == Some(MemberState::Alive)
    {
        self.suspicion.lock().await.cancel(&update.node_id);
    }
    if !self.subscribers.is_empty() {
        let Some(new) = new_state else { return outcome; };
        if old_state != Some(new) {
            for sub in &self.subscribers {
                sub.on_state_change(&update.node_id, old_state, new);
            }
        }
    }
    outcome
}
```

Both call sites (`ingest_piggyback` ~line 118, `on_tick` ~line 176/218) are already async — just add `.await`.

**(c) F1 — `ingest_piggyback` returns refutations** (~line 118):

```rust
/// Returns updates whose incoming form was stale so the caller can echo
/// our stored view directly back to the sender (deterministic refutation
/// delivery instead of random piggyback gossip).
async fn ingest_piggyback(&self, piggyback: &[MemberUpdate]) -> Vec<MemberUpdate> {
    let mut refutations = Vec::new();
    for update in piggyback {
        let outcome = self.apply_and_notify(update).await;
        match outcome {
            MergeOutcome::SelfRefute { new_incarnation } => {
                let mut guard = self.local_incarnation.lock().await;
                if new_incarnation > *guard {
                    *guard = new_incarnation;
                    if let Some(sink) = &self.incarnation_sink {
                        sink.save(new_incarnation);   // F4 hook
                    }
                }
            }
            MergeOutcome::Refute => {
                if let Some(stored) = self.membership.get(&update.node_id) {
                    refutations.push(MemberUpdate::from(&stored));
                }
            }
            _ => {}
        }
    }
    refutations
}
```

**(d) F1 wiring in `on_incoming`** (~line 230):

```rust
let refutations = self.ingest_piggyback(msg.piggyback()).await;
match msg {
    SwimMessage::Ping(ping) => self.handle_ping(from_addr, ping, refutations).await,
    SwimMessage::PingReq(req) => self.handle_ping_req(from_addr, req, refutations).await,
    ...
}
```

`handle_ping_req` appends `refutations` to the relayed `Ack` piggyback (dedupe + `truncate_piggyback(max)`).

**(e) F2 — `handle_ping`** (~line 250), new signature and body:

```rust
async fn handle_ping(&self, from_addr: SocketAddr, ping: Ping, refutations: Vec<MemberUpdate>) {
    // Learn liveness from the ping itself: anyone who pings us is alive
    // at the incarnation they advertise. Merge handles staleness (this
    // also replaces seed placeholders on the pinger's own initiative).
    self.apply_and_notify(&MemberUpdate {
        node_id: ping.from.clone(),
        addr: from_addr.to_string(),
        state: MemberState::Alive,
        incarnation: ping.incarnation,
    })
    .await;

    // Re-advertise our own current self-record on every reply so any peer
    // that pings us deterministically learns our incarnation — even after
    // the bootstrap prime drained past its fanout threshold.
    let local_inc = *self.local_incarnation.lock().await;
    if let Some(me) = self.membership.get(self.membership.local_node_id()) {
        self.dissemination.enqueue(MemberUpdate {
            node_id: me.node_id.clone(),
            addr: me.addr.to_string(),
            state: MemberState::Alive,
            incarnation: local_inc,
        });
    }

    let fanout = DisseminationQueue::fanout_threshold(self.membership.len(), self.cfg.fanout_lambda);
    let mut piggyback = self.dissemination.take_for_message(self.cfg.max_piggyback, fanout);
    // Echo refutations directly back to the sender (dedupe by NodeId).
    for r in refutations {
        if !piggyback.iter().any(|p| p.node_id == r.node_id) {
            piggyback.push(r);
        }
    }
    piggyback.truncate(self.cfg.max_piggyback);

    let ack = SwimMessage::Ack(Ack {
        probe_id: ping.probe_id,
        from: self.membership.local_node_id().clone(),
        incarnation: local_inc,
        piggyback,
    });
    let _ = self.transport.send(from_addr, ack).await;
}
```

**(f) F4 hook in test helper** — `bump_local_incarnation` (~line 329): also call `sink.save(*guard)` after the bump (guarded by `#[cfg(test)]`, harmless in prod).

**Existing tests to update in this file:** `spawn_node` calls `FailureDetector::new` (unchanged, passes `None`); `ping_triggers_ack_reply` builds a raw Ping — now asserts the Ack also carries the self-Alive piggyback (strengthened); `self_refute_bumps_incarnation_via_piggyback` unchanged in behavior.

### 2.6 `nodedb-cluster/src/swim/bootstrap.rs` — thread the sink

`spawn_with_subscribers` (~line 96) gains a parameter; `spawn` (~line 90) passes `None`:

```rust
pub async fn spawn_with_subscribers(
    cfg: SwimConfig,
    local_id: NodeId,
    local_addr: SocketAddr,
    seeds: Vec<SocketAddr>,
    transport: Arc<dyn Transport>,
    subscribers: Vec<Arc<dyn MembershipSubscriber>>,
    incarnation_sink: Option<Arc<dyn IncarnationSink>>,
) -> Result<SwimHandle, SwimError>
```

- Line 114 (`cfg.initial_incarnation` → `MembershipList::new_local`) and line 132 (`let initial_inc = cfg.initial_incarnation;` → priming `Alive`) stay as-is: the _caller_ now supplies a non-zero initial incarnation (F4). No code change needed at those lines — only the import of `IncarnationSink` and the `FailureDetector::with_subscribers(..., incarnation_sink)` call.
- Update the doc comment on `spawn_with_subscribers` to document the sink param.

### 2.7 `nodedb-cluster/src/swim/config.rs` — doc fix

`initial_incarnation` field (~line 48): replace "Always `0` in production; exposed for deterministic unit tests." with:

```rust
/// Seed incarnation for a freshly-booted local node. The production
/// caller (`SwimSubsystem`) sets this to `persisted.bump()` loaded from
/// the catalog so a restart rejoins above the cluster's last-known value;
/// `0` remains the first-boot default and the test value.
```

### 2.8 `nodedb-cluster/src/subsystem/context.rs` — catalog into `BootstrapCtx`

Add field after `multi_raft` (~line 43):

```rust
/// Cluster catalog (redb). Subsystems persist per-node state here
/// (e.g. SWIM persists its incarnation via `save_swim_incarnation`).
pub catalog: Arc<crate::catalog::ClusterCatalog>,
```

Extend `BootstrapCtx::new` params + field init; update the struct doc.

### 2.9 `nodedb-cluster/src/bootstrap/start.rs` — plumb catalog

`start_cluster_subsystems` (~line 183) gains `catalog: Arc<ClusterCatalog>`; pass it into `BootstrapCtx::new(...)` at line 192. `register_default_subsystems` needs no change (it already receives `ctx`; the swim config still defaults to `SwimConfig::default()` — the persisted value is applied in `SwimSubsystem::start`).

### 2.10 `nodedb/src/control/cluster/start_raft/loop_build.rs` — host call site

At line 168 (`nodedb_cluster::start_cluster_subsystems(...)`), add `Arc::clone(&handle.catalog)` as the new first (or last) argument. `handle.catalog: Arc<nodedb_cluster::ClusterCatalog>` already exists (handle.rs:56).

### 2.11 `nodedb-cluster/src/subsystem/impls/swim_subsystem.rs` — load + persist

In `SwimSubsystem::start` (~line 86), after the transport bind and before `spawn_with_subscribers`:

```rust
// Resume the incarnation from the catalog so a restart announces
// Alive(persisted + 1) instead of Alive(0) — the cluster may still hold
// Suspect/Dead at a much higher value for us.
let mut swim_cfg = self.cfg.swim.clone();
if let Some(persisted) = _ctx.catalog.load_swim_incarnation().map_err(|e| {
    BootstrapError::SubsystemStart { name: "swim", cause: Box::new(e) }
})? {
    swim_cfg.initial_incarnation = Incarnation::new(persisted).bump();
}

struct CatalogIncarnationSink { catalog: Arc<crate::catalog::ClusterCatalog> }
impl IncarnationSink for CatalogIncarnationSink {
    fn save(&self, incarnation: Incarnation) {
        if let Err(e) = self.catalog.save_swim_incarnation(incarnation.get()) {
            tracing::warn!(incarnation = incarnation.get(), error = %e,
                "failed to persist swim incarnation; next restart may need an extra refutation round");
        }
    }
}
let sink: Option<Arc<dyn IncarnationSink>> =
    Some(Arc::new(CatalogIncarnationSink { catalog: Arc::clone(&_ctx.catalog) }));
```

Then pass `sink` into the existing `spawn_with_subscribers(...)` call (after `subscribers`). Imports: `crate::swim::incarnation::{Incarnation, IncarnationSink}`.

**Why catalog instead of a file in `data_dir`:** the catalog is already the single redb store opened by both `start_cluster` (bootstrap/restart) and the host (`handle.catalog`), with an established u64-LE metadata pattern (`cluster_epoch`). A free-form file in `data_dir` would need its own atomicity/fsync story and a second path threaded through the subsystem; the catalog gives durability + one canonical load point for free. `METADATA_TABLE` is per-node (each node has its own catalog file), so the incarnation is node-local, exactly as required.

---

## 3. Test plan

Test support in use today: `TransportFabric` + `InMemoryTransport` (`swim/detector/transport/in_memory.rs`) with `#[tokio::test(start_paused = true)]` + `tokio::time::advance(...)`; real-UDP integration via `nodedb-cluster-tests/tests/swim_udp_convergence.rs` using `spawn_swim` / `UdpTransport` and wall-clock `poll()`.

### 3.1 Unit — `nodedb-cluster/src/swim/detector/runner.rs` (in-memory fabric)

1. **`restart_node_adopts_incarnation_from_peer_refutation`** — A spawned fresh (`Incarnation::ZERO`); B pre-loaded with A at `Dead(5)` (synthetic `membership.apply` before running, as in existing tests). Run ~30 paused intervals. Assert: A's self record is `Alive` with incarnation `> 5`; B's view of A becomes `Alive` with incarnation `> 5`. (Exercises F1 echo + SelfRefute + F2.)
2. **`ping_reply_readvertises_self_alive`** — spawn A solo; drain its dissemination queue with `take_for_message` until empty; raw Ping from a probe endpoint. Assert the Ack's piggyback contains A's self-`Alive` at current incarnation **and** the probe's record appears in A's membership as `Alive(inc = ping.incarnation)` (F2 sender-liveness).
3. **`alive_refutation_cancels_suspicion_timer`** — A+B mesh; `drop_edge` both directions so B suspects A (timer armed, `det_b.suspicion.len() == 1`); restore edges; deliver a synthetic `Alive(N+1)` for A into B (or let the refutation echo run); advance time past the suspicion timeout; assert B still holds A `Alive` — never `Dead` (F3). Control variant: without F3 this test fails.
4. Strengthen existing **`ping_triggers_ack_reply`** to assert the Ack piggyback carries the self-record.

### 3.2 Unit — `nodedb-cluster/src/catalog/core.rs`

5. **`swim_incarnation_roundtrip`** — `temp_catalog()` (pattern from `bootstrap/restart.rs` tests): `load_swim_incarnation() == Ok(None)` → `save_swim_incarnation(42)` → `load == Some(42)` → overwrite with `7` → `Some(7)`.

### 3.3 Integration — `nodedb-cluster-tests/tests/swim_udp_convergence.rs`

6. **`restart_rejoins_with_persisted_incarnation`** — converge 3-node UDP mesh (as today); `h_b.shutdown().await` → poll A,C see B `Suspect|Dead`; respawn B at same addr/NodeId with `fast_cfg()` but `initial_incarnation: Incarnation::new(9)` (simulating persisted+bump); poll: A and C both hold B `Alive` with `incarnation > previous` within 5 s.
7. **`restart_rejoins_via_refutation_echo_without_persistence`** — same but respawn B at `ZERO`; assert convergence still happens within 5 s purely via F1/F2 (deterministic echo), proving the protocol-level fix independent of persistence.

### 3.4 Optional subsystem-level — `swim_subsystem.rs` tests

8. **`start_loads_persisted_incarnation`** — temp catalog + `save_swim_incarnation(5)`; construct `BootstrapCtx` (needs `NexarTransport::new(1, "127.0.0.1:0", TransportCredentials::Insecure)` as in `restart.rs` tests); `SwimSubsystem::start`; assert `swim_cfg.initial_incarnation == Incarnation::new(6)`. Mark optional — covers the F4 load path end-to-end.

---

## 4. Verification commands

```bash
cd /home/maya/projects/nodedb-rebase
cargo test -p nodedb-cluster swim::                       # detector/bootstrap/incarnation units
cargo test -p nodedb-cluster catalog::core                # incarnation persistence roundtrip
cargo test -p nodedb-cluster-tests --test swim_udp_convergence   # real-UDP restart tests
cargo test -p nodedb                                     # host plumbing (loop_build.rs) compiles + tests
cargo clippy -p nodedb-cluster -p nodedb -- -D warnings
```

Full-suite sanity (optional, slower): `cargo test -p nodedb-cluster-tests`.

---

## 5. Refactor suggestion / landmines (Left)

- **Landmine:** `SwimConfig::production()` still yields `initial_incarnation: ZERO`, and `spawn` (no-sink variant) is public and used by integration tests. Any future production caller that constructs `SwimConfig` directly and calls `spawn` bypasses persistence silently. Mitigation now: doc comment on `spawn` stating it is test/bootstrap-only and the subsystem is the sole production path. Follow-up refactor (optional): move persistence into a `SwimConfig::load_or_default(&ClusterCatalog) -> Result<SwimConfig>` helper so the load/bump logic can't drift from the subsystem.
- **Left `Incarnation::bump()`:** now used in production for the first time (subsystem load path). Keep it; the "dead API" is resolved by this plan rather than by deleting it.
- **`apply_and_notify` becomes async** — it's called from exactly two async sites today; if future sync call sites appear, they must not block_on (detector loop rule). Consider extracting the pure part into a sync helper if that happens.
- **Echo dedupe bound:** refutations are appended to the Ack after `take_for_message` and truncated to `max_piggyback`; a saturated queue can still drop a refutation in a single reply, but the queue copy (F1 enqueue behavior already present in `apply_and_disseminate`) plus repeated pings make delivery effectively guaranteed — document this in the plan's design note rather than adding unbounded piggyback.
- **`IncarnationSink` is sync and best-effort** — redb write happens on the detector task; a slow disk could stall one probe tick. Acceptable (writes are tiny, rare), but if profiling shows stalls, move the save to `tokio::spawn` in the subsystem impl (the sink contract already tolerates async fire-and-forget).

---

## 6. Effort

| Item                                                                                        | Estimate             |
| ------------------------------------------------------------------------------------------- | -------------------- |
| F4 plumbing (catalog key/API, BootstrapCtx, start.rs, loop_build.rs, subsystem load + sink) | 0.5–1 day            |
| F1/F2/F3 runner changes + existing-test updates                                             | 0.5 day              |
| Unit tests (3 new + 1 strengthened + catalog roundtrip)                                     | 0.5 day              |
| UDP integration tests (2 new) + stabilization                                               | 0.5 day              |
| Clippy, doc comments, review                                                                | 0.25 day             |
| **Total**                                                                                   | **~2–2.75 dev-days** |

Risk: low-medium. The trickiest part is the real-UDP restart test timing (5 s polls, 50 ms cadence); the paused-time fabric tests are deterministic and will carry most of the regression weight.

# Fix Plan — Wire Version Rolling Upgrade (range window)

Repo: `/home/maya/projects/nodedb-rebase`, base = `origin/main` (54fe575c0). All line numbers refer to that tree.

---

## 1. Goal

Replace the exact-equality join gate with a **range window** `[MIN, WIRE]` so mixed-version clusters can form (N-1 rolling upgrade), make `MIN_WIRE_FORMAT_VERSION` live, move `ClusterVersionView` into `nodedb-cluster` (fixing the dangling rustdoc link in `topology.rs`), and enforce the `ClusterSettings.min_wire_version` knob. The transport handshake in `nodedb-cluster/src/wire_version/` is already range-negotiate capable — **do not touch it**.

**Decision — open the window NOW (MIN=1, WIRE=2):** recommended because (a) the fix's whole purpose is to make MIN<WIRE possible; (b) pre-1.0 there are no deployed clusters, so cost ≈ 0 — the constant is stamped on `NodeInfo` only, never persisted into raft-log/metadata (that uses `wire_version::WireVersion::CURRENT`, a separate system), and old records already decode with `serde(default = "default_wire_version") = 1`; (c) it turns every dormant test path (view tests currently `return;` when `WIRE_FORMAT_VERSION < 2`, `versions.rs` gates, `check_wire_compatibility` floor branch) into live coverage before 1.0 ships. The old "DO NOT BUMP BEFORE 1.0" essay's core argument ("a bump cannot buy a rolling upgrade") was true **only because MIN==WIRE** — this fix removes that premise. Deferred alternative (keep 1/1, range-gate + param injection only): mixed-version clusters stay impossible pre-1.0 and the bug is only half fixed; choose it only if product forbids any version drift pre-1.0. 1.0 then ships as wire 2 with a proven `[1,2]` window.

---

## 2. Per-file changes (exact)

### 2.1 `nodedb-types/src/wire_version.rs` — open the window

- Line 52: `pub const MIN_WIRE_FORMAT_VERSION: u16 = WIRE_FORMAT_VERSION;` → `pub const MIN_WIRE_FORMAT_VERSION: u16 = 1;`
- Line 49: `pub const WIRE_FORMAT_VERSION: u16 = 1;` → `pub const WIRE_FORMAT_VERSION: u16 = 2;`
- Replace the whole "DO NOT BUMP THIS BEFORE 1.0" doc essay (lines 17–44) with window semantics:

```
//! # Window semantics
//!
//! A peer is compatible iff its version lies in
//! `[MIN_WIRE_FORMAT_VERSION, WIRE_FORMAT_VERSION]`. Bump WIRE only
//! alongside an actual wire-shape change (new enum variant, RPC,
//! payload field). Keep MIN at the oldest release this build supports
//! (N-1 policy); never raise MIN without a coordinated cluster-wide
//! migration. The value is stamped on `NodeInfo`, never persisted into
//! raft-log/metadata (that is `wire_version::WireVersion::CURRENT`,
//! separate and independent), so a bump cannot orphan on-disk state.
```

- Keep both compile-time asserts (lines 56–57) unchanged.

### 2.2 `nodedb-cluster/src/topology.rs` — export MIN + fix dangling doc links

- After line 14 (`pub use nodedb_types::wire_version::WIRE_FORMAT_VERSION as CLUSTER_WIRE_FORMAT_VERSION;`) insert:

```rust
/// Minimum accepted cluster wire version (floor of the join window).
/// Re-exported from `nodedb_types::wire_version`.
pub use nodedb_types::wire_version::MIN_WIRE_FORMAT_VERSION as MIN_CLUSTER_WIRE_FORMAT_VERSION;
```

- Doc-link fixes (currently dangling — `nodedb-cluster` has no `control` module; the view lives in crate `nodedb` today):
  - Line ~12: `control::rolling_upgrade::view::ClusterVersionView` → `crate::topology::version_view::ClusterVersionView`
  - Line ~114: `control::rolling_upgrade::view::compute` → `crate::topology::version_view::compute_from_topology`
- Add module declaration near the top: `pub mod version_view;` (see 2.4).

### 2.3 `nodedb-cluster/src/bootstrap/handle_join.rs` — range gate (THE core fix)

**Insert point A** — after the module doc comment, before `handle_join_request` (line ~39):

```rust
/// Accept any joiner whose cluster wire version lies within
/// `[min_wire_version, CLUSTER_WIRE_FORMAT_VERSION]`.
///
/// Pure so tests can inject synthetic windows (a raised operator floor,
/// or a version beyond CURRENT simulating a future build) without
/// compiling a second binary.
pub fn wire_version_in_window(v: u16, min_wire_version: u16) -> bool {
    v >= min_wire_version && v <= CLUSTER_WIRE_FORMAT_VERSION
}
```

**Insert point B** — signature (line 39–44), add floor param:

```rust
pub fn handle_join_request(
    req: &JoinRequest,
    topology: &mut ClusterTopology,
    routing: &RoutingTable,
    cluster_id: u64,
    min_wire_version: u16,
) -> JoinResponse {
```

**Insert point C** — replace the exact-equality block (lines 43–62):

```rust
    // Range gate: accept any joiner inside [min_wire_version, CURRENT].
    // `min_wire_version` is the effective cluster floor — max(compile-time
    // MIN, operator's persisted ClusterSettings.min_wire_version).
    // (The transport handshake already negotiated frame compatibility;
    // this is the cluster-schema-level check.)
    if !wire_version_in_window(req.wire_version, min_wire_version) {
        warn!(
            node_id = req.node_id,
            joiner_wire_version = req.wire_version,
            accepted_window = format!("{min_wire_version}..={CLUSTER_WIRE_FORMAT_VERSION}"),
            "join request rejected: joiner cluster wire_version outside accepted window"
        );
        return reject(format!(
            "joiner wire_version {} outside accepted window {}..={} — \
             rolling upgrade (or downgrade) is required before this node can join",
            req.wire_version, min_wire_version, CLUSTER_WIRE_FORMAT_VERSION
        ));
    }
```

- Import: add `MIN_CLUSTER_WIRE_FORMAT_VERSION` (only if referenced in tests/helper — otherwise not needed in this file).
- Update the `with_wire_version(req.wire_version)` stamping at line ~126: unchanged (now stamps the joiner's real N-1 version — that is what feeds the view).

**Callers** (compile fixes):

- `nodedb-cluster/src/raft_loop/join.rs:215` and `:445` — both call sites already have `self.catalog` (used at line ~195 for `cluster_id`). Add a private helper on `JoinFlow`:

```rust
/// Effective join floor: max(compile-time MIN, persisted operator floor).
fn effective_min_wire_version(&self) -> u16 {
    self.catalog
        .as_ref()
        .and_then(|c| c.load_cluster_settings().ok().flatten())
        .map(|s| s.min_wire_version)
        .unwrap_or(MIN_CLUSTER_WIRE_FORMAT_VERSION)
        .max(MIN_CLUSTER_WIRE_FORMAT_VERSION)
}
```

then `handle_join_request(&req, &mut topo, &routing, cluster_id, self.effective_min_wire_version())` at both sites. Add `use crate::topology::MIN_CLUSTER_WIRE_FORMAT_VERSION;` (extend existing import).

- Test call sites to update with a 5th arg (`CLUSTER_WIRE_FORMAT_VERSION` or an injected value): `handle_join.rs` tests (lines 212, 237, 261, 293, 332, 351), `bootstrap/join.rs:554`, `nodedb-cluster-tests/tests/wire_version_handshake.rs:630`.

### 2.4 `ClusterVersionView` — new location (fixes the dangling doc link)

**Move** `nodedb/src/control/rolling_upgrade/view.rs` → **`nodedb-cluster/src/topology/version_view.rs`** (new file). It only depends on `ClusterTopology`/`NodeInfo`, which live in `nodedb-cluster`; today the dependency is inverted and `nodedb-cluster`'s docs point at a module that doesn't exist there.

- Struct + API unchanged (copy as-is, incl. all 6 tests):

```rust
pub struct ClusterVersionView {
    pub min_version: u16,
    pub max_version: u16,
    pub node_count: usize,
    pub node_versions: Vec<(u64, u16)>,
}
impl ClusterVersionView {
    pub fn single_node() -> Self;            // uses CLUSTER_WIRE_FORMAT_VERSION (was nodedb::version::WIRE_FORMAT_VERSION)
    pub fn is_mixed_version(&self) -> bool;
    pub fn all_upgraded(&self) -> bool;
    pub fn can_activate_feature(&self, required_version: u16) -> bool;
    pub fn version_spread(&self) -> u16;
    pub fn is_supported_spread(&self) -> bool;
}
pub fn compute_from_topology(topology: &ClusterTopology) -> ClusterVersionView;
```

- `nodedb/src/control/rolling_upgrade/view.rs` becomes a 2-line re-export shim so every nodedb call site (`observability.rs:223`, `metadata_proposer.rs:325/386`, `ddl_buffer.rs:135`, `state/methods.rs:265-270`, `versions.rs`) keeps compiling:

```rust
//! Re-exported from `nodedb_cluster::topology::version_view` (single
//! implementation, lives next to `ClusterTopology`).
pub use nodedb_cluster::topology::version_view::{ClusterVersionView, compute_from_topology};
```

- The view's existing tests move with the code; the `if WIRE_FORMAT_VERSION < 2 { return; }` skips in `mixed_version_n_minus_1` / `unsupported_spread_detected` now execute (WIRE=2).

### 2.5 `JoinResponse` window echo — two phases (rkyv constraint)

rkyv structs have **no field-tolerance**: appending fields to `JoinResponse` (rpc_codec/cluster_mgmt.rs:29) breaks `from_bytes` in BOTH cross-version directions (new decoder reads past the end of an old archive; old decoder can't validate a new layout). So:

**Phase 1 (this PR — no wire change):**

- The per-node `JoinNodeInfo.wire_version` entries already echo every node's version (joiner itself included, since `handle_join_request` admits before `build_response`).
- New helper, insert in `nodedb-cluster/src/bootstrap/join.rs` next to `apply_join_response` (~line 250):

```rust
/// Observed cluster wire-version window derived from a successful
/// `JoinResponse`. Falls back to the local build when the node list is
/// empty (defensive; a success always carries nodes).
pub(crate) fn window_from_join_response(resp: &JoinResponse) -> (u16, u16) {
    let mut min = u16::MAX;
    let mut max = 0u16;
    for n in &resp.nodes {
        min = min.min(n.wire_version);
        max = max.max(n.wire_version);
    }
    if min == u16::MAX {
        (crate::topology::CLUSTER_WIRE_FORMAT_VERSION,
         crate::topology::CLUSTER_WIRE_FORMAT_VERSION)
    } else {
        (min, max)
    }
}
```

- Call at the top of `apply_join_response` with `info!(node_id, cluster_min = min, cluster_max = max, mixed = min != max, "join response window")`.
- The rejection path already echoes the leader's window — the enriched error string from 2.3 carries `accepted window min..=WIRE`.

**Phase 2 (deferred, post-1.0, when real wire-shape changes ship):** add `cluster_min_wire_version`/`cluster_wire_version` fields to `JoinResponse`, bump the transport envelope `WireVersion` (currently `CURRENT = 2`), and dual-decode (`LegacyJoinResponse` for envelope ≤ 2, new struct for 3+), keyed on the **negotiated** connection version. Do not do this while the transport handshake is off-limits.

### 2.6 `nodedb-cluster/src/catalog/cluster_settings.rs` — enforce the knob

The field (line 63) is written at bootstrap but never read outside tests (grep: zero readers outside this file). Fix = enforcement at the leader (done in 2.3 via `effective_min_wire_version`), plus doc update:

- Line 62–63 doc: → `/// Minimum wire-protocol version peers must speak. Enforced by the leader at join (handle_join_request); raise only after every node has upgraded.`
- Optional (flag, do only if operator knob wanted now): add `ClusterConfig.min_wire_version: Option<u16>` plumbed through `ClusterSettings::from_config` (line 84 currently hardcodes `min_wire_version: 1`). Keep default 1 → zero behavior change today.

### 2.7 Doc/comment corrections (stale claims, must-fix)

- `nodedb/src/version.rs` line ~28: "Readers MUST reject messages with wire_version != their own" → "Readers accept wire_version in `[MIN_WIRE_FORMAT_VERSION, WIRE_FORMAT_VERSION]`" (the function `check_wire_compatibility` at line 93 already implements the range).
- `nodedb/src/control/rolling_upgrade/versions.rs` header comment (lines 10–29, "MIN == WIRE … constant-true" paragraph): rewrite for the open window; keep `DISTRIBUTED_CATALOG_VERSION` / `DESCRIPTOR_VERSIONING_VERSION` / `DESCRIPTOR_DRAIN_VERSION` pinned at `1` with the rule: _"bump a gate to the new WIRE value in the same PR that lands its wire-shape change."_
- `handle_join.rs` gate comment (lines 45–48 "require an exact match because floor == ceiling"): replaced by insert point C above.

---

## 3. Test plan

### 3.1 Mixed-version without two builds — inject constants

All three gates are pure/param-driven after this fix, so no second binary is needed:

| Concern         | Injection point                                                  | Technique                                                                     |
| --------------- | ---------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| Join admission  | `handle_join_request(..., min_wire_version)`                     | craft `JoinRequest { wire_version: v }` (plain struct) + explicit floor param |
| Cluster view    | `NodeInfo::new(...).with_wire_version(v)`                        | build topology with v1/v2 nodes, call `compute_from_topology`                 |
| Sync/RPC compat | `check_wire_compatibility(v)` / `wire_version_in_window(v, min)` | pure `u16` functions, any value                                               |

### 3.2 New/updated unit tests

`nodedb-cluster/src/bootstrap/handle_join.rs` (tests module):

- `wire_version_in_window_boundaries` — `(1,1)→true`, `(2,1)→true`, `(0,1)→false`, `(3,1)→false`.
- `accepts_joiner_at_floor_in_mixed_window` — min=1, `req.wire_version=1` → success; assert stamped `topology.get_node(2).wire_version == 1`.
- `rejects_joiner_below_effective_floor` — min=2, `req.wire_version=1` → reject, error contains `"outside accepted window"` and `"2..=2"`.
- `rejects_joiner_above_current` — min=1, `req.wire_version = CLUSTER_WIRE_FORMAT_VERSION + 1` → reject.
- `rejects_zero_wire_version` — min=1, v=0 → reject (preserves existing integration behavior).
- Existing 6 tests: add 5th arg.

`nodedb-cluster/src/topology/version_view.rs` (moved tests, now live): `mixed_version_n_minus_1` (v2/v2/v1 → mixed, spread ok), `unsupported_spread_detected`, `feature_gated_on_min_version`, `node_removal_recomputes`, plus new `window_from_topology_with_floor_v1` (nodes at 1 and 2 → `min_version == 1`, `can_activate_feature(2) == false`).

`nodedb/src/version.rs`:

- Keep `newer_version_rejected`; add `older_in_window_accepted` (guard `WIRE > MIN`: `check_wire_compatibility(MIN).is_ok()`); add `older_than_floor_rejected` (`check_wire_compatibility(0).is_err()`).

`nodedb/src/control/rolling_upgrade/versions.rs`:

- Replace `reject_older` (currently asserts `WIRE-1` rejected — **breaks once the window opens**) with `older_in_window_accepted` (guarded) + `older_than_floor_rejected` (`accept_message(0)`).

`nodedb-cluster-tests/tests/wire_version_handshake.rs`:

- Existing `handle_join_request_rejects_incompatible_wire_version` (wire_version=0) still passes; add sibling `handle_join_request_accepts_n_minus_1` (wire_version=1, min=1 → success) and `handle_join_request_rejects_newer` (wire_version=3 → reject). Update line 630 call with the 5th arg.

`bootstrap/join.rs` tests: `window_from_join_response` — empty nodes → local fallback; nodes [1,2] → `(1,2)`.

### 3.3 Optional (flagged, not required now)

- Two-binary integration: build nodedb twice with a `build.rs`/`env!` override for `WIRE_FORMAT_VERSION` (e.g. `NODEDB_WIRE_VERSION_OVERRIDE`), start v1 node, join v2 node, assert mixed view + gated feature stays in compat mode. List as post-1.0 CI follow-up.
- Fail-fast on version rejection (hardening): in `bootstrap/join.rs::try_join_once`, the `Ok(RaftRpc::JoinResponse(resp))` arm treats non-redirect rejections as retryable (outer loop re-runs `max_attempts` with backoff). A version mismatch never resolves by retry — detect `resp.error.contains("wire_version")` and return immediately from `try_join_once` (verify available `ClusterError` variant in `nodedb-cluster/src/error.rs`; add `VersionCompat` if absent).

---

## 4. Verification

1. `cargo build --workspace` clean.
2. `cargo test -p nodedb-types -p nodedb-cluster -p nodedb -p nodedb-cluster-tests` — all green; view tests now execute (not skipped).
3. Dangling link gone: `RUSTDOCFLAGS="-D rustdoc::broken_intra_doc_links" cargo doc -p nodedb-cluster --no-deps` passes.
4. No exact-equality gates left: `git grep -n '!= CLUSTER_WIRE_FORMAT_VERSION'` → zero hits (only `wire_version_in_window` range check remains).
5. MIN is live: `git grep -n MIN_WIRE_FORMAT_VERSION` → wire_version.rs (def), topology.rs (re-export), handle_join.rs (gate), nodedb/version.rs (compat), versions.rs (docs).
6. Knob enforced: unit test at leader flow level — catalog with `min_wire_version: 2` rejects a v1 joiner (`effective_min_wire_version` path), catalog with default 1 accepts.
7. Manual: `nodedb-cluster-tests` wire_version_handshake suite (harness) exercises real transport + join gate.
8. Flagged gap to verify post-1.0: the `restart()` bootstrap path loads persisted topology without re-stamping self `NodeInfo.wire_version` (old records stay at serde default 1 until re-join). Cosmetic pre-1.0; before the first real deployment upgrade, add self-re-stamp on restart (in `bootstrap_fn.rs` restart branch) so `min_version` converges and gates can flip.

---

## 5. Refactor suggestion (3 version systems → 1)

Currently three independent systems:

1. `WIRE_FORMAT_VERSION` / `MIN_WIRE_FORMAT_VERSION` (nodedb-types, `u16`) — cluster schema + feature gates.
2. `WireVersion` (nodedb-cluster::wire_version/types.rs, `u16`, `CURRENT = 2`) — transport envelope (0xc1 marker, range-negotiated [1,2]).
3. `RPC_FRAME_VERSION` (nodedb-cluster/src/rpc_codec/header.rs, private `u8 = 3`) — inner frame header byte.

Suggested consolidation:

- **Step 1**: fold #3 into #2 — the frame header's version byte duplicates what the outer versioned envelope already guarantees (negotiated per connection). Keep the byte as a magic/sanity marker, drop its "version" meaning.
- **Step 2**: move the `WireVersion` newtype into `nodedb-types` so transport + schema share one type/one file, with clearly named constants (`TRANSPORT_WIRE_VERSION` vs `SCHEMA_WIRE_VERSION`). Keep two numbers (they change at different rates: schema bumps gate features, transport bumps gate framing) but one type, one source file, and cross-referenced docs — after this fix both are `2`, which is exactly the kind of coincidence that needs a comment so nobody merges them by accident.

Out of scope for this fix; the transport handshake stays untouched.

---

## 6. Effort

| Item                                                            | Est          |
| --------------------------------------------------------------- | ------------ |
| 2.1 constants + doc rewrite                                     | 0.25d        |
| 2.3 gate + threading + call-site/test updates                   | 0.3d         |
| 2.4 view move + doc-link fix + shim                             | 0.25d        |
| 2.5 echo helper + log + tests                                   | 0.1d         |
| 2.6 knob enforcement (+optional config field)                   | 0.1d (+0.1d) |
| 2.7 stale docs + §3.2 test updates                              | 0.2d         |
| §4 verification                                                 | 0.2d         |
| Optional: fail-fast retry, two-binary CI test, restart re-stamp | +0.5–1d      |
| **Total (core)**                                                | **~1.5d**    |

One PR, no transport/handshake changes, no wire-layout changes (JoinResponse untouched in phase 1).

# Fix Plan — cluster_epoch Enforcement (fence stale-epoch peers)

Base: `origin/main` @ `54fe575c0`. All line numbers verified against `git show origin/main:<path>`.

---

## (1) Goal

`cluster_epoch` is currently **stamp-only**: `write_frame` stamps the local epoch on every outbound
frame (`rpc_codec/header.rs:41-46`), and `parse_frame` only _observes_ the inbound stamp via
`fetch_max` (`header.rs:112-114`). The module docs (`cluster_epoch.rs:12`) claim peers "stuck on a
strictly older epoch" are rejected — that rejection does not exist anywhere.

Make it true: **on every inbound rpc_codec frame, if `peer_epoch < local_epoch` and the RPC type is
not exempt (join handshake / ping-pong), reject with a typed `ClusterError::StalePeerEpoch`** at the
single decode chokepoint. This fences out peers that missed a topology transition (old leader in a
split brain, ghost node, node that missed a leadership-change bump) instead of letting their stale
frames into the raft/app layer.

Non-goals (explicitly out of scope): bootstrap_listener protocol (`bootstrap_listener/protocol.rs`),
mirror bootstrap handshake (`mirror/handshake.rs`), and SWIM — they use their own framing, not
`header::write_frame`, and are pre-membership/bootstrap channels that must stay open. Persistence
semantics of the epoch (leader-only, best-effort) also unchanged.

---

## (2) Per-file changes (exact)

### 2a. `nodedb-cluster/src/error.rs` — new typed variant

**Insert point**: immediately after the `UnsupportedWireVersion { ... }` variant (ends ~line 118,
right before `CircuitOpen`). Same family as `UnsupportedWireVersion` (wire-level frame rejection),
so it belongs next to it.

```rust
    /// A peer sent a frame stamped with a cluster epoch strictly older than
    /// the local high-water mark. The peer missed a topology transition and is
    /// fenced out: its frames are dropped at decode until it rejoins the
    /// cluster (join handshake and ping/pong frames are exempt).
    #[error(
        "stale cluster epoch: peer stamp {peer_epoch} < local {local_epoch} \
         (peer missed a topology transition and must rejoin)"
    )]
    StalePeerEpoch {
        peer_epoch: u64,
        local_epoch: u64,
    },
```

Fields: `peer_epoch: u64`, `local_epoch: u64` — both needed for operator diagnostics
(the only current visibility of a rejection is a `debug!` stream teardown log at
`transport/server.rs:210-211` and the client-side error at `send.rs`; the variant carries the
numbers so they land in logs even at debug level).

`ClusterError` derives `#[derive(Debug, Error)]` (no `PartialEq`, no `non_exhaustive`) — adding a
variant breaks nothing structurally. Verified: no exhaustive `match` on `ClusterError` exists in
`nodedb/src` (only `matches!` on specific variants, e.g.
`nodedb/src/control/cluster/array_cluster_exec/dispatch.rs:80`) and only `header.rs` +
`distributed_array/wire.rs` (doc comment) reference it inside the crate. Compile will confirm.

### 2b. `nodedb-cluster/src/cluster_epoch.rs` — the validation function

**Where**: top-level, next to the other public epoch functions (after
`observe_peer_cluster_epoch`, ~line 70). This module already owns the global atomic and its
serialized-test harness (`TEST_LOCK`), so the comparison logic + its unit tests live here.

**New imports** (top of file, after `use crate::error::Result;`):

```rust
use crate::error::{ClusterError, Result};
use crate::rpc_codec::discriminants::{RPC_JOIN_REQ, RPC_JOIN_RESP, RPC_PING, RPC_PONG};
```

(`discriminants.rs` is a const-only module — no cycle risk with `rpc_codec` importing
`cluster_epoch`; intra-crate module cycles are fine in Rust.)

**New code**:

```rust
/// RPC types exempt from the cluster-epoch fence.
///
/// * Join handshake (`RPC_JOIN_REQ` / `RPC_JOIN_RESP`): a joining or rejoining
///   node legitimately carries a zero or stale epoch — the join response is the
///   mechanism by which it learns (observes) the cluster's current epoch.
/// * Ping/pong (`RPC_PING` / `RPC_PONG`): the pre-join bootstrap probe
///   (`bootstrap/probe.rs`) pings the elected bootstrapper before joining, and
///   ping is the side-effect-free liveness channel a fenced peer needs in order
///   to be discovered and told to rejoin.
///
/// Everything else (raft consensus, topology, execute, shuffle, calvin,
/// surrogate, data/metadata propose, vshard envelopes) is fenced.
pub(crate) const EPOCH_EXEMPT_RPC_TYPES: &[u8] =
    &[RPC_JOIN_REQ, RPC_JOIN_RESP, RPC_PING, RPC_PONG];

/// Enforce the cluster-epoch fence on one inbound frame.
///
/// Rejects `peer_epoch < local` unless the RPC type is exempt. Called from
/// the decode path ([`crate::rpc_codec::header::parse_frame`]) for every
/// inbound rpc_codec frame, in both directions (server-side requests and
/// client-side responses).
///
/// `peer_epoch == 0` is *not* special-cased: against a local epoch of 0
/// (genesis / pre-init startup) it passes; against a local epoch > 0 it is
/// rejected exactly like any other stale stamp. The epoch check runs on
/// MAC-authenticated header bytes (the envelope MAC is verified before
/// decode in `transport/server.rs` and `transport/client/send.rs`), so a
/// spoofed stamp cannot trigger spurious rejections.
pub fn validate_peer_cluster_epoch(rpc_type: u8, peer_epoch: u64) -> Result<()> {
    if EPOCH_EXEMPT_RPC_TYPES.contains(&rpc_type) {
        return Ok(());
    }
    let local_epoch = LOCAL_CLUSTER_EPOCH.load(Ordering::Acquire);
    if peer_epoch < local_epoch {
        return Err(ClusterError::StalePeerEpoch {
            peer_epoch,
            local_epoch,
        });
    }
    Ok(())
}
```

Note on the existing observe: keep the `if peer_epoch > 0 { observe_peer_cluster_epoch(peer_epoch); }`
block (`header.rs:112-114`) exactly where it is — it runs only on the accept path, and observing a
rejected (stale) epoch would be a `fetch_max` no-op anyway, so no ordering change is needed.

### 2c. `nodedb-cluster/src/rpc_codec/header.rs` — call site

**Line 17**, extend the existing import:

```rust
use crate::cluster_epoch::{
    current_local_cluster_epoch, observe_peer_cluster_epoch, validate_peer_cluster_epoch,
};
```

**Insert point**: in `parse_frame`, immediately after the `peer_epoch` parse block
(lines 82-85), i.e. between the closing `]);` of the `u64::from_le_bytes` and line 86
(`if payload_len > MAX_RPC_PAYLOAD_SIZE`). Both `rpc_type` (line 79) and `peer_epoch` are in
scope at that point; it's the cheapest check after the version/size guards and it fails before the
CRC work.

```rust
    let peer_epoch = u64::from_le_bytes([
        data[10], data[11], data[12], data[13], data[14], data[15], data[16], data[17],
    ]);

    // Enforce the cluster-epoch fence. Join handshake + ping/pong frames are
    // exempt (see cluster_epoch::EPOCH_EXEMPT_RPC_TYPES); everything else from
    // a peer on a strictly older epoch is dropped here, before any dispatch.
    validate_peer_cluster_epoch(rpc_type, peer_epoch)?;
```

This is the **single enforcement point** for the whole transport: `rpc_codec::decode` →
`parse_frame` is the only path for every rpc_codec frame in both directions —
`transport/server.rs:276` (requests) and `:390` (shuffle-push stream frames), and
`transport/client/send.rs:297` + `:486` (responses). No changes needed in server/send; the new
`Err` propagates through the existing `?` paths (server: stream teardown with `debug!` log;
client: surfaced to the RPC caller as `ClusterError::StalePeerEpoch`).

### 2d. `nodedb-cluster/src/lib.rs` — export (consistency)

**Line 88-91**, extend the existing `pub use cluster_epoch::{...}` list:

```rust
pub use cluster_epoch::{
    bump_local_cluster_epoch, current_local_cluster_epoch, init_local_cluster_epoch_from_catalog,
    observe_peer_cluster_epoch, set_local_cluster_epoch, validate_peer_cluster_epoch,
};
```

(Needed only for integration-test use and API symmetry with the other four; the crate-internal
call in `header.rs` uses the `crate::` path directly.)

### 2e. Exemption mechanism — why rpc_type and not request context

The decode chokepoint runs **before** handler dispatch and before any per-peer state is consulted
uniformly (identity checks happen after decode at `server.rs:277-295`). A transport-level
"bootstrapping" flag would require plumbing join state into `AuthContext`/`StreamContext` and
touching both transport directions. The rpc_type exemption is stateless, testable, and covers both
directions automatically:

- **Joiner → leader** (`RPC_JOIN_REQ`, exempt): fresh node stamps epoch 0; a rejoining node stamps
  its stale persisted epoch. Leader accepts, admits, and its `JoinResponse` frame carries the
  leader's current epoch.
- **Leader → joiner** (`RPC_JOIN_RESP`, exempt): protects the split-brain-rejoin case where the
  joiner's _own_ mark (e.g. 9) is higher than the cluster it is rejoining (8) — without the
  exemption the joiner would reject the join response and be stranded.
- **Probe** (`RPC_PING`/`RPC_PONG`, exempt): `bootstrap/probe.rs` sends `RaftRpc::Ping` to the
  elected bootstrapper _before_ joining, from epoch 0 or a stale persisted epoch; the bootstrapper
  must answer `Pong` or the join path is degraded (probe failures merely fall through to `join()`,
  but a hard fenced ping is noise and removes the only side-effect-free "are you alive / you are
  stale" channel).

The joiner's **post-join epoch adoption needs no new code**: the `JoinResponse` frame header
carries the leader's epoch and `parse_frame`'s existing observe (`fetch_max`) advances the joiner's
mark on that very frame — so the joiner stamps the cluster epoch on all subsequent traffic.
(Verified: nothing else in the codebase sets the joiner's epoch — the only other call sites are
`raft_loop/builder.rs:292` init-from-catalog and `raft_loop/tick/apply_committed.rs:103,117-118`
bump-on-leadership.)

### 2f. Explicitly NO changes needed (verified)

- `raft_loop/handle_rpc/dispatch.rs`, `raft_loop/handle_rpc/membership.rs`, `raft_loop/join.rs`,
  `bootstrap/handle_join.rs`: enforcement is at decode, before dispatch; the join exemption is
  rpc_type-based, so `handle_join_request` / `join_flow` are untouched. `handle_rpc` never sees a
  stale frame.
- `transport/server.rs` / `transport/client/send.rs`: existing `?` propagation carries the new
  variant; the epoch check is on MAC-verified bytes (envelope MAC verified before decode in both).
- `bootstrap/restart.rs`: restart takes the join path (`JoinRequest` is exempt), re-observing the
  cluster epoch from the `JoinResponse`.

---

## (3) Test plan

### Unit — `cluster_epoch.rs` (add to existing `mod tests`, reusing `reset()` / `TEST_LOCK`)

1. `validate_rejects_stale_non_exempt` — `set_local_cluster_epoch(5)`; assert
   `validate_peer_cluster_epoch(RPC_APPEND_ENTRIES_REQ, 3)` is
   `Err(ClusterError::StalePeerEpoch { peer_epoch: 3, local_epoch: 5 })`; assert local epoch still 5.
2. `validate_accepts_equal_and_newer` — local 5; `validate(RPC_EXECUTE_REQ, 5)` Ok;
   `validate(RPC_EXECUTE_REQ, 6)` Ok.
3. `validate_genesis_zero_zero_ok` — local 0; `validate(RPC_APPEND_ENTRIES_REQ, 0)` Ok
   (pre-init startup must not reject).
4. `validate_exempts_join_and_ping` — local 9; `validate(RPC_JOIN_REQ, 0)` Ok,
   `validate(RPC_JOIN_RESP, 0)` Ok, `validate(RPC_PING, 0)` Ok, `validate(RPC_PONG, 0)` Ok.
5. `validate_rejects_stale_for_other_mgmt_types` — local 5; `validate(RPC_TOPOLOGY_ACK, 4)` Err;
   `validate(RPC_REQUEST_VOTE_RESP, 4)` Err (elections are fenced too).

### Unit — `header.rs` (add to existing `mod tests`, reusing `make_frame(payload, epoch)`)

6. `parse_frame_rejects_stale_epoch` — `set_local_cluster_epoch(7)`; frame with `rpc_type = 0xAB`
   (non-exempt), `epoch = 5` → `Err(StalePeerEpoch { peer_epoch: 5, local_epoch: 7 })`; assert
   `current_local_cluster_epoch() == 7` (observe skipped, no-op anyway); reset to 0.
7. `parse_frame_accepts_newer_epoch_and_observes` — local 3; frame epoch 9 → Ok; assert local == 9.
8. `parse_frame_join_and_ping_exempt` — local 9; frame `rpc_type = RPC_JOIN_REQ`, epoch 0 → Ok;
   frame `rpc_type = RPC_PING`, epoch 0 → Ok. (Build via the `make_frame` helper — it already
   takes `rpc_type` as a parameter today: hardcode `0xAB` → parameterize.)
9. Existing `v3_frame_round_trips_with_epoch` still passes: local 0 at parse time, peer stamp 7 →
   not stale → Ok + observed. Existing version-rejection tests unaffected (fail earlier).

### Integration (optional, medium effort)

`nodedb-cluster-tests` — extend the `bootstrap_listener_join` pattern: boot a 2-node cluster,
force one leader transition (so the cluster epoch ≥ 1), join a third node → join must succeed
(join exempt), then have the joiner send a non-exempt RPC (e.g. `DataProposeRequest`) → accepted,
proving the joiner adopted the cluster epoch from the `JoinResponse` frame. If skipped, unit tests
8 + the existing multi-node join tests in `nodedb-cluster-tests` still exercise the exemption
end-to-end (any stale-epoch join rejection would fail the existing join tests).

---

## (4) Verification

1. `cargo test -p nodedb-cluster cluster_epoch` — new + existing epoch tests.
2. `cargo test -p nodedb-cluster rpc_codec::header` — new decode-path tests.
3. `cargo test -p nodedb-cluster` — full crate (compilation proves no exhaustive
   `ClusterError` match missed an arm).
4. `cargo test -p nodedb-cluster-tests` — existing multi-node join/election/leader-transition
   integration tests must stay green; these are the regression net for the exemption (a fenced
   join would fail them).
5. `cargo clippy -p nodedb-cluster` — clean.
6. Behavioral spot-check (manual or ad-hoc test): 3-node cluster, kill the metadata leader →
   new leader bumps (`apply_committed.rs:103` log line "bumped cluster epoch on metadata-group
   leadership acquisition"). Resurrect old node on a stale persisted epoch → its non-join frames
   are rejected; log shows `StalePeerEpoch`; it rejoins cleanly and resumes traffic.
7. Grep check for pre-encoded/cached frames that could carry a stale stamp long-term:
   `git grep -n "encode(" nodedb-cluster/src/raft_loop | grep -v "encode_"` — confirm every
   outbound frame is encoded at send time (stamp read at encode), not from a long-lived cache.
   (Current code encodes per-send; this is a confirm-only step.)

---

## (5) Refactor suggestion (post-fix, optional)

The fix already puts enforcement in **one function** (`validate_peer_cluster_epoch`) at **one
chokepoint** (`parse_frame`). A follow-up refactor for cleanliness, _not required now_:

- Have `parse_frame` return the epoch (`Result<(u8, &[u8], u64)>`) and move the validation call up
  into `raft_rpc::decode` where RPC-type semantics already live (it already switches on
  `rpc_type`). Cost: churns 4 call sites (`server.rs:276,390`, `send.rs:297,486`) + header tests
  for zero behavioral gain. **Recommendation: don't.** The current design keeps header.rs the
  single authority over frame-level rejection (it already rejects on version/size/CRC there), and
  the exemption list lives in `cluster_epoch.rs` next to its unit tests.

Related latent observation (out of scope, note for a follow-up ticket): `bump_local_cluster_epoch`
uses `fetch_add(1)`; a split-brain rejoin with a higher joiner epoch is safe only because the
leader _observes_ the joiner's stamp (fetch_max) before the next leadership bump. Worth a comment
in `cluster_epoch.rs` documenting that observe-then-bump ordering invariant.

---

## (6) Effort

| Change                                             | Estimate |
| -------------------------------------------------- | -------- |
| 2a `ClusterError::StalePeerEpoch` variant          | 10 min   |
| 2b `validate_peer_cluster_epoch` + const + imports | 20 min   |
| 2c `header.rs` import + call                       | 5 min    |
| 2d `lib.rs` export                                 | 2 min    |
| Unit tests (2b/2c, ~9 new)                         | 40 min   |
| Full test + clippy + fix fallout                   | 1-1.5 h  |
| Optional integration test                          | 1-2 h    |

**Total: ~half a day without the integration test, ~1 day with it. S-sized change.**

### Risk notes

- **Transient false positives are by design and self-heal**: frames encoded before a bump that
  arrive after it get rejected; the sender converges on the next inbound frame carrying the bumped
  epoch (leader's heartbeat `AppendEntries` within one heartbeat interval). Raft is retry-based;
  no permanent liveness break.
- **Follow restarts with stale persisted epoch** (only the leader persists bumps): non-exempt
  outbound frames are rejected until the first inbound heartbeat observes the cluster epoch or the
  restart join path re-adopts it. One-heartbeat convergence; acceptable.
- **No spoofed-rejection DoS**: epoch bytes are inside the MAC-authenticated envelope, verified
  before decode.
- Old leader in a split brain is fenced immediately — this is the intended behavior of the fence
  token, but it is a behavior change for any cluster that currently tolerates split-brain traffic:
  ship with the `warn!`-level visibility (variant fields) and consider upgrading the stream-error
  log at `transport/server.rs:210` from `debug!` to `warn!` when the error is
  `StalePeerEpoch`, so operators actually see fence rejections.

# Fix Plan: Descriptor-Lease Crash-Wedge GC

Repo: `nodedb-rebase` @ `origin/main` = `54fe575c0`
Semua laluan/line-number dirujuk dari `origin/main` (read-only).

---

## 1. Goal

**Bug:** Node crash (kill -9, bukan SIGTERM) sambil memegang descriptor lease → rekod
`DescriptorLease` kekal dalam `MetadataCache.leases` (HashMap `(DescriptorId, node_id) -> DescriptorLease`)
pada **setiap** node, selama-lamanya. Tiada apa-apa dalam codebase yang prune rekod lease:
bukan TTL, bukan expiry check, bukan restart. Satu-satunya path yang membuang rekod ialah
`MetadataEntry::DescriptorLeaseRelease` (SIGTERM release / query-scope drop / shutdown_release).

Kesan: setiap DDL `Put*` pada descriptor itu menjalankan `drain_for_ddl` →
`poll_leases_drained` → `count_matching_leases` yang **hanya** filter
`lid == id && l.version <= up_to_version` (tiada expiry, tiada membership) →
wait 35s (`DEFAULT_DRAIN_TIMEOUT`) → timeout → ALTER/CREATE OR REPLACE gagal **kekal**
(availability gap; retry juga gagal, setiap kali burn 35s).

**Bukti dari kod (disahkan):**

- `nodedb/src/control/lease/drain_propose.rs:216-233` — `count_matching_leases` tiada filter expiry/membership.
- `nodedb-cluster/src/metadata_group/cache.rs:111-119` — satu-satunya mutation `leases` ialah
  `DescriptorLeaseGrant` (insert) dan `DescriptorLeaseRelease` (remove).
- `git grep 'leases.remove|leases.retain'` ke seluruh tree → tiada prune lain.
- `nodedb/src/control/lease/shutdown_release.rs:11-13` — dokumen kata "Leases then drain via TTL …
  same behavior as a crashed process", tetapi poller tidak pernah semak TTL → dakwaan itu salah;
  inilah wedge.

**Fix utama (3 bahagian):**

1. **Drain filter** — `poll_leases_drained` abaikan lease (a) pemegang bukan ahli topology,
   (b) sudah expired. Ini sahaja menukar wedge kekal → bounded (DDL lulus sebaik removal/expiry).
2. **GC trigger topologi** — bila `TopologyChange::Leave { node_id }` apply, propose
   `DescriptorLeaseRelease` untuk semua lease node itu (fire-and-forget, leader sahaja).
3. **Periodic sweep di metadata leader** — ikut corak `reconcile_placement` (leader-gated,
   throttle by tick) untuk cover kes di mana hook terlepas (crash sebelum Leave commit,
   node yang Leave-nya berlaku sebelum fix deploy, dsb.).

---

## 2. Per-file Changes (EXACT)

### 2.1 `nodedb/src/control/lease/drain_propose.rs` — filter bukan-ahli + expired

**Fungsi:** `fn count_matching_leases(shared: &SharedState, id: &DescriptorId, up_to_version: u64) -> usize`
**Lokasi:** baris ~216-233 (modul root, bukan dalam tests).

**Ganti body filter:**

```rust
fn count_matching_leases(shared: &SharedState, id: &DescriptorId, up_to_version: u64) -> usize {
    let now = shared.hlc_clock.peek();
    let cache = shared
        .metadata_cache
        .read()
        .unwrap_or_else(|p| p.into_inner());
    let metadata_holds = cache
        .leases
        .iter()
        .filter(|((lid, holder), l)| {
            lid == id
                && l.version <= up_to_version
                && l.expires_at > now
                && lease_holder_is_member(shared, *holder)
        })
        .count();
    drop(cache);

    if shared.lease_refcount.current_at_or_below(id, up_to_version) == 0 {
        metadata_holds
    } else {
        metadata_holds.saturating_add(1)
    }
}

/// Whether `node_id` is a current cluster member. Missing topology
/// (single-node / belum di-wire) treats every holder as member —
/// fail-safe: filter ini hanya PERNAH membuang hold yang ia pasti.
fn lease_holder_is_member(shared: &SharedState, node_id: u64) -> bool {
    match &shared.cluster_topology {
        Some(topo) => topo
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .contains(node_id),
        None => true,
    }
}
```

Nota: `cluster_topology` field wujud di `nodedb/src/control/state/fields.rs:103`
(`Option<Arc<RwLock<nodedb_cluster::ClusterTopology>>>`); `ClusterTopology::contains(node_id)`
wujud di `nodedb-cluster/src/topology.rs:255`. Expired-filter selaras dengan semantik grant
(`propose.rs:59-61` treat lease expired sebagai tiada).

### 2.2 `nodedb/src/control/lease/release.rs` — generalize release untuk node lain

**Fungsi sedia ada:** `impl LeaseReleaseHandle { fn release_raw(&self, descriptor_ids: Vec<DescriptorId>) }`
(baris ~76-113) — hard-coded `node_id: self.node_id`.

**Insert point:** dalam `impl LeaseReleaseHandle`, selepas `release_raw`. Refactor `release_raw`
jadi delegate ke helper baru:

```rust
/// Release leases held by an ARBITRARY node. Used by lease GC for
/// nodes that left the topology (crashed/decommissioned). Does NOT take
/// `grant_gate` (no contention with local grants — the foreign holder
/// cannot grant anymore).
pub(crate) fn release_for_node(
    &self,
    node_id: u64,
    descriptor_ids: Vec<DescriptorId>,
) -> Result<(), Error> {
    self.release_raw_for_node(node_id, descriptor_ids)
}

/// Raw metadata release for `node_id`. `release_raw` keeps its gate-taking
/// wrapper for the self path; this is the ungated core.
fn release_raw_for_node(
    &self,
    node_id: u64,
    descriptor_ids: Vec<DescriptorId>,
) -> Result<(), Error> {
    if descriptor_ids.is_empty() {
        return Ok(());
    }

    let Some(metadata_raft) = &self.metadata_raft else {
        // Single-node fallback: hanya meaningful untuk self.
        if node_id == self.node_id {
            let mut cache = self
                .metadata_cache
                .write()
                .unwrap_or_else(|poison| poison.into_inner());
            for id in descriptor_ids {
                cache.leases.remove(&(id, node_id));
            }
        }
        return Ok(());
    };

    let entry = MetadataEntry::DescriptorLeaseRelease {
        node_id,
        descriptor_ids,
    };
    let raw = nodedb_cluster::encode_entry(&entry).map_err(|error| Error::Config {
        detail: format!("descriptor lease release encode: {error}"),
    })?;
    let log_index = metadata_raft.propose(raw)?;
    let outcome = self
        .applied_watcher
        .wait_for(log_index, super::PROPOSE_TIMEOUT);
    if !outcome.is_reached() {
        return Err(Error::Config {
            detail: format!(
                "descriptor lease release did not apply within {:?} \
                 (log index {log_index}, current: {}, outcome: {outcome:?})",
                super::PROPOSE_TIMEOUT,
                self.applied_watcher.current()
            ),
        });
    }
    Ok(())
}
```

Dan ubah `release_raw` sedia ada untuk panggil `self.release_raw_for_node(self.node_id, descriptor_ids)`
(pelihara tingkah laku + gate sedia ada).

### 2.3 BARU: `nodedb/src/control/lease/gc.rs` — koleksi + proposer GC

```rust
// SPDX-License-Identifier: BUSL-1.1

//! Descriptor lease garbage collection for nodes that left the cluster.
//!
//! A crashed node's leases are never TTL-pruned from `MetadataCache.leases`
//! (only a `DescriptorLeaseRelease` entry removes them), so every DDL drain
//! on those descriptors times out forever. Two triggers run this module:
//! the `TopologyChange::Leave` apply hook (immediate) and the metadata
//! leader's periodic sweep (safety net).

use nodedb_cluster::DescriptorId;

use crate::control::lease::release::LeaseReleaseHandle;
use crate::control::state::SharedState;

/// Collect `(node_id, descriptor_ids)` for every lease holder that is no
/// longer a cluster member. Missing topology → empty (never GC on guesswork).
pub(crate) fn collect_non_member_leases(
    shared: &SharedState,
) -> Vec<(u64, Vec<DescriptorId>)> {
    let Some(topo) = &shared.cluster_topology else {
        return Vec::new();
    };
    let cache = shared
        .metadata_cache
        .read()
        .unwrap_or_else(|p| p.into_inner());
    let topo = topo.read().unwrap_or_else(|p| p.into_inner());

    let mut by_holder: std::collections::HashMap<u64, Vec<DescriptorId>> =
        std::collections::HashMap::new();
    for (id, holder) in cache.leases.keys() {
        if !topo.contains(*holder) {
            by_holder.entry(*holder).or_default().push(id.clone());
        }
    }
    let mut out: Vec<(u64, Vec<DescriptorId>)> = by_holder.into_iter().collect();
    out.sort_by_key(|(node_id, _)| *node_id);
    out
}

/// Propose `DescriptorLeaseRelease` for every lease held by `node_id`.
/// No-op if the cache has no entries for that node (idempotent vs. the
/// periodic sweep). Blocks on the local applied watermark like the
/// normal release path; callers on hot paths must spawn this.
pub(crate) fn gc_leases_for_node(
    shared: &SharedState,
    node_id: u64,
) -> Result<(), crate::Error> {
    let ids: Vec<DescriptorId> = {
        let cache = shared
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        cache
            .leases
            .keys()
            .filter(|(_, holder)| *holder == node_id)
            .map(|(id, _)| id.clone())
            .collect()
    };
    if ids.is_empty() {
        return Ok(());
    }
    LeaseReleaseHandle::from_shared(shared).release_for_node(node_id, ids)
}
```

### 2.4 `nodedb/src/control/lease/mod.rs` — daftar modul + export

Tambah selepas `pub mod drain;` (baris ~29):

```rust
pub mod gc;
```

(Export terhad `pub(crate)` sudah cukup; jadikan `pub` hanya jika dipanggil dari
`metadata_applier` — ia berada dalam crate yang sama, jadi `pub(crate)` memadai.)

### 2.5 `nodedb/src/control/cluster/metadata_applier/dispatch.rs` — topology-change hook

**Fungsi:** `impl MetadataCommitApplier { pub(super) fn apply_host_side_effects(...) }`
**Insert point:** dalam `match entry { ... }`, TEPAT selepas arm
`MetadataEntry::RoutingChange(RoutingChange::SetPlacement { ... })` (baris ~243-249),
SEBELUM `_ => {}`:

```rust
            MetadataEntry::TopologyChange(TopologyChange::Leave { node_id }) => {
                // Lease GC: a node that left the topology can never release
                // its own leases. Spawn (do NOT propose-and-wait inline —
                // apply runs on the raft loop task; blocking here would
                // deadlock the applied-index watcher).
                if let Some(shared) = self.shared.get().and_then(std::sync::Weak::upgrade) {
                    let shared = std::sync::Arc::clone(&shared);
                    tokio::spawn(async move {
                        if !shared.is_singleton_worker() {
                            return;
                        }
                        if let Err(e) =
                            crate::control::lease::gc::gc_leases_for_node(&shared, *node_id)
                        {
                            tracing::warn!(
                                node_id,
                                error = %e,
                                "lease GC after Leave failed; periodic sweep will retry"
                            );
                        }
                    });
                }
                return Ok(());
            }
```

Import: tambah `TopologyChange` — baris import sekarang:
`use nodedb_cluster::{MetadataApplier, MetadataEntry, RoutingChange, decode_entry};`
→ `use nodedb_cluster::{MetadataApplier, MetadataEntry, RoutingChange, TopologyChange, decode_entry};`

Nota `node_id`: `match entry` borrows `&MetadataEntry`; arm pattern `Leave { node_id }`
mengikat `&u64` — guna `*node_id` (seperti arm lain dalam fail ini).

**Kenapa gated pada `is_singleton_worker()`:** `apply_host_side_effects` jalan pada SEMUA node;
tanpa gate, setiap node akan propose release duplikat. `is_singleton_worker()`
(`nodedb/src/control/state/methods.rs:81`) = `metadata_raft.is_none() || is_metadata_leader()`.
Duplikat pun sebenarnya idempotent (release key tak wujud = no-op) — gate ini cuma kurangkan
log noise.

**Kenapa spawn task, bukan inline:** `apply` dipanggil dalam `do_tick` pada task raft loop
(`raft_loop/tick/core.rs` → `apply_group_commits`). `release_for_node` → `wait_for(applied_index)`
menunggu progress yang hanya dihasilkan oleh task raft loop itu sendiri → deadlock kalau inline.
Corak spawn-side-effect memang sedia digunakan oleh applier ini (doc `types.rs:32-35`).

### 2.6 BARU: `nodedb-cluster/src/raft_loop/lease_gc.rs` — periodic sweep di leader (ikut corak `reconcile_placement`)

Corak rujukan: `nodedb-cluster/src/raft_loop/placement_reconcile.rs:28-70`
(`group_role_is_leader(METADATA_GROUP_ID)` → baca `self.topology.read()` dalam `mr` lock →
`propose_to_metadata_group(bytes)` selepas semua guard drop).

```rust
// SPDX-License-Identifier: BUSL-1.1

//! Periodic lease GC on the metadata-group leader.
//!
//! Mirrors `placement_reconcile`: leader-gated, throttled by tick count in
//! `tick::core::do_tick`. Sweeps `MetadataCache.leases` and proposes
//! `DescriptorLeaseRelease` for every holder no longer in the cluster
//! topology. This is the safety net behind the Leave apply hook.

use std::collections::HashMap;
use tracing::{debug, warn};

use crate::forward::PlanExecutor;
use crate::metadata_group::descriptors::DescriptorId;

use super::loop_core::{CommitApplier, RaftLoop};

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// On the metadata-group leader, propose `DescriptorLeaseRelease` for
    /// every lease whose holder is no longer in `ClusterTopology`.
    pub(super) fn gc_stale_node_leases(&self) {
        let Some(cache) = &self.metadata_cache else {
            return; // not wired (some tests) — nothing to sweep
        };
        let to_release: Vec<(u64, Vec<DescriptorId>)> = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            if !mr.group_role_is_leader(crate::metadata_group::METADATA_GROUP_ID) {
                return;
            }
            let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
            let cache = cache.read().unwrap_or_else(|p| p.into_inner());
            let mut by_holder: HashMap<u64, Vec<DescriptorId>> = HashMap::new();
            for (id, holder) in cache.leases.keys() {
                if !topo.contains(*holder) {
                    by_holder.entry(*holder).or_default().push(id.clone());
                }
            }
            by_holder.into_iter().collect()
        };

        for (node_id, descriptor_ids) in to_release {
            let entry =
                crate::metadata_group::entry::MetadataEntry::DescriptorLeaseRelease {
                    node_id,
                    descriptor_ids,
                };
            let bytes = match crate::metadata_group::codec::encode_entry(&entry) {
                Ok(b) => b,
                Err(e) => {
                    warn!(node_id, error = %e, "lease GC: encode DescriptorLeaseRelease failed");
                    continue;
                }
            };
            match self.propose_to_metadata_group(bytes) {
                Ok(idx) => debug!(
                    node_id,
                    log_index = idx,
                    "lease GC: released leases of non-member node"
                ),
                Err(e) => debug!(node_id, error = %e, "lease GC: proposal deferred"),
            }
        }
    }
}
```

Lock discipline: `mr` → `topology.read()` → `cache.read()` nested, sama disiplin
`mr → routing` yang didokumenkan dalam `reconcile_placement`.

### 2.7 `nodedb-cluster/src/raft_loop/loop_core.rs` — field baru

**Insert point:** dalam struct `RaftLoop<A, P>` field definitions, berdekatan
`pub(super) topology` / `pub(super) partial_snapshots` (sekitar baris 240-285):

```rust
    /// Optional handle to the metadata group's `MetadataCache` (the same
    /// `Arc` the `CacheApplier` writes into). When set, the leader's
    /// periodic lease-GC sweep can read committed lease state directly.
    /// `None` in cluster-only tests that don't wire it.
    pub(super) metadata_cache: Option<Arc<RwLock<MetadataCache>>>,
```

Dan dalam `RaftLoop::new(...)` initializer (selepas `topology,` baris ~297):

```rust
            metadata_cache: None,
```

Import: `use crate::metadata_group::cache::MetadataCache;` (cek import sedia ada
di header loop_core.rs; `RwLock` + `Arc` sudah diimport).

### 2.8 `nodedb-cluster/src/raft_loop/builder.rs` — builder setter

**Insert point:** berdekatan `with_metadata_applier` (baris ~257-262):

```rust
    /// Wire the metadata cache used by the periodic lease-GC sweep. The
    /// host passes the same `Arc` the production metadata applier holds.
    pub fn with_metadata_cache(mut self, cache: Arc<RwLock<MetadataCache>>) -> Self {
        self.metadata_cache = Some(cache);
        self
    }
```

### 2.9 `nodedb-cluster/src/raft_loop/tick/core.rs` — const + panggilan throttle

**Insert point 1 (const):** selepas `const ORPHAN_PARTIAL_GC_TICK_INTERVAL: u64 = 6000;`
(baris ~59):

```rust
/// Lease GC for nodes that left the topology: every 200 ticks (~2s at the
/// 10ms default tick). Cheaper than placement reconcile (single map scan),
/// and the Leave apply hook usually beats it to the release — this sweep
/// only catches cases the hook missed.
const LEASE_GC_TICK_INTERVAL: u64 = 200;
```

**Insert point 2 (call):** dalam `do_tick`, SELEPAS blok `ORPHAN_PARTIAL_GC_TICK_INTERVAL`
(selepas baris ~172, tempat `if tick.is_multiple_of(...) { ... }` blok orphan GC berakhir):

```rust
        if tick.is_multiple_of(LEASE_GC_TICK_INTERVAL) {
            self.gc_stale_node_leases();
        }
```

### 2.10 `nodedb-cluster/src/raft_loop/mod.rs` — daftar modul

Tambah `mod lease_gc;` dalam senarai `mod` sedia ada (sebelah `mod placement_reconcile;`).

### 2.11 `nodedb/src/control/cluster/start_raft/loop_build.rs` — wire cache (host)

**Insert point:** selepas `.with_metadata_applier(metadata_applier)` (baris 95):

```rust
        .with_metadata_cache(shared.metadata_cache.clone())
```

(`shared.metadata_cache` ialah `Arc<RwLock<nodedb_cluster::MetadataCache>>` —
`nodedb/src/control/state/fields.rs:111`; instance yang sama diberikan kepada
`MetadataCommitApplier::new` di `types.rs:43`.)

### 2.12 (Jika mahu sweep juga berjalan dalam harness test) `nodedb-test-support/src/cluster_harness/...`

Cari tempat harness build `RaftLoop` (bringup) dan wire `with_metadata_cache` dengan
cache yang sama digunakan oleh applier harness. **Jika tidak diwire**, sweep cluster-side
tidak berjalan dalam test integrasi — test GC cluster boleh sama ada (a) wire ini, atau
(b) panggil `gc_leases_for_node`/`collect_non_member_leases` (host-side) secara langsung
dari test. Pilihan (a) lebih jujur kepada production path.

---

### Ringkasan senarai fail

| #    | Fail                                                      | Jenis | Apa                                                            |
| ---- | --------------------------------------------------------- | ----- | -------------------------------------------------------------- |
| 2.1  | `nodedb/src/control/lease/drain_propose.rs`               | edit  | filter non-member + expired dalam `count_matching_leases`      |
| 2.2  | `nodedb/src/control/lease/release.rs`                     | edit  | generalize `release_raw` → `release_raw_for_node(node_id, ..)` |
| 2.3  | `nodedb/src/control/lease/gc.rs`                          | BARU  | `collect_non_member_leases` + `gc_leases_for_node`             |
| 2.4  | `nodedb/src/control/lease/mod.rs`                         | edit  | `pub mod gc;`                                                  |
| 2.5  | `nodedb/src/control/cluster/metadata_applier/dispatch.rs` | edit  | arm `TopologyChange::Leave` → spawn GC (leader-gated)          |
| 2.6  | `nodedb-cluster/src/raft_loop/lease_gc.rs`                | BARU  | sweep periodik leader, corak `reconcile_placement`             |
| 2.7  | `nodedb-cluster/src/raft_loop/loop_core.rs`               | edit  | field `metadata_cache`                                         |
| 2.8  | `nodedb-cluster/src/raft_loop/builder.rs`                 | edit  | `with_metadata_cache`                                          |
| 2.9  | `nodedb-cluster/src/raft_loop/tick/core.rs`               | edit  | const `LEASE_GC_TICK_INTERVAL` + panggilan                     |
| 2.10 | `nodedb-cluster/src/raft_loop/mod.rs`                     | edit  | `mod lease_gc;`                                                |
| 2.11 | `nodedb/src/control/cluster/start_raft/loop_build.rs`     | edit  | wire `with_metadata_cache(shared.metadata_cache.clone())`      |
| 2.12 | `nodedb-test-support/...`                                 | edit  | wire cache dalam harness (untuk test integrasi)                |

**Edge cases yang dipertimbangkan:**

- **Race grant in-flight vs GC:** grant lama dari node mati yang commit antara Leave dan
  release GC akan tetap dibuang (release ter-order selepasnya dalam log); grant yang commit
  selepas release mustahil (holder sudah keluar topology, dan drain filter abaikan node
  bukan-ahli walau apa pun) → **filter (2.1) ialah safety primer, GC ialah hygiene**.
- **`FinishDecommission` vs `Leave`:** node `Decommissioned` masih dalam topology (masih
  "ahli" mengikut `contains()`) sehingga `Leave` apply. Hook + sweep berfungsi pada `Leave`.
  _Keputusan:_ boleh juga tambah arm `FinishDecommission` pada hook (node decommissioned
  tidak akan plan lagi) — tetapi bukan wajib untuk fix; cadang sertakan sekiranya mahu
  bounded-gap lebih awal semasa decommission yang perlahan.
- **Idempotency:** dua trigger (hook + sweep) boleh propose release duplikat — no-op pada
  cache, log noise sahaja. Kedua-dua path semak dulu `ids.is_empty()`.
- **Single-node mode:** `metadata_raft` None → `release_for_node` no-op untuk node asing;
  hook gated `is_singleton_worker()`; sweep cluster-side tidak relevan (tiada group).

---

## 3. Test Plan

### 3.1 Unit tests — `nodedb/src/control/lease/drain_propose.rs` (modul `tests` sedia ada)

Infra sedia ada: test membina `SharedState` via `SharedState::new(dispatcher, wal)`
(digunakan oleh `in_flight_admission_reservation_blocks_drain_count`, baris ~277).
Tambahan baru (copy pattern):

- `non_member_lease_does_not_block_drain_count` — set `state.cluster_topology = Some(Arc::new(RwLock::new(topo)))`
  dengan topo tanpa node 99; masukkan lease holder=99 terus ke cache
  (`state.metadata_cache.write().leases.insert((id.clone(), 99), lease)`);
  assert `count_matching_leases(&state, &descriptor, 1) == 0`.
- `expired_lease_does_not_block_drain_count` — lease holder=1 (ahli) tapi
  `expires_at` < `hlc_clock.peek()`; assert `0`.
- `member_unexpired_lease_still_blocks_drain_count` — holder ahli + belum expired; assert `1`.
- Bina `ClusterTopology` macam test decommission: `ClusterTopology::new()` +
  `add_node(NodeInfo::new(id, addr, NodeState::Active))`
  (rujuk `nodedb-cluster/src/decommission/flow.rs` tests, baris ~115-125).

### 3.2 Unit tests — `nodedb/src/control/lease/gc.rs` (modul `#[cfg(test)]` BARU)

- `collect_non_member_leases_returns_only_foreign_holders` — cache dengan lease
  holder 1 (ahli) + holder 2 (tiada); assert hasil == `[(2, vec![id])]`.
- `collect_non_member_leases_empty_without_topology` — `cluster_topology = None`; assert kosong.
- `gc_leases_for_node_proposes_descriptor_lease_release` — guna fake `MetadataRaftHandle`
  (corak `RecordingProposer` dalam `nodedb-cluster/src/decommission/coordinator.rs` tests,
  baris ~180-200); assert entry yang di-propose ==
  `DescriptorLeaseRelease { node_id: 2, descriptor_ids }` dan watcher return reached.
- `gc_leases_for_node_noop_when_no_entries` — node tanpa lease; assert tiada propose.

### 3.3 Unit tests — `nodedb-cluster/src/raft_loop/lease_gc.rs` (modul `#[cfg(test)]`)

- `gc_stale_node_leases_proposes_for_non_members_only` — `RaftLoop` test-mini (corak
  placement_reconcile tests jika ada; jika tiada, test `plan`-style: extract
  `collect` logic jadi pure helper `collect_non_member_lease_releases(topology, cache)`
  supaya testable tanpa loop penuh — **cadang extract helper pure begini** untuk
  memudahkan test, sama corak `plan_entering_learners` yang pure di
  `raft_loop/membership_convergence.rs`).
- `gc_is_noop_when_all_holders_are_members`.

### 3.4 Cluster test — BARU `nodedb-cluster-tests/tests/descriptor_lease_gc.rs`

Test support sedia ada (rujuk, bukan tulis baru):

- `nodedb-test-support/src/cluster_harness/node/inspect/lease.rs` — `lease_count()`,
  `active_lease_count()`, `has_lease(kind, tenant, name, holder_node_id, min_version)`,
  `leases_for_descriptor(...)`, `has_drain_for(...)`.
- `nodedb-test-support/src/cluster_harness/cluster/membership.rs` — join/leave helpers.
- `nodedb-cluster-tests/tests/decommission_flow.rs:72`
  `end_to_end_decommission_drains_node_and_signals_shutdown` — corak propose-plan +
  wait convergence.
- `nodedb-cluster-tests/tests/descriptor_lease_drain.rs` — corak acquire/release + drain
  (rujuk `drain_blocks_new_acquires_at_drained_version`:34, `ddl_waits_for_existing_lease_to_release`:164,
  `drain_timeout_clears_state`:282).
- `nodedb-cluster-tests/tests/descriptor_lease_cross_node.rs` — corak lease dipegang node lain.

Test-test:

1. **`crashed_node_lease_gc_after_removal`** — 3-node cluster; node 2 acquire lease
   (guna `shared.acquire_descriptor_lease` pada node 2 atau harness API); matikan node 2
   secara keras (elak path SIGTERM release — abort task handle; teardown internal guna
   `h.abort()` — `nodedb-test-support/.../lifecycle/teardown.rs:69-107`); propose
   `TopologyChange::Leave { node_id: 2 }` (atau `plan_full_decommission(2, ...)`); wait
   sehingga SEMUA node `lease_count()` berkenaan descriptor itu == 0 (guna harness `wait::`
   helper + `has_lease(..., holder_node_id=2, ...) == false`). **Assert juga log index /
   cache konsisten** — release kena commit, bukan sekadar filter.
2. **`drain_ignores_non_member_leases`** — selepas Leave (atau simulasi: masukkan lease
   holder bukan-ahli terus ke cache pada semua node), panggil `drain_for_ddl(...)`
   (atau DDL ALTER sebenar) dengan `max_wait = 2s`; assert `Ok(())` cepat (≪ 35s).
3. **`wedge_becomes_bounded_success`** — repro wedge: node 2 pegang lease → crash →
   remove dari topology → ALTER pada descriptor itu mesti LULUS dalam << 35s
   (sebelum fix: timeout 35s + error `drain timed out`). Assert query/DML selepas DDL
   berjaya (planner dapat lease versi baru).

### 3.5 Regression — pastikan tidak pecah

- `cargo test -p nodedb-cluster-tests --test descriptor_lease_drain` — semua test sedia
  ada mesti hijau (esp. `drain_timeout_clears_state`, `ddl_waits_for_existing_lease_to_release`).
- `cargo test -p nodedb-cluster-tests --test decommission_flow` — decommission path tak terjejas.
- `cargo test -p nodedb-cluster-tests --test descriptor_lease_cross_node` —
  renewal/forwarding tak terjejas oleh filter membership (holder ahli mesti masih dikira).

---

## 4. Verification

```bash
# 1. Unit host-side
cargo test -p nodedb --lib control::lease::drain_propose
cargo test -p nodedb --lib control::lease::gc
cargo test -p nodedb --lib control::lease::release

# 2. Unit cluster-side
cargo test -p nodedb-cluster --lib raft_loop::lease_gc
cargo test -p nodedb-cluster --lib metadata_group::cache

# 3. Integrasi (bounded-wedge proof)
cargo test -p nodedb-cluster-tests --test descriptor_lease_gc -- --nocapture
cargo test -p nodedb-cluster-tests --test descriptor_lease_drain
cargo test -p nodedb-cluster-tests --test decommission_flow
cargo test -p nodedb-cluster-tests --test descriptor_lease_cross_node

# 4. Hygiene
cargo fmt --check
cargo clippy -p nodedb -p nodedb-cluster --all-targets

# 5. Manual smoke (bila ada env cluster 3-node):
#    - node2: acquire lease pada collection X, kill -9
#    - decommission node2 → ALTER COLLECTION X ... mesti lulus < 5s
#    - semak route debug leases: nodedb/src/control/server/http/routes/cluster_debug/leases.rs
#      lease_count turun ke 0 untuk holder node2
```

Kriteria selesai:

- [ ] `count_matching_leases` abaikan lease holder bukan-ahli DAN lease expired (test 3.1).
- [ ] `TopologyChange::Leave` mencetuskan release (test 3.4.1) — bukan sekadar filter.
- [ ] Sweep periodik leader propose release tanpa Leave hook (simulasi: masukkan lease
      asing, biarkan 200+ tick, assert hilang).
- [ ] Test 3.4.3 lulus: wedge → success bounded (regression kekal yang paling penting).
- [ ] Tiada deadlock pada apply path (hook guna spawn; release path guna
      `PROPOSE_TIMEOUT` 5s bounding).

---

## 5. Refactor Suggestion: `LeaseManager` (konsolidasi 9 fail)

Hari ini logik lease berselerak dalam `nodedb/src/control/lease/` (9 fail + `methods_lease.rs`
di `state/`), setiap fail memegang fungsi longgar + gate-gate berasingan
(`lease_admission_gate`, `lease_grant_gate`, `lease_refcount`, `lease_drain`,
`LeaseReleaseHandle` yang menyalin 5 field dari `SharedState`):

- `propose.rs` — acquire + `force_refresh_lease` + `ensure_not_draining`
- `release.rs` — `LeaseReleaseHandle` (salinan handle) + `release_leases`
- `drain.rs` — `DescriptorDrainTracker`
- `drain_propose.rs` — `drain_for_ddl` + `poll_leases_drained` + implicit-clear helpers
- `refcount.rs` — `LeaseRefCount` + `QueryLeaseScope`
- `renewal.rs` — `LeaseRenewalLoop`
- `shutdown_release.rs` — SIGTERM release
- `wall_time.rs` — helper waktu
- `gc.rs` (BARU dari fix ini) — GC
- `state/methods_lease.rs` — facade `SharedState` (acquire_descriptor_lease,
  release_descriptor_leases, acquire_plan_lease_scope)

**Cadangan:** satu struct `LeaseManager` yang MEMILIKI semua handle yang kini disalin
(`node_id`, `metadata_cache`, `metadata_raft`, `applied_watcher`, `lease_grant_gate`,
`lease_admission_gate`, `lease_refcount`, `lease_drain`) dan mendedahkan satu API:

```rust
pub struct LeaseManager { /* semua Arc/Mutex di atas */ }
impl LeaseManager {
    pub fn acquire(&self, id, version, duration) -> Result<DescriptorLease>;
    pub fn renew(&self, id, version, duration) -> Result<DescriptorLease>;
    pub fn release(&self, ids) -> Result<()>;                 // self
    pub fn release_for_node(&self, node_id, ids) -> Result<()>; // GC / foreign
    pub fn drain_for_ddl(&self, id, up_to_version, max_wait) -> Result<()>;
    pub fn gc_non_member_leases(&self) -> usize;              // collect + release
    fn holder_is_member(&self, node_id) -> bool;              // single predicate,
                                                              // dikongsi filter + GC
}
```

- `SharedState.lease: LeaseManager` (gantikan field berasingan, kekalkan field lama
  sebagai `pub(crate)` shim sementara untuk elak sentuh ratusan call site sekaligus).
- Loop (`LeaseRenewalLoop`, loop GC baharu) jadi kaedah async pada manager atau module
  `loops.rs` kecil.
- Layout sasaran: `mod.rs` (manager + exports), `grant.rs`, `release.rs`, `drain.rs`,
  `refcount.rs`, `gc.rs`, `loops.rs`, `wall_time.rs` — turun dari 9 fail → ~7 dan hapus
  kelas bug "gate terlupa dipegang" (semua entry point melalui manager).
- **Do this AS FOLLOW-UP, bukan dalam PR fix ini** — fix crash-wedge mesti kecil dan
  cepat review; refactor ini risiko besar (lock order, async/sync boundary) dan patut
  ada test harness penuh.

---

## 6. Effort

| Kerja                                                                 | Anggaran            |
| --------------------------------------------------------------------- | ------------------- |
| 2.1 drain filter + unit tests                                         | 0.5 hari            |
| 2.2–2.4 release generalize + `gc.rs` + unit tests                     | 0.5 hari            |
| 2.5 topology hook (dispatch) + spawn                                  | 0.25 hari           |
| 2.6–2.10 cluster-side sweep (loop_core/builder/tick/mod) + unit tests | 0.5 hari            |
| 2.11–2.12 wiring host + harness                                       | 0.25 hari           |
| 3.4 cluster tests (3 test baru, corak sedia ada)                      | 1 hari              |
| Review + polish (clippy, fmt, doc)                                    | 0.5 hari            |
| **Jumlah**                                                            | **~3.5 hari (S/M)** |
| Refactor LeaseManager (fasa 2)                                        | 2–3 hari berasingan |

**Urutan implement disyorkan:** 2.1 (filter — kesan terbesar, risiko terendah) → 2.2–2.4
(GC API) → 2.5 (hook) → 2.6–2.12 (sweep) → 3.1–3.3 → 3.4 → 4.

---

# Fix Plan 5 — Combined Integration Test (GLM R11)

Selepas semua 4 fix land, satu integration test gabungan membuktikan interaksi selamat:

```
Scenario: "rolling upgrade + crash + epoch bump + lease GC"
1. Cluster 3-node v1 (simulasi via injectable WIRE_FORMAT_VERSION constant)
2. Node A ambil descriptor lease + SWIM join penuh
3. Kill -9 node A (crash, bukan graceful) → peers tandakan Dead(N)
4. Epoch bump (metadata leadership transfer ke node B) → node C stale
5. Upgrade node A ke v2 + restart (persisted incarnation N+1 → rejoin SWIM terus dominate)
6. Fence: node C hantar RPC lama → StalePeerEpoch → auto-rejoin → fence lift
7. Lease GC: SWIM Dead hook + topology change release lease node A yang lama
8. ALTER collection → mesti lulus < 5s (tiada drain-wedge)
9. Akhir: assert semua node converge — SWIM Alive semua, topology konsisten, cluster view single window, 0 lease orphan
```

Lokasi: `nodedb-cluster-tests/tests/combined_safety.rs` (atau modul dalam harness sedia ada). Effort: +1 hari.

## Final Sequencing (GLM R7)

```
Fix 1 (SWIM)     → 2.5–3 hari
Fix 2 (Wire)     → 1.5 + 1 hari (termasuk restart re-stamp MUST-FIX)
Fix 3 (Epoch)    → 1.5–2 hari (SELEPAS Fix 2)
Fix 4 (Lease GC) → 3.5 hari (+2-3 hari refactor berasingan)
Fix 5 (Combined) → 1 hari
Total: ~10–11.5 hari
```
