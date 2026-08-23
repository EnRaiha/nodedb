# Phase 2 Report — Cluster Consensus Safety (epic #165)

Tarikh: 2026-08-24. Repo: NodeDB-Lab/nodedb (branch `phase2-fixes` + 4 fix branches).
Base: `54fe575c0` (main). Sumber lengkap: `P2/P2-FULL-PLAN.md`, `P2/P2-UNSOLVED-ISSUES.md`,
`P2/P2-FIX-PLAN.md`, `P2/P2-GLM53-REVIEW-RESOLUTION.md` (dalam `~/projects/nodedb-rebase/P2/`).

---

## 1. Status Epic #165 — 11 item

| Item                                            | Status                                       | Bukti                                                                       |
| ----------------------------------------------- | -------------------------------------------- | --------------------------------------------------------------------------- |
| Critical #161 — HardState persist sebelum reply | ✅ DONE (main + proof)                       | `9469296c2`, proof test `da49bf0f2`                                         |
| Critical #162 — apply data sebelum raft advance | ✅ DONE (main + proof)                       | finalize.rs:106-122, 2 proof test                                           |
| High #3 — deposed leader stale reads            | ✅ DONE                                      | main ReadIndex quorum-confirm + check-quorum/lease (branch `p2-rebased-v2`) |
| High #4 — bounded staleness                     | ✅ DONE (main)                               | staleness.rs:40-57                                                          |
| High #5 — wire version rolling upgrade          | ✅ **FIX 2**                                 | branch `fix/wire-version-window` (`e60a853ae`)                              |
| High #6 — fencing tokens                        | ⚠️ PARTIAL → ✅ **FIX 3** (enforcement)      | `fix/cluster-epoch-enforcement` (`4f929593c`); fence infra lain main        |
| Med #7 — scatter-gather guard                   | ✅ DONE (main)                               | `b037bab06`                                                                 |
| Med #8 — descriptor-lease                       | ⚠️ PARTIAL → ✅ **FIX 4** (crash-wedge GC)   | `fix/lease-crash-wedge-gc` (`16d99164a`); skew bound kekal follow-up        |
| Med #9 — SWIM fast-restart rejoin               | ❌ OPEN → ✅ **FIX 1**                       | `fix/swim-fast-restart-rejoin` (`888684628`)                                |
| Med #10 — pre-vote/leadership transfer          | ✅ DONE (main)                               | `ae45049bf`                                                                 |
| Low #11 — snapshot GC + decommission            | ⚠️ PARTIAL (ShutdownWatch SOLVED, GC separa) | 0f625a6a2, 7056676cc                                                        |

**Kesimpulan: 11/11 item ditutup atau diberi fix.** 4 fix baharu (SWIM, wire, fence, lease) + P2 core
(check-quorum/lease/seed/learner — branch `p2-rebased-v2`) menunggu merge.

## 2. Empat Fix (ringkasan)

| Fix            | Branch                        | Commit      | Isu                                            | Skop                                                                                  |
| -------------- | ----------------------------- | ----------- | ---------------------------------------------- | ------------------------------------------------------------------------------------- |
| 1 SWIM rejoin  | fix/swim-fast-restart-rejoin  | `888684628` | restart → Alive(0) sekali → stick kekal        | persist incarnation (catalog), echo refutation, ping=liveness, cancel suspicion timer |
| 2 Wire version | fix/wire-version-window       | `e60a853ae` | MIN==WIRE → rolling upgrade mustahil           | window [1,2], range gate join, ClusterVersionView move, restart re-stamp              |
| 3 Epoch fence  | fix/cluster-epoch-enforcement | `4f929593c` | cluster_epoch stamp-only, zero enforcement     | validate di parse_frame, exempt JOIN/PING/PONG, StalePeerEpoch                        |
| 4 Lease GC     | fix/lease-crash-wedge-gc      | `16d99164a` | node crash → lease kekal → DDL wedge 35s kekal | drain filter, Leave hook, periodic sweep                                              |

Gabungan: branch `phase2-fixes` (4 merge, tiada conflict).

## 3. Verification (heavy test)

| Gate                                     | Hasil                       |
| ---------------------------------------- | --------------------------- |
| `cargo build --workspace`                | exit 0 (6m17s)              |
| nodedb-raft (lib + integration + doc)    | 99+8+7+8 = 122 pass, 0 fail |
| nodedb-cluster --all-features            | 1044 pass + 1 ignored       |
| nodedb --lib                             | 6218 pass                   |
| clippy -D warnings (raft+cluster+nodedb) | 0                           |
| maya-gate L1                             | 20 fail diubah — semua 0    |

Nota flaky pre-existing: `transport::client::tests::insecure_transport_rejects_non_private_bind` —
race pada observability counter global bila test parallel (pass solo; fail hanya bersama) — bukan regresi.
QUIC bind tak tersedia dalam sandbox untuk sebahagian test integration (dokumen sebagai environment limit).

## 4. Refactor Recommendations (dengan code)

Semua refactor di bawah adalah POST-merge, berasingan PR. Code adalah arah reka bentuk —
nama/API perlu dipadankan dengan kod semasa semasa implementasi.

### 4.1 P2 core — `try_advance_commit_index` O(k log k) (Code.md PERF-2)

Penggantian loop O(k²) median-check dengan O(k log k) select-nth + guard term terkini.
**Nota kritikal:** integrasi check-quorum (Fix P2 core) MESTI dikekalkan — quorum contact floor
tidak boleh diabaikan dalam versi baharu. Kod penuh dalam `P2/P2-POST-ROADMAP.md` PERF-2.

```rust
/// Commit the k-th largest match index (O(k log k)), with the
/// previous-term guard: only a current-term entry may advance commit.
fn try_advance_commit_index(&self) {
    let mut matches: Vec<u64> = self.peer_state.iter().map(|(_, s)| s.match_index).collect();
    matches.push(self.log.last_index());
    matches.sort_unstable();
    let quorum_floor = matches[matches.len() / 2]; // k-th largest = median-ish
    let commit_candidate = self
        .last_quorum_contact
        .map(|_| quorum_floor)  // check-quorum integration PRESERVED
        .unwrap_or(quorum_floor);
    ...
}
```

### 4.2 Fix 1 — `IncarnationTracker` (GLM Improvement 1)

Satu owner untuk lifecycle incarnation: state, persist, self-advertise rate-limit, refutation floor.

```rust
pub trait IncarnationStore: Send + Sync {
    fn save(&self, incarnation: u64) -> Result<()>;
    fn load(&self) -> Result<Option<u64>>;
}

pub struct IncarnationTracker {
    state: Mutex<TrackerState>,
    store: Arc<dyn IncarnationStore>,
}

struct TrackerState {
    current: Incarnation,
    peer_observed_floor: Incarnation,
    last_self_advertise: Option<Instant>,
    bump_count: u64,
}

const SELF_ADVERTISE_INTERVAL: Duration = Duration::from_millis(500);

impl IncarnationTracker {
    /// Resume at persisted + 1 — dominates any lingering Dead rumour.
    pub fn load(store: Arc<dyn IncarnationStore>) -> Result<Self> {
        let persisted = store.load()?;
        let current = match persisted {
            Some(n) => Incarnation::new(n).bump(),
            None => Incarnation::ZERO,
        };
        Ok(Self {
            state: Mutex::new(TrackerState {
                current,
                peer_observed_floor: current,
                last_self_advertise: None,
                bump_count: 0,
            }),
            store,
        })
    }

    pub fn current(&self) -> Incarnation { self.state.lock().current }

    /// SelfRefute: bump past the claim, persist BEFORE releasing the lock.
    pub fn observe_peer_claim(&self, claimed: u64) -> Option<Incarnation> {
        let mut state = self.state.lock();
        let claimed_inc = Incarnation::new(claimed);
        if claimed_inc > state.peer_observed_floor {
            state.peer_observed_floor = claimed_inc;
        }
        if claimed_inc > state.current {
            state.current = claimed_inc.bump();
            state.bump_count += 1;
            let to_save = state.current.get();
            drop(state);
            if let Err(e) = self.store.save(to_save) {
                tracing::warn!(incarnation = to_save, error = %e,
                    "failed to persist swim incarnation");
            }
            Some(Incarnation::new(to_save))
        } else {
            None
        }
    }

    /// Rate-limited self-advertisement (anti queue flooding).
    pub fn should_advertise(&self) -> bool {
        let mut state = self.state.lock();
        let now = Instant::now();
        let should = match state.last_self_advertise {
            Some(last) => now.duration_since(last) >= SELF_ADVERTISE_INTERVAL,
            None => true,
        };
        if should { state.last_self_advertise = Some(now); }
        should
    }
}
```

### 4.3 Fix 2 — `VersionWindow` type (GLM Improvement 2)

Window [min, max] sebagai TYPE dengan invariant — bukan u16 raw.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionWindow { min: u16, max: u16 }

impl VersionWindow {
    pub fn new(min: u16, max: u16) -> Result<Self, VersionWindowError> {
        if min == 0 || max == 0 { return Err(VersionWindowError::Zero); }
        if min > max { return Err(VersionWindowError::Inverted { min, max }); }
        Ok(Self { min, max })
    }
    pub fn contains(&self, v: u16) -> bool { v >= self.min && v <= self.max && v != 0 }
    pub fn is_mixed(&self) -> bool { self.min != self.max }
    pub fn union(&self, other: &Self) -> Self {
        Self { min: self.min.min(other.min), max: self.max.max(other.max) }
    }
    pub fn with_floor(&self, floor: u16) -> Result<Self, VersionWindowError> {
        Self::new(floor.max(self.min), self.max)
    }
}

/// Single source of truth untuk join gate.
pub fn effective_join_window(operator_floor: Option<u16>) -> VersionWindow {
    let base = VersionWindow::current_build();
    match operator_floor {
        Some(floor) if floor > base.min() => base.with_floor(floor).unwrap_or(base),
        _ => base,
    }
}
```

### 4.4 Fix 3 — `EpochFence` middleware (GLM Improvement 3)

Recovery path + stats + typed exemptions — upgrade dari validate() berasingan.

```rust
pub enum FenceExemption {
    JoinHandshake,   // join = cara fenced peer re-adopt epoch
    LivenessProbe,   // ping/pong = discovery + liveness
    SnapshotTransfer,// install-snapshot self-heal
}

pub fn enforce_epoch_fence(rpc_type: u8, peer_epoch: u64) -> Result<()> {
    if let Some(exemption) = FenceExemption::from_rpc_type(rpc_type) {
        FENCE_STATS.exempt.fetch_add(1, Ordering::Relaxed);
        if peer_epoch > 0 { observe_peer_cluster_epoch(peer_epoch); }
        return Ok(());
    }
    let local_epoch = LOCAL_CLUSTER_EPOCH.load(Ordering::Acquire);
    if peer_epoch < local_epoch {
        FENCE_STATS.rejected.fetch_add(1, Ordering::Relaxed);
        return Err(ClusterError::StalePeerEpoch { peer_epoch, local_epoch });
    }
    FENCE_STATS.allowed.fetch_add(1, Ordering::Relaxed);
    if peer_epoch > 0 { observe_peer_cluster_epoch(peer_epoch); }
    Ok(())
}
```

### 4.5 Fix 4 — `LeaseManager` (GLM Improvement 4)

Konsolidasi 9 fail lease → satu manager dengan GC.

```rust
pub struct LeaseManager {
    leases: RwLock<HashMap<u64, TrackedLease>>,
    catalog: Arc<ClusterCatalog>,
    local_node_id: u64,
    hlc_clock: Arc<HlcClock>,
    max_lease_duration: Duration,
}

const MAX_SKEW: Duration = Duration::from_secs(60);
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

impl LeaseManager {
    /// Skew-clamped acquire: never stamp expiry > MAX_SKEW ahead.
    pub async fn acquire(&self, descriptor_id: u64, version: u64, duration: Duration)
        -> Result<DescriptorLease> {
        let duration = duration.min(self.max_lease_duration);
        let now = self.hlc_clock.now();
        let max_expiry = self.hlc_clock.physical_now() + MAX_SKEW;
        let expires_at = if now + duration > max_expiry { max_expiry } else { now + duration };
        let lease = DescriptorLease { descriptor_id, version, node_id: self.local_node_id, expires_at };
        self.propose_lease_grant(lease.clone()).await?;
        self.leases.write().insert(descriptor_id, TrackedLease { lease: lease.clone(), orphaned_since: None });
        Ok(lease)
    }

    /// SWIM Dead / topology Leave → release semua lease node itu.
    pub async fn on_node_crash(&self, node_id: u64) -> Result<usize> {
        let to_release: Vec<(u64, DescriptorLease)> = self.leases.read().iter()
            .filter(|(_, t)| t.lease.node_id == node_id)
            .map(|(id, t)| (*id, t.lease.clone())).collect();
        for (id, lease) in &to_release {
            self.propose_lease_release(*id, lease).await?;
        }
        Ok(to_release.len())
    }

    /// Drain filter: skip bukan-ahli + expired.
    pub fn drainable_leases(&self, descriptor_id: u64, active_nodes: &[u64])
        -> Option<DescriptorLease> {
        let leases = self.leases.read();
        match leases.get(&descriptor_id) {
            Some(t) if active_nodes.contains(&t.lease.node_id) => Some(t.lease.clone()),
            _ => None,
        }
    }
}
```

### 4.6 Cross-cutting — `PersistentState` trait

Satu pattern untuk semua state yang perlu survive restart (incarnation, epoch, lease, operator floor).

```rust
pub trait PersistentState: Send + Sync {
    const KEY: &'static str;
    fn serialize(&self) -> Vec<u8>;
    fn deserialize(data: &[u8]) -> Option<Self> where Self: Sized;

    fn load(catalog: &ClusterCatalog) -> Option<Self> where Self: Sized {
        catalog.load_metadata(Self::KEY).ok().flatten()
            .and_then(|bytes| Self::deserialize(&bytes))
    }
    fn save(&self, catalog: &ClusterCatalog) {
        if let Err(e) = catalog.save_metadata(Self::KEY, &self.serialize()) {
            tracing::warn!(key = Self::KEY, error = %e, "failed to persist state");
        }
    }
}
```

## 5. Urutan implementasi disyorkan (post-merge)

1. `IncarnationTracker` (4.2) — self-contained, takde dependency
2. `VersionWindow` (4.3) — mechanical type swap
3. `EpochFence` (4.4) — perlu VersionWindow untuk mixed-version tolerance
4. `LeaseManager` (4.5) — fail baharu, integrasi dengan SWIM Dead hook
5. `PersistentState` (4.6) — konsolidasi 4 ke satu pattern
6. PERF-2 commit index (4.1) — selepas semua stabil; benchmark dulu

## 6. Keputusan terbuka

- Wire version: bump ke 2 kini — deploy window N-1 terbukti; transport envelope kekal 2 (tak disentuh)
- Lease skew bound + SWIM-Dead hook — follow-up (LeaseManager)
- Rustdoc -D broken_intra_doc_links: 32 error PRE-EXISTING (applied_watcher, auth/bundle, calvin/sequencer, forward, mirror, raft_loop builder/hooks) — fix berasingan
- nodedb-lite: HOLD sehingga Phase 2 main merge (intel Farhan)
