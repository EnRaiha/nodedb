# Performance & Test Architecture Roadmap (Post-P2)

> **Scope:** Plan untuk selepas P2 merge. Semua cadangan di sini adalah untuk **P3+ / v0.7+**, bukan blok P2. P2 fokus correctness — kita keep existing tests dan architecture as-is.

---

## Bahagian 1: Performance Refactoring

### PERF-1: Per-Group Lock Granularity (Critical)

**Masalah:** `MultiRaft` menggunakan satu `Mutex` untuk semua groups. Setiap RPC handler — `handle_append_entries_rpc`, `handle_request_vote_rpc`, tick loop — semuanya acquire lock yang sama:

```rust
// consensus.rs — SEMUA groups berkongsi lock ini:
let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
```

NodeDB adalah multi-group Raft — satu node host metadata group + N vshard groups. Dengan global lock:

- Group A's AppendEntries **blocks** Group B's RequestVote
- Tick loop untuk 50 groups serialize melalui satu lock
- Read path (`last_applied()`, `match_index_for()`) juga perlu lock

**Refactor:**

```rust
// BEFORE (current):
pub struct MultiRaft {
    groups: HashMap<u64, RaftNode<WalStorage>>,
}

// AFTER — per-group lock:
use dashmap::DashMap;
use parking_lot::Mutex;

pub struct MultiRaft {
    groups: DashMap<u64, Mutex<GroupState>>,
}

pub struct GroupState {
    node: RaftNode<WalStorage>,
    watchers: AppliedIndexWatcher,
    // per-group metadata...
}

impl MultiRaft {
    pub fn handle_append_entries(
        &self,
        req: &AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse> {
        // Lock hanya group yang terlibat — groups lain terus jalan
        let entry = self.groups.get(&req.group_id)
            .ok_or(ClusterError::GroupNotFound { group_id: req.group_id })?;
        let mut state = entry.value().lock();
        let resp = state.node.handle_append_entries(req)?;
        state.node.persist_hard_state_if_dirty()?;
        drop(state); // release lock sebelum reply serialization
        Ok(resp)
    }
}
```

**Impact:** Linear throughput scaling dengan jumlah groups. Untuk 10 vshard groups, ini adalah ~10x throughput improvement pada consensus path.

**Priority:** HIGH — biggest single bottleneck dalam architecture semasa.

**Migration path:** Boleh buat incremental — start dengan `RwLock<HashMap<u64, Arc<Mutex<GroupState>>>>`, kemudian migrate ke `DashMap`.

---

### PERF-2: Commit Index Advancement — O(n·k) → O(k log k)

**Masalah:** `try_advance_commit_index()` dipanggil pada SETIAP AppendEntries response. Algorithm semasa:

```rust
// internal.rs — O(entries × peers) setiap response:
for n in (self.volatile.commit_index + 1..=last).rev() {
    // For each candidate index from high to low...
    for &peer in &self.config.peers {
        if leader.match_index_for(peer) >= n { count += 1; }
    }
    if count >= quorum { ... break; }
}
```

Leader dengan 10,000 uncommitted entries dan 5 peers = 50,000 iterations **per response**. Dengan 100 responses/detik = 5M iterations/detik.

**Refactor — median-of-match-indexes algorithm:**

```rust
impl<S: LogStorage> RaftNode<S> {
    /// O(k log k) di mana k = number of voters.
    ///
    /// Standard algorithm: k-th largest match_index (termasuk self)
    /// adalah highest index yang boleh commit. Ini adalah cara
    /// etcd/raft, CockroachDB, dan TiKV implement.
    pub(super) fn try_advance_commit_index(&mut self) {
        let leader = match &self.leader_state {
            Some(ls) => ls,
            None => return,
        };

        // Collect match_indexes untuk semua voters + self
        let mut indexes: SmallVec<[u64; 9]> = SmallVec::new();
        for &peer in &self.config.peers {
            if let Some(mi) = leader.match_index_for(peer) {
                indexes.push(mi);
            }
        }
        indexes.push(self.log.last_index()); // self

        // Sort descending, pick quorum-th
        indexes.sort_unstable_by(|a, b| b.cmp(a));
        let quorum_pos = self.config.quorum() - 1;

        if let Some(&candidate) = indexes.get(quorum_pos) {
            // Previous-term guard: hanya commit jika entry dari term semasa
            if candidate > self.volatile.commit_index
                && self.log.term_at(candidate) == Some(self.hard_state.current_term)
            {
                self.volatile.commit_index = candidate;
                self.collect_committed_entries();
            }
        }
    }
}
```

**Impact:** Untuk 5-voter cluster dengan 10k entries: 50,000 ops → 5 ops (sort 6 elements). ~10,000x reduction pada hot path.

**Priority:** HIGH — easy win, big impact.

---

### PERF-3: Eliminate Per-Tick Allocations

**Masalah:** `replicate_to_all()` dipanggil setiap `heartbeat_interval` (default 50ms). Setiap panggilan:

```rust
// internal.rs — allocation setiap tick:
pub(super) fn replicate_to_all(&mut self) {
    let voters_and_learners: Vec<u64> = self.config.peers.iter()
        .chain(self.config.learners.iter())
        .copied()
        .collect();  // NEW Vec allocation, setiap 50ms
    for peer in voters_and_learners { ... }

    let observers: Vec<u64> = self.config.observers.clone();  // ANOTHER clone
    for observer in observers { ... }
}
```

Dengan 100 groups × 20 ticks/detik × 2 allocations = **4,000 allocations/detik** hanya untuk heartbeat.

**Refactor — cache peer list:**

```rust
pub struct LeaderState {
    // ... existing fields ...

    /// Cached flat list of all replication targets (voters + learners).
    /// Invalidated by config changes; rebuilt lazily.
    replication_targets: Vec<u64>,
    targets_dirty: bool,
}

impl LeaderState {
    fn replication_targets(&mut self, voters: &[u64], learners: &[u64]) -> &[u64] {
        if self.targets_dirty {
            self.replication_targets.clear();
            self.replication_targets.extend_from_slice(voters);
            self.replication_targets.extend_from_slice(learners);
            self.targets_dirty = false;
        }
        &self.replication_targets
    }

    fn mark_targets_dirty(&mut self) {
        self.targets_dirty = true;
    }
}

// Usage dalam replicate_to_all:
pub(super) fn replicate_to_all(&mut self) {
    let targets: Vec<u64> = {
        let leader = self.leader_state.as_mut().unwrap();
        leader.replication_targets(&self.config.peers, &self.config.learners).to_vec()
        // Kalau nak zero-alloc: pakai drain + re-fill pattern
    };
    for peer in targets {
        self.send_append_entries(peer);
    }
    // Observers: iterate tanpa clone
    for &observer in &self.config.observers {
        self.send_append_entries_to_observer(observer);
    }
}
```

**Priority:** MEDIUM — impact bergantung pada jumlah groups.

---

### PERF-4: Ready Struct — Object Pool / Reuse

**Masalah:** `take_ready()` menggunakan `std::mem::take` — ini OK untuk Vec (leaves empty), tapi Ready struct dibuat fresh setiap kali node start. Yang lebih terukur: `committed_entries: Vec<LogEntry>` allocates dan grows untuk setiap batch.

```rust
// Semasa: setiap Ready = heap allocations untuk semua Vec fields
// LogEntry punya `data: Vec<u8>` — setiap entry adalah allocation
```

**Refactor — reusable buffer:**

```rust
pub struct RaftNode<S: LogStorage> {
    // ... existing ...
    ready: Ready,
    /// Pre-allocated buffer untuk outgoing entries.
    /// `collect_committed_entries` clones into this; caller drains.
    committed_buffer: Vec<LogEntry>,
}

impl<S: LogStorage> RaftNode<S> {
    pub(super) fn collect_committed_entries(&mut self) {
        let from = self.volatile.last_applied + 1;
        let to = self.volatile.commit_index;
        if from > to { return; }

        // Reserve capacity sekali, reuse antara calls
        self.committed_buffer.reserve((to - from + 1) as usize);

        if let Ok(entries) = self.log.entries_range(from, to) {
            for entry in entries {
                self.committed_buffer.push(entry.clone());
            }
        }
    }

    pub fn take_ready(&mut self) -> Ready {
        // Move committed_buffer into Ready (transfer ownership, no alloc)
        let mut ready = std::mem::take(&mut self.ready);
        ready.committed_entries = std::mem::take(&mut self.committed_buffer);
        ready
    }

    /// Return consumed Ready untuk reuse (caller calls this setelah apply)
    pub fn return_ready(&mut self, ready: Ready) {
        // Clear dan keep capacity
        self.committed_buffer = ready.committed_entries;
        self.committed_buffer.clear();
    }
}
```

**Priority:** LOW-MEDIUM — butuh profiling untuk justify.

---

### PERF-5: ConfigChange Serialization — JSON → Binary

**Masalah (dari P2 fix):** `LogEntry::encode_config()` menggunakan `serde_json`:

```rust
pub fn encode_config(change: ConfigChange) -> Vec<u8> {
    let mut buf = vec![0x01];
    buf.extend_from_slice(&serde_json::to_vec(&change).unwrap_or_default());
    buf
}
```

JSON encoding untuk internal Raft protocol:

- Verbose: `{"AddVoter":{"node_id":42}}` = 28 bytes vs binary 10 bytes
- Parsing overhead pada setiap follower
- Tidak deterministik field order dengan HashMap (jika ada)

**Refactor — bincode atau custom encoding:**

```rust
// Custom compact encoding — 0 allocations untuk encode
impl ConfigChange {
    const TAG_ADD_VOTER: u8 = 1;
    const TAG_REMOVE_VOTER: u8 = 2;
    const TAG_ADD_LEARNER: u8 = 3;
    // ...

    pub fn encode(&self) -> Vec<u8> {
        match self {
            ConfigChange::AddVoter { node_id } => {
                vec![Self::TAG_ADD_VOTER, node_id.to_be_bytes()[0], /* ... */]
            }
            // ...
        }
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        match data.first()? {
            &Self::TAG_ADD_VOTER => {
                let node_id = u64::from_be_bytes(data[1..9].try_into().ok()?);
                Some(ConfigChange::AddVoter { node_id })
            }
            // ...
            _ => None,
        }
    }
}
```

Atau gunakan `bincode` dengan `serde` derive — sudah ada dalam dependency tree mungkin.

**Priority:** LOW — hanya relevan jika config changes kerap.

---

### PERF-6: Storage I/O — Group Commit & Batched Fsync

**Masalah:** Setiap `storage.append()` dipanggil dengan 1-5 entries. Jika backend `nodedb-wal` fsync setiap append, ini adalah latency bottleneck (fsync = ~1-10ms).

**Refactor — group commit pattern:**

```rust
// Bukan dalam nodedb-raft — ini untuk WAL backend
pub struct WalStorage {
    wal: nodedb_wal::Wal,
    /// Pending entries yang belum fsync.
    pending: Vec<LogEntry>,
    /// Trigger fsync batch — dipanggil oleh caller secara explicit.
}

impl LogStorage for WalStorage {
    fn append(&mut self, entries: &[LogEntry]) -> Result<()> {
        // Write ke WAL buffer (no fsync)
        self.wal.append(entries)?;
        self.pending.extend_from_slice(entries);
        Ok(())
    }

    /// Explicit barrier — dipanggil SEBELUM AppendEntriesResponse dihantar.
    fn sync(&mut self) -> Result<()> {
        if self.pending.is_empty() { return Ok(()); }
        self.wal.sync()?; // single fsync untuk semua pending
        self.pending.clear();
        Ok(())
    }
}

// Caller pattern (rpc_dispatch):
node.handle_append_entries(req)?;
node.storage_mut().sync()?; // batched fsync
let resp = /* build response */;
```

**Priority:** MEDIUM — bergantung pada WAL implementation semasa.

---

## Bahagian 2: Test Architecture Evolution

### Status Semasa (keep untuk P2)

| Layer                                         | Approach                                | Verdict                       |
| --------------------------------------------- | --------------------------------------- | ----------------------------- |
| Unit tests (per-file `#[cfg(test)]`)          | Direct function calls, MemStorage       | ✅ Keep — fast, precise       |
| Raft integration tests (`nodedb-raft/tests/`) | Multi-node simulation, real timeouts    | ⚠️ Works tapi slow            |
| Cluster tests (`nodedb-cluster-tests/`)       | Full cluster, tokio, network            | ⚠️ 1015 tests, slow           |
| Heavy test loop (7× full runs)                | Manual repetition untuk flake detection | ❌ Symptom of non-determinism |

**Masalah utama:** Non-deterministic time + network ordering = flaky tests → perlu 7 runs untuk confidence.

### TEST-1: Deterministic Simulation Harness (Foundation)

**Masalah:** Tests semasa guna real time (`Instant::now()`, `tokio::time::sleep`). Election timeout 150-300ms real time × 1000+ tests = minutes of pure waiting. Dan races antara timeout events menyebabkan flakiness.

**Refactor — Clock abstraction + simulation:**

```rust
// nodedb-raft/src/clock.rs (new file)
use std::time::{Duration, Instant};

/// Abstract clock — RaftNode uses this instead of Instant::now() directly.
pub trait Clock: Send + Sync {
    fn now(&self) -> Instant;
}

/// Production clock — delegates to system.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant { Instant::now() }
}

/// Simulation clock — manually advanced by test harness.
pub struct SimClock {
    current: std::cell::RefCell<Instant>,
}

impl Clock for SimClock {
    fn now(&self) -> Instant { *self.current.borrow() }
}

impl SimClock {
    pub fn advance(&self, d: Duration) {
        *self.current.borrow_mut() += d;
    }
}
```

```rust
// Usage dalam RaftNode:
pub struct RaftNode<S: LogStorage, C: Clock = SystemClock> {
    clock: C,
    // ...
}

impl<S: LogStorage> RaftNode<S, SystemClock> {
    pub fn new(config: RaftConfig, storage: S) -> Self {
        Self::with_clock(config, storage, SystemClock)
    }
}

impl<S: LogStorage, C: Clock> RaftNode<S, C> {
    pub fn with_clock(config: RaftConfig, storage: S, clock: C) -> Self {
        let now = clock.now();
        // ... use `now` instead of Instant::now()
    }

    pub fn tick(&mut self) {
        let now = self.clock.now(); // ← deterministic dalam simulation
        // ...
    }
}
```

**Test harness — simulated cluster:**

```rust
// nodedb-raft/tests/sim/harness.rs (new)
pub struct SimCluster {
    nodes: Vec<RaftNode<MemStorage, SimClock>>,
    /// Message queue — deterministic ordering
    inbox: VecDeque<(u64 /* from */, u64 /* to */, RaftRpc)>,
    /// Partition matrix — which links are up
    connectivity: Vec<Vec<bool>>,
    clock: Arc<SimClock>,
    rng: StdRng, // seeded, deterministic
}

impl SimCluster {
    /// Run until stable (no more messages, all deadlines passed)
    /// or max_iterations reached.
    pub fn run_until_stable(&mut self, max_iter: usize) {
        for _ in 0..max_iter {
            self.clock.advance(Duration::from_millis(1));

            // Process all timeouts
            for node in &mut self.nodes {
                node.tick();
                self.drain_ready(node);
            }

            // Process all queued messages (with partition filter)
            self.deliver_messages();

            if self.is_stable() { break; }
        }
    }

    /// Inject partition — block messages between two sets of nodes.
    pub fn partition(&mut self, left: &[usize], right: &[usize]) {
        for &i in left {
            for &j in right {
                self.connectivity[i][j] = false;
                self.connectivity[j][i] = false;
            }
        }
    }

    /// Heal partition.
    pub fn heal(&mut self) {
        for row in &mut self.connectivity {
            for cell in row { *cell = true; }
        }
    }

    /// Kill and restart a node (crash recovery test).
    pub fn crash_and_restart(&mut self, idx: usize) {
        let config = self.nodes[idx].config.clone();
        let storage = /* extract and re-create from MemStorage */;
        self.nodes[idx] = RaftNode::with_clock(config, storage, self.clock.clone());
        self.nodes[idx].restore().unwrap();
    }
}
```

**Test example — deterministic:**

```rust
#[test]
fn partition_heal_elects_new_leader_deterministically() {
    let mut sim = SimCluster::new(5, seed(42)); // 5 nodes, seed 42
    sim.run_until_stable(1000);
    let leader1 = sim.leader().unwrap();

    // Partition: leader + 1 follower vs 3 followers
    sim.partition(&[leader1, (leader1+1)%5], &[(leader1+2)%5, (leader1+3)%5, (leader1+4)%5]);

    sim.run_until_stable(1000); // new leader elected on majority side

    let leader2 = sim.leader_on_majority().unwrap();
    assert_ne!(leader1, leader2);

    // Old leader should have stepped down (check-quorum)
    assert_ne!(sim.nodes[leader1].role(), NodeRole::Leader);

    // Heal
    sim.heal();
    sim.run_until_stable(1000);

    // Verify safety: no two leaders in same term, log consistency
    sim.assert_safety_invariants();
}
```

**Impact:** Tests yang ambil 500ms real-time → <1ms simulated. Flaky races → deterministic. 7 runs → 1 run.

**Priority:** HIGH — foundation untuk semua test improvements.

---

### TEST-2: Property-Based Testing (Raft Invariants)

**Masalah:** Unit tests cover specific scenarios. Tapi Raft punya bug biasanya muncul dari **unexpected interleavings** — bukan scenario yang developer imagine.

**Refactor — proptest untuk invariant verification:**

```rust
// nodedb-raft/tests/prop/invariants.rs (new)
use proptest::prelude::*;

/// Arbitrary operation untuk property-based testing
#[derive(Debug, Clone)]
enum Op {
    Tick(usize),           // Tick node i
    DeliverMessage,        // Deliver one pending message
    DropMessage,           // Drop one pending message (network loss)
    Partition(Vec<usize>, Vec<usize>), // Network partition
    Heal,                  // Heal all partitions
    Crash(usize),          // Crash node i
    Restart(usize),        // Restart crashed node i
    Propose(usize, Vec<u8>), // Propose entry on node i
    AdvanceTime(Duration), // Advance simulation clock
}

fn arb_op(num_nodes: usize) -> impl Strategy<Value = Op> {
    prop_oneof![
        (0..num_nodes).prop_map(Op::Tick),
        Just(Op::DeliverMessage),
        Just(Op::DropMessage),
        // ... other strategies
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    /// State Machine Safety: if a server applies an entry at index N,
    /// no other server applies a DIFFERENT entry at index N.
    #[test]
    fn prop_state_machine_safety(
        ops in vec(arb_op(5), 0..500),
        seed in any::<u64>(),
    ) {
        let mut sim = SimCluster::new(5, seed);
        sim.run_operations(ops);

        // Check invariant
        let applied_per_node: Vec<Vec<LogEntry>> = sim.nodes.iter()
            .map(|n| sim.get_applied_entries(n))
            .collect();

        for (i, entries_i) in applied_per_node.iter().enumerate() {
            for (j, entries_j) in applied_per_node.iter().enumerate() {
                if i == j { continue; }
                for (idx, entry_i) in entries_i.iter().enumerate() {
                    if let Some(entry_j) = entries_j.get(idx) {
                        prop_assert_eq!(
                            entry_i.data, entry_j.data,
                            "Divergence at index {}: node {} has {:?}, node {} has {:?}",
                            idx, i, entry_i.data, j, entry_j.data
                        );
                    }
                }
            }
        }
    }

    /// Election Safety: at most one leader per term.
    #[test]
    fn prop_election_safety(
        ops in vec(arb_op(5), 0..500),
        seed in any::<u64>(),
    ) {
        let mut sim = SimCluster::new(5, seed);
        sim.run_operations(ops);

        // Collect (term, leader) pairs from all nodes that ever were leader
        let leaders: Vec<(u64, u64)> = sim.nodes.iter()
            .filter(|n| n.role() == NodeRole::Leader)
            .map(|n| (n.current_term(), n.node_id()))
            .collect();

        // Group by term, assert each term has <= 1 leader
        let mut terms: HashMap<u64, HashSet<u64>> = HashMap::new();
        for (term, leader) in leaders {
            terms.entry(term).or_default().insert(leader);
        }
        for (term, leaders_in_term) in terms {
            prop_assert!(
                leaders_in_term.len() <= 1,
                "Term {} has {} leaders: {:?}",
                term, leaders_in_term.len(), leaders_in_term
            );
        }
    }

    /// Log Matching: if two logs have entry with same (term, index),
    /// all preceding entries are identical.
    #[test]
    fn prop_log_matching(
        ops in vec(arb_op(5), 0..300),
        seed in any::<u64>(),
    ) {
        let mut sim = SimCluster::new(5, seed);
        sim.run_operations(ops);

        let logs: Vec<Vec<LogEntry>> = sim.nodes.iter()
            .map(|n| sim.get_log_entries(n))
            .collect();

        for (i, log_i) in logs.iter().enumerate() {
            for (j, log_j) in logs.iter().enumerate() {
                if i >= j { continue; }
                // For each common (term, index), verify prefix matches
                for (idx_i, entry_i) in log_i.iter().enumerate() {
                    if let Some(entry_j) = log_j.get(idx_i) {
                        if entry_i.term == entry_j.term {
                            prop_assert_eq!(entry_i.data, entry_j.data);
                        }
                    }
                }
            }
        }
    }
}
```

**Impact:** Menemui bugs yang specific test cases miss. 1000 random scenarios per CI run vs ~50 manual cases.
**Priority:** HIGH — complement simulation harness (TEST-1).

---

### TEST-3: E2E Integration — Black-Box with Fault Injection

**Status semasa:** 1015 cluster tests. Kemungkinan white-box, coupled ke implementation details. Susah maintain, slow.

**Refactor — Jepsen-style black-box testing:**

```rust
// nodedb-cluster-tests/src/jepsen/mod.rs (new module)

/// Black-box testing: treat cluster sebagai opaque system.
/// Hanya interact melalui client API + fault injection.
pub struct JepsenHarness {
    /// Real cluster processes (tokio tasks, bukan simulation)
    cluster: ClusterHandle,
    /// Client yang issues operations
    clients: Vec<ClusterClient>,
    /// Fault injector
    faults: FaultInjector,
    /// Operation history untuk linearizability checking
    history: Vec<Operation>,
}

#[derive(Debug, Clone)]
pub struct Operation {
    pub id: usize,
    pub op_type: OpType,
    pub start: Instant,
    pub end: Option<Instant>,
    pub result: Option<OpResult>,
}

pub enum OpType {
    Write { key: String, value: String },
    Read { key: String },
    /// Compare-and-swap untuk linearizability detection
    Cas { key: String, expected: String, new: String },
}

impl JepsenHarness {
    /// Run a Jepsen-style check: inject faults, record history,
    /// verify linearizability afterwards.
    pub async fn check(&mut self, fault_plan: FaultPlan) -> JepsenVerdict {
        // 1. Start background fault injector (partitions, kills, pauses)
        // 2. Run concurrent client operations, record (start, end, result)
        // 3. Stop faults, drain clients
        // 4. Run linearizability checker on the history
        //    (e.g. porcupine / elle-compatible checker, or Knossos port)
        unimplemented!()
    }
}
```

**Impact:** Verify LINEARIZABILITY (bukan sekadar safety invariants) end-to-end: real network, real timing, real storage.
**Priority:** MEDIUM-HIGH — final confidence layer sebelum release.

**Linearizability checker options:**

- `porcupine` (Go) — jepsen's checker, port ke Rust
- Knossos (Clojure) — original Jepsen checker
- Model-checked history reduction (simpler, self-contained)

---

## Priorities Summary (Post-P2)

| Item                             | Priority | Effort | Impact                        |
| -------------------------------- | -------- | ------ | ----------------------------- |
| PERF-1 per-group lock            | HIGH     | M      | Linear scaling with groups    |
| PERF-2 commit index O(k log k)   | HIGH     | S      | ~10,000x on hot path          |
| PERF-3 eliminate per-tick allocs | MEDIUM   | S      | Scales with groups            |
| PERF-4 Ready pooling             | LOW-MED  | M      | Needs profiling               |
| PERF-5 ConfigChange binary       | LOW      | S      | Only if conf changes frequent |
| PERF-6 group commit fsync        | MEDIUM   | M      | Depends on WAL impl           |
| TEST-1 deterministic sim         | HIGH     | L      | Foundation for all tests      |
| TEST-2 property-based            | HIGH     | M      | 1000 scenarios/CI run         |
| TEST-3 Jepsen black-box          | MED-HIGH | L      | Linearizability proof         |

> Order cadangan: TEST-1 → TEST-2 → PERF-2 → PERF-1 → TEST-3 → PERF-6 → PERF-3 → PERF-4 → PERF-5
