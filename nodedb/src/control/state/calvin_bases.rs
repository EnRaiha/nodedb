// SPDX-License-Identifier: BUSL-1.1

//! Where this node's Calvin state of each vShard is whole from, and the
//! sequencer log range the vShards still waiting for a scheduler need.
//!
//! See [`CalvinBase`] for the durable record. This registry holds the same
//! bases in memory, for the checks that run under the `MultiRaft` lock and
//! on the sequencer log compactor:
//!
//! - A data group mounted here takes log entries only once every vShard of
//!   it has a base that reaches the first index the sequencer log holds.
//!   Otherwise its replica refuses entries until a snapshot installs.
//! - A vShard of a mounted group whose scheduler has not started from its
//!   base is waiting. The sequencer log keeps every index the scheduler will
//!   replay from that base.
//! - A snapshot install replaces a vShard's base and moves its generation.
//!   A scheduler started under an older generation installs nothing more.
//! - A sequencer snapshot installed here skips entries the running
//!   schedulers never received. Every kept base it skips is whole no more:
//!   each read sees it as no base.
//!
//! The mutex is a leaf: no other lock is taken while it is held.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::calvin_base::CalvinBase;

#[derive(Debug, Default)]
struct BasesState {
    bases: BTreeMap<u32, CalvinBase>,
    /// vShards of mounted data groups whose scheduler has not started.
    waiting: BTreeSet<u32>,
    /// vShards whose scheduler runs in this process.
    started: BTreeSet<u32>,
    generations: BTreeMap<u32, u64>,
    /// The index of the last sequencer snapshot this node installed.
    installed: u64,
    /// vShards already reported as stopped with no replica to repair them.
    reported_stopped: BTreeSet<u32>,
}

impl BasesState {
    /// The base of `vshard_id` still whole after the last sequencer install.
    fn whole(&self, vshard_id: u32) -> Option<CalvinBase> {
        CalvinBase::after_install(self.bases.get(&vshard_id).copied(), self.installed)
    }
}

/// This node's Calvin bases, by vShard.
#[derive(Debug, Default)]
pub struct CalvinBases {
    state: Mutex<BasesState>,
}

impl CalvinBases {
    fn state(&self) -> std::sync::MutexGuard<'_, BasesState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Load the bases the catalog saved and its last sequencer install, at
    /// boot.
    pub fn load(&self, saved: Vec<(u32, CalvinBase)>, installed: u64) {
        let mut state = self.state();
        for (vshard_id, base) in saved {
            state.bases.insert(vshard_id, base);
        }
        state.installed = state.installed.max(installed);
    }

    /// The base of `vshard_id`, `None` when this node holds no whole one.
    pub fn base(&self, vshard_id: u32) -> Option<CalvinBase> {
        self.state().whole(vshard_id)
    }

    /// Whether a sequencer snapshot this node records as installed is not
    /// yet the boundary of its sequencer log, whose first index is
    /// `sequencer_first`. The log then still moves past the entries a
    /// scheduler replays.
    pub fn sequencer_install_pending(&self, sequencer_first: u64) -> bool {
        self.state().installed >= sequencer_first
    }

    /// Record that this node installs a sequencer snapshot at `index`.
    /// Durable before the sequencer log adopts it. Every kept base whose
    /// scheduler caught up from at or below `index` is whole no more.
    pub fn record_sequencer_install(
        &self,
        catalog: &SystemCatalog,
        index: u64,
    ) -> crate::Result<()> {
        let installed = self.state().installed.max(index);
        catalog.save_calvin_sequencer_install(installed)?;
        let mut state = self.state();
        state.installed = state.installed.max(installed);
        let skipped: Vec<u32> = state
            .started
            .iter()
            .copied()
            .filter(|&vshard_id| state.whole(vshard_id).is_none())
            .collect();
        for vshard_id in skipped {
            state.started.remove(&vshard_id);
            state.waiting.insert(vshard_id);
        }
        Ok(())
    }

    /// The generation of `vshard_id`'s base. A snapshot install moves it.
    pub fn generation(&self, vshard_id: u32) -> u64 {
        self.state()
            .generations
            .get(&vshard_id)
            .copied()
            .unwrap_or(0)
    }

    /// Whether every vShard in `vshards` has a base that reaches
    /// `sequencer_first`, the first index the sequencer log holds here. Every
    /// vShard starts waiting for its scheduler: a kept base too, whose
    /// scheduler after a restart replays the inputs it had not made durable.
    pub fn group_reaches(&self, vshards: &[u32], sequencer_first: u64) -> bool {
        let mut state = self.state();
        let mut reaches = true;
        for &vshard_id in vshards {
            let base = state.whole(vshard_id);
            if !state.started.contains(&vshard_id) {
                state.waiting.insert(vshard_id);
            }
            reaches &= CalvinBase::reaches(base, sequencer_first);
        }
        reaches
    }

    /// Stop waiting for every vShard not in `mounted`, the vShards of the
    /// data groups this node mounts: an unmounted group starts no scheduler.
    pub fn retain_waiting(&self, mounted: &BTreeSet<u32>) {
        self.state()
            .waiting
            .retain(|vshard_id| mounted.contains(vshard_id));
    }

    /// The lowest sequencer index a waiting vShard's scheduler will replay
    /// from its base. The sequencer log keeps it. A waiting vShard with a
    /// kept base replays from wherever the log starts, so the log keeps all
    /// it holds until that scheduler starts.
    pub fn replay_floor(&self) -> Option<u64> {
        let state = self.state();
        state
            .waiting
            .iter()
            .map(|vshard_id| CalvinBase::replay_start(state.whole(*vshard_id)).unwrap_or(1))
            .min()
    }

    /// Whether `vshard_id` is reported as stopped for the first time since its
    /// base last became whole. Each loss is reported once.
    pub fn first_stopped_report(&self, vshard_id: u32) -> bool {
        self.state().reported_stopped.insert(vshard_id)
    }

    /// Record that a snapshot brought every input of each of `vshards`
    /// sequenced at or below `through`. Durable before it takes effect.
    /// Moves each vShard's generation.
    pub fn record_snapshot(
        &self,
        catalog: &SystemCatalog,
        vshards: &[u32],
        through: u64,
    ) -> crate::Result<()> {
        let base = CalvinBase::Snapshot { through };
        let rows: Vec<(u32, Option<CalvinBase>)> =
            vshards.iter().map(|&v| (v, Some(base))).collect();
        catalog.save_calvin_bases(&rows)?;
        let mut state = self.state();
        for &vshard_id in vshards {
            state.bases.insert(vshard_id, base);
            state.waiting.insert(vshard_id);
            state.started.remove(&vshard_id);
            state.reported_stopped.remove(&vshard_id);
            *state.generations.entry(vshard_id).or_default() += 1;
        }
        Ok(())
    }

    /// Record that the schedulers of `vshards`, each `(vshard_id, from)`,
    /// started from bases that reach the sequencer log and caught up from
    /// index `from`. They keep the state whole from here. The running
    /// schedulers keep them even when the record does not persist: only a
    /// restart then takes a snapshot for those vShards. A base saved kept
    /// already is not written again.
    pub fn record_kept(
        &self,
        catalog: &SystemCatalog,
        vshards: &[(u32, u64)],
    ) -> crate::Result<()> {
        let mut rows = Vec::new();
        {
            let mut state = self.state();
            for &(vshard_id, from) in vshards {
                let kept = CalvinBase::Kept { from };
                state.reported_stopped.remove(&vshard_id);
                if state.bases.insert(vshard_id, kept) != Some(kept) {
                    rows.push((vshard_id, Some(kept)));
                }
                // A sequencer install since the scheduler started leaves the
                // vShard waiting: its scheduler stops on the next reconcile.
                if state.whole(vshard_id).is_some() {
                    state.waiting.remove(&vshard_id);
                    state.started.insert(vshard_id);
                }
            }
        }
        catalog.save_calvin_bases(&rows)
    }

    /// Record that this node's Calvin state of `vshard_id` has a hole: an
    /// input it never applied is gone from the sequencer log. Only a snapshot
    /// brings a base back.
    pub fn record_lost(&self, catalog: &SystemCatalog, vshard_id: u32) -> crate::Result<()> {
        catalog.save_calvin_bases(&[(vshard_id, None)])?;
        let mut state = self.state();
        state.bases.remove(&vshard_id);
        state.waiting.insert(vshard_id);
        state.started.remove(&vshard_id);
        Ok(())
    }

    /// Forget `vshards`: this node left their groups and holds none of their
    /// state.
    pub fn forget(&self, catalog: &SystemCatalog, vshards: &[u32]) -> crate::Result<()> {
        let rows: Vec<(u32, Option<CalvinBase>)> = vshards.iter().map(|&v| (v, None)).collect();
        catalog.save_calvin_bases(&rows)?;
        let mut state = self.state();
        for vshard_id in vshards {
            state.bases.remove(vshard_id);
            state.waiting.remove(vshard_id);
            state.started.remove(vshard_id);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_mounted_vshard_waits_until_its_scheduler_keeps_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).expect("catalog");
        let bases = CalvinBases::default();

        assert!(bases.group_reaches(&[1, 2], 1), "the whole log is held");
        assert_eq!(bases.replay_floor(), Some(1));
        assert!(!bases.group_reaches(&[1, 2], 30), "index 1 is gone");

        bases
            .record_snapshot(&catalog, &[1, 2], 40)
            .expect("snapshot");
        assert_eq!(bases.generation(1), 1);
        assert!(bases.group_reaches(&[1, 2], 30));
        assert_eq!(bases.replay_floor(), Some(41));

        bases
            .record_kept(&catalog, &[(1, 41), (2, 41)])
            .expect("kept");
        assert_eq!(bases.replay_floor(), None);
        assert!(bases.group_reaches(&[1, 2], 9_000));
        assert_eq!(
            bases.replay_floor(),
            None,
            "a running scheduler waits for nothing"
        );

        // After a restart a kept vShard waits until its scheduler starts
        // again, and the log keeps all it holds meanwhile.
        let restarted = CalvinBases::default();
        restarted.load(catalog.load_calvin_bases().expect("load"), 0);
        assert!(restarted.group_reaches(&[1, 2], 9_000));
        assert_eq!(restarted.replay_floor(), Some(1));

        bases.record_lost(&catalog, 2).expect("lost");
        assert!(!bases.group_reaches(&[1, 2], 9_000));
        bases.retain_waiting(&BTreeSet::new());
        assert_eq!(
            bases.replay_floor(),
            None,
            "an unmounted group waits for nothing"
        );

        let reloaded = CalvinBases::default();
        reloaded.load(catalog.load_calvin_bases().expect("load"), 0);
        assert_eq!(reloaded.base(1), Some(CalvinBase::Kept { from: 41 }));
        assert_eq!(reloaded.base(2), None);
    }

    /// A sequencer snapshot installed at or above the index a running
    /// scheduler caught up from skips inputs it never received. The vShard
    /// waits for a data-group snapshot, also after a restart.
    #[test]
    fn a_sequencer_install_ends_the_kept_bases_it_skips() {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).expect("catalog");
        let bases = CalvinBases::default();
        assert!(bases.group_reaches(&[1, 2], 1));
        bases
            .record_kept(&catalog, &[(1, 1), (2, 50)])
            .expect("kept");

        bases
            .record_sequencer_install(&catalog, 30)
            .expect("install");
        assert_eq!(bases.base(1), None, "inputs 1..=30 were skipped");
        assert_eq!(bases.base(2), Some(CalvinBase::Kept { from: 50 }));
        assert!(!bases.group_reaches(&[1], 31));
        assert_eq!(bases.replay_floor(), Some(1), "vShard 1 waits again");

        // A scheduler that caught up from below the install is kept no more.
        bases.record_kept(&catalog, &[(1, 30)]).expect("kept");
        assert_eq!(bases.base(1), None);

        let restarted = CalvinBases::default();
        restarted.load(
            catalog.load_calvin_bases().expect("load"),
            catalog.load_calvin_sequencer_install().expect("install"),
        );
        assert_eq!(restarted.base(1), None);
        assert_eq!(restarted.base(2), Some(CalvinBase::Kept { from: 50 }));
    }
}
