// SPDX-License-Identifier: BUSL-1.1

//! Persistent Calvin base of each vShard, backing `_system.calvin_base`.
//!
//! A replica's state of a vShard is its data group's log through some index
//! plus every Calvin input the sequencer log holds for the vShard. The
//! sequencer compacts its log on its own schedule, so a replica whose Calvin
//! state is not whole up to the first index the log still holds can never
//! catch it up from the log. The base records where this node's Calvin state
//! of each vShard is whole from:
//!
//! - [`CalvinBase::Snapshot`]: a data-group snapshot brought every input
//!   sequenced at or below `through`, and no scheduler has started from it.
//! - [`CalvinBase::Kept`]: a scheduler started from a whole base and caught
//!   up from sequencer index `from`. While it runs, the sequencer log keeps
//!   every input it has not made durable, so the state stays whole across a
//!   restart. A sequencer snapshot installed here at or above `from` skips
//!   entries the scheduler never received, and the base is whole no more.
//!
//! A vShard with no row has no base: this node never held its Calvin state.
//!
//! `_system.calvin_sequencer_install` holds the index of the last sequencer
//! snapshot this node installed. It is durable before the sequencer log
//! adopts the snapshot.

use redb::{ReadableDatabase, ReadableTable, TableDefinition};

use super::types::{SystemCatalog, catalog_err};

/// Table: `vshard_id` -> `(kind, index)`. `kind` is [`KIND_SNAPSHOT`] or
/// [`KIND_KEPT`]; `index` is the snapshot's `through` or the kept `from`.
pub(super) const CALVIN_BASE: TableDefinition<u32, (u8, u64)> =
    TableDefinition::new("_system.calvin_base");

/// Table: [`SEQUENCER_INSTALL_KEY`] -> the index of the last sequencer
/// snapshot this node installed.
pub(super) const CALVIN_SEQUENCER_INSTALL: TableDefinition<u8, u64> =
    TableDefinition::new("_system.calvin_sequencer_install");

const SEQUENCER_INSTALL_KEY: u8 = 0;

const KIND_SNAPSHOT: u8 = 1;
const KIND_KEPT: u8 = 2;

/// Where this node's Calvin state of one vShard is whole from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CalvinBase {
    /// A snapshot brought every input sequenced at or below `through`.
    Snapshot { through: u64 },
    /// A scheduler that caught up from sequencer index `from` keeps the
    /// state whole, while no sequencer snapshot installs at or above it.
    Kept { from: u64 },
}

impl CalvinBase {
    /// Whether a scheduler that catches up from `sequencer_first`, the first
    /// index the sequencer log holds, misses no input: the base holds every
    /// input below it.
    pub fn reaches(base: Option<Self>, sequencer_first: u64) -> bool {
        match base {
            Some(Self::Kept { .. }) => true,
            Some(Self::Snapshot { through }) => sequencer_first <= through.saturating_add(1),
            None => sequencer_first <= 1,
        }
    }

    /// The first sequencer index a scheduler started from this base must
    /// replay: the sequencer log must keep it until the scheduler starts.
    /// `None` for a kept base, whose scheduler keeps its own range.
    pub fn replay_start(base: Option<Self>) -> Option<u64> {
        match base {
            Some(Self::Kept { .. }) => None,
            Some(Self::Snapshot { through }) => Some(through.saturating_add(1)),
            None => Some(1),
        }
    }

    /// Whether this is a kept base.
    pub fn is_kept(base: Option<Self>) -> bool {
        matches!(base, Some(Self::Kept { .. }))
    }

    /// The base still whole after the last sequencer snapshot this node
    /// installed, at `installed` (`0` for none): a kept base whose scheduler
    /// caught up from at or below it lost the entries the install skipped.
    pub fn after_install(base: Option<Self>, installed: u64) -> Option<Self> {
        match base {
            Some(Self::Kept { from }) if installed >= from => None,
            base => base,
        }
    }

    fn encode(self) -> (u8, u64) {
        match self {
            Self::Snapshot { through } => (KIND_SNAPSHOT, through),
            Self::Kept { from } => (KIND_KEPT, from),
        }
    }

    fn decode(kind: u8, index: u64) -> crate::Result<Self> {
        match kind {
            KIND_SNAPSHOT => Ok(Self::Snapshot { through: index }),
            KIND_KEPT => Ok(Self::Kept { from: index }),
            other => Err(crate::Error::Internal {
                detail: format!("calvin base: unknown kind {other} in _system.calvin_base"),
            }),
        }
    }
}

impl SystemCatalog {
    /// Every saved Calvin base, by vShard.
    pub fn load_calvin_bases(&self) -> crate::Result<Vec<(u32, CalvinBase)>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("load_calvin_bases read txn", e))?;
        let table = read_txn
            .open_table(CALVIN_BASE)
            .map_err(|e| catalog_err("open calvin_base", e))?;
        let mut out = Vec::new();
        for row in table
            .iter()
            .map_err(|e| catalog_err("iter calvin_base", e))?
        {
            let (vshard, value) = row.map_err(|e| catalog_err("read calvin_base", e))?;
            let (kind, through) = value.value();
            out.push((vshard.value(), CalvinBase::decode(kind, through)?));
        }
        Ok(out)
    }

    /// Save each `(vshard_id, base)` in one transaction: `Some` as the
    /// vShard's Calvin base, `None` removes its row.
    pub fn save_calvin_bases(&self, bases: &[(u32, Option<CalvinBase>)]) -> crate::Result<()> {
        if bases.is_empty() {
            return Ok(());
        }
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("save_calvin_bases txn", e))?;
        {
            let mut table = write_txn
                .open_table(CALVIN_BASE)
                .map_err(|e| catalog_err("open calvin_base", e))?;
            for &(vshard_id, base) in bases {
                match base {
                    Some(base) => {
                        table
                            .insert(vshard_id, base.encode())
                            .map_err(|e| catalog_err("insert calvin_base", e))?;
                    }
                    None => {
                        table
                            .remove(vshard_id)
                            .map_err(|e| catalog_err("remove calvin_base", e))?;
                    }
                }
            }
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("commit calvin_base", e))
    }

    /// The index of the last sequencer snapshot this node installed, `0`
    /// for none.
    pub fn load_calvin_sequencer_install(&self) -> crate::Result<u64> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("load_calvin_sequencer_install read txn", e))?;
        let table = read_txn
            .open_table(CALVIN_SEQUENCER_INSTALL)
            .map_err(|e| catalog_err("open calvin_sequencer_install", e))?;
        let index = table
            .get(SEQUENCER_INSTALL_KEY)
            .map_err(|e| catalog_err("read calvin_sequencer_install", e))?
            .map_or(0, |value| value.value());
        Ok(index)
    }

    /// Save `index` as the last sequencer snapshot this node installed.
    pub fn save_calvin_sequencer_install(&self, index: u64) -> crate::Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("save_calvin_sequencer_install txn", e))?;
        {
            let mut table = write_txn
                .open_table(CALVIN_SEQUENCER_INSTALL)
                .map_err(|e| catalog_err("open calvin_sequencer_install", e))?;
            table
                .insert(SEQUENCER_INSTALL_KEY, index)
                .map_err(|e| catalog_err("insert calvin_sequencer_install", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("commit calvin_sequencer_install", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bases_save_reload_and_remove() {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).expect("catalog");
        assert!(catalog.load_calvin_bases().expect("load").is_empty());

        catalog
            .save_calvin_bases(&[
                (3, Some(CalvinBase::Snapshot { through: 40 })),
                (5, Some(CalvinBase::Kept { from: 7 })),
            ])
            .expect("save");
        catalog
            .save_calvin_bases(&[(3, Some(CalvinBase::Kept { from: 41 })), (5, None)])
            .expect("save and remove");
        assert_eq!(
            catalog.load_calvin_bases().expect("load"),
            vec![(3, CalvinBase::Kept { from: 41 })]
        );

        assert_eq!(catalog.load_calvin_sequencer_install().expect("load"), 0);
        catalog.save_calvin_sequencer_install(88).expect("save");
        assert_eq!(catalog.load_calvin_sequencer_install().expect("load"), 88);
    }

    /// A sequencer snapshot installed at or above a kept base's `from`
    /// skipped entries its scheduler never received.
    #[test]
    fn a_sequencer_install_ends_the_kept_bases_it_skips() {
        let kept = Some(CalvinBase::Kept { from: 10 });
        assert_eq!(CalvinBase::after_install(kept, 9), kept);
        assert_eq!(CalvinBase::after_install(kept, 10), None);
        let snapshot = Some(CalvinBase::Snapshot { through: 5 });
        assert_eq!(CalvinBase::after_install(snapshot, 80), snapshot);
    }

    #[test]
    fn a_base_reaches_the_log_only_without_a_gap() {
        assert!(CalvinBase::reaches(None, 1), "the whole log is still held");
        assert!(!CalvinBase::reaches(None, 2));
        let snapshot = Some(CalvinBase::Snapshot { through: 40 });
        assert!(CalvinBase::reaches(snapshot, 41));
        assert!(CalvinBase::reaches(snapshot, 12));
        assert!(!CalvinBase::reaches(snapshot, 42), "index 41 is gone");
        let kept = Some(CalvinBase::Kept { from: 3 });
        assert!(CalvinBase::reaches(kept, 9_000));
        assert_eq!(CalvinBase::replay_start(snapshot), Some(41));
        assert_eq!(CalvinBase::replay_start(None), Some(1));
        assert_eq!(CalvinBase::replay_start(kept), None);
    }
}
