// SPDX-License-Identifier: BUSL-1.1

//! Which collections feed a permission tree, readable without the cache lock.
//!
//! A write path decides on every write whether the write changes
//! authorization state: it does when it writes a governed collection or a
//! permission table of a registered tree. That check runs on hot write paths,
//! so it reads this index under a plain read lock rather than the cache's
//! async lock.
//!
//! Two sets feed the answer, and a collection in either counts:
//!
//! - **Cached:** rebuilt by the cache whenever a tree definition changes.
//! - **Committed:** updated by the metadata applier as it commits a change,
//!   before the cache takes it from the queue.
//!
//! A write therefore counts as an authorization change from the moment its
//! tree definition committed on this node. A removed tree can count a little
//! longer, until both sets drop it, which only adds a barrier.
//!
//! A source lives in its tree's database. Collections are held by the
//! database-qualified name plans carry, so the same name in two databases is
//! two sources, each homing on its own vShard.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use super::scope::TreeKey;
use super::types::PermissionTreeDef;

#[derive(Debug, Default)]
struct SourceSet {
    collections: HashSet<String>,
    vshards: HashSet<u32>,
}

impl SourceSet {
    fn from_defs<'a>(defs: impl IntoIterator<Item = (&'a TreeKey, &'a PermissionTreeDef)>) -> Self {
        let mut set = Self::default();
        for (governed, def) in defs {
            let table = governed.scope.collection(def.permission_table.clone());
            for source in [governed, &table] {
                set.collections
                    .insert(source.qualified().as_str().to_owned());
                set.vshards.insert(source.vshard().as_u32());
            }
        }
        set
    }
}

#[derive(Debug, Default)]
struct Committed {
    defs: HashMap<TreeKey, PermissionTreeDef>,
    set: SourceSet,
}

/// The source collections of every registered or committed tree.
#[derive(Debug, Default)]
pub struct SourceIndex {
    cached: RwLock<SourceSet>,
    committed: RwLock<Committed>,
}

impl SourceIndex {
    /// Rebuild the cached set from the cache's tree definitions.
    pub(super) fn rebuild(&self, tree_defs: &HashMap<TreeKey, PermissionTreeDef>) {
        *self.cached.write().unwrap_or_else(|p| p.into_inner()) = SourceSet::from_defs(tree_defs);
    }

    /// Record a tree definition the metadata applier committed. `None`
    /// removes the tree of `key`.
    pub fn note_committed(&self, key: &TreeKey, def: Option<&PermissionTreeDef>) {
        let mut committed = self.committed.write().unwrap_or_else(|p| p.into_inner());
        match def {
            Some(def) => {
                committed.defs.insert(key.clone(), def.clone());
            }
            None => {
                committed.defs.remove(key);
            }
        }
        committed.set = SourceSet::from_defs(&committed.defs);
    }

    fn any(&self, test: impl Fn(&SourceSet) -> bool) -> bool {
        test(&self.cached.read().unwrap_or_else(|p| p.into_inner()))
            || test(&self.committed.read().unwrap_or_else(|p| p.into_inner()).set)
    }

    /// Whether no tree is registered or committed.
    pub fn is_empty(&self) -> bool {
        !self.any(|set| !set.collections.is_empty())
    }

    /// Whether `collection`, a database-qualified name as a plan carries it,
    /// feeds a tree.
    pub fn is_source_collection(&self, collection: &str) -> bool {
        self.any(|set| set.collections.contains(collection))
    }

    /// Whether `vshard_id` homes a collection that feeds a tree.
    pub fn is_source_vshard(&self, vshard_id: u32) -> bool {
        self.any(|set| set.vshards.contains(&vshard_id))
    }

    /// Every vShard that homes a source collection.
    pub fn source_vshards(&self) -> Vec<u32> {
        let mut vshards: HashSet<u32> = self
            .cached
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .vshards
            .clone();
        vshards.extend(
            self.committed
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .set
                .vshards
                .iter()
                .copied(),
        );
        vshards.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DatabaseId;

    fn def() -> PermissionTreeDef {
        sonic_rs::from_str(
            r#"{"resource_column":"id","graph_index":"tree","permission_table":"grants"}"#,
        )
        .expect("tree def")
    }

    #[test]
    fn the_index_names_governed_collections_and_permission_tables() {
        let mut defs = HashMap::new();
        defs.insert(TreeKey::new(DatabaseId::DEFAULT, 1, "docs"), def());
        let index = SourceIndex::default();
        assert!(index.is_empty());
        index.rebuild(&defs);
        assert!(index.is_source_collection("docs"));
        assert!(index.is_source_collection("grants"));
        assert!(!index.is_source_collection("other"));
        let grants_vshard = TreeKey::new(DatabaseId::DEFAULT, 1, "grants")
            .vshard()
            .as_u32();
        assert!(index.is_source_vshard(grants_vshard));
        defs.clear();
        index.rebuild(&defs);
        assert!(index.is_empty());
        assert!(!index.is_source_vshard(grants_vshard));
    }

    /// A tree in a named database names its sources by their qualified
    /// names and their own vShards, never the default database's.
    #[test]
    fn a_named_database_tree_names_its_own_sources() {
        let db = DatabaseId::new(7);
        let mut defs = HashMap::new();
        defs.insert(TreeKey::new(db, 1, "docs"), def());
        let index = SourceIndex::default();
        index.rebuild(&defs);
        assert!(index.is_source_collection("7/docs"));
        assert!(index.is_source_collection("7/grants"));
        assert!(!index.is_source_collection("docs"));
        assert!(!index.is_source_collection("grants"));
        let vshards = index.source_vshards();
        assert!(vshards.contains(&TreeKey::new(db, 1, "grants").vshard().as_u32()));
        assert!(vshards.contains(&TreeKey::new(db, 1, "docs").vshard().as_u32()));
    }

    #[test]
    fn a_committed_tree_counts_before_the_cache_takes_it() {
        let key = TreeKey::new(DatabaseId::DEFAULT, 1, "docs");
        let index = SourceIndex::default();
        index.note_committed(&key, Some(&def()));
        assert!(index.is_source_collection("grants"));
        assert!(index.is_source_collection("docs"));
        index.note_committed(&key, None);
        assert!(index.is_empty());
    }
}
