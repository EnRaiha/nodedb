// SPDX-License-Identifier: Apache-2.0

//! Structural reconstruction of Loro containers for `restore_to_version`.
//!
//! `read_at_version` / `read_row` flatten nested containers into plain
//! `LoroValue::Map` / `LoroValue::List` for cheap point reads — fine for
//! display, wrong for restore. Feeding that flattened shape back through the
//! scalar `insert` path would destroy the identity of any CRDT-backed nested
//! container (e.g. the Notion-style block list in `list_ops.rs`). These
//! helpers instead walk the *live* historical `ValueOrContainer` tree —
//! obtained by forking the doc at the target version rather than reading its
//! flattened projection — and rebuild each container from scratch in the
//! live document: `insert_container` + recursive repopulation of children.
//!
//! `insert` / `insert_container` overwrite whatever was previously stored at
//! a key or index, so this naturally implements the same "restore is a
//! forward, full-projection replace" contract as `CrdtState::upsert`.

use loro::{LoroList, LoroMap, LoroMovableList, LoroText, LoroValue, ValueOrContainer};

use crate::error::{CrdtError, Result};

/// Rebuild a single map field from a historical `ValueOrContainer`.
pub(crate) fn rebuild_map_field(dst: &LoroMap, key: &str, src: ValueOrContainer) -> Result<()> {
    match src {
        ValueOrContainer::Value(v) => dst
            .insert(key, v)
            .map_err(|e| CrdtError::Loro(format!("restore field '{key}': {e}"))),
        ValueOrContainer::Container(loro::Container::Map(m)) => {
            let new_map = dst
                .insert_container(key, LoroMap::new())
                .map_err(|e| CrdtError::Loro(format!("restore nested map '{key}': {e}")))?;
            populate_map(&new_map, &m)
        }
        ValueOrContainer::Container(loro::Container::MovableList(l)) => {
            let new_list = dst
                .insert_container(key, LoroMovableList::new())
                .map_err(|e| CrdtError::Loro(format!("restore movable list '{key}': {e}")))?;
            populate_movable_list(&new_list, &l)
        }
        ValueOrContainer::Container(loro::Container::List(l)) => {
            let new_list = dst
                .insert_container(key, LoroList::new())
                .map_err(|e| CrdtError::Loro(format!("restore list '{key}': {e}")))?;
            populate_list(&new_list, &l)
        }
        ValueOrContainer::Container(loro::Container::Text(t)) => {
            let new_text = dst
                .insert_container(key, LoroText::new())
                .map_err(|e| CrdtError::Loro(format!("restore text '{key}': {e}")))?;
            new_text
                .insert(0, &t.to_string())
                .map_err(|e| CrdtError::Loro(format!("restore text content '{key}': {e}")))
        }
        ValueOrContainer::Container(other) => Err(CrdtError::Loro(format!(
            "restore field '{key}': unsupported container variant {other:?} cannot be reconstructed"
        ))),
    }
}

fn populate_map(dst: &LoroMap, src: &LoroMap) -> Result<()> {
    for key in src.keys() {
        if let Some(value) = src.get(&key) {
            rebuild_map_field(dst, &key, value)?;
        }
    }
    Ok(())
}

fn populate_movable_list(dst: &LoroMovableList, src: &LoroMovableList) -> Result<()> {
    for idx in 0..src.len() {
        if let Some(value) = src.get(idx) {
            rebuild_list_element(dst, idx, value)?;
        }
    }
    Ok(())
}

fn populate_list(dst: &LoroList, src: &LoroList) -> Result<()> {
    for idx in 0..src.len() {
        if let Some(value) = src.get(idx) {
            rebuild_list_element(dst, idx, value)?;
        }
    }
    Ok(())
}

/// Common insertion surface shared by `LoroList` and `LoroMovableList` so
/// `rebuild_list_element` can be written once instead of duplicated per
/// list-container type.
trait ListSink {
    fn insert_value(&self, idx: usize, v: LoroValue) -> loro::LoroResult<()>;
    fn insert_map(&self, idx: usize) -> loro::LoroResult<LoroMap>;
    fn insert_movable_list(&self, idx: usize) -> loro::LoroResult<LoroMovableList>;
    fn insert_list(&self, idx: usize) -> loro::LoroResult<LoroList>;
    fn insert_text(&self, idx: usize) -> loro::LoroResult<LoroText>;
}

impl ListSink for LoroMovableList {
    fn insert_value(&self, idx: usize, v: LoroValue) -> loro::LoroResult<()> {
        self.insert(idx, v)
    }
    fn insert_map(&self, idx: usize) -> loro::LoroResult<LoroMap> {
        self.insert_container(idx, LoroMap::new())
    }
    fn insert_movable_list(&self, idx: usize) -> loro::LoroResult<LoroMovableList> {
        self.insert_container(idx, LoroMovableList::new())
    }
    fn insert_list(&self, idx: usize) -> loro::LoroResult<LoroList> {
        self.insert_container(idx, LoroList::new())
    }
    fn insert_text(&self, idx: usize) -> loro::LoroResult<LoroText> {
        self.insert_container(idx, LoroText::new())
    }
}

impl ListSink for LoroList {
    fn insert_value(&self, idx: usize, v: LoroValue) -> loro::LoroResult<()> {
        self.insert(idx, v)
    }
    fn insert_map(&self, idx: usize) -> loro::LoroResult<LoroMap> {
        self.insert_container(idx, LoroMap::new())
    }
    fn insert_movable_list(&self, idx: usize) -> loro::LoroResult<LoroMovableList> {
        self.insert_container(idx, LoroMovableList::new())
    }
    fn insert_list(&self, idx: usize) -> loro::LoroResult<LoroList> {
        self.insert_container(idx, LoroList::new())
    }
    fn insert_text(&self, idx: usize) -> loro::LoroResult<LoroText> {
        self.insert_container(idx, LoroText::new())
    }
}

fn rebuild_list_element<S: ListSink>(dst: &S, idx: usize, src: ValueOrContainer) -> Result<()> {
    match src {
        ValueOrContainer::Value(v) => dst
            .insert_value(idx, v)
            .map_err(|e| CrdtError::Loro(format!("restore list element {idx}: {e}"))),
        ValueOrContainer::Container(loro::Container::Map(m)) => {
            let new_map = dst
                .insert_map(idx)
                .map_err(|e| CrdtError::Loro(format!("restore list element map {idx}: {e}")))?;
            populate_map(&new_map, &m)
        }
        ValueOrContainer::Container(loro::Container::MovableList(l)) => {
            let new_list = dst
                .insert_movable_list(idx)
                .map_err(|e| CrdtError::Loro(format!("restore nested movable list {idx}: {e}")))?;
            populate_movable_list(&new_list, &l)
        }
        ValueOrContainer::Container(loro::Container::List(l)) => {
            let new_list = dst
                .insert_list(idx)
                .map_err(|e| CrdtError::Loro(format!("restore nested list {idx}: {e}")))?;
            populate_list(&new_list, &l)
        }
        ValueOrContainer::Container(loro::Container::Text(t)) => {
            let new_text = dst
                .insert_text(idx)
                .map_err(|e| CrdtError::Loro(format!("restore list text {idx}: {e}")))?;
            new_text
                .insert(0, &t.to_string())
                .map_err(|e| CrdtError::Loro(format!("restore list text content {idx}: {e}")))
        }
        ValueOrContainer::Container(other) => Err(CrdtError::Loro(format!(
            "restore list element {idx}: unsupported container variant {other:?} cannot be reconstructed"
        ))),
    }
}
