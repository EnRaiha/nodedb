// SPDX-License-Identifier: BUSL-1.1

//! Node-binding resolution and compatibility checks shared by triple
//! evaluation and continuation resumption.

use crate::engine::graph::csr::CsrIndex;
use crate::engine::graph::pattern::ast::NodeBinding;
use crate::engine::graph::pattern::executor::types::BindingRow;

/// Resolve a node binding to the set of candidate node ids.
///
/// If `binding.name` is already bound in `row`, returns the single resolved
/// id (or empty if the label constraint fails). Otherwise enumerates all
/// nodes, filtered by label and the optional frontier bitmap.
pub(super) fn resolve_binding(
    binding: &NodeBinding,
    csr: &CsrIndex,
    row: &BindingRow,
    frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
) -> Vec<u32> {
    if let Some(ref name) = binding.name
        && let Some(value) = row.get(name)
    {
        if let Some(id) = csr.node_id_raw(value) {
            // Check label constraint if specified.
            if let Some(ref label) = binding.label
                && !csr.node_has_label(id, label)
            {
                return Vec::new();
            }
            return vec![id];
        }
        return Vec::new();
    }
    // No binding yet — enumerate all nodes, filtering by label and bitmap.
    let all = 0..csr.node_count() as u32;
    all.filter(|&id| {
        let label_ok = binding
            .label
            .as_ref()
            .is_none_or(|l| csr.node_has_label(id, l));
        let bitmap_ok = frontier_bitmap
            .is_none_or(|bm| bm.contains(nodedb_types::Surrogate::new(csr.node_surrogate_raw(id))));
        label_ok && bitmap_ok
    })
    .collect()
}

pub(in crate::engine::graph::pattern::executor) fn binding_compatible(
    binding: &NodeBinding,
    csr: &CsrIndex,
    row: &BindingRow,
    node_id: u32,
) -> bool {
    // Check label constraint.
    if let Some(ref label) = binding.label
        && !csr.node_has_label(node_id, label)
    {
        return false;
    }
    if let Some(ref name) = binding.name
        && let Some(existing) = row.get(name)
    {
        return existing == csr.node_name_raw(node_id);
    }
    true
}

pub(in crate::engine::graph::pattern::executor) fn bind_node(
    row: &mut BindingRow,
    binding: &NodeBinding,
    csr: &CsrIndex,
    node_id: u32,
) {
    if let Some(ref name) = binding.name {
        row.entry(name.clone())
            .or_insert_with(|| csr.node_name_raw(node_id).to_string());
    }
}
