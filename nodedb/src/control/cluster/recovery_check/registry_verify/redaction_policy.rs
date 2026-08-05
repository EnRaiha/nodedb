// SPDX-License-Identifier: BUSL-1.1

//! `RedactionStore` verifier.
//!
//! Checks that the in-memory `RedactionStore` is consistent with
//! the `_system.redaction_policies` redb table.
//!
//! **What it checks:**
//! - Every policy in redb has a matching entry in the in-memory store
//!   (key = `{tenant_id}|{collection}|{for_role}`, value encodes the
//!   policy name and the full rule list, so any edit that left the key
//!   unchanged still surfaces as a value mismatch — redaction policies
//!   have no `enabled` flag like RLS does, and a rule *count* alone
//!   would miss the divergence that matters most: a field whose mode
//!   changed, e.g. `Mask` degraded to a weaker mask string).
//! - Every policy in memory has a matching row in redb (ghost entries
//!   from a buggy load_from path).
//!
//! **What it does NOT check:**
//! - Whether the target collection is active or even exists — that
//!   cross-entity check is deferred to a future integrity pass.
//!   The verifier strictly covers load_from coherence.

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::redaction::{RedactionMode, RedactionRule, RedactionStore};

use super::super::divergence::{Divergence, DivergenceKind};
use super::diff::diff_sorted;

/// Canonical rendering of a rule list, used as the comparison value on both
/// sides of the diff. Covers the mask payload, not just the mode tag, so a
/// policy whose mask string drifted is reported rather than passing as equal.
fn render_rules(rules: &[RedactionRule]) -> String {
    let mut parts: Vec<String> = rules
        .iter()
        .map(|r| {
            let mode = match &r.mode {
                RedactionMode::Mask(m) => format!("Mask({m})"),
                RedactionMode::Hash => "Hash".to_string(),
                RedactionMode::Null => "Null".to_string(),
            };
            format!("{}={mode}", r.field)
        })
        .collect();
    // Rule order is not semantically meaningful, and the two sides reach this
    // point through different paths (redb decode vs live map). Sort so an
    // ordering difference is not reported as a divergence.
    parts.sort();
    parts.join(",")
}

pub fn verify_redaction_policies(
    store: &RedactionStore,
    catalog: &SystemCatalog,
) -> crate::Result<Vec<Divergence>> {
    let mut expected: Vec<(String, String)> = catalog
        .load_all_redaction_policies()?
        .into_iter()
        .map(|p| {
            let key = format!("{}|{}|{}", p.tenant_id, p.collection, p.for_role);
            // An unparseable row must never compare equal to a well-formed
            // in-memory policy, so it gets a rendering no rule list can
            // produce rather than being flattened to "no rules".
            let rules = match sonic_rs::from_str::<Vec<RedactionRule>>(&p.rules_json) {
                Ok(r) => render_rules(&r),
                Err(_) => "<unparseable>".to_string(),
            };
            let value = format!("name={},rules=[{}]", p.name, rules);
            (key, value)
        })
        .collect();
    expected.sort_by(|a, b| a.0.cmp(&b.0));

    let mut actual: Vec<(String, String)> = store
        .list_all_flat()
        .into_iter()
        .map(|p| {
            let key = format!("{}|{}|{}", p.tenant_id, p.collection, p.for_role);
            let value = format!("name={},rules=[{}]", p.name, render_rules(&p.rules));
            (key, value)
        })
        .collect();
    actual.sort_by(|a, b| a.0.cmp(&b.0));

    let diff = diff_sorted(&expected, &actual, |a, b| a == b);
    let mut out = Vec::new();
    for (key, _) in &diff.only_in_expected {
        out.push(Divergence::new(DivergenceKind::MissingInRegistry {
            registry: "redaction_policies",
            key: key.clone(),
        }));
    }
    for (key, _) in &diff.only_in_actual {
        out.push(Divergence::new(DivergenceKind::ExtraInRegistry {
            registry: "redaction_policies",
            key: key.clone(),
        }));
    }
    for (key, redb_val, mem_val) in &diff.mismatched {
        out.push(Divergence::new(DivergenceKind::ValueMismatch {
            registry: "redaction_policies",
            key: key.clone(),
            detail: format!("redb={redb_val}, memory={mem_val}"),
        }));
    }
    Ok(out)
}

/// Repair: clear in-memory store and reload from redb.
pub fn repair_redaction_policies(
    store: &RedactionStore,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    store.clear_and_reload(catalog)
}
