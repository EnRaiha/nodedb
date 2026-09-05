// SPDX-License-Identifier: BUSL-1.1

use std::sync::Arc;

use tracing::info;

use nodedb_mem::EngineId;
use nodedb_mem::governor::{GovernorConfig, MemoryGovernor};

use crate::config::engine::EngineByteBudgets;

/// Initialize the memory governor from engine byte budgets.
///
/// Called once at startup. The returned governor is shared (via `Arc`)
/// across the Control Plane and all Data Plane cores.
///
/// `budgets` carries a byte limit for every [`EngineId`] —
/// [`nodedb_mem::EngineLimits`] is total by construction, so a partial
/// registration cannot compile.
/// [`EngineByteBudgets`] is built by
/// [`crate::config::EngineConfig::to_byte_budgets`], which derives one
/// entry per `EngineId::ALL` member with a strictly-positive fraction.
pub fn init_governor(
    global_ceiling: usize,
    budgets: &EngineByteBudgets,
) -> crate::Result<Arc<MemoryGovernor>> {
    let engine_limits = budgets.as_engine_limits().clone();

    let config = GovernorConfig {
        global_ceiling,
        engine_limits,
    };

    let governor = MemoryGovernor::new(config).map_err(|e| crate::Error::Config {
        detail: format!("failed to initialize memory governor: {e}"),
    })?;

    info!(
        global_ceiling,
        engines = EngineId::ALL.len(),
        total_engine_budget = budgets.total(),
        "memory governor initialized"
    );

    Ok(Arc::new(governor))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::engine::EngineConfig;

    #[test]
    fn init_from_default_config() {
        let cfg = EngineConfig::default();
        let budgets = cfg.to_byte_budgets(1024 * 1024 * 1024); // 1 GiB
        let gov = init_governor(1024 * 1024 * 1024, &budgets).unwrap();

        assert!(gov.budget(EngineId::Vector).limit() > 0);
        assert!(gov.budget(EngineId::Query).limit() > 0);
        assert!(gov.budget(EngineId::Crdt).limit() > 0);
    }

    #[test]
    fn init_rejects_impossible_budgets() {
        // Byte budgets derived against a 1 MiB ceiling sum to ~1 MiB; feeding
        // them to a governor whose ceiling is only 1000 bytes must fail
        // `GovernorConfig::validate`.
        let budgets = EngineConfig::default().to_byte_budgets(1024 * 1024);
        assert!(budgets.total() > 1000);
        let result = init_governor(1000, &budgets);
        assert!(result.is_err());
    }

    /// Build the governor exactly as the server does on a fresh boot:
    /// default engine fractions of a 1 GiB ceiling.
    fn fresh_boot_governor() -> Arc<MemoryGovernor> {
        let global = 1024 * 1024 * 1024usize;
        let budgets = EngineConfig::default().to_byte_budgets(global);
        init_governor(global, &budgets).expect("default config must produce a governor")
    }

    /// `MemoryGovernor::engine_pressure` reports `PressureLevel::Emergency`
    /// for any engine with a zero limit. The Data Plane calls
    /// `check_engine_pressure` at the top of every write handler, so an
    /// engine with no positive share on a fresh server turns its very first
    /// write into a client-facing `resources exhausted` error — even though
    /// the box has gigabytes free.
    ///
    /// This is the reported `document_schemaless` symptom; the other
    /// assertions below cover the sibling engines that share the same
    /// write-pressure gate.
    #[test]
    fn document_schemaless_writes_not_starved_on_fresh_governor() {
        let gov = fresh_boot_governor();
        assert!(
            gov.budget(EngineId::DocumentSchemaless).limit() > 0,
            "document_schemaless has no memory budget on a fresh server"
        );
        assert_ne!(
            gov.engine_pressure(EngineId::DocumentSchemaless),
            nodedb_mem::PressureLevel::Emergency,
            "document_schemaless reports Emergency pressure on a fresh, empty governor — \
             first INSERT will fail with `resources exhausted`"
        );
    }

    #[test]
    fn kv_writes_not_starved_on_fresh_governor() {
        let gov = fresh_boot_governor();
        assert!(
            gov.budget(EngineId::Kv).limit() > 0,
            "kv has no memory budget"
        );
        assert_ne!(
            gov.engine_pressure(EngineId::Kv),
            nodedb_mem::PressureLevel::Emergency,
            "kv reports Emergency pressure on a fresh governor"
        );
    }

    #[test]
    fn columnar_writes_not_starved_on_fresh_governor() {
        let gov = fresh_boot_governor();
        assert!(
            gov.budget(EngineId::Columnar).limit() > 0,
            "columnar has no memory budget"
        );
        assert_ne!(
            gov.engine_pressure(EngineId::Columnar),
            nodedb_mem::PressureLevel::Emergency,
            "columnar reports Emergency pressure on a fresh governor"
        );
    }

    #[test]
    fn array_writes_not_starved_on_fresh_governor() {
        let gov = fresh_boot_governor();
        assert!(
            gov.budget(EngineId::Array).limit() > 0,
            "array has no memory budget"
        );
        assert_ne!(
            gov.engine_pressure(EngineId::Array),
            nodedb_mem::PressureLevel::Emergency,
            "array reports Emergency pressure on a fresh governor"
        );
    }

    #[test]
    fn graph_writes_not_starved_on_fresh_governor() {
        let gov = fresh_boot_governor();
        assert!(
            gov.budget(EngineId::Graph).limit() > 0,
            "graph has no memory budget"
        );
        assert_ne!(
            gov.engine_pressure(EngineId::Graph),
            nodedb_mem::PressureLevel::Emergency,
            "graph reports Emergency pressure on a fresh governor"
        );
    }

    #[test]
    fn fts_indexing_not_starved_on_fresh_governor() {
        // Every document write also runs `check_engine_pressure(EngineId::Fts)`
        // because FTS indexing is a side effect of the write.
        let gov = fresh_boot_governor();
        assert!(
            gov.budget(EngineId::Fts).limit() > 0,
            "fts has no memory budget"
        );
        assert_ne!(
            gov.engine_pressure(EngineId::Fts),
            nodedb_mem::PressureLevel::Emergency,
            "fts reports Emergency pressure on a fresh governor"
        );
    }

    /// The root invariant: every engine identifier the rest of the system
    /// can name must have a strictly-positive budget after `init_governor`.
    /// `EngineConfig::validate` enforces a positive fraction per engine and
    /// `EngineLimits` is total by construction, so the only way this can
    /// fail is a zero-fraction default slipping past validation.
    #[test]
    fn every_engine_has_a_budget_on_fresh_governor() {
        let gov = fresh_boot_governor();
        let unfunded: Vec<_> = EngineId::ALL
            .iter()
            .filter(|&&e| gov.budget(e).limit() == 0)
            .map(|e| e.to_string())
            .collect();
        assert!(
            unfunded.is_empty(),
            "engines with a zero memory budget after init_governor: {unfunded:?}"
        );
        for &engine in EngineId::ALL {
            assert_ne!(
                gov.engine_pressure(engine),
                nodedb_mem::PressureLevel::Emergency,
                "{engine} reports Emergency pressure on a fresh governor"
            );
        }
    }
}
