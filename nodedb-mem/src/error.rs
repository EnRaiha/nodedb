// SPDX-License-Identifier: Apache-2.0

use nodedb_types::{DatabaseId, TenantId};

use crate::engine::EngineId;

/// Errors produced by the memory governor.
#[derive(Debug, thiserror::Error)]
pub enum MemError {
    /// Allocation request exceeds the engine's remaining budget.
    #[error(
        "memory budget exhausted for {engine:?}: requested {requested} bytes, \
         available {available} bytes (limit: {limit} bytes)"
    )]
    BudgetExhausted {
        engine: EngineId,
        requested: usize,
        available: usize,
        limit: usize,
    },

    /// The global memory ceiling would be exceeded.
    #[error(
        "global memory ceiling exceeded: total allocated {allocated} bytes, \
         ceiling {ceiling} bytes, requested {requested} bytes"
    )]
    GlobalCeilingExceeded {
        allocated: usize,
        ceiling: usize,
        requested: usize,
    },

    /// The per-database memory budget would be exceeded.
    #[error(
        "database memory budget exhausted for database {db:?}: \
         requested {requested} bytes, available {available} bytes (limit: {limit} bytes)"
    )]
    DatabaseBudgetExhausted {
        db: DatabaseId,
        requested: usize,
        available: usize,
        limit: usize,
    },

    /// The per-tenant memory budget would be exceeded.
    #[error(
        "tenant memory budget exhausted for tenant {tenant:?} in database {db:?}: \
         requested {requested} bytes, available {available} bytes (limit: {limit} bytes)"
    )]
    TenantBudgetExhausted {
        db: DatabaseId,
        tenant: TenantId,
        requested: usize,
        available: usize,
        limit: usize,
    },

    /// Engine is not registered with the governor.
    #[error("unknown engine: {0:?}")]
    UnknownEngine(EngineId),

    /// jemalloc introspection error.
    #[error("jemalloc error: {0}")]
    Jemalloc(String),
}

pub type Result<T> = std::result::Result<T, MemError>;
