// SPDX-License-Identifier: Apache-2.0

//! RAII unwind scope for a multi-layer budget reservation.
//!
//! [`ReserveScope`] owns the rollback for a reservation spanning any subset
//! of the global, database, tenant, and engine counters. A caller credits
//! each layer as it succeeds and calls [`ReserveScope::commit`] once every
//! layer needed is credited. Dropping the scope before `commit` releases
//! every credited layer, in reverse order, so a caller never hand-writes a
//! rollback block for a failed layer.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::budget::atomic_saturating_sub;
use crate::error::{MemError, Result};
use crate::governor::GlobalCounter;

/// Owns the unwind for a reservation across the global, database, tenant,
/// and engine counters.
///
/// Every layer credited before `commit` is released on `Drop` if the scope
/// was never committed. `commit` consumes the scope and hands back the
/// credited layers as a [`ReservedLayers`] for the caller to hold.
pub(crate) struct ReserveScope {
    global: Arc<GlobalCounter>,
    global_credited: bool,
    database: Option<Arc<AtomicUsize>>,
    tenant: Option<Arc<AtomicUsize>>,
    engine: Option<Arc<AtomicUsize>>,
    size: usize,
    committed: bool,
}

/// The counters a committed [`ReserveScope`] credited, handed to the caller
/// so it can build a [`crate::reservation_token::ReservationToken`] on top
/// of them.
pub(crate) struct ReservedLayers {
    pub global: Arc<GlobalCounter>,
    pub database: Option<Arc<AtomicUsize>>,
    pub tenant: Option<Arc<AtomicUsize>>,
    pub engine: Option<Arc<AtomicUsize>>,
}

impl ReserveScope {
    /// Start a scope for a `size`-byte reservation. Credits nothing yet.
    pub(crate) fn new(global: Arc<GlobalCounter>, size: usize) -> Self {
        Self {
            global,
            global_credited: false,
            database: None,
            tenant: None,
            engine: None,
            size,
            committed: false,
        }
    }

    /// Credit the global ceiling via a CAS loop.
    ///
    /// A `size` of zero credits nothing and always succeeds. Returns
    /// [`MemError::GlobalCeilingExceeded`] without crediting anything when
    /// the ceiling would be exceeded.
    pub(crate) fn try_credit_global(&mut self) -> Result<()> {
        if self.size == 0 {
            self.global_credited = true;
            return Ok(());
        }
        loop {
            let current = self.global.allocated.load(Ordering::Relaxed);
            if current + self.size > self.global.ceiling {
                return Err(MemError::GlobalCeilingExceeded {
                    allocated: current,
                    ceiling: self.global.ceiling,
                    requested: self.size,
                });
            }
            match self.global.allocated.compare_exchange_weak(
                current,
                current + self.size,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.global_credited = true;
                    return Ok(());
                }
                Err(_) => continue,
            }
        }
    }

    /// Record an already-credited database counter so `Drop` releases it.
    pub(crate) fn credit_database(&mut self, counter: Arc<AtomicUsize>) {
        self.database = Some(counter);
    }

    /// Record an already-credited tenant counter so `Drop` releases it.
    pub(crate) fn credit_tenant(&mut self, counter: Arc<AtomicUsize>) {
        self.tenant = Some(counter);
    }

    /// Record an already-credited engine counter so `Drop` releases it.
    pub(crate) fn credit_engine(&mut self, counter: Arc<AtomicUsize>) {
        self.engine = Some(counter);
    }

    /// Finish the scope successfully. Disarms `Drop` and hands back every
    /// credited layer.
    ///
    /// Takes each layer out of `self` rather than cloning it: `Drop` is a
    /// no-op once `committed` is set, so a clone here would only add an
    /// atomic refcount round-trip on a value nobody else holds.
    pub(crate) fn commit(mut self) -> ReservedLayers {
        self.committed = true;
        ReservedLayers {
            global: Arc::clone(&self.global),
            database: self.database.take(),
            tenant: self.tenant.take(),
            engine: self.engine.take(),
        }
    }
}

impl Drop for ReserveScope {
    fn drop(&mut self) {
        if self.committed || self.size == 0 {
            return;
        }
        // Release in reverse order: engine → tenant → database → global.
        if let Some(ref counter) = self.engine {
            atomic_saturating_sub(counter, self.size);
        }
        if let Some(ref counter) = self.tenant {
            atomic_saturating_sub(counter, self.size);
        }
        if let Some(ref counter) = self.database {
            atomic_saturating_sub(counter, self.size);
        }
        if self.global_credited {
            atomic_saturating_sub(&self.global.allocated, self.size);
        }
    }
}
