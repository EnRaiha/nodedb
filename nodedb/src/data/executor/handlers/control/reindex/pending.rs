// SPDX-License-Identifier: BUSL-1.1

//! In-flight rebuilds a core tracks, and the per-tick poll that cuts each
//! finished one over.

use std::sync::mpsc::{self, TryRecvError};

use nodedb_graph::csr::rebuild::CsrRebuilt;
use nodedb_types::DatabaseId;
use tracing::error;

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::sparse::inverted::FtsRebuilt;
use crate::types::TenantId;

/// The collection one REINDEX covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebuildTarget {
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
    pub collection: String,
}

/// A build running on its own OS thread. `token` ties it to the write
/// journal on the live index.
pub(in crate::data::executor) enum PendingBuild {
    Fts {
        token: u64,
        rx: mpsc::Receiver<crate::Result<FtsRebuilt>>,
    },
    Csr {
        token: u64,
        rx: mpsc::Receiver<Result<CsrRebuilt, nodedb_graph::GraphError>>,
    },
}

/// One in-flight rebuild of one index of one collection.
pub struct PendingReindex {
    pub target: RebuildTarget,
    pub(in crate::data::executor) build: PendingBuild,
}

/// What one poll of a build found.
enum BuildPoll {
    Running(PendingBuild),
    Fts {
        token: u64,
        result: crate::Result<FtsRebuilt>,
    },
    Csr {
        token: u64,
        result: crate::Result<CsrRebuilt>,
    },
}

impl PendingBuild {
    fn poll(self) -> BuildPoll {
        match self {
            Self::Fts { token, rx } => match rx.try_recv() {
                Ok(result) => BuildPoll::Fts { token, result },
                Err(TryRecvError::Empty) => BuildPoll::Running(Self::Fts { token, rx }),
                Err(TryRecvError::Disconnected) => BuildPoll::Fts {
                    token,
                    result: Err(thread_lost("fts")),
                },
            },
            Self::Csr { token, rx } => match rx.try_recv() {
                Ok(result) => BuildPoll::Csr {
                    token,
                    result: result.map_err(crate::Error::from),
                },
                Err(TryRecvError::Empty) => BuildPoll::Running(Self::Csr { token, rx }),
                Err(TryRecvError::Disconnected) => BuildPoll::Csr {
                    token,
                    result: Err(thread_lost("csr")),
                },
            },
        }
    }
}

fn thread_lost(index: &str) -> crate::Error {
    crate::Error::Internal {
        detail: format!("{index} rebuild thread exited without a result"),
    }
}

impl CoreLoop {
    /// Cut over every finished rebuild. Called from `tick()`.
    pub fn poll_pending_reindex(&mut self) {
        if self.maintenance.pending_reindex.is_empty() {
            return;
        }
        let entries = std::mem::take(&mut self.maintenance.pending_reindex);
        let mut running = Vec::with_capacity(entries.len());
        for PendingReindex { target, build } in entries {
            match build.poll() {
                BuildPoll::Running(build) => running.push(PendingReindex { target, build }),
                BuildPoll::Fts { token, result } => {
                    let installed = result.and_then(|rebuilt| self.install_fts(&target, rebuilt));
                    if let Err(e) = installed {
                        self.inverted.abort_rebuild(token);
                        self.report_rebuild_refused("fts", &target, &e);
                        self.note_reindex_refused(&target, &e);
                    }
                }
                BuildPoll::Csr { token, result } => {
                    let installed = result.and_then(|rebuilt| self.install_csr(&target, rebuilt));
                    if let Err(e) = installed {
                        self.abort_csr_rebuild(&target, token);
                        self.report_rebuild_refused("csr", &target, &e);
                        self.note_reindex_refused(&target, &e);
                    }
                }
            }
        }
        self.maintenance.pending_reindex.extend(running);
    }

    /// Log and record a rebuild the core discarded. The live index keeps
    /// every write; only the rebuild is lost.
    pub(super) fn report_rebuild_refused(
        &self,
        index: &'static str,
        target: &RebuildTarget,
        err: &crate::Error,
    ) {
        error!(
            target: "nodedb::reindex",
            core = self.core_id,
            index,
            collection = %target.collection,
            error = %err,
            "rebuild_refused"
        );
        crate::diag::index_rebuild_not_installed(
            err,
            &crate::diag::IndexRebuildTarget {
                index,
                database_id: target.database_id.as_u64(),
                tenant_id: target.tenant_id.as_u64(),
                collection: &target.collection,
            },
        );
    }
}
