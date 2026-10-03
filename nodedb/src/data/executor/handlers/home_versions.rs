// SPDX-License-Identifier: BUSL-1.1

//! `MetaOp::HomeVersions`: this core's current version of each probed home.
//!
//! The Control Plane hands a core only the probes whose vShard the core owns.
//! A probe with a collection answers the collection's write floor on this
//! core. A probe without one answers the core watermark. Nothing is read
//! beyond those two values, and nothing is written.

use nodedb_physical::physical_plan::{HomeAnswer, HomeVersion, HomeVersionProbe};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::write_index::CollKey;
use crate::data::executor::task::ExecutionTask;
use crate::types::Lsn;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_home_versions(
        &self,
        task: &ExecutionTask,
        probes: &[HomeVersionProbe],
    ) -> Response {
        let answers: Vec<HomeVersion> = probes
            .iter()
            .map(|probe| HomeVersion {
                probe: probe.clone(),
                answer: HomeAnswer::Version(self.home_version(task, probe).as_u64()),
            })
            .collect();
        match zerompk::to_msgpack_vec(&answers) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("home versions: encoding the answer failed: {e}"),
                },
            ),
        }
    }

    fn home_version(&self, task: &ExecutionTask, probe: &HomeVersionProbe) -> Lsn {
        match probe.collection.as_deref() {
            Some(collection) => self
                .write_index
                .collection_write_lsn(&CollKey {
                    db: task.request.database_id,
                    tenant: task.request.tenant_id,
                    collection: Box::from(collection),
                })
                .unwrap_or(Lsn::ZERO),
            None => self.watermark,
        }
    }
}
