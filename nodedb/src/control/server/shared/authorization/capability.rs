//! Linear authorization capabilities for physical task dispatch.

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};
use nodedb_types::{DatabaseId, TenantId};

use crate::control::security::identity::Permission;

use crate::types::{TxnId, VShardId};

/// A collection-scoped authorization capability for non-physical side effects.
#[derive(Debug)]
#[must_use = "an authorized collection capability must be consumed"]
pub struct AuthorizedCollection {
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: String,
    permission: Permission,
}

impl AuthorizedCollection {
    pub(super) fn new(
        tenant_id: TenantId,
        database_id: DatabaseId,
        collection: &str,
        permission: Permission,
    ) -> Self {
        Self {
            tenant_id,
            database_id,
            collection: collection.to_owned(),
            permission,
        }
    }

    pub(crate) fn into_scope(self) -> (TenantId, DatabaseId, String, Permission) {
        (
            self.tenant_id,
            self.database_id,
            self.collection,
            self.permission,
        )
    }
}

/// A task set that passed the shared authorization boundary.
///
/// The inner tasks are private so callers cannot attach authorization to a
/// different plan after the check. Dispatch consumes each capability.
#[derive(Debug)]
#[must_use = "authorized tasks must be dispatched or explicitly discarded"]
pub struct AuthorizedTaskSet {
    tasks: Vec<AuthorizedTask>,
}

impl AuthorizedTaskSet {
    pub(super) fn new(tasks: &[PhysicalTask]) -> Self {
        Self {
            tasks: tasks.iter().cloned().map(AuthorizedTask::new).collect(),
        }
    }

    /// Number of authorized tasks.
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Whether no tasks were authorized.
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Consume the set as independently linear task capabilities.
    pub fn into_tasks(self) -> Vec<AuthorizedTask> {
        self.tasks
    }
}

/// One physical task bound to a successful authorization decision.
#[derive(Debug)]
#[must_use = "an authorized task must be dispatched or explicitly discarded"]
pub struct AuthorizedTask {
    task: PhysicalTask,
}

impl AuthorizedTask {
    fn new(task: PhysicalTask) -> Self {
        Self { task }
    }

    pub fn tenant_id(&self) -> TenantId {
        self.task.tenant_id
    }

    pub fn database_id(&self) -> DatabaseId {
        self.task.database_id
    }

    pub fn vshard_id(&self) -> VShardId {
        self.task.vshard_id
    }

    pub fn txn_id(&self) -> Option<TxnId> {
        self.task.txn_id
    }

    pub fn post_set_op(&self) -> PostSetOp {
        self.task.post_set_op
    }

    pub(crate) fn plan(&self) -> &PhysicalPlan {
        &self.task.plan
    }

    /// Consume authorization before an exact task enters transaction staging.
    ///
    /// The staging gate may buffer the task for trusted COMMIT replay or derive
    /// a `MetaOp::StageWrite` task that is authorized again before dispatch.
    pub(crate) fn into_staging_task(self) -> PhysicalTask {
        self.task
    }

    pub(crate) fn into_physical_task(self) -> PhysicalTask {
        self.task
    }
}
