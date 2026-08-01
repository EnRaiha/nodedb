//! Protocol-neutral authorization for SQL physical tasks.

pub mod capability;
pub mod error;
pub mod requirements;
pub mod service;

pub use capability::{AuthorizedCollection, AuthorizedTask, AuthorizedTaskSet};
pub use error::AuthorizationError;
pub use requirements::{AuthorizationRequirement, plan_requirements};
pub use service::{
    authorize_collection, authorize_collection_capability, authorize_database,
    authorize_database_permission, authorize_task_set,
};
