// SPDX-License-Identifier: BUSL-1.1

//! A real surrogate assigner for converter unit tests: a single-node registry
//! over an in-memory credential store.

use std::sync::{Arc, RwLock};

use crate::control::security::credential::CredentialStore;
use crate::control::surrogate::SurrogateAssigner;
use crate::control::surrogate::registry::SurrogateRegistry;
use crate::control::surrogate::wal_appender::{NoopWalAppender, SurrogateWalAppender};

/// A single-node assigner that binds every key in its own in-memory catalog.
pub(crate) fn test_assigner() -> Arc<SurrogateAssigner> {
    let credentials = Arc::new(CredentialStore::new().expect("in-memory credential store"));
    let registry = Arc::new(RwLock::new(SurrogateRegistry::new()));
    let wal: Arc<dyn SurrogateWalAppender> = Arc::new(NoopWalAppender);
    Arc::new(SurrogateAssigner::new(registry, credentials, wal))
}
