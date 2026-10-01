// SPDX-License-Identifier: BUSL-1.1

//! A RESTORE batch: bind every identity its rows and edge endpoints are
//! stored under.
//!
//! The batch's rows and edge versions carry their surrogates inside their
//! encoded records, and nothing rewrites them. So each identity binds
//! exactly: a catalog that binds the key to another surrogate has diverged
//! from the coordinator's, and installing on top of it stores a row no
//! lookup by key reaches.

use nodedb_physical::physical_plan::RestoredRedo;
use nodedb_types::Surrogate;

use super::binder::IdentityBinder;

pub(super) fn bind(binder: &IdentityBinder<'_>, batch: &RestoredRedo) -> crate::Result<()> {
    for identity in &batch.identities {
        let carried = Surrogate::new(identity.surrogate);
        let bound = binder.resolve(
            binder.bare_key(&identity.collection),
            &identity.pk_bytes,
            carried,
        )?;
        if bound != carried {
            return Err(crate::Error::Internal {
                detail: format!(
                    "surrogate binding diverged on '{}': this node binds the key to {}, the \
                     RESTORE stores its row under {}",
                    identity.collection,
                    bound.as_u32(),
                    carried.as_u32()
                ),
            });
        }
    }
    Ok(())
}
