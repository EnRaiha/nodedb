// SPDX-License-Identifier: BUSL-1.1

//! The seam through which a cluster node obtains a key's surrogate from the
//! key's collection home.
//!
//! In a cluster a key's surrogate is bound in exactly one place: the Raft log
//! of the key's collection home vShard, first-wins in log order. Every other
//! node, a coordinator planning a write or a read included, obtains the
//! bound value from the home's leader before it uses the key. It never plans
//! or reads with a value of its own. The implementation routes to the home's
//! leader (`surrogate_exchange`), which lives above this module, so it is
//! installed on the assigner after the node's state is built.

use futures::future::BoxFuture;
use nodedb_types::{CollectionKey, Surrogate, TenantId};

/// Resolves keys at their collection home. Every method awaits the home and
/// never blocks the runtime.
pub trait HomeSurrogateAuthority: Send + Sync {
    /// The bound surrogate of each of `pks` in `key`, bound at the home when
    /// a key has none. The answers are in `pks` order.
    fn assign<'a>(
        &'a self,
        key: CollectionKey<'a>,
        tenant_id: TenantId,
        pks: &'a [&'a [u8]],
    ) -> BoxFuture<'a, crate::Result<Vec<Surrogate>>>;

    /// The bound surrogate of each of `pks` in `key`, `None` for a key the
    /// home binds none of. Never binds. The answers are in `pks` order.
    fn lookup_many<'a>(
        &'a self,
        key: CollectionKey<'a>,
        tenant_id: TenantId,
        pks: &'a [&'a [u8]],
    ) -> BoxFuture<'a, crate::Result<Vec<Option<Surrogate>>>>;
}
