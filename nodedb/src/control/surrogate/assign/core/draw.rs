// SPDX-License-Identifier: BUSL-1.1

//! Fresh draws from this node's surrogate registry on
//! [`super::SurrogateAssigner`]: `mint_candidate`, `assign_fresh` and
//! `assign_anonymous`.
//!
//! A cluster node draws from its reserved batch. When the batch is empty, a
//! draw awaits the background refill loop's next batch and never blocks the
//! runtime.

use nodedb_types::{CollectionKey, Surrogate, TenantId};

use super::types::SurrogateAssigner;

impl SurrogateAssigner {
    /// Draw one surrogate under the registry write lock and run `bind` on it
    /// before the lock is released, then check the flush threshold. Returns
    /// `None` when this node's reserved batch is empty, after nudging the
    /// refill loop.
    fn try_draw<T>(
        &self,
        bind: &mut impl FnMut(Surrogate) -> crate::Result<T>,
    ) -> crate::Result<Option<T>> {
        let registry = self.registry_write()?;
        let Some(surrogate) = self.alloc_locked(&registry)? else {
            drop(registry);
            self.refill_notify.notify_one();
            return Ok(None);
        };
        self.nudge_refill_if_low(&registry);
        let bound = bind(surrogate)?;
        self.maybe_flush(&registry, self.credential_store.catalog())?;
        Ok(Some(bound))
    }

    /// [`Self::try_draw`] until it draws. An empty batch is awaited from the
    /// background refill loop, so the runtime thread never blocks.
    async fn draw<T>(
        &self,
        mut bind: impl FnMut(Surrogate) -> crate::Result<T>,
    ) -> crate::Result<T> {
        loop {
            if let Some(bound) = self.try_draw(&mut bind)? {
                return Ok(bound);
            }
            self.await_reserved_batch().await?;
        }
    }

    /// Bind `surrogate` in this node's catalog and WAL under `pk_bytes`.
    fn bind_drawn(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        pk_bytes: &[u8],
        surrogate: Surrogate,
    ) -> crate::Result<()> {
        self.credential_store
            .catalog()
            .put_surrogate(key, tenant_id, pk_bytes, surrogate)?;
        self.wal_appender
            .record_bind_to_wal(key, tenant_id, surrogate.as_u32(), pk_bytes)
    }

    /// The fresh row identity of `surrogate`: its bound surrogate and the
    /// identity string the row is keyed by.
    fn bind_fresh(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        surrogate: Surrogate,
    ) -> crate::Result<(Surrogate, String)> {
        let pk =
            crate::engine::document::store::RowIdentity::for_surrogate(surrogate).into_string();
        self.bind_drawn(key, tenant_id, pk.as_bytes(), surrogate)?;
        Ok((surrogate, pk))
    }

    /// The anonymous binding of `surrogate`, keyed by its own big-endian
    /// bytes.
    fn bind_anonymous(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        surrogate: Surrogate,
    ) -> crate::Result<Surrogate> {
        self.bind_drawn(key, tenant_id, &surrogate.as_u32().to_be_bytes(), surrogate)?;
        Ok(surrogate)
    }

    /// Draw a fresh surrogate from the registry and bind nothing. The caller
    /// binds it: the collection home proposes it in its Raft log
    /// (`surrogate_exchange::authority`).
    pub async fn mint_candidate(&self) -> crate::Result<Surrogate> {
        self.draw(Ok).await
    }

    /// Allocate a FRESH surrogate for a row with no content primary key — a
    /// collection whose primary key is the auto-generated `_rowid` (no
    /// `PRIMARY KEY` was declared), or a timeseries row.
    ///
    /// [`assign`](Self::assign) has a fast-path lookup. This does not. Every
    /// call allocates a new value, so N rows get N distinct surrogates.
    ///
    /// The surrogate self-binds under its identity, so a later keyed lookup
    /// resolves. The identity is the surrogate's decimal string, matching the
    /// `_rowid` value the Data Plane writes for an auto-`_rowid` row, so
    /// `WHERE _rowid = N` resolves back to it.
    ///
    /// The draw, the bind and the flush check run under one registry write
    /// lock. Returns the bound identity string, which the caller uses
    /// verbatim.
    pub async fn assign_fresh(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
    ) -> crate::Result<(Surrogate, String)> {
        self.draw(|surrogate| self.bind_fresh(key, tenant_id, surrogate))
            .await
    }

    /// Allocate a fresh surrogate for an entity that has no user-facing
    /// primary key (e.g. headless vector inserts). The surrogate is
    /// self-keyed in the catalog (`pk_bytes = surrogate.as_u32().to_be_bytes()`)
    /// so the binding round-trips homogeneously with named-PK rows: a
    /// later lookup via the self-bytes returns the same surrogate, and
    /// the reverse lookup returns the self-bytes back. Keeps the
    /// catalog single-shaped — no special-case "unbound" rows.
    pub async fn assign_anonymous(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
    ) -> crate::Result<Surrogate> {
        self.draw(|surrogate| self.bind_anonymous(key, tenant_id, surrogate))
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use nodedb_types::{CollectionKey, DatabaseId, TenantId};

    use super::super::types::SurrogateAssigner;
    use crate::control::security::credential::CredentialStore;
    use crate::control::surrogate::registry::SurrogateRegistry;
    use crate::control::surrogate::wal_appender::{NoopWalAppender, SurrogateWalAppender};

    fn open_test() -> (tempfile::TempDir, SurrogateAssigner) {
        let dir = tempfile::tempdir().unwrap();
        let credentials = Arc::new(CredentialStore::open(&dir.path().join("system.redb")).unwrap());
        let registry = Arc::new(RwLock::new(SurrogateRegistry::new()));
        let wal: Arc<dyn SurrogateWalAppender> = Arc::new(NoopWalAppender);
        (dir, SurrogateAssigner::new(registry, credentials, wal))
    }

    const T0: TenantId = TenantId::new(0);

    #[tokio::test]
    async fn draws_bind_their_identity() {
        let (_dir, assigner) = open_test();
        let key = CollectionKey::from_bare(DatabaseId::DEFAULT, "rows");

        let (fresh, identity) = assigner.assign_fresh(key, T0).await.unwrap();
        assert_eq!(
            assigner.lookup_bound(key, T0, identity.as_bytes()).unwrap(),
            Some(fresh)
        );

        let anonymous = assigner.assign_anonymous(key, T0).await.unwrap();
        assert_ne!(anonymous, fresh);
        assert_eq!(
            assigner
                .lookup_bound(key, T0, &anonymous.as_u32().to_be_bytes())
                .unwrap(),
            Some(anonymous)
        );

        let candidate = assigner.mint_candidate().await.unwrap();
        assert_ne!(candidate, anonymous);
        let (next_fresh, _) = assigner.assign_fresh(key, T0).await.unwrap();
        assert_ne!(next_fresh, candidate);
    }

    /// An async draw on an empty cluster batch waits for the refill to install
    /// the next batch, then draws from it.
    #[tokio::test]
    async fn async_draw_awaits_the_installed_batch() {
        let dir = tempfile::tempdir().unwrap();
        let credentials = Arc::new(CredentialStore::open(&dir.path().join("system.redb")).unwrap());
        let registry = Arc::new(RwLock::new(SurrogateRegistry::from_persisted_cluster(0, 0)));
        let wal: Arc<dyn SurrogateWalAppender> = Arc::new(NoopWalAppender);
        let assigner = Arc::new(SurrogateAssigner::new(registry, credentials, wal));

        let drawer = Arc::clone(&assigner);
        let draw = tokio::spawn(async move {
            let key = CollectionKey::from_bare(DatabaseId::DEFAULT, "rows");
            drawer.assign_anonymous(key, T0).await
        });
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(!draw.is_finished(), "no batch is installed yet");

        let _waiter = assigner.await_reservation_for_test(9);
        assigner.complete_reservation(9, 100, 116);
        let drawn = draw.await.unwrap().unwrap();
        assert!((100..116).contains(&drawn.as_u32()));
    }
}
