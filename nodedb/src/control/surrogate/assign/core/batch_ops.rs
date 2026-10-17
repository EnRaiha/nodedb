// SPDX-License-Identifier: BUSL-1.1

//! Batched binding on [`super::SurrogateAssigner`]: `assign_bound_many` and
//! `bind_many`.
//!
//! A batch takes the registry write lock once and writes every new binding,
//! plus a due watermark row, in one catalog transaction. The transaction
//! commits durably once, so N keys cost one durable sync. The WAL bind records
//! follow the commit under the same lock.

use nodedb_types::{CollectionKey, Surrogate, TenantId};
use rustc_hash::{FxHashMap, FxHashSet};

use super::types::SurrogateAssigner;
use crate::control::security::catalog::surrogate_pk_batch::BatchBinding;

impl SurrogateAssigner {
    /// Resolve every key of `pks` to a stable surrogate and bind it in this
    /// node's catalog, in `pks` order.
    ///
    /// - A key already bound keeps its binding (no allocation).
    /// - A key repeated in `pks` gets one surrogate.
    /// - Every other key draws a fresh surrogate. The pass draws one
    ///   contiguous range for all of them, in first-seen order. All new
    ///   bindings commit in one catalog transaction under one registry write
    ///   lock.
    ///
    /// Only a node that is the keys' home binds this way. A cluster node whose
    /// reserved batch runs empty commits what it drew, awaits the refill loop's
    /// next batch with the lock released, and continues with the rest.
    pub async fn assign_bound_many(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        pks: &[&[u8]],
    ) -> crate::Result<Vec<Surrogate>> {
        let catalog = self.credential_store.catalog();
        let mut out = catalog.get_surrogates_for_pks(key, tenant_id, pks)?;
        while out.iter().any(Option::is_none) {
            if self.try_assign_bound_batch(key, tenant_id, pks, &mut out)? {
                break;
            }
            self.await_reserved_batch().await?;
        }
        out.into_iter()
            .map(|slot| {
                slot.ok_or_else(|| crate::Error::Internal {
                    detail: format!("a key in '{}' stayed unbound after assign", key.name()),
                })
            })
            .collect()
    }

    /// One [`Self::assign_bound_many`] pass under the registry write lock.
    /// Fills every `None` slot it can. Returns `false` when this node's
    /// reserved batch ran empty before every slot was drawn.
    fn try_assign_bound_batch(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        pks: &[&[u8]],
        out: &mut [Option<Surrogate>],
    ) -> crate::Result<bool> {
        let registry = self.registry_write()?;
        let missing: Vec<usize> = (0..out.len()).filter(|&i| out[i].is_none()).collect();
        // Each distinct unbound key once, in first-seen order.
        let mut seen: FxHashSet<&[u8]> =
            FxHashSet::with_capacity_and_hasher(missing.len(), Default::default());
        let unbound: Vec<&[u8]> = missing
            .iter()
            .map(|&i| pks[i])
            .filter(|pk| seen.insert(*pk))
            .collect();
        // One contiguous draw for every unbound key. A key another assigner
        // bound since the caller's read keeps that binding: the commit below
        // is first-wins, so its drawn value goes unused.
        let drawn = self.alloc_range_locked(&registry, unbound.len())?;
        let rows: Vec<(&[u8], Surrogate)> = match drawn {
            Some(range) => unbound
                .iter()
                .copied()
                .zip(range.map(Surrogate::new))
                .collect(),
            None => Vec::new(),
        };
        let complete = rows.len() == unbound.len();
        if !complete {
            self.refill_notify.notify_one();
        }
        self.nudge_refill_if_low(&registry);
        if rows.is_empty() {
            return Ok(complete);
        }
        let hwm = self.due_flush_hwm(&registry);
        let bound = self.commit_bindings(key, tenant_id, &rows, hwm)?;
        if hwm.is_some() {
            self.finish_committed_flush(&registry)?;
        }
        let winners: FxHashMap<&[u8], Surrogate> = rows
            .iter()
            .zip(&bound)
            .map(|(&(pk, _), binding)| (pk, binding.surrogate))
            .collect();
        for &index in &missing {
            if out[index].is_none() {
                out[index] = winners.get(pks[index]).copied();
            }
        }
        Ok(complete)
    }

    /// Bind each `(pk, carried)` row of one collection first-wins, in one
    /// catalog transaction, and return the authoritative surrogate per row in
    /// `rows` order.
    ///
    /// - A key already bound keeps its binding; the carried value is dropped.
    /// - A key repeated in `rows` binds its first carried value.
    /// - Every new binding writes its WAL bind record before the lock
    ///   releases, and the registry watermark rises past it.
    ///
    /// Never allocates and never advances the allocator's draw position.
    pub fn bind_many(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        rows: &[(&[u8], Surrogate)],
    ) -> crate::Result<Vec<Surrogate>> {
        let catalog = self.credential_store.catalog();
        let pks: Vec<&[u8]> = rows.iter().map(|&(pk, _)| pk).collect();
        // First-wins pre-check without the write lock: a batch whose keys are
        // all bound (replay, retry, the planning node itself) writes nothing.
        let existing = catalog.get_surrogates_for_pks(key, tenant_id, &pks)?;
        if existing.iter().all(Option::is_some) {
            return Ok(existing.into_iter().flatten().collect());
        }
        let registry = self.registry_write()?;
        let bound = self.commit_bindings(key, tenant_id, rows, None)?;
        if let Some(highest) = bound
            .iter()
            .filter(|binding| binding.inserted)
            .map(|binding| binding.surrogate.as_u32())
            .max()
        {
            // Keeps a later local draw from re-issuing a carried value.
            registry
                .restore_hwm(highest)
                .map_err(|e| crate::Error::Internal {
                    detail: format!("surrogate bind restore_hwm failed: {e}"),
                })?;
        }
        Ok(bound.into_iter().map(|binding| binding.surrogate).collect())
    }

    /// Write `rows` first-wins and the due watermark `hwm` in one catalog
    /// transaction, then append the rows it inserted as WAL bind batch records.
    /// The caller holds the registry write lock.
    pub(super) fn commit_bindings(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        rows: &[(&[u8], Surrogate)],
        hwm: Option<u32>,
    ) -> crate::Result<Vec<BatchBinding>> {
        let bound = self
            .credential_store
            .catalog()
            .put_surrogates_with_hwm(key, tenant_id, rows, hwm)?;
        let inserted: Vec<(u32, &[u8])> = rows
            .iter()
            .zip(&bound)
            .filter(|(_, binding)| binding.inserted)
            .map(|(&(pk, _), binding)| (binding.surrogate.as_u32(), pk))
            .collect();
        self.wal_appender
            .record_binds_to_wal(key, tenant_id, &inserted)?;
        Ok(bound)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use nodedb_types::{CollectionKey, DatabaseId, Surrogate, TenantId};

    use super::super::types::SurrogateAssigner;
    use crate::control::security::credential::CredentialStore;
    use crate::control::surrogate::registry::SurrogateRegistry;
    use crate::control::surrogate::wal_appender::{NoopWalAppender, SurrogateWalAppender};

    const T0: TenantId = TenantId::new(0);

    fn open() -> (tempfile::TempDir, SurrogateAssigner) {
        let dir = tempfile::tempdir().expect("tempdir");
        let credentials =
            Arc::new(CredentialStore::open(&dir.path().join("system.redb")).expect("open"));
        let registry = Arc::new(RwLock::new(SurrogateRegistry::new()));
        let wal: Arc<dyn SurrogateWalAppender> = Arc::new(NoopWalAppender);
        (dir, SurrogateAssigner::new(registry, credentials, wal))
    }

    fn users() -> CollectionKey<'static> {
        CollectionKey::from_bare(DatabaseId::DEFAULT, "users")
    }

    #[tokio::test]
    async fn a_batch_assigns_what_per_key_assignment_assigns() {
        let (_batch_dir, batch) = open();
        let (_single_dir, single) = open();
        let pks: Vec<Vec<u8>> = (0..300).map(|i| format!("k{i}").into_bytes()).collect();
        let refs: Vec<&[u8]> = pks.iter().map(Vec::as_slice).collect();

        let batched = batch
            .assign_bound_many(users(), T0, &refs)
            .await
            .expect("batch");
        let mut one_by_one = Vec::new();
        for pk in &refs {
            one_by_one.push(single.assign(users(), T0, pk).await.expect("single"));
        }
        assert_eq!(batched, one_by_one);
        let catalog = batch.credential_store.catalog();
        for (pk, surrogate) in refs.iter().zip(&batched) {
            assert_eq!(
                catalog
                    .get_pk_for_surrogate(users(), T0, *surrogate)
                    .expect("rev"),
                Some(pk.to_vec())
            );
        }
    }

    #[tokio::test]
    async fn a_batch_gives_a_repeated_key_one_surrogate() {
        let (_dir, assigner) = open();
        let pks: [&[u8]; 3] = [b"a", b"b", b"a"];
        let out = assigner
            .assign_bound_many(users(), T0, &pks)
            .await
            .expect("assign");
        assert_eq!(out[0], out[2]);
        assert_ne!(out[0], out[1]);
        assert_eq!(assigner.current_hwm(), 2, "a repeated key draws nothing");
    }

    #[tokio::test]
    async fn a_batch_keeps_existing_bindings() {
        let (_dir, assigner) = open();
        let first = assigner.assign(users(), T0, b"a").await.expect("assign");
        let pks: [&[u8]; 2] = [b"a", b"b"];
        let out = assigner
            .assign_bound_many(users(), T0, &pks)
            .await
            .expect("assign");
        assert_eq!(out[0], first);
        assert_ne!(out[1], first);
    }

    #[tokio::test]
    async fn unbound_keys_draw_one_contiguous_range() {
        let (_dir, assigner) = open();
        let pks: Vec<Vec<u8>> = (0..64).map(|i| format!("k{i}").into_bytes()).collect();
        let refs: Vec<&[u8]> = pks.iter().map(Vec::as_slice).collect();
        let out = assigner
            .assign_bound_many(users(), T0, &refs)
            .await
            .expect("assign");
        let values: Vec<u32> = out.iter().map(|s| s.as_u32()).collect();
        let first = values[0];
        let expected: Vec<u32> = (first..first + 64).collect();
        assert_eq!(values, expected, "keys take the range in pks order");
        assert_eq!(assigner.current_hwm(), first + 63);
    }

    #[tokio::test]
    async fn a_second_batch_of_the_same_keys_reuses_every_binding() {
        let (_dir, assigner) = open();
        let pks: [&[u8]; 3] = [b"a", b"b", b"c"];
        let first = assigner
            .assign_bound_many(users(), T0, &pks)
            .await
            .expect("assign");
        let hwm = assigner.current_hwm();
        let again = assigner
            .assign_bound_many(users(), T0, &pks)
            .await
            .expect("assign");
        assert_eq!(first, again);
        assert_eq!(assigner.current_hwm(), hwm, "bound keys draw nothing");
    }

    /// A load chunk that rewrites half of an earlier chunk's coordinates keeps
    /// their surrogates, draws a range for the rest, and leaves every reverse
    /// row pointing at its key.
    #[tokio::test]
    async fn a_later_overlapping_chunk_keeps_every_earlier_surrogate() {
        let (_dir, assigner) = open();
        let pks: Vec<Vec<u8>> = (0..450u32).map(|i| i.to_be_bytes().to_vec()).collect();
        let refs: Vec<&[u8]> = pks.iter().map(Vec::as_slice).collect();
        let first = assigner
            .assign_bound_many(users(), T0, &refs[..300])
            .await
            .expect("first chunk");
        let hwm = assigner.current_hwm();
        let second = assigner
            .assign_bound_many(users(), T0, &refs[150..])
            .await
            .expect("second chunk");
        assert_eq!(
            &second[..150],
            &first[150..],
            "overwritten cells keep identity"
        );
        let drawn: Vec<u32> = second[150..].iter().map(|s| s.as_u32()).collect();
        assert_eq!(drawn, (hwm + 1..=hwm + 150).collect::<Vec<_>>());
        let catalog = assigner.credential_store.catalog();
        for (pk, surrogate) in refs[150..].iter().zip(&second) {
            assert_eq!(
                catalog
                    .get_pk_for_surrogate(users(), T0, *surrogate)
                    .expect("rev"),
                Some(pk.to_vec())
            );
        }
    }

    #[tokio::test]
    async fn a_mixed_batch_reuses_bound_keys_and_draws_a_range_for_the_rest() {
        let (_dir, assigner) = open();
        let a = assigner.assign(users(), T0, b"a").await.expect("assign");
        let c = assigner.assign(users(), T0, b"c").await.expect("assign");
        let hwm = assigner.current_hwm();
        let pks: [&[u8]; 6] = [b"a", b"x", b"c", b"y", b"x", b"z"];
        let out = assigner
            .assign_bound_many(users(), T0, &pks)
            .await
            .expect("assign");
        assert_eq!(out[0], a);
        assert_eq!(out[2], c);
        assert_eq!(out[1], out[4], "a repeated key gets one surrogate");
        let drawn = [out[1].as_u32(), out[3].as_u32(), out[5].as_u32()];
        assert_eq!(drawn, [hwm + 1, hwm + 2, hwm + 3]);
        assert_eq!(assigner.current_hwm(), hwm + 3);
    }

    #[tokio::test]
    async fn a_batch_past_the_flush_threshold_persists_the_watermark() {
        let (_dir, assigner) = open();
        let n = crate::control::surrogate::registry::FLUSH_OPS_THRESHOLD as usize;
        let pks: Vec<Vec<u8>> = (0..n).map(|i| format!("k{i}").into_bytes()).collect();
        let refs: Vec<&[u8]> = pks.iter().map(Vec::as_slice).collect();
        assigner
            .assign_bound_many(users(), T0, &refs)
            .await
            .expect("assign");
        let persisted = assigner
            .credential_store
            .catalog()
            .get_surrogate_hwm()
            .expect("hwm");
        assert_eq!(persisted, n as u32);
    }

    #[test]
    fn bind_many_is_first_wins() {
        let (_dir, assigner) = open();
        assigner
            .bind(users(), T0, b"a", Surrogate::new(10))
            .expect("bind");
        let rows: [(&[u8], Surrogate); 3] = [
            (b"a", Surrogate::new(20)),
            (b"b", Surrogate::new(30)),
            (b"b", Surrogate::new(40)),
        ];
        let out = assigner.bind_many(users(), T0, &rows).expect("bind");
        assert_eq!(
            out,
            vec![Surrogate::new(10), Surrogate::new(30), Surrogate::new(30)]
        );
        let catalog = assigner.credential_store.catalog();
        assert_eq!(
            catalog
                .get_pk_for_surrogate(users(), T0, Surrogate::new(30))
                .expect("rev"),
            Some(b"b".to_vec())
        );
        for lost in [20, 40] {
            assert_eq!(
                catalog
                    .get_pk_for_surrogate(users(), T0, Surrogate::new(lost))
                    .expect("rev"),
                None
            );
        }
        assert!(
            assigner.current_hwm() >= 30,
            "the watermark rises past a bind"
        );
    }
}
