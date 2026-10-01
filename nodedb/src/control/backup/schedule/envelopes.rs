// SPDX-License-Identifier: BUSL-1.1

//! The envelopes a scheduled backup writes under its target, and `keep`
//! retention over them.
//!
//! An envelope is named `<database>-<unix_ms>.ndbb`, the time zero-padded to
//! 20 digits. Retention reads only names of that shape, for the schedule's
//! own database, directly under the target. It never deletes anything else.
//! On a local root it lists and deletes through directory fds and follows no
//! symlink, so a symlink under the target is never listed and never deleted
//! through.

use object_store::path::Path as ObjectPath;

use crate::control::backup::store::BackupStore;

/// File extension of a database backup envelope.
const ENVELOPE_EXTENSION: &str = ".ndbb";

/// Digits of the zero-padded time in an envelope name.
const TIME_DIGITS: usize = 20;

/// The name of the envelope of `database` taken at `at_unix_ms`.
pub fn envelope_name(database: &str, at_unix_ms: u64) -> String {
    format!("{database}-{at_unix_ms:020}{ENVELOPE_EXTENSION}")
}

/// The time an envelope name of `database` carries, or `None` when `name` is
/// no envelope of `database`.
fn envelope_time(database: &str, name: &str) -> Option<u64> {
    let time = name
        .strip_prefix(database)?
        .strip_prefix('-')?
        .strip_suffix(ENVELOPE_EXTENSION)?;
    if time.len() != TIME_DIGITS || !time.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    time.parse().ok()
}

/// One envelope under a target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    pub path: ObjectPath,
    pub at_unix_ms: u64,
}

/// Every envelope of `database` directly under `prefix`, oldest first.
pub async fn list_envelopes(
    store: &BackupStore,
    prefix: &ObjectPath,
    database: &str,
) -> crate::Result<Vec<Envelope>> {
    let listed = store.list_files(prefix).await?;
    let mut envelopes: Vec<Envelope> = listed
        .into_iter()
        .filter_map(|path| {
            let at_unix_ms = envelope_time(database, path.filename()?)?;
            Some(Envelope { path, at_unix_ms })
        })
        .collect();
    envelopes.sort_by(|a, b| (a.at_unix_ms, &a.path).cmp(&(b.at_unix_ms, &b.path)));
    Ok(envelopes)
}

/// The envelopes beyond the newest `keep`, oldest first.
fn expired(mut envelopes: Vec<Envelope>, keep: u64) -> Vec<Envelope> {
    let keep = usize::try_from(keep).unwrap_or(usize::MAX);
    let excess = envelopes.len().saturating_sub(keep);
    envelopes.truncate(excess);
    envelopes
}

/// Delete the envelopes of `database` under `prefix` beyond the newest
/// `keep`, oldest first. Returns the number deleted.
pub async fn apply_keep(
    store: &BackupStore,
    prefix: &ObjectPath,
    database: &str,
    keep: u64,
) -> crate::Result<u64> {
    let mut deleted = 0;
    for envelope in expired(list_envelopes(store, prefix, database).await?, keep) {
        store.delete(&envelope.path).await?;
        deleted += 1;
    }
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

    use super::*;
    use crate::config::server::BackupStorageSettings;
    use crate::control::backup::store::BackupObject;

    #[test]
    fn only_names_of_the_schedule_database_parse() {
        let name = envelope_name("sales", 1_700_000_000_123);
        assert_eq!(name, "sales-00000001700000000123.ndbb");
        assert_eq!(envelope_time("sales", &name), Some(1_700_000_000_123));
        assert_eq!(envelope_time("sal", &name), None);
        let other = envelope_name("sales-eu", 5);
        assert_eq!(envelope_time("sales", &other), None);
        assert_eq!(envelope_time("sales", "sales-12.ndbb"), None);
        assert_eq!(
            envelope_time("sales", "sales-00000001700000000123.bak"),
            None
        );
    }

    #[tokio::test]
    async fn keep_deletes_the_oldest_envelopes_of_its_database_only() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = ObjectPath::from("nightly");
        let put = |name: String| {
            let store = Arc::clone(&store);
            async move {
                let path = ObjectPath::from(format!("nightly/{name}"));
                store
                    .put(&path, PutPayload::from_static(b"env"))
                    .await
                    .unwrap();
            }
        };
        for at in [30, 10, 20] {
            put(envelope_name("sales", at)).await;
        }
        put(envelope_name("hr", 1)).await;
        put("notes.txt".into()).await;
        put(format!("deeper/{}", envelope_name("sales", 5))).await;

        let backup_store = BackupStore::Remote(Arc::clone(&store));
        assert_eq!(
            apply_keep(&backup_store, &prefix, "sales", 2)
                .await
                .unwrap(),
            1
        );
        let left: Vec<u64> = list_envelopes(&backup_store, &prefix, "sales")
            .await
            .unwrap()
            .into_iter()
            .map(|envelope| envelope.at_unix_ms)
            .collect();
        assert_eq!(left, [20, 30]);
        assert_eq!(
            list_envelopes(&backup_store, &prefix, "hr")
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(
            store
                .head(&ObjectPath::from("nightly/notes.txt"))
                .await
                .is_ok()
        );
        let deeper = format!("nightly/deeper/{}", envelope_name("sales", 5));
        assert!(store.head(&ObjectPath::from(deeper)).await.is_ok());
    }

    /// A symlink under a local target that points out of the root is never
    /// listed, and retention never deletes through it.
    #[cfg(unix)]
    #[tokio::test]
    async fn keep_never_lists_or_deletes_through_a_symlink_out_of_the_root() {
        let outside = tempfile::tempdir().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        let nightly = root.join("nightly");
        std::fs::create_dir(&nightly).unwrap();
        for at in [20, 30] {
            std::fs::write(nightly.join(envelope_name("sales", at)), b"env").unwrap();
        }
        let secret = outside.path().join(envelope_name("sales", 10));
        std::fs::write(&secret, b"outside").unwrap();
        std::os::unix::fs::symlink(&secret, nightly.join(envelope_name("sales", 10))).unwrap();
        std::os::unix::fs::symlink(outside.path(), nightly.join("escape")).unwrap();

        let storage = BackupStorageSettings {
            local_root: Some(root.clone()),
            ..Default::default()
        };
        let target = BackupObject::resolve(
            &format!("file://{}/nightly/", root.display()),
            Some(&storage),
        )
        .unwrap();
        let listed: Vec<u64> = list_envelopes(target.store(), target.path(), "sales")
            .await
            .unwrap()
            .into_iter()
            .map(|envelope| envelope.at_unix_ms)
            .collect();
        assert_eq!(listed, [20, 30], "the symlink is never listed");

        assert_eq!(
            apply_keep(target.store(), target.path(), "sales", 1)
                .await
                .unwrap(),
            1
        );
        assert!(!nightly.join(envelope_name("sales", 20)).exists());
        assert!(nightly.join(envelope_name("sales", 30)).exists());
        assert_eq!(std::fs::read(&secret).unwrap(), b"outside");
        assert!(
            std::fs::symlink_metadata(nightly.join(envelope_name("sales", 10))).is_ok(),
            "retention leaves the symlink alone"
        );

        let through = target
            .path()
            .clone()
            .join(envelope_name("sales", 10).as_str());
        assert!(
            target.store().delete(&through).await.is_err(),
            "a deletion refuses a symlink"
        );
        assert_eq!(std::fs::read(&secret).unwrap(), b"outside");
    }
}
