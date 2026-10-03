// SPDX-License-Identifier: BUSL-1.1

//! Cluster restore generations.
//!
//! A cluster restore starts every Raft group of every node at one new term,
//! and the cluster epoch at the same value. The value is a generation, taken
//! once per restore from cold storage, shifted above every term and epoch
//! the cluster reached since the generation before. A node the
//! restore missed then holds lower terms and a lower epoch: Raft refuses its
//! log, and the epoch fence stands it down.
//!
//! Under `{prefix}raft/restore/`:
//! - `gen-{G:010}.claim` names the restore point generation `G` restores.
//!   Every node restoring that point before the cluster starts takes `G`.
//! - `gen-{G:010}.sealed` marks `G` as started: a node of the restored
//!   cluster writes it at boot, before its Raft groups start. A restore
//!   after that takes the next generation.
//!
//! A claim is a conditional create. A store without one cannot claim.

use std::path::Path;
use std::sync::Arc;

use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutPayload};

/// Terms and epochs of one generation stay below `1 << GENERATION_SHIFT`
/// above its fence: one term per election, one epoch per metadata leader.
pub const GENERATION_SHIFT: u32 = 40;

/// The file in a restored data directory that names its generation until
/// the node's first boot seals it.
pub const GENERATION_MARKER_FILE: &str = "restore_generation";

/// Attempts to claim a generation while other nodes claim concurrently.
const CLAIM_ATTEMPTS: usize = 16;

/// What a claim object holds.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
struct Claim {
    restore_point: u64,
}

fn generation_err(detail: String) -> crate::Error {
    crate::Error::ColdStorage { detail }
}

/// The term and cluster epoch generation `generation` starts at.
pub fn fence_of(generation: u64) -> crate::Result<u64> {
    generation
        .checked_mul(1u64 << GENERATION_SHIFT)
        .ok_or_else(|| generation_err(format!("restore generation {generation} overflows a term")))
}

fn restore_dir(prefix: &str) -> String {
    format!("{prefix}raft/restore/")
}

fn claim_key(prefix: &str, generation: u64) -> ObjectPath {
    ObjectPath::from(format!("{}gen-{generation:010}.claim", restore_dir(prefix)))
}

fn seal_key(prefix: &str, generation: u64) -> ObjectPath {
    ObjectPath::from(format!(
        "{}gen-{generation:010}.sealed",
        restore_dir(prefix)
    ))
}

/// `(generation, is_seal)` of a generation object name.
fn parse_name(name: &str) -> Option<(u64, bool)> {
    let rest = name.strip_prefix("gen-")?;
    if let Some(generation) = rest.strip_suffix(".claim") {
        return Some((generation.parse().ok()?, false));
    }
    let generation = rest.strip_suffix(".sealed")?;
    Some((generation.parse().ok()?, true))
}

/// The highest claimed generation, and whether it is sealed.
async fn highest(store: &Arc<dyn ObjectStore>, prefix: &str) -> crate::Result<Option<(u64, bool)>> {
    let dir = restore_dir(prefix);
    let objects: Vec<_> = store
        .list(Some(&ObjectPath::from(dir.clone())))
        .try_collect()
        .await
        .map_err(|e| generation_err(format!("list restore generations under {dir}: {e}")))?;
    let mut claimed = None;
    let mut sealed = std::collections::BTreeSet::new();
    for meta in &objects {
        match meta.location.filename().and_then(parse_name) {
            Some((generation, false)) => claimed = claimed.max(Some(generation)),
            Some((generation, true)) => {
                sealed.insert(generation);
            }
            None => {}
        }
    }
    Ok(claimed.map(|generation| (generation, sealed.contains(&generation))))
}

async fn read_claim(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    generation: u64,
) -> crate::Result<Claim> {
    let key = claim_key(prefix, generation);
    let fetch_err = |e: object_store::Error| generation_err(format!("fetch {key}: {e}"));
    let raw = store
        .get(&key)
        .await
        .map_err(fetch_err)?
        .bytes()
        .await
        .map_err(fetch_err)?;
    zerompk::from_msgpack(&raw).map_err(|e| generation_err(format!("decode {key}: {e}")))
}

/// Why a generation claim failed.
#[derive(Debug, thiserror::Error)]
pub enum GenerationError {
    #[error(
        "cold store {store} has no conditional create (put if absent). A cluster restore \
         claims its generation with one, so every node takes the same generation. Configure \
         a store that supports it, such as S3 with conditional writes or a local directory"
    )]
    ConditionalCreateUnsupported { store: String },
    #[error(transparent)]
    Store(#[from] crate::Error),
}

/// Create `key` holding `body` unless it exists. `false` when it exists.
async fn create(
    store: &Arc<dyn ObjectStore>,
    key: &ObjectPath,
    body: Vec<u8>,
) -> Result<bool, GenerationError> {
    match store
        .put_opts(key, PutPayload::from(body), PutMode::Create.into())
        .await
    {
        Ok(_) => Ok(true),
        Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
        Err(
            object_store::Error::NotImplemented { .. } | object_store::Error::NotSupported { .. },
        ) => Err(GenerationError::ConditionalCreateUnsupported {
            store: store.to_string(),
        }),
        Err(e) => Err(generation_err(format!("put {key}: {e}")).into()),
    }
}

/// The generation a restore to `restore_point` takes: the open generation
/// claimed for the same point, or a new one above every earlier claim.
pub async fn claim_generation(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    restore_point: u64,
) -> Result<u64, GenerationError> {
    let body = zerompk::to_msgpack_vec(&Claim { restore_point })
        .map_err(|e| generation_err(format!("encode restore generation claim: {e}")))?;
    for _ in 0..CLAIM_ATTEMPTS {
        let latest = highest(store, prefix).await?;
        if let Some((generation, false)) = latest
            && read_claim(store, prefix, generation).await?.restore_point == restore_point
        {
            return Ok(generation);
        }
        let next = latest.map_or(1, |(generation, _)| generation + 1);
        if create(store, &claim_key(prefix, next), body.clone()).await? {
            return Ok(next);
        }
    }
    Err(generation_err(format!(
        "no restore generation claimed for restore point {restore_point} after \
         {CLAIM_ATTEMPTS} attempts; other restores claim concurrently"
    ))
    .into())
}

/// Mark `generation` as started.
pub async fn seal_generation(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    generation: u64,
) -> crate::Result<()> {
    let key = seal_key(prefix, generation);
    store
        .put(&key, PutPayload::from_static(b""))
        .await
        .map_err(|e| generation_err(format!("put {key}: {e}")))?;
    Ok(())
}

/// Record `generation` in the restored data directory `data_dir`, durably.
pub fn write_generation_marker(data_dir: &Path, generation: u64) -> crate::Result<()> {
    // no-objectstore: the marker lives in the local data directory.
    let path = data_dir.join(GENERATION_MARKER_FILE);
    std::fs::write(&path, generation.to_string())?;
    std::fs::File::open(&path)?.sync_all()?;
    std::fs::File::open(data_dir)?.sync_all()?;
    Ok(())
}

/// The generation `data_dir` was restored at, until its first boot seals it.
pub fn read_generation_marker(data_dir: &Path) -> crate::Result<Option<u64>> {
    // no-objectstore: the marker lives in the local data directory.
    let path = data_dir.join(GENERATION_MARKER_FILE);
    let raw = match std::fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    raw.trim()
        .parse()
        .map(Some)
        .map_err(|_| crate::Error::Storage {
            engine: "restore".into(),
            detail: format!("{} does not hold a generation: {raw:?}", path.display()),
        })
}

/// Drop the marker once the generation is sealed.
pub fn remove_generation_marker(data_dir: &Path) -> crate::Result<()> {
    // no-objectstore: the marker lives in the local data directory.
    std::fs::remove_file(data_dir.join(GENERATION_MARKER_FILE))?;
    std::fs::File::open(data_dir)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;

    #[tokio::test]
    async fn one_restore_shares_a_generation_until_it_starts() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert_eq!(claim_generation(&store, "c/", 7).await.unwrap(), 1);
        assert_eq!(
            claim_generation(&store, "c/", 7).await.unwrap(),
            1,
            "a second node restoring the same point takes the same generation"
        );
        assert_eq!(
            claim_generation(&store, "c/", 9).await.unwrap(),
            2,
            "another point takes a new generation"
        );
        seal_generation(&store, "c/", 2).await.unwrap();
        assert_eq!(
            claim_generation(&store, "c/", 9).await.unwrap(),
            3,
            "a started generation is never taken again"
        );
        assert!(fence_of(3).unwrap() > fence_of(2).unwrap() + (1 << 39));
    }

    /// A store that refuses conditional creates.
    #[derive(Debug)]
    struct NoCreate(InMemory);

    impl std::fmt::Display for NoCreate {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "NoCreate")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for NoCreate {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            if matches!(opts.mode, PutMode::Create) {
                return Err(object_store::Error::NotImplemented {
                    operation: "put_opts with PutMode::Create".into(),
                    implementer: "NoCreate".into(),
                });
            }
            self.0.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.0.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.0.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
            self.0.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.0.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.0.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.0.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn a_store_without_conditional_create_is_refused() {
        let store: Arc<dyn ObjectStore> = Arc::new(NoCreate(InMemory::new()));
        assert!(matches!(
            claim_generation(&store, "c/", 7).await,
            Err(GenerationError::ConditionalCreateUnsupported { .. })
        ));
    }

    #[test]
    fn the_marker_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(read_generation_marker(dir.path()).unwrap(), None);
        write_generation_marker(dir.path(), 4).unwrap();
        assert_eq!(read_generation_marker(dir.path()).unwrap(), Some(4));
        remove_generation_marker(dir.path()).unwrap();
        assert_eq!(read_generation_marker(dir.path()).unwrap(), None);
    }

    #[test]
    fn names_parse() {
        assert_eq!(parse_name("gen-0000000003.claim"), Some((3, false)));
        assert_eq!(parse_name("gen-0000000003.sealed"), Some((3, true)));
        assert_eq!(parse_name("log-1-2.bin"), None);
    }
}
