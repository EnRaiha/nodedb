// SPDX-License-Identifier: BUSL-1.1

//! The object a `BACKUP DATABASE` writes and a `RESTORE DATABASE` reads.
//!
//! A URI names only the object. Credentials and the local root come from the
//! `[backup_storage]` config section, never from the SQL text:
//!
//! * `file:///<path>` must lie inside `local_root`, with every symlink on it
//!   followed. Without `local_root` every `file://` URI is refused, so SQL
//!   cannot name an arbitrary server path. Reads, writes, listings and
//!   deletions open the path below a directory fd of the canonical root and
//!   follow no symlink, so a component swapped for a symlink after the URI
//!   resolved refuses them (see [`super::store_local`]).
//! * `s3://<bucket>/<key>` uses the section's endpoint, region and keys.

use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use crate::Error;
use crate::config::server::BackupStorageSettings;

use super::store_local::{self, LocalIoError};

/// A backup URI the server refuses before it touches any store.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BackupUriError {
    #[error("backup URI '{uri}': unsupported scheme; use file:///<path> or s3://<bucket>/<key>")]
    UnsupportedScheme { uri: String },
    #[error("backup URI '{uri}': {detail}")]
    Malformed { uri: String, detail: String },
    #[error(
        "backup URI '{uri}': file:// URIs need [backup_storage] local_root in the server config"
    )]
    NoLocalRoot { uri: String },
    #[error("backup URI '{uri}': the path must lie inside [backup_storage] local_root '{root}'")]
    OutsideLocalRoot { uri: String, root: String },
    #[error("backup URI '{uri}': cannot open the store: {detail}")]
    Store { uri: String, detail: String },
}

impl From<BackupUriError> for Error {
    fn from(e: BackupUriError) -> Self {
        Error::BadRequest {
            detail: e.to_string(),
        }
    }
}

/// Why a read or write of a backup object did not complete.
#[derive(Debug, thiserror::Error)]
pub enum BackupIoError {
    /// The path left the local root at the open, through a symlink placed on
    /// it after the URI resolved.
    #[error(transparent)]
    Refused(BackupUriError),
    #[error(transparent)]
    Failed(Error),
}

impl From<BackupIoError> for Error {
    fn from(e: BackupIoError) -> Self {
        match e {
            BackupIoError::Refused(refusal) => refusal.into(),
            BackupIoError::Failed(error) => error,
        }
    }
}

/// One backup object in one store.
pub struct BackupObject {
    uri: String,
    store: BackupStore,
    path: ObjectPath,
}

impl std::fmt::Debug for BackupObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackupObject")
            .field("uri", &self.uri)
            .field("path", &self.path)
            .finish()
    }
}

/// The store a backup object lives in.
#[derive(Debug, Clone)]
pub enum BackupStore {
    /// Below the canonical `[backup_storage] local_root`. Every read, write,
    /// listing and deletion opens its path below a directory fd of the root
    /// and follows no symlink (see [`super::store_local`]).
    Local(PathBuf),
    /// An object store reached over the network.
    Remote(Arc<dyn ObjectStore>),
}

impl BackupStore {
    /// Every object directly under `dir`. A local root lists only regular
    /// files, never a symlink.
    pub async fn list_files(&self, dir: &ObjectPath) -> Result<Vec<ObjectPath>, BackupIoError> {
        let uri = self.uri_of(dir);
        match self {
            Self::Local(root) => {
                let components = components(dir);
                let names = local_io(root, &uri, "list", move |fd| {
                    store_local::list_files(fd, &components)
                })
                .await?;
                Ok(names
                    .iter()
                    .map(|name| dir.clone().join(name.as_str()))
                    .collect())
            }
            Self::Remote(store) => {
                let listed = store
                    .list_with_delimiter(Some(dir))
                    .await
                    .map_err(|e| failed("list", &uri, e))?;
                Ok(listed
                    .objects
                    .into_iter()
                    .map(|meta| meta.location)
                    .collect())
            }
        }
    }

    /// Delete the object at `path`. A missing object counts as deleted. A
    /// local root refuses a symlink, so no deletion reaches its target.
    pub async fn delete(&self, path: &ObjectPath) -> Result<(), BackupIoError> {
        let uri = self.uri_of(path);
        match self {
            Self::Local(root) => {
                let components = components(path);
                local_io(root, &uri, "delete", move |fd| {
                    store_local::delete_beneath(fd, &components)
                })
                .await
            }
            Self::Remote(store) => match store.delete(path).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                Err(e) => Err(failed("delete", &uri, e)),
            },
        }
    }

    async fn put(&self, uri: &str, path: &ObjectPath, bytes: Vec<u8>) -> Result<(), BackupIoError> {
        match self {
            Self::Local(root) => {
                let components = components(path);
                local_io(root, uri, "write", move |fd| {
                    store_local::write_beneath(fd, &components, &bytes)
                })
                .await
            }
            Self::Remote(store) => store
                .put(path, PutPayload::from(bytes))
                .await
                .map(drop)
                .map_err(|e| failed("write", uri, e)),
        }
    }

    async fn get(&self, uri: &str, path: &ObjectPath) -> Result<Vec<u8>, BackupIoError> {
        match self {
            Self::Local(root) => {
                let components = components(path);
                local_io(root, uri, "read", move |fd| {
                    store_local::read_beneath(fd, &components)
                })
                .await
            }
            Self::Remote(store) => {
                let result = store.get(path).await.map_err(|e| failed("read", uri, e))?;
                let bytes = result.bytes().await.map_err(|e| failed("read", uri, e))?;
                Ok(bytes.to_vec())
            }
        }
    }

    /// The URI error messages name `path` by.
    fn uri_of(&self, path: &ObjectPath) -> String {
        match self {
            Self::Local(root) => format!("file://{}/{path}", root.display()),
            Self::Remote(store) => format!("{store}/{path}"),
        }
    }
}

impl BackupObject {
    /// Resolve `uri` against `settings`.
    pub fn resolve(
        uri: &str,
        settings: Option<&BackupStorageSettings>,
    ) -> Result<Self, BackupUriError> {
        let malformed = |detail: &str| BackupUriError::Malformed {
            uri: uri.to_string(),
            detail: detail.to_string(),
        };
        let store_error = |detail: String| BackupUriError::Store {
            uri: uri.to_string(),
            detail,
        };
        if let Some(rest) = uri.strip_prefix("file://") {
            let root = settings
                .and_then(|s| s.local_root.as_deref())
                .ok_or_else(|| BackupUriError::NoLocalRoot {
                    uri: uri.to_string(),
                })?;
            let (canonical_root, target) =
                local_target(root, Path::new(rest)).map_err(|refusal| match refusal {
                    LocalRefusal::Outside => BackupUriError::OutsideLocalRoot {
                        uri: uri.to_string(),
                        root: root.display().to_string(),
                    },
                    LocalRefusal::Unreadable(detail) => store_error(detail),
                })?;
            let key = target
                .strip_prefix(&canonical_root)
                .ok()
                .and_then(object_key)
                .ok_or_else(|| malformed("the path names no file"))?;
            return Ok(Self {
                uri: uri.to_string(),
                store: BackupStore::Local(canonical_root),
                path: key,
            });
        }
        if let Some(rest) = uri.strip_prefix("s3://") {
            let (bucket, key) = rest
                .split_once('/')
                .ok_or_else(|| malformed("expected s3://<bucket>/<key>"))?;
            if bucket.is_empty() || key.trim_matches('/').is_empty() {
                return Err(malformed("expected s3://<bucket>/<key>"));
            }
            let path = ObjectPath::parse(key.trim_matches('/'))
                .map_err(|e| malformed(&format!("invalid object key: {e}")))?;
            let defaults = BackupStorageSettings::default();
            let settings = settings.unwrap_or(&defaults);
            let mut builder = AmazonS3Builder::new()
                .with_bucket_name(bucket)
                .with_region(&settings.region);
            if !settings.endpoint.is_empty() {
                builder = builder
                    .with_endpoint(&settings.endpoint)
                    .with_allow_http(settings.endpoint.starts_with("http://"));
            }
            if !settings.access_key.is_empty() {
                builder = builder
                    .with_access_key_id(&settings.access_key)
                    .with_secret_access_key(&settings.secret_key);
            }
            let store = builder
                .build()
                .map_err(|e| store_error(format!("S3 client: {e}")))?;
            return Ok(Self {
                uri: uri.to_string(),
                store: BackupStore::Remote(Arc::new(store)),
                path,
            });
        }
        Err(BackupUriError::UnsupportedScheme {
            uri: uri.to_string(),
        })
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// The store the object lives in.
    pub fn store(&self) -> &BackupStore {
        &self.store
    }

    /// The object key inside [`Self::store`].
    pub fn path(&self) -> &ObjectPath {
        &self.path
    }

    /// Write `bytes` as the whole object, replacing any object there.
    pub async fn put(&self, bytes: Vec<u8>) -> Result<(), BackupIoError> {
        self.store.put(&self.uri, &self.path, bytes).await
    }

    /// Read the whole object.
    pub async fn get(&self) -> Result<Vec<u8>, BackupIoError> {
        self.store.get(&self.uri, &self.path).await
    }
}

/// The components of an object key below a local root.
fn components(path: &ObjectPath) -> Vec<String> {
    path.parts().map(|part| part.as_ref().to_string()).collect()
}

/// Run the blocking local `io` off the async runtime, on a directory fd of
/// `root`.
async fn local_io<T: Send + 'static>(
    root: &Path,
    uri: &str,
    action: &str,
    io: impl FnOnce(&std::os::fd::OwnedFd) -> Result<T, LocalIoError> + Send + 'static,
) -> Result<T, BackupIoError> {
    let opened = root.to_path_buf();
    let outcome = tokio::task::spawn_blocking(move || {
        let fd = store_local::open_root(&opened)?;
        io(&fd)
    })
    .await;
    let failed = |detail: String| {
        BackupIoError::Failed(Error::Storage {
            engine: "backup".into(),
            detail: format!("{action} backup object '{uri}': {detail}"),
        })
    };
    match outcome {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(LocalIoError::Escapes)) => {
            Err(BackupIoError::Refused(BackupUriError::OutsideLocalRoot {
                uri: uri.to_string(),
                root: root.display().to_string(),
            }))
        }
        Ok(Err(LocalIoError::Io(e))) => Err(failed(e.to_string())),
        Err(join) => Err(failed(join.to_string())),
    }
}

fn failed(action: &str, uri: &str, e: object_store::Error) -> BackupIoError {
    BackupIoError::Failed(Error::Storage {
        engine: "backup".into(),
        detail: format!("{action} backup object '{uri}': {e}"),
    })
}

/// Why a `file://` path does not resolve inside the local root.
#[derive(Debug)]
enum LocalRefusal {
    /// The path, or a symlink on it, leaves the root.
    Outside,
    /// The root or an existing component cannot be read.
    Unreadable(String),
}

/// The canonical local root, and `requested` resolved under it with every
/// symlink followed.
///
/// Refuses a `..` component. Canonicalizes the root, then walks `requested`
/// below it one component at a time: each component that exists is
/// canonicalized and must stay under the canonical root, so a symlink that
/// leaves the root refuses the path. Components past the first missing one
/// do not exist, so no symlink can sit on them.
fn local_target(root: &Path, requested: &Path) -> Result<(PathBuf, PathBuf), LocalRefusal> {
    if requested
        .components()
        .any(|c| matches!(c, Component::ParentDir | Component::Prefix(_)))
    {
        return Err(LocalRefusal::Outside);
    }
    let unreadable = |path: &Path, e: std::io::Error| {
        LocalRefusal::Unreadable(format!("'{}': {e}", path.display()))
    };
    let canonical_root = std::fs::canonicalize(root).map_err(|e| unreadable(root, e))?;
    let relative = requested
        .strip_prefix(root)
        .or_else(|_| requested.strip_prefix(&canonical_root))
        .map_err(|_| LocalRefusal::Outside)?;
    let mut target = canonical_root.clone();
    let mut exists = true;
    for component in relative.components() {
        let Component::Normal(part) = component else {
            continue;
        };
        target.push(part);
        if !exists {
            continue;
        }
        match std::fs::symlink_metadata(&target) {
            Ok(_) => {
                target = std::fs::canonicalize(&target).map_err(|e| unreadable(&target, e))?;
                if !target.starts_with(&canonical_root) {
                    return Err(LocalRefusal::Outside);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => exists = false,
            Err(e) => return Err(unreadable(&target, e)),
        }
    }
    Ok((canonical_root, target))
}

/// The object key of a root-relative path, `None` for an empty one.
fn object_key(relative: &Path) -> Option<ObjectPath> {
    let parts: Vec<&str> = relative
        .components()
        .filter_map(|c| match c {
            Component::Normal(part) => part.to_str(),
            _ => None,
        })
        .collect();
    if parts.is_empty() {
        return None;
    }
    ObjectPath::parse(parts.join("/")).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(root: &Path) -> BackupStorageSettings {
        BackupStorageSettings {
            local_root: Some(root.to_path_buf()),
            ..Default::default()
        }
    }

    #[test]
    fn a_file_uri_inside_the_root_resolves() {
        let dir = tempfile::tempdir().expect("tempdir");
        let uri = format!("file://{}/nightly/db.ndbb", dir.path().display());
        let object = BackupObject::resolve(&uri, Some(&settings(dir.path()))).expect("resolve");
        assert_eq!(object.path.as_ref(), "nightly/db.ndbb");
    }

    #[test]
    fn a_file_uri_outside_the_root_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let s = settings(dir.path());
        for uri in [
            "file:///etc/passwd".to_string(),
            format!("file://{}/../escape", dir.path().display()),
            format!("file://{}", dir.path().display()),
        ] {
            assert!(BackupObject::resolve(&uri, Some(&s)).is_err(), "{uri}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_out_of_the_root_is_refused() {
        let outside = tempfile::tempdir().expect("outside dir");
        let dir = tempfile::tempdir().expect("tempdir");
        std::os::unix::fs::symlink(outside.path(), dir.path().join("escape"))
            .expect("symlink a directory out");
        std::fs::write(outside.path().join("file.ndbb"), b"x").expect("outside file");
        std::os::unix::fs::symlink(
            outside.path().join("file.ndbb"),
            dir.path().join("link.ndbb"),
        )
        .expect("symlink a file out");
        let s = settings(dir.path());
        for name in ["escape/new.ndbb", "escape/deeper/new.ndbb", "link.ndbb"] {
            let uri = format!("file://{}/{name}", dir.path().display());
            assert!(
                matches!(
                    BackupObject::resolve(&uri, Some(&s)),
                    Err(BackupUriError::OutsideLocalRoot { .. })
                ),
                "{uri} leaves the root through a symlink"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_that_stays_in_the_root_resolves_to_its_target() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir(dir.path().join("real")).expect("real dir");
        std::os::unix::fs::symlink(dir.path().join("real"), dir.path().join("alias"))
            .expect("symlink inside the root");
        let uri = format!("file://{}/alias/db.ndbb", dir.path().display());
        let object = BackupObject::resolve(&uri, Some(&settings(dir.path()))).expect("resolve");
        assert_eq!(object.path.as_ref(), "real/db.ndbb");
    }

    #[test]
    fn a_file_uri_without_a_root_is_refused() {
        assert!(matches!(
            BackupObject::resolve("file:///tmp/x", None),
            Err(BackupUriError::NoLocalRoot { .. })
        ));
    }

    #[test]
    fn an_unknown_scheme_and_a_bare_bucket_are_refused() {
        assert!(matches!(
            BackupObject::resolve("ftp://host/x", None),
            Err(BackupUriError::UnsupportedScheme { .. })
        ));
        assert!(matches!(
            BackupObject::resolve("s3://bucket", None),
            Err(BackupUriError::Malformed { .. })
        ));
    }

    /// A component swapped for an out-of-root symlink between the resolve and
    /// the write refuses the write, and nothing lands outside the root.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_symlink_swapped_in_after_resolve_refuses_the_write_and_the_read() {
        let outside = tempfile::tempdir().expect("outside dir");
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir(dir.path().join("nightly")).expect("in-root dir");
        let uri = format!("file://{}/nightly/db.ndbb", dir.path().display());
        let object = BackupObject::resolve(&uri, Some(&settings(dir.path()))).expect("resolve");

        std::fs::remove_dir(dir.path().join("nightly")).expect("remove the dir");
        std::os::unix::fs::symlink(outside.path(), dir.path().join("nightly"))
            .expect("swap in a symlink out of the root");

        assert!(
            matches!(
                object.put(vec![1, 2, 3]).await,
                Err(BackupIoError::Refused(
                    BackupUriError::OutsideLocalRoot { .. }
                ))
            ),
            "the write follows the swapped-in symlink"
        );
        assert!(
            !outside.path().join("db.ndbb").exists(),
            "the write landed outside the root"
        );
        std::fs::write(outside.path().join("db.ndbb"), b"outside").expect("outside file");
        assert!(
            matches!(
                object.get().await,
                Err(BackupIoError::Refused(
                    BackupUriError::OutsideLocalRoot { .. }
                ))
            ),
            "the read follows the swapped-in symlink"
        );
    }

    /// The final component swapped for a symlink refuses the read.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_file_swapped_for_a_symlink_after_resolve_refuses_the_read() {
        let outside = tempfile::tempdir().expect("outside dir");
        std::fs::write(outside.path().join("secret"), b"outside").expect("outside file");
        let dir = tempfile::tempdir().expect("tempdir");
        let uri = format!("file://{}/db.ndbb", dir.path().display());
        let object = BackupObject::resolve(&uri, Some(&settings(dir.path()))).expect("resolve");
        std::os::unix::fs::symlink(outside.path().join("secret"), dir.path().join("db.ndbb"))
            .expect("place a symlink at the object");
        assert!(matches!(
            object.get().await,
            Err(BackupIoError::Refused(
                BackupUriError::OutsideLocalRoot { .. }
            ))
        ));
    }

    #[tokio::test]
    async fn an_object_round_trips_through_the_local_root() {
        let dir = tempfile::tempdir().expect("tempdir");
        let uri = format!("file://{}/a/b.ndbb", dir.path().display());
        let object = BackupObject::resolve(&uri, Some(&settings(dir.path()))).expect("resolve");
        object.put(vec![1, 2, 3]).await.expect("put");
        assert_eq!(object.get().await.expect("get"), vec![1, 2, 3]);
    }
}
