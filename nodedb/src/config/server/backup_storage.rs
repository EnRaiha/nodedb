// SPDX-License-Identifier: BUSL-1.1

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Where `BACKUP DATABASE ... TO '<uri>'` writes and `RESTORE DATABASE ...
/// FROM '<uri>'` reads. Credentials come from here, never from the SQL text.
///
/// Example TOML:
/// ```toml
/// [backup_storage]
/// local_root = "/srv/nodedb/backups"
/// endpoint = "http://localhost:9000"
/// access_key = "..."
/// secret_key = "..."
/// region = "us-east-1"
/// ```
///
/// * A `file://` URI names a path inside `local_root`. Without `local_root`,
///   every `file://` URI is refused: SQL never names an arbitrary server path.
/// * An `s3://<bucket>/<key>` URI uses `endpoint` (empty = AWS), `region` and
///   the keys (empty = IAM role or instance credentials).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupStorageSettings {
    #[serde(default)]
    pub local_root: Option<PathBuf>,
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub access_key: String,
    #[serde(default)]
    pub secret_key: String,
    #[serde(default = "default_region")]
    pub region: String,
}

fn default_region() -> String {
    "us-east-1".into()
}

impl Default for BackupStorageSettings {
    fn default() -> Self {
        Self {
            local_root: None,
            endpoint: String::new(),
            access_key: String::new(),
            secret_key: String::new(),
            region: default_region(),
        }
    }
}
