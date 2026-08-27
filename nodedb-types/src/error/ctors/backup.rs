// SPDX-License-Identifier: Apache-2.0

//! Backup / restore error constructors (1800-range).

use super::super::code::ErrorCode;
use super::super::details::ErrorDetails;
use super::super::types::NodeDbError;

impl NodeDbError {
    /// RESTORE targeted `expected` but the envelope belongs to `actual`.
    ///
    /// Names the exact mismatch and the two next actions: restore the
    /// tenant-`expected` backup instead, or target tenant `actual`.
    pub fn backup_tenant_mismatch(expected: u64, actual: u64) -> Self {
        Self {
            code: ErrorCode::BACKUP_TENANT_MISMATCH,
            message: format!(
                "backup tenant mismatch: RESTORE targeted tenant {expected} but this \
                 envelope belongs to tenant {actual}; restore tenant {expected}'s own \
                 backup, or target tenant {actual} instead"
            ),
            details: ErrorDetails::BackupTenantMismatch { expected, actual },
            cause: None,
        }
    }

    /// The envelope did not decrypt under this server's configured backup KEK.
    ///
    /// Never includes key material or a key fingerprint/hash — only that the
    /// keys disagree and where to look.
    pub fn backup_key_mismatch() -> Self {
        Self {
            code: ErrorCode::BACKUP_KEY_MISMATCH,
            message: "wrong backup KEK: this envelope was not encrypted with the key \
                       at this server's configured backup_encryption.key_path; verify \
                       the key file matches the one used when this backup was created"
                .to_string(),
            details: ErrorDetails::BackupKeyMismatch,
            cause: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backup_tenant_mismatch_code() {
        let e = NodeDbError::backup_tenant_mismatch(1, 99);
        assert_eq!(e.code(), ErrorCode::BACKUP_TENANT_MISMATCH);
        assert!(e.message().contains("tenant 1"));
        assert!(e.message().contains("tenant 99"));
    }

    #[test]
    fn backup_key_mismatch_code() {
        let e = NodeDbError::backup_key_mismatch();
        assert_eq!(e.code(), ErrorCode::BACKUP_KEY_MISMATCH);
        assert!(e.message().contains("key_path"));
    }
}
