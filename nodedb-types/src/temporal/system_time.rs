// SPDX-License-Identifier: Apache-2.0

//! System-time scope for bitemporal scans.
//!
//! Replaces the former `Option<i64>` system-AS-OF value with an explicit
//! three-state enum so that the "all versions" (audit-log) mode is
//! representable instead of being an out-of-band sentinel.

use serde::{Deserialize, Serialize};

/// System-time selection for a temporal scan.
///
/// All timestamps are **milliseconds since Unix epoch**. Valid time is a
/// separate axis and is unaffected by this enum.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum SystemTimeScope {
    /// Latest committed version (no `AS OF SYSTEM TIME` clause).
    #[default]
    Current,
    /// Point-in-time snapshot: the version visible AS OF `ms`.
    AsOf(i64),
    /// `AS OF SYSTEM TIME NULL` — every system-time version of each matching
    /// row, ordered ascending by system time, with the system-time column
    /// projected into the output (audit-log semantics).
    AllVersions,
}

impl SystemTimeScope {
    /// True when this scope selects something other than current state.
    pub const fn is_temporal(&self) -> bool {
        !matches!(self, Self::Current)
    }

    /// The point-in-time cutoff, if this is an `AsOf` scope.
    pub const fn as_of_ms(&self) -> Option<i64> {
        match self {
            Self::AsOf(ms) => Some(*ms),
            Self::Current | Self::AllVersions => None,
        }
    }

    /// True when this scope requests every system-time version.
    pub const fn is_all_versions(&self) -> bool {
        matches!(self, Self::AllVersions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_current() {
        assert_eq!(SystemTimeScope::default(), SystemTimeScope::Current);
        assert!(!SystemTimeScope::Current.is_temporal());
    }

    #[test]
    fn as_of_exposes_ms() {
        assert_eq!(SystemTimeScope::AsOf(42).as_of_ms(), Some(42));
        assert_eq!(SystemTimeScope::Current.as_of_ms(), None);
        assert_eq!(SystemTimeScope::AllVersions.as_of_ms(), None);
    }

    #[test]
    fn all_versions_is_temporal() {
        assert!(SystemTimeScope::AllVersions.is_temporal());
        assert!(SystemTimeScope::AllVersions.is_all_versions());
        assert!(!SystemTimeScope::AsOf(1).is_all_versions());
    }
}
