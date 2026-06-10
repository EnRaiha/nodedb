// SPDX-License-Identifier: Apache-2.0

//! Bitemporal filter predicates used by SQL planner and scan operators.

use crate::temporal::system_time::SystemTimeScope;
use serde::{Deserialize, Serialize};

/// Valid-time predicate attached to a scan.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[non_exhaustive]
pub enum ValidTimePredicate {
    /// Match rows whose valid interval contains `t`.
    Contains(i64),
    /// Match rows whose valid interval overlaps `[from, to)`.
    Overlaps { from: i64, to: i64 },
    /// Match rows that were valid AS OF `t` (equivalent to `Contains(t)` but
    /// carries the intent of a point-in-time snapshot rather than a predicate).
    AsOf(i64),
}

/// Bitemporal filter combining system-time and valid-time predicates.
///
/// `None` on a dimension means "current state along that axis".
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
pub struct BitemporalFilter {
    /// System-time selection. `Current` = latest committed state.
    pub system_time: SystemTimeScope,
    /// Valid-time predicate. None = unconstrained along the valid axis.
    pub valid: Option<ValidTimePredicate>,
}

impl BitemporalFilter {
    /// Filter that requests strictly current state on both axes.
    pub const CURRENT: Self = Self {
        system_time: SystemTimeScope::Current,
        valid: None,
    };

    /// Whether this filter is the pure current-state filter.
    pub const fn is_current(&self) -> bool {
        matches!(self.system_time, SystemTimeScope::Current) && self.valid.is_none()
    }

    /// Construct a filter for "as of system time `t`".
    pub const fn system_as_of(t: i64) -> Self {
        Self {
            system_time: SystemTimeScope::AsOf(t),
            valid: None,
        }
    }

    /// Construct a filter for "valid-time contains `t`".
    pub const fn valid_at(t: i64) -> Self {
        Self {
            system_time: SystemTimeScope::Current,
            valid: Some(ValidTimePredicate::Contains(t)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_filter_is_trivial() {
        assert!(BitemporalFilter::CURRENT.is_current());
        assert!(BitemporalFilter::default().is_current());
    }

    #[test]
    fn constructors_populate_expected_fields() {
        let f = BitemporalFilter::system_as_of(1_000);
        assert_eq!(f.system_time, SystemTimeScope::AsOf(1_000));
        assert!(f.valid.is_none());
        let g = BitemporalFilter::valid_at(2_000);
        assert_eq!(g.system_time, SystemTimeScope::Current);
        assert_eq!(g.valid, Some(ValidTimePredicate::Contains(2_000)));
    }

    #[test]
    fn is_current_detects_non_trivial() {
        assert!(!BitemporalFilter::system_as_of(0).is_current());
        assert!(!BitemporalFilter::valid_at(0).is_current());
    }
}
