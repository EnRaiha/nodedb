// SPDX-License-Identifier: BUSL-1.1

//! Typed group key of a distributed array aggregate partial.
//!
//! The key mirrors the array coordinate variants without a dependency on
//! `nodedb-array`. Two shards report one group only when their keys are the
//! same typed value, so a Float64 or String dim groups by its value.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

/// The group a partial aggregate belongs to.
///
/// `Scalar` keys the one partial of an aggregate with no group-by. Float64
/// keys compare by `f64::total_cmp`, so equal keys have equal bits.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub enum ArrayAggGroupKey {
    Scalar,
    Int64(i64),
    Float64(f64),
    TimestampMs(i64),
    String(String),
}

impl ArrayAggGroupKey {
    fn rank(&self) -> u8 {
        match self {
            ArrayAggGroupKey::Scalar => 0,
            ArrayAggGroupKey::Int64(_) => 1,
            ArrayAggGroupKey::Float64(_) => 2,
            ArrayAggGroupKey::TimestampMs(_) => 3,
            ArrayAggGroupKey::String(_) => 4,
        }
    }
}

impl Ord for ArrayAggGroupKey {
    fn cmp(&self, other: &Self) -> Ordering {
        use ArrayAggGroupKey::*;
        match (self, other) {
            (Scalar, Scalar) => Ordering::Equal,
            (Int64(a), Int64(b)) | (TimestampMs(a), TimestampMs(b)) => a.cmp(b),
            (Float64(a), Float64(b)) => a.total_cmp(b),
            (String(a), String(b)) => a.cmp(b),
            _ => self.rank().cmp(&other.rank()),
        }
    }
}

impl PartialOrd for ArrayAggGroupKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ArrayAggGroupKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for ArrayAggGroupKey {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn float_keys_group_by_value() {
        assert_eq!(
            ArrayAggGroupKey::Float64(1.5),
            ArrayAggGroupKey::Float64(1.5)
        );
        assert!(ArrayAggGroupKey::Float64(1.5) < ArrayAggGroupKey::Float64(2.5));
        assert_ne!(
            ArrayAggGroupKey::Float64(1.5),
            ArrayAggGroupKey::Float64(2.5)
        );
    }

    #[test]
    fn keys_of_different_kinds_never_match() {
        assert_ne!(ArrayAggGroupKey::Int64(3), ArrayAggGroupKey::TimestampMs(3));
        assert_ne!(ArrayAggGroupKey::Scalar, ArrayAggGroupKey::Int64(0));
    }

    #[test]
    fn a_string_key_round_trips_through_msgpack() {
        let key = ArrayAggGroupKey::String("chrX".into());
        let bytes = zerompk::to_msgpack_vec(&key).expect("encode");
        let back: ArrayAggGroupKey = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(back, key);
    }
}
