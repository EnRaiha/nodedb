// SPDX-License-Identifier: Apache-2.0

//! Per-collection verification records of a backup envelope.
//!
//! A backup records, per collection and engine part, the number of canonical
//! rows and an order-independent digest of them. A restore recomputes both
//! from the envelope before it writes, and from the destination after it
//! writes.
//!
//! The digest is the sum, modulo 2^256, of the SHA-256 of every row:
//!
//! * A sum is order-independent, so rows add in any order and the partial
//!   digests of several nodes merge by addition.
//! * A duplicated row changes the sum. Under XOR two copies cancel.
//! * A row's contribution subtracts out, so a restore can drop a TTL row that
//!   expired after the backup. A sorted Merkle hash cannot, and it needs every
//!   row hash in memory to sort.

use std::fmt;

/// The engine part of a collection a verification record covers. A collection
/// with graph edges has an `Edges` record next to its `Documents` record.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum VerifiedPart {
    Documents,
    Edges,
    KeyValue,
    Vectors,
    Timeseries,
    Columnar,
    Crdt,
    /// Array cell versions.
    Array,
}

impl VerifiedPart {
    /// The byte that separates the parts' row hashes.
    pub fn tag(self) -> u8 {
        match self {
            Self::Documents => 1,
            Self::Edges => 2,
            Self::KeyValue => 3,
            Self::Vectors => 4,
            Self::Timeseries => 5,
            Self::Columnar => 6,
            Self::Crdt => 7,
            Self::Array => 8,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Documents => "documents",
            Self::Edges => "edges",
            Self::KeyValue => "kv",
            Self::Vectors => "vectors",
            Self::Timeseries => "timeseries",
            Self::Columnar => "columnar",
            Self::Crdt => "crdt",
            Self::Array => "array",
        }
    }
}

impl fmt::Display for VerifiedPart {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A row count and the sum of the rows' SHA-256, a little-endian 256-bit
/// integer.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct VerifiedTally {
    pub count: u64,
    pub digest: [u8; 32],
}

impl VerifiedTally {
    /// Add one row by its SHA-256.
    pub fn add(&mut self, row: &[u8; 32]) {
        self.count = self.count.wrapping_add(1);
        let mut carry = 0u16;
        for (sum, byte) in self.digest.iter_mut().zip(row) {
            let total = u16::from(*sum) + u16::from(*byte) + carry;
            *sum = total as u8;
            carry = total >> 8;
        }
    }

    /// Remove one row [`Self::add`] added.
    pub fn remove(&mut self, row: &[u8; 32]) {
        self.count = self.count.wrapping_sub(1);
        let mut borrow = 0i16;
        for (sum, byte) in self.digest.iter_mut().zip(row) {
            let total = i16::from(*sum) - i16::from(*byte) - borrow;
            borrow = i16::from(total < 0);
            *sum = total.rem_euclid(256) as u8;
        }
    }
}

impl fmt::Display for VerifiedTally {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tail: Vec<u8> = self.digest.iter().rev().take(8).copied().collect();
        write!(f, "{} rows, digest {}", self.count, hex::encode(tail))
    }
}

/// The body of a `SECTION_ORIGIN_VERIFICATION` section is a
/// msgpack-encoded `Vec<CollectionVerification>`: one record per collection
/// part that holds at least one row.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct CollectionVerification {
    /// Source id of the database the collection lives in.
    pub database_id: u64,
    /// Bare catalog name of the collection.
    pub collection: String,
    pub part: VerifiedPart,
    pub tally: VerifiedTally,
}

/// Which check a verification ran as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerificationPhase {
    /// The envelope's rows against the tallies it records. Runs before the
    /// first write.
    Envelope,
    /// The destination's rows against the envelope's. Runs after the last
    /// re-issue.
    Destination,
    /// A MOVE TENANT target's rows against the source capture. Runs after the
    /// re-issue and before the catalog moves.
    Move,
}

/// One collection part whose recomputed tally differs from the expected one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerificationMismatch {
    /// The database the collection lives in: the source id in the envelope
    /// phase, the destination id in the destination phase.
    pub database_id: u64,
    pub collection: String,
    pub part: VerifiedPart,
    pub expected: VerifiedTally,
    pub found: VerifiedTally,
}

impl fmt::Display for VerificationMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "collection '{}' in database {} ({}): expected {}, found {}",
            self.collection, self.database_id, self.part, self.expected, self.found
        )
    }
}

/// The message of a failed verification. It names every mismatched
/// collection and states what the restore or move left behind.
pub fn verification_failure_message(
    phase: &VerificationPhase,
    mismatches: &[VerificationMismatch],
) -> String {
    let list = mismatches
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ");
    match phase {
        VerificationPhase::Envelope => format!(
            "restore verification failed: the backup's rows do not match the counts and \
             digests it records for {list}; nothing was restored"
        ),
        VerificationPhase::Destination => format!(
            "restore verification failed: the destination does not hold the backed-up rows \
             of {list}; the restore is failed and not rolled back, and the restored data \
             stays in place for inspection"
        ),
        VerificationPhase::Move => format!(
            "move verification failed: the target does not hold the moved rows of {list}; \
             the move was not applied and the source is untouched"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(seed: u8) -> [u8; 32] {
        let mut bytes = [0u8; 32];
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = seed
                .wrapping_mul(31)
                .wrapping_add(i as u8)
                .wrapping_mul(0x9D);
        }
        bytes
    }

    #[test]
    fn the_sum_ignores_row_order() {
        let mut forward = VerifiedTally::default();
        let mut backward = VerifiedTally::default();
        for seed in 0..50 {
            forward.add(&row(seed));
        }
        for seed in (0..50).rev() {
            backward.add(&row(seed));
        }
        assert_eq!(forward, backward);
        assert_eq!(forward.count, 50);
    }

    #[test]
    fn a_duplicated_row_changes_the_sum() {
        let mut once = VerifiedTally::default();
        once.add(&row(1));
        once.add(&row(2));
        let mut twice = once;
        twice.add(&row(2));
        twice.remove(&row(1));
        assert_eq!(once.count, twice.count);
        assert_ne!(once.digest, twice.digest);
    }

    #[test]
    fn remove_undoes_add_across_carries() {
        let full = [0xFFu8; 32];
        let mut tally = VerifiedTally::default();
        tally.add(&row(7));
        let before = tally;
        tally.add(&full);
        tally.add(&full);
        tally.remove(&full);
        tally.remove(&full);
        assert_eq!(tally, before);
    }

    #[test]
    fn a_record_round_trips() {
        let mut tally = VerifiedTally::default();
        tally.add(&row(3));
        let records = vec![CollectionVerification {
            database_id: 1025,
            collection: "orders".into(),
            part: VerifiedPart::Edges,
            tally,
        }];
        let bytes = zerompk::to_msgpack_vec(&records).expect("encode");
        let decoded: Vec<CollectionVerification> = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, records);
    }

    #[test]
    fn the_message_names_every_collection_and_the_outcome() {
        let mismatch = |collection: &str| VerificationMismatch {
            database_id: 0,
            collection: collection.into(),
            part: VerifiedPart::Documents,
            expected: VerifiedTally::default(),
            found: VerifiedTally::default(),
        };
        let both = [mismatch("orders"), mismatch("users")];
        let message = verification_failure_message(&VerificationPhase::Destination, &both);
        assert!(message.contains("'orders'") && message.contains("'users'"));
        assert!(message.contains("not rolled back"));
        let message = verification_failure_message(&VerificationPhase::Envelope, &both);
        assert!(message.contains("nothing was restored"));
        let message = verification_failure_message(&VerificationPhase::Move, &both);
        assert!(message.starts_with("move verification failed"));
        assert!(message.contains("'orders'") && message.contains("'users'"));
        assert!(message.contains("the move was not applied and the source is untouched"));
    }
}
