// SPDX-License-Identifier: BUSL-1.1

//! The counts each install of a resolved timeseries batch stored and
//! rejected.
//!
//! An install rejects the rows whose value conflicts with the schema at its
//! log position, beyond the lines its resolve rejected. Only the install
//! knows that count. A committed redo install answers these counts in its
//! response payload, so the writer that waits on the apply reports the
//! apply's count, not the resolve's.
//!
//! The payload has the shape an ingest answers, `accepted`, `rejected` and
//! `collection`, so every reader of an ingest's counts reads it, and adds
//! `ts_installs`, the count of each install.

use std::collections::BTreeMap;

/// What one install of one resolved batch stored.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct TsInstallCount {
    pub collection: String,
    /// The rows that landed.
    pub accepted: u64,
    /// The lines the resolve rejected plus the rows the install rejected.
    pub rejected: u64,
}

/// The counts of every resolved batch one apply installed, in install order,
/// with their totals.
#[derive(
    Debug, Clone, Default, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct TsInstallCounts {
    /// The rows every install stored.
    pub accepted: u64,
    /// The lines and rows every install rejected.
    pub rejected: u64,
    /// The collection of the first install.
    pub collection: String,
    pub ts_installs: Vec<TsInstallCount>,
}

impl TsInstallCounts {
    /// The counts of `installs`, with their totals.
    pub fn new(ts_installs: Vec<TsInstallCount>) -> Self {
        let (accepted, rejected) = ts_installs.iter().fold((0u64, 0u64), |(a, r), install| {
            (
                a.saturating_add(install.accepted),
                r.saturating_add(install.rejected),
            )
        });
        let collection = ts_installs
            .first()
            .map(|install| install.collection.clone())
            .unwrap_or_default();
        Self {
            accepted,
            rejected,
            collection,
            ts_installs,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.ts_installs.is_empty()
    }

    pub fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("timeseries install counts: {e}"),
        })
    }

    /// The counts `payload` carries. `None` when `payload` is not an encoded
    /// [`TsInstallCounts`] with at least one install.
    pub fn from_payload(payload: &[u8]) -> Option<Self> {
        zerompk::from_msgpack::<Self>(payload)
            .ok()
            .filter(|counts| !counts.is_empty())
    }

    /// The accepted and rejected totals, by collection.
    pub fn by_collection(&self) -> BTreeMap<String, (u64, u64)> {
        let mut out: BTreeMap<String, (u64, u64)> = BTreeMap::new();
        for install in &self.ts_installs {
            let entry = out.entry(install.collection.clone()).or_default();
            entry.0 = entry.0.saturating_add(install.accepted);
            entry.1 = entry.1.saturating_add(install.rejected);
        }
        out
    }
}

/// The counts both `first` and `second` carry, encoded. `None` when neither
/// payload carries install counts. A payload that carries none contributes
/// nothing.
pub fn merge_count_payloads(first: &[u8], second: &[u8]) -> Option<Vec<u8>> {
    let merged = match (
        TsInstallCounts::from_payload(first),
        TsInstallCounts::from_payload(second),
    ) {
        (None, None) => return None,
        (Some(counts), None) | (None, Some(counts)) => counts,
        (Some(counts), Some(more)) => {
            let mut installs = counts.ts_installs;
            installs.extend(more.ts_installs);
            TsInstallCounts::new(installs)
        }
    };
    merged.to_bytes().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counts(entries: &[(&str, u64, u64)]) -> TsInstallCounts {
        TsInstallCounts::new(
            entries
                .iter()
                .map(|(collection, accepted, rejected)| TsInstallCount {
                    collection: (*collection).into(),
                    accepted: *accepted,
                    rejected: *rejected,
                })
                .collect(),
        )
    }

    #[test]
    fn counts_round_trip_and_sum_by_collection() {
        let encoded = counts(&[("cpu", 2, 1), ("cpu", 1, 0), ("mem", 0, 3)])
            .to_bytes()
            .expect("encode");
        let decoded = TsInstallCounts::from_payload(&encoded).expect("decode");
        assert_eq!((decoded.accepted, decoded.rejected), (3, 4));
        assert_eq!(decoded.collection, "cpu");
        let totals = decoded.by_collection();
        assert_eq!(totals.get("cpu"), Some(&(3, 1)));
        assert_eq!(totals.get("mem"), Some(&(0, 3)));
    }

    /// The payload reads as an ingest's counts: its affected count and its
    /// rejected lines.
    #[test]
    fn the_payload_reads_as_an_ingest_answer() {
        let encoded = counts(&[("cpu", 2, 1)]).to_bytes().expect("encode");
        let json: serde_json::Value = nodedb_types::json_from_msgpack(&encoded).expect("transcode");
        assert_eq!(json["accepted"], serde_json::json!(2));
        assert_eq!(json["rejected"], serde_json::json!(1));
        assert_eq!(json["collection"], serde_json::json!("cpu"));
    }

    #[test]
    fn a_payload_without_counts_carries_none() {
        let empty = TsInstallCounts::default().to_bytes().expect("encode");
        assert!(TsInstallCounts::from_payload(&empty).is_none());
        assert!(TsInstallCounts::from_payload(&[]).is_none());
        let ingest = nodedb_types::json_to_msgpack(&serde_json::json!({
            "accepted": 1,
            "rejected": 0,
            "collection": "cpu",
        }))
        .expect("encode ingest answer");
        assert!(TsInstallCounts::from_payload(&ingest).is_none());
    }

    #[test]
    fn merging_keeps_every_install_of_both_payloads() {
        let first = counts(&[("cpu", 2, 1)]).to_bytes().expect("encode");
        let second = counts(&[("mem", 1, 1)]).to_bytes().expect("encode");
        let merged = merge_count_payloads(&first, &second).expect("merged");
        let decoded = TsInstallCounts::from_payload(&merged).expect("decode");
        assert_eq!(decoded.ts_installs.len(), 2);
        assert_eq!((decoded.accepted, decoded.rejected), (3, 2));
        let plain = nodedb_types::json_to_msgpack(&serde_json::json!({"affected": 1}))
            .expect("encode plain");
        assert_eq!(
            merge_count_payloads(&plain, &first).as_deref(),
            Some(first.as_slice())
        );
        assert!(merge_count_payloads(&plain, &plain).is_none());
    }
}
