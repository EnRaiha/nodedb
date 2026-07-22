// SPDX-License-Identifier: BUSL-1.1

//! Decoding compatibility for `TimeseriesBatch` WAL records.

/// Decoded fields of a `TimeseriesBatch` WAL record.
///
/// `kind` is `Some("columnar")` / `Some("timeseries")` for tagged records and
/// `None` for the legacy 2-tuple shape. `format` is present only in the new
/// five-element timeseries tuple; absent records use the legacy UTF-8 heuristic.
/// `surrogates` is only non-empty for map-shaped columnar records.
pub(super) type DecodedBatchRecord = (
    Option<String>,
    String,
    Vec<u8>,
    Option<nodedb_types::sync::wire::SyncProvenance>,
    Option<String>,
    Vec<nodedb_types::Surrogate>,
);

/// Decode a `TimeseriesBatch` WAL payload into its logical fields.
///
/// Tries the newest map and format-preserving tuple forms before all legacy
/// tuple forms. The map form is unambiguous from the tuple forms.
pub(super) fn decode_batch_record(payload: &[u8]) -> Result<DecodedBatchRecord, ()> {
    if let Ok(rec) = zerompk::from_msgpack::<nodedb_types::columnar::ColumnarWalRecord>(payload) {
        return Ok((
            Some(rec.kind),
            rec.collection,
            rec.payload,
            rec.provenance,
            None,
            rec.surrogates,
        ));
    }
    zerompk::from_msgpack::<(
        String,
        String,
        Vec<u8>,
        Option<nodedb_types::sync::wire::SyncProvenance>,
        String,
    )>(payload)
    .map(|(kind, collection, payload, provenance, format)| {
        (
            Some(kind),
            collection,
            payload,
            provenance,
            Some(format),
            Vec::new(),
        )
    })
    .or_else(|_| {
        zerompk::from_msgpack::<(
            String,
            String,
            Vec<u8>,
            Option<nodedb_types::sync::wire::SyncProvenance>,
        )>(payload)
        .map(|(kind, collection, payload, provenance)| {
            (
                Some(kind),
                collection,
                payload,
                provenance,
                None,
                Vec::new(),
            )
        })
    })
    .or_else(|_| {
        zerompk::from_msgpack::<(String, String, Vec<u8>)>(payload).map(
            |(kind, collection, payload)| (Some(kind), collection, payload, None, None, Vec::new()),
        )
    })
    .or_else(|_| {
        zerompk::from_msgpack::<(String, Vec<u8>)>(payload)
            .map(|(collection, payload)| (None, collection, payload, None, None, Vec::new()))
    })
    .map_err(|_| ())
}

/// Record-level fields for replaying a single columnar WAL batch.
pub(super) struct ColumnarReplayArgs<'a> {
    pub collection: &'a str,
    pub payload: &'a [u8],
    pub record_lsn: u64,
    pub provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
    /// Per-row surrogates index-aligned with `payload` rows. An empty `Vec`
    /// falls back to fresh surrogate allocation for legacy records.
    pub surrogates: Vec<nodedb_types::Surrogate>,
}

/// Record-level fields for replaying one timeseries WAL batch.
pub(super) struct TimeseriesReplayArgs<'a> {
    pub collection: &'a str,
    pub payload: &'a [u8],
    pub record_lsn: u64,
    pub provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
    pub format: Option<&'a str>,
}
