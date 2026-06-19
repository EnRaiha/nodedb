// SPDX-License-Identifier: BUSL-1.1

//! Versioned-key builders, parsers, sentinel constants, and `EdgeRef`.

use nodedb_types::{DatabaseId, TenantId};

/// Soft-delete marker.
pub const TOMBSTONE_SENTINEL: &[u8] = &[0xFF];

/// GDPR erasure marker — preserves coordinate existence, removes content.
/// Distinct from tombstone so audits can tell "user-deleted" from "legally erased".
pub const GDPR_ERASURE_SENTINEL: &[u8] = &[0xFE];

/// Width of the zero-padded `system_from` ordinal suffix.
pub const SYSTEM_TIME_WIDTH: usize = 20;

/// Identifies a base edge: database + tenant + collection + `(src, label, dst)`
/// triple. Borrowed so write paths don't allocate per edge.
#[derive(Debug, Clone, Copy)]
pub struct EdgeRef<'a> {
    pub db: DatabaseId,
    pub tid: TenantId,
    pub collection: &'a str,
    pub src: &'a str,
    pub label: &'a str,
    pub dst: &'a str,
}

impl<'a> EdgeRef<'a> {
    pub const fn new(
        db: DatabaseId,
        tid: TenantId,
        collection: &'a str,
        src: &'a str,
        label: &'a str,
        dst: &'a str,
    ) -> Self {
        Self {
            db,
            tid,
            collection,
            src,
            label,
            dst,
        }
    }

    /// Return an `EdgeRef` with `src` and `dst` swapped — used when building
    /// the reverse-index key shape.
    pub const fn reversed(self) -> Self {
        Self {
            db: self.db,
            tid: self.tid,
            collection: self.collection,
            src: self.dst,
            label: self.label,
            dst: self.src,
        }
    }
}

/// Is this raw redb value a soft-delete tombstone?
pub fn is_tombstone(bytes: &[u8]) -> bool {
    bytes == TOMBSTONE_SENTINEL
}

/// Is this raw redb value a GDPR erasure marker?
pub fn is_gdpr_erasure(bytes: &[u8]) -> bool {
    bytes == GDPR_ERASURE_SENTINEL
}

/// Is this raw redb value any non-payload sentinel?
pub fn is_sentinel(bytes: &[u8]) -> bool {
    is_tombstone(bytes) || is_gdpr_erasure(bytes)
}

/// Build a versioned edge key.
///
/// Returns an error if `system_from` is negative — key ordering semantics
/// require a non-negative suffix.
pub fn versioned_edge_key(
    collection: &str,
    src: &str,
    label: &str,
    dst: &str,
    system_from: i64,
) -> crate::Result<String> {
    if system_from < 0 {
        return Err(crate::Error::BadRequest {
            detail: format!("versioned_edge_key: negative system_from={system_from}"),
        });
    }
    Ok(format!(
        "{collection}\x00{src}\x00{label}\x00{dst}\x00{system_from:0width$}",
        width = SYSTEM_TIME_WIDTH
    ))
}

/// Build the version-range prefix for a base edge.
pub fn edge_version_prefix(collection: &str, src: &str, label: &str, dst: &str) -> String {
    format!("{collection}\x00{src}\x00{label}\x00{dst}\x00")
}

/// Decompose a versioned edge key into its components.
pub fn parse_versioned_edge_key(key: &str) -> Option<(&str, &str, &str, &str, i64)> {
    let mut parts = key.splitn(5, '\x00');
    let collection = parts.next()?;
    let src = parts.next()?;
    let label = parts.next()?;
    let dst = parts.next()?;
    let version = parts.next()?;
    if version.len() != SYSTEM_TIME_WIDTH {
        return None;
    }
    let system_from: i64 = version.parse().ok()?;
    Some((collection, src, label, dst, system_from))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_key_zero_padded_20_digits() {
        let k = versioned_edge_key("c", "a", "L", "b", 42).unwrap();
        assert!(k.ends_with("\x0000000000000000000042"));
        assert_eq!(k.len(), "c\x00a\x00L\x00b\x00".len() + SYSTEM_TIME_WIDTH);
    }

    #[test]
    fn version_key_sorts_chronologically() {
        let a = versioned_edge_key("c", "a", "L", "b", 100).unwrap();
        let b = versioned_edge_key("c", "a", "L", "b", 2_000).unwrap();
        let c = versioned_edge_key("c", "a", "L", "b", 30_000_000).unwrap();
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn negative_system_time_rejected() {
        assert!(versioned_edge_key("c", "a", "L", "b", -1).is_err());
    }

    #[test]
    fn parse_versioned_roundtrip() {
        let k = versioned_edge_key("coll", "alice", "KNOWS", "bob", 1_700_000_000_000).unwrap();
        let (c, s, l, d, t) = parse_versioned_edge_key(&k).unwrap();
        assert_eq!(
            (c, s, l, d, t),
            ("coll", "alice", "KNOWS", "bob", 1_700_000_000_000)
        );
    }

    #[test]
    fn parse_rejects_wrong_width() {
        let bad = "c\x00a\x00L\x00b\x0042";
        assert!(parse_versioned_edge_key(bad).is_none());
    }

    #[test]
    fn sentinel_distinctness() {
        assert!(is_tombstone(TOMBSTONE_SENTINEL));
        assert!(!is_tombstone(GDPR_ERASURE_SENTINEL));
        assert!(is_gdpr_erasure(GDPR_ERASURE_SENTINEL));
        assert!(!is_gdpr_erasure(TOMBSTONE_SENTINEL));
        assert!(is_sentinel(TOMBSTONE_SENTINEL));
        assert!(is_sentinel(GDPR_ERASURE_SENTINEL));
        assert!(!is_sentinel(&[0x93]));
        assert!(!is_sentinel(&[0x83]));
    }

    #[test]
    fn edge_ref_reversed_swaps_src_dst() {
        let e = EdgeRef::new(DatabaseId::DEFAULT, TenantId::new(1), "c", "a", "L", "b");
        let r = e.reversed();
        assert_eq!(r.src, "b");
        assert_eq!(r.dst, "a");
        assert_eq!(r.collection, "c");
        assert_eq!(r.label, "L");
    }
}
