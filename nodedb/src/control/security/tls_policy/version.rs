// SPDX-License-Identifier: BUSL-1.1

//! [`TlsVersion`] — the ordered protocol version a TLS policy compares against.
//!
//! The operator writes `"1.2"` / `"1.3"` in config, but the comparison is never
//! made on that string: lexical order puts `"1.10"` below `"1.2"`, so a string
//! minimum silently mis-compares the moment a version past `1.9` exists. The
//! parsed form is a closed enum whose declaration order *is* its protocol
//! order, so `>=` on it is the protocol comparison and nothing else.

use std::fmt;

use serde::{Deserialize, Serialize};

/// A TLS protocol version, ordered oldest → newest.
///
/// `Ord` is derived from the declaration order, which is deliberately the
/// protocol order: `Tls1_2 < Tls1_3`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum TlsVersion {
    Tls1_0,
    Tls1_1,
    Tls1_2,
    Tls1_3,
}

impl TlsVersion {
    /// The operator-facing spelling, as accepted by [`TlsVersion::parse`].
    pub fn label(self) -> &'static str {
        match self {
            Self::Tls1_0 => "1.0",
            Self::Tls1_1 => "1.1",
            Self::Tls1_2 => "1.2",
            Self::Tls1_3 => "1.3",
        }
    }

    /// Parse an operator-supplied minimum version.
    ///
    /// Accepts `1.2`, `TLS1.2`, `TLSv1.2` (any case, surrounding whitespace
    /// ignored). Anything else is a configuration error: a server must not
    /// start with a TLS minimum nobody can interpret, because the only
    /// alternatives are silently enforcing a different version than the
    /// operator asked for or silently enforcing none.
    pub fn parse(raw: &str) -> crate::Result<Self> {
        let trimmed = raw.trim();
        let normalized = trimmed.to_ascii_lowercase();
        let digits = normalized
            .strip_prefix("tlsv")
            .or_else(|| normalized.strip_prefix("tls"))
            .unwrap_or(normalized.as_str());

        match digits {
            "1.0" => Ok(Self::Tls1_0),
            "1.1" => Ok(Self::Tls1_1),
            "1.2" => Ok(Self::Tls1_2),
            "1.3" => Ok(Self::Tls1_3),
            _ => Err(crate::Error::Config {
                detail: format!(
                    "auth.tls_policy.min_tls_version: '{trimmed}' is not a TLS version \
                     (expected one of 1.0, 1.1, 1.2, 1.3)"
                ),
            }),
        }
    }

    /// Map a version negotiated by rustls onto the policy's version ladder.
    ///
    /// `None` for anything outside the ladder — an unrecognised ordinal, or a
    /// DTLS/SSL codepoint. The caller treats that as an unidentified transport
    /// rather than assuming it is safe.
    pub fn from_negotiated(version: tokio_rustls::rustls::ProtocolVersion) -> Option<Self> {
        use tokio_rustls::rustls::ProtocolVersion;

        match version {
            ProtocolVersion::TLSv1_0 => Some(Self::Tls1_0),
            ProtocolVersion::TLSv1_1 => Some(Self::Tls1_1),
            ProtocolVersion::TLSv1_2 => Some(Self::Tls1_2),
            ProtocolVersion::TLSv1_3 => Some(Self::Tls1_3),
            _ => None,
        }
    }
}

impl fmt::Display for TlsVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordering_is_protocol_order_not_lexical_order() {
        assert!(TlsVersion::Tls1_3 > TlsVersion::Tls1_2);
        assert!(TlsVersion::Tls1_2 > TlsVersion::Tls1_1);
        assert!(TlsVersion::Tls1_0 < TlsVersion::Tls1_2);
        // The trap the string form falls into: "1.10" sorts below "1.2".
        assert!("1.10" < "1.2");
    }

    #[test]
    fn parses_the_operator_facing_spellings() {
        assert_eq!(TlsVersion::parse("1.2").expect("1.2"), TlsVersion::Tls1_2);
        assert_eq!(
            TlsVersion::parse("  1.3 ").expect("padded 1.3"),
            TlsVersion::Tls1_3
        );
        assert_eq!(
            TlsVersion::parse("TLSv1.3").expect("TLSv1.3"),
            TlsVersion::Tls1_3
        );
        assert_eq!(
            TlsVersion::parse("tls1.2").expect("tls1.2"),
            TlsVersion::Tls1_2
        );
    }

    #[test]
    fn unparseable_versions_are_rejected_loudly() {
        for raw in ["", "1.4", "1", "tls", "1,2", "latest", "TLS1.10"] {
            assert!(
                matches!(TlsVersion::parse(raw), Err(crate::Error::Config { .. })),
                "'{raw}' must not parse as a TLS version"
            );
        }
    }

    #[test]
    fn negotiated_versions_map_onto_the_ladder() {
        use tokio_rustls::rustls::ProtocolVersion;

        assert_eq!(
            TlsVersion::from_negotiated(ProtocolVersion::TLSv1_3),
            Some(TlsVersion::Tls1_3)
        );
        assert_eq!(
            TlsVersion::from_negotiated(ProtocolVersion::TLSv1_2),
            Some(TlsVersion::Tls1_2)
        );
        assert_eq!(TlsVersion::from_negotiated(ProtocolVersion::SSLv3), None);
        assert_eq!(
            TlsVersion::from_negotiated(ProtocolVersion::Unknown(0x9999)),
            None
        );
    }

    #[test]
    fn label_round_trips_through_parse() {
        for version in [
            TlsVersion::Tls1_0,
            TlsVersion::Tls1_1,
            TlsVersion::Tls1_2,
            TlsVersion::Tls1_3,
        ] {
            assert_eq!(
                TlsVersion::parse(version.label()).expect("label parses"),
                version
            );
        }
    }
}
