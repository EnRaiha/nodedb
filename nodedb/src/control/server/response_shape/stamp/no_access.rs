// SPDX-License-Identifier: BUSL-1.1

//! The sequence access of a shaping path that holds no session.

use crate::control::sequence::SequenceAccess;

/// [`SequenceAccess`] for a path with no session sequence state: a per-batch
/// stream, gateway forwarding, a clone merge.
///
/// A computed column that calls no sequence accessor evaluates without it.
/// A call to `nextval`, `currval` or `setval` is refused, never answered
/// with a guessed value.
pub struct NoSequenceAccess;

impl NoSequenceAccess {
    fn refuse(function: &str, name: &str) -> crate::Error {
        crate::Error::FeatureNotSupported {
            detail: format!(
                "{function}('{name}') in a Control-Plane computed column needs session \
                 sequence access, which this path does not hold"
            ),
        }
    }
}

impl SequenceAccess for NoSequenceAccess {
    fn nextval(&self, name: &str) -> crate::Result<i64> {
        Err(Self::refuse("nextval", name))
    }

    fn currval(&self, name: &str) -> crate::Result<i64> {
        Err(Self::refuse("currval", name))
    }

    fn setval(&self, name: &str, _value: i64) -> crate::Result<i64> {
        Err(Self::refuse("setval", name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_accessor_is_refused_naming_the_sequence() {
        for err in [
            NoSequenceAccess.nextval("s"),
            NoSequenceAccess.currval("s"),
            NoSequenceAccess.setval("s", 1),
        ] {
            match err {
                Err(crate::Error::FeatureNotSupported { detail }) => {
                    assert!(detail.contains("('s')"), "{detail}");
                }
                other => panic!("expected FeatureNotSupported, got {other:?}"),
            }
        }
    }
}
