// SPDX-License-Identifier: BUSL-1.1

//! The key the legs of a GraphRAG fusion fuse on.
//!
//! A row named by a graph node fuses under that name. A row with a surrogate
//! and no graph node fuses under the surrogate. A vector entry with no
//! surrogate fuses with nothing. Equality keeps the three apart, so a key
//! `"7"` never meets surrogate 7. Ordering compares the rendered text first,
//! so equal RRF scores break in the order of the rendered strings.

use std::borrow::Cow;
use std::cmp::Ordering;

use nodedb_types::Surrogate;

/// Prefix of the rendered key of a vector entry with no surrogate.
pub(crate) const UNBOUND_PREFIX: &str = "__unbound_";

/// One fusion leg hit, in the key space RRF fuses on.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum RagKey {
    /// A graph node name: the row's client key.
    Node(String),
    /// A row with no graph node name. The response boundary names it.
    Row(Surrogate),
    /// A vector entry with no surrogate, keyed by its HNSW entry id.
    Unbound(u32),
}

impl RagKey {
    /// The key of a hit carrying `surrogate`: the graph node it names, else
    /// the surrogate.
    pub(crate) fn of_hit(name: Option<&str>, surrogate: Surrogate) -> Self {
        match name {
            Some(name) => Self::Node(name.to_owned()),
            None => Self::Row(surrogate),
        }
    }

    /// The text this key renders as before the response boundary names it.
    pub(crate) fn rendered(&self) -> Cow<'_, str> {
        match self {
            Self::Node(name) => Cow::Borrowed(name),
            Self::Row(surrogate) => Cow::Owned(surrogate.as_u32().to_string()),
            Self::Unbound(entry) => Cow::Owned(format!("{UNBOUND_PREFIX}{entry}")),
        }
    }

    /// The bytes of [`Self::rendered`], with no allocation: a node name
    /// borrows itself, and a number writes into `buf`.
    fn rendered_bytes<'a>(&'a self, buf: &'a mut [u8; RENDER_CAP]) -> &'a [u8] {
        match self {
            Self::Node(name) => name.as_bytes(),
            Self::Row(surrogate) => write_decimal(buf, 0, surrogate.as_u32()),
            Self::Unbound(entry) => {
                let prefix = UNBOUND_PREFIX.as_bytes();
                buf[..prefix.len()].copy_from_slice(prefix);
                write_decimal(buf, prefix.len(), *entry)
            }
        }
    }

    fn variant_rank(&self) -> u8 {
        match self {
            Self::Node(_) => 0,
            Self::Row(_) => 1,
            Self::Unbound(_) => 2,
        }
    }
}

/// Room for the longest numeric rendering: the unbound prefix and the ten
/// digits of `u32::MAX`.
const RENDER_CAP: usize = UNBOUND_PREFIX.len() + 10;

/// Write the decimal digits of `n` into `buf` from `start`. Returns
/// `buf[..start + digits]`.
fn write_decimal(buf: &mut [u8; RENDER_CAP], start: usize, n: u32) -> &[u8] {
    let mut digits = [0u8; 10];
    let mut len = 0;
    let mut rest = n;
    loop {
        digits[len] = b'0' + (rest % 10) as u8;
        len += 1;
        rest /= 10;
        if rest == 0 {
            break;
        }
    }
    for (offset, digit) in digits[..len].iter().rev().enumerate() {
        buf[start + offset] = *digit;
    }
    &buf[..start + len]
}

/// Byte order of the rendered texts, the same order `str` comparison gives,
/// then the variant to keep equal texts apart.
impl Ord for RagKey {
    fn cmp(&self, other: &Self) -> Ordering {
        let mut mine = [0u8; RENDER_CAP];
        let mut theirs = [0u8; RENDER_CAP];
        self.rendered_bytes(&mut mine)
            .cmp(other.rendered_bytes(&mut theirs))
            .then_with(|| self.variant_rank().cmp(&other.variant_rank()))
    }
}

impl PartialOrd for RagKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ord_matches_rendered_text_order() {
        let mut keys = [
            RagKey::Node("b".into()),
            RagKey::Row(Surrogate::new(10)),
            RagKey::Row(Surrogate::new(9)),
            RagKey::Unbound(3),
            RagKey::Node("10a".into()),
        ];
        keys.sort();
        let rendered: Vec<String> = keys.iter().map(|k| k.rendered().into_owned()).collect();
        let mut expected = rendered.clone();
        expected.sort();
        assert_eq!(rendered, expected);
    }

    /// Mixed digit counts sort as text ("10" before "9"), and a node name
    /// equal to a decimal sorts beside it, apart from it.
    #[test]
    fn ord_matches_rendered_text_order_across_digit_counts() {
        let mut keys = vec![
            RagKey::Row(Surrogate::new(9)),
            RagKey::Row(Surrogate::new(10)),
            RagKey::Row(Surrogate::new(u32::MAX)),
            RagKey::Row(Surrogate::new(0)),
            RagKey::Node("9".into()),
            RagKey::Node("10".into()),
            RagKey::Unbound(9),
            RagKey::Unbound(10),
            RagKey::Unbound(u32::MAX),
        ];
        keys.sort();
        let rendered: Vec<String> = keys.iter().map(|k| k.rendered().into_owned()).collect();
        let mut expected = rendered.clone();
        expected.sort();
        assert_eq!(rendered, expected);
        let nine = keys
            .iter()
            .position(|k| *k == RagKey::Node("9".into()))
            .expect("node 9");
        assert_eq!(keys[nine + 1], RagKey::Row(Surrogate::new(9)));
    }

    #[test]
    fn decimal_node_never_equals_an_equal_surrogate() {
        assert_ne!(RagKey::Node("7".into()), RagKey::Row(Surrogate::new(7)));
        assert_ne!(
            RagKey::Node("7".into()).cmp(&RagKey::Row(Surrogate::new(7))),
            Ordering::Equal
        );
    }

    #[test]
    fn unbound_renders_the_unbound_sentinel() {
        assert_eq!(RagKey::Unbound(4).rendered(), "__unbound_4");
    }

    #[test]
    fn of_hit_prefers_the_graph_node_name() {
        let s = Surrogate::new(5);
        assert_eq!(RagKey::of_hit(Some("a"), s), RagKey::Node("a".into()));
        assert_eq!(RagKey::of_hit(None, s), RagKey::Row(s));
    }
}
