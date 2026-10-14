// SPDX-License-Identifier: BUSL-1.1

//! Label matchers the Data Plane can evaluate during the scan.
//!
//! A label is a string column, and a series without the label stores NULL
//! there. Prometheus treats an absent label as `""`, so only a matcher that
//! never matches `""` keeps its meaning under SQL NULL semantics:
//!
//! - `l="v"` with `v` non-empty becomes `Eq`.
//! - `l=~"a|b|c"`, where every alternative is a non-empty literal, becomes
//!   `In`. Grafana template variables produce this form.
//!
//! Every other matcher runs on the Control Plane after the scan. The Control
//! Plane also re-applies the pushed matchers, so a pushdown only narrows the
//! rows the scan returns and never decides the answer alone.

use crate::bridge::scan_filter::{FilterOp, ScanFilter};
use crate::control::promql::{LabelMatchOp, LabelMatcher};

/// Characters that make a regex alternative more than a literal.
const REGEX_META: &[char] = &[
    '\\', '.', '+', '*', '?', '(', ')', '[', ']', '{', '}', '^', '$',
];

/// The scan filters `matchers` push down. `__name__` matchers select the
/// collection and never reach a scan.
pub(crate) fn pushdown_filters(matchers: &[LabelMatcher]) -> Vec<ScanFilter> {
    matchers
        .iter()
        .filter(|m| m.name != "__name__")
        .filter_map(|m| match m.op {
            LabelMatchOp::Equal if !m.value.is_empty() => Some(filter(
                &m.name,
                FilterOp::Eq,
                nodedb_types::Value::String(m.value.clone()),
            )),
            LabelMatchOp::RegexMatch => literal_alternation(&m.value).map(|alts| {
                filter(
                    &m.name,
                    FilterOp::In,
                    nodedb_types::Value::Array(
                        alts.into_iter().map(nodedb_types::Value::String).collect(),
                    ),
                )
            }),
            LabelMatchOp::Equal | LabelMatchOp::NotEqual | LabelMatchOp::RegexNotMatch => None,
        })
        .collect()
}

/// Encode `filters` as the scan's MessagePack filter payload.
pub(crate) fn encode_filters(filters: &[ScanFilter]) -> crate::Result<Vec<u8>> {
    if filters.is_empty() {
        return Ok(Vec::new());
    }
    zerompk::to_msgpack_vec(&filters.to_vec()).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("promql scan filter serialization: {e}"),
    })
}

/// The alternatives of `pattern` when it is a plain `a|b|c` of non-empty
/// literals. Prometheus anchors the regex, so such a pattern matches exactly
/// one of its alternatives.
fn literal_alternation(pattern: &str) -> Option<Vec<String>> {
    let alts: Vec<String> = pattern.split('|').map(str::to_owned).collect();
    alts.iter()
        .all(|a| !a.is_empty() && !a.contains(REGEX_META))
        .then_some(alts)
}

fn filter(field: &str, op: FilterOp, value: nodedb_types::Value) -> ScanFilter {
    ScanFilter {
        field: field.to_owned(),
        op,
        value,
        clauses: Vec::new(),
        expr: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(name: &str, op: LabelMatchOp, value: &str) -> LabelMatcher {
        LabelMatcher::new(name.into(), op, value.into()).unwrap()
    }

    #[test]
    fn non_empty_equality_pushes_down() {
        let f = pushdown_filters(&[m("job", LabelMatchOp::Equal, "api")]);
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].field, "job");
        assert_eq!(f[0].op, FilterOp::Eq);
    }

    #[test]
    fn empty_equality_stays_on_the_control_plane() {
        assert!(pushdown_filters(&[m("env", LabelMatchOp::Equal, "")]).is_empty());
    }

    #[test]
    fn literal_alternation_pushes_down_as_in() {
        let f = pushdown_filters(&[m("job", LabelMatchOp::RegexMatch, "api|web")]);
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].op, FilterOp::In);
        assert_eq!(
            f[0].value,
            nodedb_types::Value::Array(vec![
                nodedb_types::Value::String("api".into()),
                nodedb_types::Value::String("web".into()),
            ])
        );
    }

    #[test]
    fn real_regex_and_negations_stay_on_the_control_plane() {
        let f = pushdown_filters(&[
            m("job", LabelMatchOp::RegexMatch, "a.*"),
            m("job", LabelMatchOp::RegexMatch, "api|"),
            m("job", LabelMatchOp::NotEqual, "api"),
            m("job", LabelMatchOp::RegexNotMatch, "api"),
        ]);
        assert!(f.is_empty());
    }

    #[test]
    fn name_matchers_never_push_down() {
        assert!(pushdown_filters(&[m("__name__", LabelMatchOp::Equal, "up")]).is_empty());
    }
}
