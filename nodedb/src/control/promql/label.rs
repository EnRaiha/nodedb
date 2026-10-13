// SPDX-License-Identifier: BUSL-1.1

//! PromQL label matchers: `=`, `!=`, `=~`, `!~`.

/// Label match operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LabelMatchOp {
    /// Exact equality (`=`).
    Equal,
    /// Exact inequality (`!=`).
    NotEqual,
    /// Regex match (`=~`).
    RegexMatch,
    /// Negated regex match (`!~`).
    RegexNotMatch,
}

/// A single label matcher: `label_name op "value"`.
#[derive(Debug, Clone)]
pub struct LabelMatcher {
    pub name: String,
    pub op: LabelMatchOp,
    pub value: String,
    /// Compiled regex (only for `=~` and `!~`).
    regex: Option<regex::Regex>,
}

impl LabelMatcher {
    /// Build a matcher. A regex that does not compile is an error, so `=~`
    /// never matches nothing and `!~` never matches everything by accident.
    pub fn new(
        name: String,
        op: LabelMatchOp,
        value: String,
    ) -> Result<Self, super::error::PromqlError> {
        let regex = match &op {
            LabelMatchOp::RegexMatch | LabelMatchOp::RegexNotMatch => {
                // Prometheus anchors regexes implicitly: ^value$
                let pattern = format!("^(?:{value})$");
                Some(regex::Regex::new(&pattern).map_err(|e| {
                    super::error::PromqlError::InvalidRegex {
                        name: name.clone(),
                        detail: e.to_string(),
                    }
                })?)
            }
            LabelMatchOp::Equal | LabelMatchOp::NotEqual => None,
        };
        Ok(Self {
            name,
            op,
            value,
            regex,
        })
    }

    /// Test whether a label value matches this matcher.
    pub fn matches(&self, actual: &str) -> bool {
        match &self.op {
            LabelMatchOp::Equal => actual == self.value,
            LabelMatchOp::NotEqual => actual != self.value,
            LabelMatchOp::RegexMatch => self.regex.as_ref().is_some_and(|r| r.is_match(actual)),
            LabelMatchOp::RegexNotMatch => self.regex.as_ref().is_none_or(|r| !r.is_match(actual)),
        }
    }

    /// Test whether a full label set matches this matcher.
    ///
    /// If the label is absent, treats it as empty string (Prometheus semantics).
    pub fn matches_labels(&self, labels: &super::types::Labels) -> bool {
        let actual = labels.get(&self.name).map_or("", |s| s.as_str());
        self.matches(actual)
    }
}

/// Check if a label set matches ALL matchers.
pub fn matches_all(matchers: &[LabelMatcher], labels: &super::types::Labels) -> bool {
    matchers.iter().all(|m| m.matches_labels(labels))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn matcher(name: &str, op: LabelMatchOp, value: &str) -> LabelMatcher {
        LabelMatcher::new(name.into(), op, value.into()).unwrap()
    }

    #[test]
    fn equal() {
        let m = matcher("job", LabelMatchOp::Equal, "api");
        assert!(m.matches("api"));
        assert!(!m.matches("web"));
    }

    #[test]
    fn not_equal() {
        let m = matcher("job", LabelMatchOp::NotEqual, "api");
        assert!(!m.matches("api"));
        assert!(m.matches("web"));
    }

    #[test]
    fn regex_match() {
        let m = matcher("job", LabelMatchOp::RegexMatch, "api.*");
        assert!(m.matches("api-server"));
        assert!(m.matches("api"));
        assert!(!m.matches("web"));
    }

    #[test]
    fn regex_not_match() {
        let m = matcher("job", LabelMatchOp::RegexNotMatch, "api.*");
        assert!(!m.matches("api-server"));
        assert!(m.matches("web"));
    }

    #[test]
    fn invalid_regex_is_an_error() {
        for op in [LabelMatchOp::RegexMatch, LabelMatchOp::RegexNotMatch] {
            let err = LabelMatcher::new("job".into(), op, "(".into()).unwrap_err();
            assert!(matches!(
                err,
                crate::control::promql::PromqlError::InvalidRegex { .. }
            ));
        }
    }

    #[test]
    fn absent_label_is_empty() {
        let m = matcher("env", LabelMatchOp::Equal, "");
        let l = labels(&[("job", "api")]);
        assert!(m.matches_labels(&l)); // absent = ""
    }

    #[test]
    fn matches_all_fn() {
        let matchers = vec![
            matcher("job", LabelMatchOp::Equal, "api"),
            matcher("env", LabelMatchOp::NotEqual, "test"),
        ];
        let l = labels(&[("job", "api"), ("env", "prod")]);
        assert!(matches_all(&matchers, &l));

        let l2 = labels(&[("job", "api"), ("env", "test")]);
        assert!(!matches_all(&matchers, &l2));
    }
}
