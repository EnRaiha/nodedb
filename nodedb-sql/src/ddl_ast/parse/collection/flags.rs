// SPDX-License-Identifier: Apache-2.0

//! Modifier flags of `CREATE COLLECTION` / `CREATE TABLE`.
//!
//! A flag is set by its bare keyword at a clause position, by `FLAG = <value>`
//! there, or by a `flag = <value>` WITH option. An assigned flag is set only
//! for a true value (`true`, `1`, `on`, `yes`). The body reader works on
//! tokens, so a column named `append_only`, an identifier such as
//! `is_append_only`, a quoted identifier, or a literal sets nothing.

/// Every recognised flag, in the order the parsed list reports them.
pub(super) const FLAGS: [&str; 4] = ["APPEND_ONLY", "HASH_CHAIN", "BITEMPORAL", "SIGNED_DELTAS"];

/// The position of `word` in `FLAGS`, compared case-insensitively.
pub(super) fn flag_index(word: &str) -> Option<usize> {
    FLAGS.iter().position(|f| word.eq_ignore_ascii_case(f))
}

/// Whether an assigned flag value turns the flag on.
pub(super) fn is_truthy(value: &str) -> bool {
    ["true", "1", "on", "yes"]
        .iter()
        .any(|truthy| value.trim().eq_ignore_ascii_case(truthy))
}

/// The names of the set flags, in `FLAGS` order.
pub(super) fn flag_names(set: [bool; FLAGS.len()]) -> Vec<String> {
    FLAGS
        .iter()
        .zip(set)
        .filter(|(_, enabled)| *enabled)
        .map(|(flag, _)| (*flag).to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flag_words_match_whole_and_case_insensitive() {
        assert_eq!(flag_index("append_only"), Some(0));
        assert_eq!(flag_index("Signed_Deltas"), Some(3));
        assert_eq!(flag_index("hash_chains"), None);
    }

    #[test]
    fn only_true_values_set_an_assigned_flag() {
        for v in ["true", "TRUE", "1", "on", "yes"] {
            assert!(is_truthy(v), "{v}");
        }
        for v in ["false", "no", "0", "maybe"] {
            assert!(!is_truthy(v), "{v}");
        }
    }

    #[test]
    fn names_follow_flag_order() {
        assert_eq!(
            flag_names([false, true, true, false]),
            vec!["HASH_CHAIN", "BITEMPORAL"]
        );
    }
}
