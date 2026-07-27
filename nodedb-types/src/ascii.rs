// SPDX-License-Identifier: Apache-2.0

//! Byte-stable ASCII matching for untrusted text.
//!
//! These helpers never allocate or case-transform the input. They compare only
//! ASCII needles, so every returned offset and suffix addresses the unchanged
//! original UTF-8 string safely. Non-ASCII needles are deliberately rejected:
//! Unicode case folding can expand one character into multiple bytes and must
//! not be used to calculate byte offsets into the original input.

/// Return whether `text` starts with the ASCII `prefix`, ignoring ASCII case.
///
/// An empty prefix matches. Non-ASCII prefixes never match.
pub fn starts_with_ascii_case_insensitive(text: &str, prefix: &str) -> bool {
    prefix.is_ascii()
        && text.len() >= prefix.len()
        && text.as_bytes()[..prefix.len()]
            .iter()
            .zip(prefix.as_bytes())
            .all(|(left, right)| left.eq_ignore_ascii_case(right))
}

/// Strip an ASCII `prefix` from `text`, ignoring ASCII case.
///
/// The returned suffix is always a slice of the unchanged original input. An
/// empty prefix returns the original string. Non-ASCII prefixes return `None`.
pub fn strip_prefix_ascii_case_insensitive<'a>(text: &'a str, prefix: &str) -> Option<&'a str> {
    starts_with_ascii_case_insensitive(text, prefix).then(|| &text[prefix.len()..])
}

/// Find an ASCII `needle` in `text` without allocating a case-transformed
/// copy. The returned byte offset always belongs to `text`.
pub fn find_ascii_case_insensitive(text: &str, needle: &str) -> Option<usize> {
    find_ascii_case_insensitive_from(text, needle, 0)
}

/// Find the last ASCII `needle` in `text` without allocating a case-transformed
/// copy. The returned byte offset always belongs to `text`.
pub fn rfind_ascii_case_insensitive(text: &str, needle: &str) -> Option<usize> {
    if needle.is_empty() {
        return Some(text.len());
    }
    if !needle.is_ascii() || needle.len() > text.len() {
        return None;
    }

    let haystack = text.as_bytes();
    let needle = needle.as_bytes();
    (0..=haystack.len() - needle.len()).rev().find(|&position| {
        haystack[position..position + needle.len()]
            .iter()
            .zip(needle)
            .all(|(left, right)| left.eq_ignore_ascii_case(right))
    })
}

/// Find an ASCII `needle` at or after byte offset `start` in `text`.
///
/// `start` need not be a UTF-8 character boundary because matching is done on
/// bytes. A successful offset always starts at an ASCII byte and is therefore
/// a UTF-8 boundary in the unchanged original input.
pub fn find_ascii_case_insensitive_from(text: &str, needle: &str, start: usize) -> Option<usize> {
    if needle.is_empty() {
        return (start <= text.len()).then_some(start);
    }
    if !needle.is_ascii() || start > text.len() || needle.len() > text.len() - start {
        return None;
    }

    let haystack = text.as_bytes();
    let needle = needle.as_bytes();
    (start..=haystack.len() - needle.len()).find(|&position| {
        haystack[position..position + needle.len()]
            .iter()
            .zip(needle)
            .all(|(left, right)| left.eq_ignore_ascii_case(right))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_ascii_without_mutating_unicode_input() {
        let text = "ßSELECT ﬀ from İstanbul";
        let select = find_ascii_case_insensitive(text, "select").expect("ASCII match");
        let from = rfind_ascii_case_insensitive(text, "FROM").expect("ASCII match");
        assert_eq!(select, 2);
        assert_eq!(from, 13);
        assert_eq!(&text[select..select + 6], "SELECT");
        assert_eq!(&text[from..from + 4], "from");
        assert_eq!(find_ascii_case_insensitive(text, "istanbul"), None);
        assert_eq!(find_ascii_case_insensitive(text, "ssselect"), None);
        assert_eq!(find_ascii_case_insensitive(text, "ff"), None);
    }

    #[test]
    fn rejects_non_ascii_needles() {
        let text = "SELECT Straße";
        assert!(!starts_with_ascii_case_insensitive(text, "ŚELECT"));
        assert_eq!(strip_prefix_ascii_case_insensitive(text, "ŚELECT"), None);
        assert_eq!(find_ascii_case_insensitive(text, "ß"), None);
        assert_eq!(find_ascii_case_insensitive_from(text, "İ", 0), None);
        assert_eq!(rfind_ascii_case_insensitive(text, "ﬀ"), None);
    }

    #[test]
    fn prefix_helpers_return_original_suffix() {
        let text = "SeLeCt ß";
        assert!(starts_with_ascii_case_insensitive(text, "select"));
        assert_eq!(
            strip_prefix_ascii_case_insensitive(text, "SELECT"),
            Some(" ß")
        );
        assert_eq!(strip_prefix_ascii_case_insensitive(text, ""), Some(text));
        assert!(starts_with_ascii_case_insensitive(text, ""));
        assert!(!starts_with_ascii_case_insensitive(text, "select x"));
    }

    #[test]
    fn empty_and_start_bounds_are_conventional() {
        let text = "éX";
        assert_eq!(find_ascii_case_insensitive(text, ""), Some(0));
        assert_eq!(rfind_ascii_case_insensitive(text, ""), Some(text.len()));
        assert_eq!(find_ascii_case_insensitive_from(text, "", 1), Some(1));
        assert_eq!(
            find_ascii_case_insensitive_from(text, "", text.len()),
            Some(text.len())
        );
        assert_eq!(
            find_ascii_case_insensitive_from(text, "", text.len() + 1),
            None
        );
        assert_eq!(find_ascii_case_insensitive_from(text, "x", 1), Some(2));
        assert_eq!(
            find_ascii_case_insensitive_from(text, "x", text.len()),
            None
        );
    }
}
