// SPDX-License-Identifier: Apache-2.0

//! Highlight and match offset utilities for search results.

use crate::analyzer::pipeline::analyze;
use crate::backend::FtsBackend;
use crate::index::FtsIndex;
use crate::posting::MatchOffset;

impl<B: FtsBackend> FtsIndex<B> {
    /// Generate highlighted text with matched query terms wrapped in tags.
    ///
    /// Returns the original text with each occurrence of a matched query term
    /// surrounded by `prefix` and `suffix` (e.g., `<b>` and `</b>`).
    pub fn highlight(&self, text: &str, query: &str, prefix: &str, suffix: &str) -> String {
        let matches = self.find_query_matches(text, query);
        if matches.is_empty() {
            return text.to_string();
        }

        let mut result =
            String::with_capacity(text.len() + matches.len() * (prefix.len() + suffix.len()) * 2);
        let mut last_end = 0;

        for m in &matches {
            result.push_str(&text[last_end..m.start]);
            result.push_str(prefix);
            result.push_str(&text[m.start..m.end]);
            result.push_str(suffix);
            last_end = m.end;
        }
        result.push_str(&text[last_end..]);
        result
    }

    /// Return byte offsets of matched query terms in the original text.
    pub fn offsets(&self, text: &str, query: &str) -> Vec<MatchOffset> {
        self.find_query_matches(text, query)
    }

    /// Find all query term matches in `text`, returning byte offsets and stemmed terms.
    fn find_query_matches(&self, text: &str, query: &str) -> Vec<MatchOffset> {
        let query_tokens = analyze(query);
        if query_tokens.is_empty() {
            return Vec::new();
        }

        let query_set: std::collections::HashSet<&str> =
            query_tokens.iter().map(String::as_str).collect();
        let stemmer = rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::English);

        let mut matches = Vec::new();
        for (start, word) in WordBoundaryIter::new(text) {
            let lower = word.to_lowercase();
            let stemmed = stemmer.stem(&lower);
            if query_set.contains(stemmed.as_ref()) {
                matches.push(MatchOffset {
                    start,
                    end: start + word.len(),
                    term: stemmed.into_owned(),
                });
            }
        }
        matches
    }
}

/// Iterator over word boundaries in text, yielding `(byte_offset, &str)` pairs.
///
/// Words are sequences of alphanumeric chars, hyphens, and underscores.
struct WordBoundaryIter<'a> {
    text: &'a str,
    pos: usize,
}

impl<'a> WordBoundaryIter<'a> {
    fn new(text: &'a str) -> Self {
        Self { text, pos: 0 }
    }
}

impl<'a> Iterator for WordBoundaryIter<'a> {
    type Item = (usize, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        let bytes = self.text.as_bytes();
        // Skip non-word characters.
        while self.pos < bytes.len() {
            let c = self.text[self.pos..].chars().next()?;
            if c.is_alphanumeric() || c == '-' || c == '_' {
                break;
            }
            self.pos += c.len_utf8();
        }
        if self.pos >= bytes.len() {
            return None;
        }

        let start = self.pos;
        // Consume word characters.
        while self.pos < bytes.len() {
            let Some(c) = self.text[self.pos..].chars().next() else {
                break;
            };
            if c.is_alphanumeric() || c == '-' || c == '_' {
                self.pos += c.len_utf8();
            } else {
                break;
            }
        }
        let word = &self.text[start..self.pos];
        let trimmed_start = start + word.len() - word.trim_start_matches(['-', '_']).len();
        let trimmed_end = trimmed_start + word.trim_matches(['-', '_']).len();
        if trimmed_start >= trimmed_end {
            return self.next();
        }
        Some((trimmed_start, &self.text[trimmed_start..trimmed_end]))
    }
}

#[cfg(test)]
mod tests {
    use crate::backend::memory::MemoryBackend;
    use crate::index::FtsIndex;
    use crate::test_support::test_governor;

    #[test]
    fn highlight_basic() {
        let idx: FtsIndex<MemoryBackend> = FtsIndex::new(MemoryBackend::new(), test_governor());
        let result = idx.highlight(
            "The quick brown fox jumps over the lazy dog",
            "brown fox",
            "<b>",
            "</b>",
        );
        assert!(result.contains("<b>brown</b>"));
        assert!(result.contains("<b>fox</b>"));
    }

    #[test]
    fn highlight_no_match() {
        let idx: FtsIndex<MemoryBackend> = FtsIndex::new(MemoryBackend::new(), test_governor());
        let text = "hello world";
        let result = idx.highlight(text, "xyz", "<b>", "</b>");
        assert_eq!(result, text);
    }

    #[test]
    fn offsets_basic() {
        let idx: FtsIndex<MemoryBackend> = FtsIndex::new(MemoryBackend::new(), test_governor());
        let offsets = idx.offsets("The quick brown fox", "brown");
        assert_eq!(offsets.len(), 1);
        assert_eq!(offsets[0].start, 10);
        assert_eq!(offsets[0].end, 15);
    }

    #[test]
    fn word_boundary_iter() {
        let iter = super::WordBoundaryIter::new("hello, world! foo-bar");
        let words: Vec<_> = iter.collect();
        assert!(words.iter().any(|(_, w)| *w == "hello"));
        assert!(words.iter().any(|(_, w)| *w == "world"));
        assert!(words.iter().any(|(_, w)| *w == "foo-bar"));
    }
}
