// SPDX-License-Identifier: Apache-2.0

//! Dictionary-based segmentation dispatch.
//!
//! Uses dictionary segmentation for Japanese and Korean (lindera), ICU for
//! Thai (icu_segmenter), and CJK bigrams for Chinese (permanent fallback —
//! the previous jieba-rs dictionary chain depended on yanked crates).
//! All paths are compiled in unconditionally; selection is at runtime.
//!
//! lindera reads a built dictionary directory. `NODEDB_FTS_IPADIC_DIR` names
//! the Japanese one and `NODEDB_FTS_KO_DIC_DIR` the Korean one. Without it, a
//! language falls back to CJK bigrams.

use std::borrow::Cow;
use std::sync::OnceLock;

use icu_segmenter::WordSegmenter;
use icu_segmenter::options::WordBreakInvariantOptions;
use lindera::mode::Mode;
use lindera::segmenter::Segmenter;

use super::bigram::tokenize_cjk;

/// Segment text using the best available method for the given language.
///
/// Falls back to CJK bigrams if no dictionary is available.
pub fn segment(text: &str, lang: &str) -> Vec<String> {
    match lang {
        "ja" | "japanese" => segment_japanese(text),
        "zh" | "chinese" => segment_chinese(text),
        "ko" | "korean" => segment_korean(text),
        "th" | "thai" => segment_thai(text),
        _ => tokenize_cjk(text),
    }
}

/// Japanese segmentation: lindera with IPADIC, or bigrams without it.
fn segment_japanese(text: &str) -> Vec<String> {
    lindera_segment(text, &IPADIC)
}

/// Chinese segmentation: CJK bigrams (dictionary segmentation temporarily disabled).
fn segment_chinese(text: &str) -> Vec<String> {
    tokenize_cjk(text)
}

/// Korean segmentation: lindera with ko-dic, or bigrams without it.
fn segment_korean(text: &str) -> Vec<String> {
    lindera_segment(text, &KO_DIC)
}

/// Thai segmentation: icu_segmenter.
fn segment_thai(text: &str) -> Vec<String> {
    icu_segment_thai(text)
}

// ── Implementations ──────────────────────────────────────────────────────────

/// A lindera dictionary, loaded once from the directory its env var names.
struct LinderaDictionary {
    env_var: &'static str,
    segmenter: OnceLock<Option<Segmenter>>,
}

impl LinderaDictionary {
    const fn new(env_var: &'static str) -> Self {
        Self {
            env_var,
            segmenter: OnceLock::new(),
        }
    }

    /// The segmenter over this dictionary, or `None` when the env var is unset
    /// or the dictionary does not load.
    fn segmenter(&self) -> Option<&Segmenter> {
        self.segmenter
            .get_or_init(|| {
                let path = std::env::var(self.env_var).ok()?;
                let dictionary = lindera::dictionary::load_dictionary(&path).ok()?;
                Some(Segmenter::new(Mode::Normal, dictionary, None))
            })
            .as_ref()
    }
}

/// Built IPADIC dictionary directory for Japanese.
static IPADIC: LinderaDictionary = LinderaDictionary::new("NODEDB_FTS_IPADIC_DIR");

/// Built ko-dic dictionary directory for Korean.
static KO_DIC: LinderaDictionary = LinderaDictionary::new("NODEDB_FTS_KO_DIC_DIR");

fn lindera_segment(text: &str, dictionary: &LinderaDictionary) -> Vec<String> {
    let Some(segmenter) = dictionary.segmenter() else {
        return tokenize_cjk(text);
    };
    let Ok(tokens) = segmenter.segment(Cow::Borrowed(text)) else {
        return tokenize_cjk(text);
    };
    tokens
        .into_iter()
        .map(|t| t.surface.to_string())
        .filter(|t: &String| t.len() > 1 || t.chars().next().is_some_and(super::script::is_cjk))
        .collect()
}

fn icu_segment_thai(text: &str) -> Vec<String> {
    let segmenter = WordSegmenter::new_auto(WordBreakInvariantOptions::default());
    let breakpoints: Vec<usize> = segmenter.segment_str(text).collect();
    let mut words = Vec::new();
    for window in breakpoints.windows(2) {
        let word = &text[window[0]..window[1]];
        if !word.trim().is_empty() {
            words.push(word.to_string());
        }
    }
    words
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bigrams_chinese() {
        let tokens = segment("全文検索", "zh");
        assert_eq!(tokens, vec!["全文", "文検", "検索"]);
    }

    #[test]
    fn dictionary_segmentation_japanese() {
        let tokens = segment("東京タワー", "ja");
        assert!(!tokens.is_empty());
    }

    #[test]
    fn dictionary_segmentation_korean() {
        let tokens = segment("한국어", "ko");
        assert!(!tokens.is_empty());
    }

    #[test]
    fn unknown_lang_fallback() {
        let tokens = segment("全文検索", "unknown");
        assert_eq!(tokens, vec!["全文", "文検", "検索"]);
    }
}
