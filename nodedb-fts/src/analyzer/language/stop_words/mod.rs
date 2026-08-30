// SPDX-License-Identifier: Apache-2.0

//! Per-language stop word lists with O(log n) binary search lookup.
//!
//! All lists are compiled-in `static` sorted arrays (~50KB total).
//! Dispatch by ISO 639-1 code or full language name.

mod asian;
mod eastern_european;
mod european;
mod lookup;
mod semitic;

pub use lookup::{is_stop_word, is_stop_word_en, stop_words};
