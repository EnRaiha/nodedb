// SPDX-License-Identifier: BUSL-1.1

//! Walking a collection's hash chain in install order, from genesis.
//!
//! Storage yields rows in key order, and key order is not install order. The
//! walk therefore takes two passes over the rows:
//!
//! 1. [`ChainWalk::index`] records each row's position (`_chain_seq`) and
//!    stored link.
//! 2. [`ChainWalk::check`] recomputes each row's link from its predecessor's
//!    stored link, its position, and its own canonical contents.
//!
//! [`ChainWalk::finish`] then checks that every position from 1 to the
//! collection's head is held, and that the last link equals the head. The
//! verdict names the first break in install order. Memory is one position
//! entry per row. Row bodies are never held.

use std::collections::BTreeMap;

use crate::types::hash_chain::{ChainBreak, ChainHead, ChainVerdict, GENESIS_HASH};

use super::hash_chain::compute_chain_hash;

/// Verifies one collection's chain against its persisted head.
pub(in crate::data::executor) struct ChainWalk {
    head: ChainHead,
    /// Position → (row id, stored link).
    links: BTreeMap<u64, (String, String)>,
    /// The earliest break found so far.
    first: Option<ChainBreak>,
}

impl ChainWalk {
    pub(in crate::data::executor) fn new(head: ChainHead) -> Self {
        Self {
            head,
            links: BTreeMap::new(),
            first: None,
        }
    }

    /// Pass one: record where `row_id` sits in the chain.
    ///
    /// A row with no link, a position outside `1..=head`, or a position
    /// another row already holds is a break. A row outside the chain is
    /// reported at the position after the head.
    pub(in crate::data::executor) fn index(&mut self, row_id: &str, link: Option<&ChainHead>) {
        let Some(link) = link else {
            self.note(ChainBreak {
                index: self.head.seq,
                document_id: Some(row_id.to_string()),
                expected: None,
                found: None,
            });
            return;
        };
        if link.seq == 0 || link.seq > self.head.seq {
            self.note(ChainBreak {
                index: link.seq.min(self.head.seq),
                document_id: Some(row_id.to_string()),
                expected: None,
                found: Some(link.hash.clone()),
            });
            return;
        }
        if self.links.contains_key(&link.seq) {
            self.note(ChainBreak {
                index: link.seq - 1,
                document_id: Some(row_id.to_string()),
                expected: None,
                found: Some(link.hash.clone()),
            });
            return;
        }
        self.links
            .insert(link.seq, (row_id.to_string(), link.hash.clone()));
    }

    /// Pass two: recompute the link of a row pass one placed.
    ///
    /// `contents` are the row's canonical contents. A row whose predecessor
    /// is missing is skipped here: [`Self::finish`] reports the gap.
    pub(in crate::data::executor) fn check(
        &mut self,
        row_id: &str,
        link: &ChainHead,
        contents: &[u8],
    ) {
        match self.links.get(&link.seq) {
            Some((placed, _)) if placed == row_id => {}
            _ => return,
        }
        let prev = if link.seq == 1 {
            GENESIS_HASH
        } else {
            match self.links.get(&(link.seq - 1)) {
                Some((_, hash)) => hash.as_str(),
                None => return,
            }
        };
        let expected = compute_chain_hash(prev, link.seq, contents);
        if expected != link.hash {
            self.note(ChainBreak {
                index: link.seq - 1,
                document_id: Some(row_id.to_string()),
                expected: Some(expected),
                found: Some(link.hash.clone()),
            });
        }
    }

    /// The verdict after both passes.
    pub(in crate::data::executor) fn finish(mut self) -> ChainVerdict {
        if let Some(missing) = (1..=self.head.seq).find(|seq| !self.links.contains_key(seq)) {
            self.note(ChainBreak {
                index: missing - 1,
                document_id: None,
                expected: None,
                found: None,
            });
        } else if let Some((row_id, stored)) = self.links.get(&self.head.seq)
            && *stored != self.head.hash
        {
            // Every link recomputes, yet the last one is not the head: the
            // chain was rewritten from some row onward.
            let brk = ChainBreak {
                index: self.head.seq - 1,
                document_id: Some(row_id.clone()),
                expected: Some(self.head.hash.clone()),
                found: Some(stored.clone()),
            };
            self.note(brk);
        }
        let entries = self
            .first
            .as_ref()
            .map_or(self.head.seq, |brk| brk.index.min(self.head.seq));
        let last_hash = self
            .links
            .get(&entries)
            .map_or_else(|| GENESIS_HASH.to_string(), |(_, hash)| hash.clone());
        ChainVerdict {
            entries,
            last_hash,
            broken: self.first,
        }
    }

    /// Keep `brk` when it precedes every break found so far.
    fn note(&mut self, brk: ChainBreak) {
        if self
            .first
            .as_ref()
            .is_none_or(|first| brk.index < first.index)
        {
            self.first = Some(brk);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `(row id, contents)` in install order.
    fn rows() -> Vec<(String, Vec<u8>)> {
        (1..=4)
            .map(|i| (format!("row-{i}"), format!("contents {i}").into_bytes()))
            .collect()
    }

    /// The links a chain over `rows` stores, and its head.
    fn link(rows: &[(String, Vec<u8>)]) -> (Vec<ChainHead>, ChainHead) {
        let mut head = ChainHead::genesis();
        let mut links = Vec::new();
        for (_, contents) in rows {
            head = head.next(compute_chain_hash(&head.hash, head.seq + 1, contents));
            links.push(head.clone());
        }
        (links, head)
    }

    /// Walk `rows` in reverse key order, which differs from install order.
    fn walk(
        head: ChainHead,
        rows: &[(String, Vec<u8>)],
        links: &[Option<ChainHead>],
    ) -> ChainVerdict {
        let mut walk = ChainWalk::new(head);
        for ((id, _), link) in rows.iter().zip(links).rev() {
            walk.index(id, link.as_ref());
        }
        for ((id, contents), link) in rows.iter().zip(links).rev() {
            if let Some(link) = link {
                walk.check(id, link, contents);
            }
        }
        walk.finish()
    }

    #[test]
    fn an_intact_chain_verifies_in_install_order() {
        let rows = rows();
        let (links, head) = link(&rows);
        let links: Vec<Option<ChainHead>> = links.into_iter().map(Some).collect();
        let verdict = walk(head.clone(), &rows, &links);
        assert_eq!(verdict.broken, None);
        assert_eq!(verdict.entries, 4);
        assert_eq!(verdict.last_hash, head.hash);
    }

    #[test]
    fn a_tampered_body_breaks_at_its_row() {
        let mut rows = rows();
        let (links, head) = link(&rows);
        let links: Vec<Option<ChainHead>> = links.into_iter().map(Some).collect();
        rows[1].1 = b"tampered".to_vec();
        let verdict = walk(head, &rows, &links);
        let brk = verdict
            .broken
            .expect("a tampered body must break the chain");
        assert_eq!(brk.index, 1);
        assert_eq!(brk.document_id.as_deref(), Some("row-2"));
        assert_eq!(brk.found, links[1].as_ref().map(|l| l.hash.clone()));
        assert_ne!(brk.expected, brk.found);
        assert_eq!(verdict.entries, 1);
    }

    #[test]
    fn a_tampered_link_breaks_at_its_row() {
        let rows = rows();
        let (links, head) = link(&rows);
        let mut links: Vec<Option<ChainHead>> = links.into_iter().map(Some).collect();
        if let Some(link) = links[2].as_mut() {
            link.hash = "f".repeat(64);
        }
        let verdict = walk(head, &rows, &links);
        let brk = verdict
            .broken
            .expect("a tampered link must break the chain");
        assert_eq!(brk.index, 2);
        assert_eq!(brk.document_id.as_deref(), Some("row-3"));
        assert_eq!(brk.found, Some("f".repeat(64)));
    }

    #[test]
    fn a_missing_row_breaks_at_its_position() {
        let rows = rows();
        let (links, head) = link(&rows);
        let links: Vec<Option<ChainHead>> = links.into_iter().map(Some).collect();
        let kept: Vec<usize> = vec![0, 2, 3];
        let verdict = walk(
            head,
            &kept.iter().map(|&i| rows[i].clone()).collect::<Vec<_>>(),
            &kept.iter().map(|&i| links[i].clone()).collect::<Vec<_>>(),
        );
        let brk = verdict.broken.expect("a missing row must break the chain");
        assert_eq!(brk.index, 1);
        assert_eq!(brk.document_id, None);
        assert_eq!(verdict.entries, 1);
    }

    #[test]
    fn a_missing_last_row_breaks_against_the_head() {
        let rows = rows();
        let (links, head) = link(&rows);
        let links: Vec<Option<ChainHead>> = links.into_iter().map(Some).collect();
        let verdict = walk(head, &rows[..3], &links[..3]);
        let brk = verdict
            .broken
            .expect("a truncated tail must break the chain");
        assert_eq!(brk.index, 3);
        assert_eq!(verdict.entries, 3);
    }

    #[test]
    fn a_row_without_a_link_is_a_break() {
        let rows = rows();
        let (links, head) = link(&rows);
        let mut links: Vec<Option<ChainHead>> = links.into_iter().map(Some).collect();
        links.push(None);
        let mut with_extra = rows.clone();
        with_extra.push(("row-x".to_string(), b"unlinked".to_vec()));
        let verdict = walk(head, &with_extra, &links);
        let brk = verdict
            .broken
            .expect("an unlinked row must break the chain");
        assert_eq!(brk.document_id.as_deref(), Some("row-x"));
    }

    #[test]
    fn an_empty_chain_is_valid() {
        let verdict = walk(ChainHead::genesis(), &[], &[]);
        assert_eq!(verdict.broken, None);
        assert_eq!(verdict.entries, 0);
        assert_eq!(verdict.last_hash, GENESIS_HASH);
    }
}
