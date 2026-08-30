// SPDX-License-Identifier: Apache-2.0

//! Sequential block-at-a-time decoding without header re-scans.

use crate::error::CodecError;

use super::block::decode_block;
use super::header::{GLOBAL_HEADER_SIZE, expected_block_count, parse_header, validate_frame};

/// Iterator that decodes one 1024-row block at a time, tracking byte
/// offsets internally. Avoids re-scanning headers for sequential access.
pub struct BlockIterator<'a> {
    data: &'a [u8],
    offset: usize,
    total_count: usize,
    blocks_remaining: usize,
    current_block: usize,
}

impl<'a> BlockIterator<'a> {
    /// Create a block iterator over encoded FastLanes data.
    pub fn new(data: &'a [u8]) -> Result<Self, CodecError> {
        if data.len() < GLOBAL_HEADER_SIZE {
            return Err(CodecError::Truncated {
                expected: GLOBAL_HEADER_SIZE,
                actual: data.len(),
            });
        }
        let (total_count, num_blocks) = parse_header(data)?;
        validate_frame(data, total_count, num_blocks)?;
        Ok(Self {
            data,
            offset: GLOBAL_HEADER_SIZE,
            total_count,
            blocks_remaining: num_blocks,
            current_block: 0,
        })
    }

    /// Skip the next block without decoding it.
    pub fn skip_block(&mut self) -> Result<(), CodecError> {
        if self.blocks_remaining == 0 {
            return Ok(());
        }
        self.offset = super::block::skip_block(
            self.data,
            self.offset,
            self.current_block,
            expected_block_count(self.total_count, self.current_block)?,
        )?;
        self.current_block += 1;
        self.blocks_remaining -= 1;
        Ok(())
    }
}

impl Iterator for BlockIterator<'_> {
    type Item = Result<Vec<i64>, CodecError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.blocks_remaining == 0 {
            return None;
        }
        let expected_count = match expected_block_count(self.total_count, self.current_block) {
            Ok(count) => count,
            Err(error) => return Some(Err(error)),
        };
        let mut values = Vec::with_capacity(expected_count);
        match decode_block(
            self.data,
            self.offset,
            &mut values,
            self.current_block,
            expected_count,
        ) {
            Ok(new_offset) => {
                self.offset = new_offset;
                self.current_block += 1;
                self.blocks_remaining -= 1;
                Some(Ok(values))
            }
            Err(error) => {
                self.blocks_remaining = 0;
                Some(Err(error))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.blocks_remaining, Some(self.blocks_remaining))
    }
}
