// SPDX-License-Identifier: BUSL-1.1

//! Length-bounded ILP line reading.
//!
//! `AsyncBufReadExt::read_until` grows its output without limit while it hunts
//! for a delimiter, so a client that never sends a newline can drive one
//! connection into unbounded allocation. This reader enforces the cap against
//! the bytes it is about to copy, before copying them.

use tokio::io::{AsyncBufRead, AsyncBufReadExt};

/// Read one ILP line without allowing `BufReader` to allocate beyond the
/// configured line limit while it searches for a newline.
///
/// Returns `Ok(true)` when a complete line (or a final unterminated one) is in
/// `line_buf`, and `Ok(false)` at a clean EOF with nothing buffered.
///
/// Cancel-safe: the only await is `fill_buf`, and nothing is copied out of the
/// reader or consumed from it until after that await has returned, so being
/// dropped inside a `select!` can neither lose nor duplicate bytes.
pub(super) async fn read_bounded_ilp_line<R>(
    reader: &mut R,
    line_buf: &mut Vec<u8>,
    max_line_bytes: usize,
) -> std::io::Result<bool>
where
    R: AsyncBufRead + Unpin,
{
    loop {
        let (consumed, complete) = {
            let available = reader.fill_buf().await?;
            if available.is_empty() {
                return Ok(!line_buf.is_empty());
            }
            let consumed = available
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(available.len(), |position| position + 1);
            if consumed > max_line_bytes.saturating_sub(line_buf.len()) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "ILP line exceeds maximum length",
                ));
            }
            line_buf.extend_from_slice(&available[..consumed]);
            (
                consumed,
                consumed < available.len() || available[consumed - 1] == b'\n',
            )
        };
        reader.consume(consumed);
        if complete {
            return Ok(true);
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::BufReader;

    use super::read_bounded_ilp_line;

    #[tokio::test]
    async fn bounded_line_reader_rejects_before_copying_an_oversized_line() {
        let mut reader = BufReader::new(std::io::Cursor::new(b"12345\n".to_vec()));
        let mut line = Vec::new();

        let result = read_bounded_ilp_line(&mut reader, &mut line, 4).await;

        assert!(result.is_err());
        assert!(line.is_empty());
    }

    #[tokio::test]
    async fn clean_eof_with_nothing_buffered_reports_no_line() {
        let mut reader = BufReader::new(std::io::Cursor::new(Vec::new()));
        let mut line = Vec::new();

        let read = read_bounded_ilp_line(&mut reader, &mut line, 64)
            .await
            .expect("EOF is not an error");

        assert!(!read);
    }

    #[tokio::test]
    async fn final_unterminated_line_is_still_delivered() {
        let mut reader = BufReader::new(std::io::Cursor::new(b"cpu value=1i".to_vec()));
        let mut line = Vec::new();

        let read = read_bounded_ilp_line(&mut reader, &mut line, 64)
            .await
            .expect("an unterminated final line is not an error");

        assert!(read);
        assert_eq!(line, b"cpu value=1i");
    }
}
