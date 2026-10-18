// SPDX-License-Identifier: Apache-2.0

//! A bounds pass over the IFD 0 entry table before the tiff decoder reads
//! it.
//!
//! The tiff decoder reads IFD 0 under its default limits, before a caller
//! can set its own. It reserves a tag's value list from the entry's count.
//! This pass refuses any out-of-line value that is larger than
//! [`MAX_TAG_VALUE_BYTES`] or ends past the end of the file. So no tag
//! value allocation exceeds the file's own size.
//!
//! A header or entry table the pass cannot read is left to the tiff
//! decoder, which refuses it without a large allocation.

use std::io::{self, Read, Seek, SeekFrom};

use crate::error::RasterError;
use crate::limits::MAX_TAG_VALUE_BYTES;

const CLASSIC_MAGIC: u16 = 42;
const BIG_MAGIC: u16 = 43;

#[derive(Clone, Copy)]
enum Order {
    Little,
    Big,
}

/// The fixed-width entry layout of classic TIFF or BigTIFF.
#[derive(Clone, Copy)]
struct Format {
    order: Order,
    big: bool,
}

impl Format {
    fn entry_len(self) -> u64 {
        if self.big { 20 } else { 12 }
    }

    fn inline_len(self) -> u64 {
        if self.big { 8 } else { 4 }
    }
}

/// Check every out-of-line IFD 0 value of `reader`, then rewind it.
pub(crate) fn check_ifd0<R: Read + Seek>(reader: &mut R) -> Result<(), RasterError> {
    let file_len = reader.seek(SeekFrom::End(0))?;
    reader.seek(SeekFrom::Start(0))?;
    let checked = check_entries(reader, file_len);
    reader.seek(SeekFrom::Start(0))?;
    match checked {
        // A short read means the tiff decoder refuses the same bytes.
        Err(Short::Eof) | Ok(()) => Ok(()),
        Err(Short::Refused(error)) => Err(error),
        Err(Short::Io(error)) => Err(RasterError::from(error)),
    }
}

/// Why the entry walk stopped early.
enum Short {
    Eof,
    Io(io::Error),
    Refused(RasterError),
}

impl From<io::Error> for Short {
    fn from(error: io::Error) -> Self {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            Short::Eof
        } else {
            Short::Io(error)
        }
    }
}

fn check_entries<R: Read + Seek>(reader: &mut R, file_len: u64) -> Result<(), Short> {
    let mut header = [0u8; 8];
    reader.read_exact(&mut header)?;
    let order = match &header[..2] {
        b"II" => Order::Little,
        b"MM" => Order::Big,
        _ => return Ok(()),
    };
    let magic = u16_at(order, [header[2], header[3]]);
    let (format, ifd_offset) = match magic {
        CLASSIC_MAGIC => {
            let offset = u32_at(order, [header[4], header[5], header[6], header[7]]);
            (Format { order, big: false }, u64::from(offset))
        }
        BIG_MAGIC => {
            let mut offset = [0u8; 8];
            reader.read_exact(&mut offset)?;
            (Format { order, big: true }, u64_at(order, offset))
        }
        _ => return Ok(()),
    };
    if ifd_offset >= file_len {
        return Ok(());
    }
    reader.seek(SeekFrom::Start(ifd_offset))?;
    let count = if format.big {
        let mut raw = [0u8; 8];
        reader.read_exact(&mut raw)?;
        u64_at(order, raw)
    } else {
        let mut raw = [0u8; 2];
        reader.read_exact(&mut raw)?;
        u64::from(u16_at(order, raw))
    };
    // Entries past the end of the file are a short read for the decoder.
    let room = file_len.saturating_sub(ifd_offset) / format.entry_len();
    for _ in 0..count.min(room) {
        check_entry(reader, format, file_len)?;
    }
    Ok(())
}

fn check_entry<R: Read>(reader: &mut R, format: Format, file_len: u64) -> Result<(), Short> {
    let order = format.order;
    let mut head = [0u8; 4];
    reader.read_exact(&mut head)?;
    let tag = u16_at(order, [head[0], head[1]]);
    let kind = u16_at(order, [head[2], head[3]]);
    let (count, offset) = if format.big {
        let mut rest = [0u8; 16];
        reader.read_exact(&mut rest)?;
        let mut count = [0u8; 8];
        let mut value = [0u8; 8];
        count.copy_from_slice(&rest[..8]);
        value.copy_from_slice(&rest[8..]);
        (u64_at(order, count), u64_at(order, value))
    } else {
        let mut rest = [0u8; 8];
        reader.read_exact(&mut rest)?;
        let count = u32_at(order, [rest[0], rest[1], rest[2], rest[3]]);
        let value = u32_at(order, [rest[4], rest[5], rest[6], rest[7]]);
        (u64::from(count), u64::from(value))
    };
    // The decoder skips an entry of unknown type, so this pass does too.
    let Some(width) = type_width(kind) else {
        return Ok(());
    };
    let refuse = |bytes| {
        Short::Refused(RasterError::TagValueOutOfBounds {
            tag,
            bytes,
            file_len,
        })
    };
    let bytes = count.checked_mul(width).ok_or(refuse(u64::MAX))?;
    if bytes <= format.inline_len() {
        return Ok(());
    }
    let end = offset.checked_add(bytes).ok_or(refuse(bytes))?;
    if bytes > MAX_TAG_VALUE_BYTES as u64 || end > file_len {
        return Err(refuse(bytes));
    }
    Ok(())
}

/// Bytes per value of TIFF field type `kind`, or `None` for an unknown
/// type.
fn type_width(kind: u16) -> Option<u64> {
    match kind {
        // BYTE, ASCII, SBYTE, UNDEFINED.
        1 | 2 | 6 | 7 => Some(1),
        // SHORT, SSHORT.
        3 | 8 => Some(2),
        // LONG, SLONG, FLOAT, IFD.
        4 | 9 | 11 | 13 => Some(4),
        // RATIONAL, SRATIONAL, DOUBLE, LONG8, SLONG8, IFD8.
        5 | 10 | 12 | 16 | 17 | 18 => Some(8),
        _ => None,
    }
}

fn u16_at(order: Order, raw: [u8; 2]) -> u16 {
    match order {
        Order::Little => u16::from_le_bytes(raw),
        Order::Big => u16::from_be_bytes(raw),
    }
}

fn u32_at(order: Order, raw: [u8; 4]) -> u32 {
    match order {
        Order::Little => u32::from_le_bytes(raw),
        Order::Big => u32::from_be_bytes(raw),
    }
}

fn u64_at(order: Order, raw: [u8; 8]) -> u64 {
    match order {
        Order::Little => u64::from_le_bytes(raw),
        Order::Big => u64::from_be_bytes(raw),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    /// A little-endian classic TIFF: header, then one IFD of `entries`.
    fn classic(entries: &[(u16, u16, u32, u32)], padding: usize) -> Vec<u8> {
        let mut out = b"II".to_vec();
        out.extend(42u16.to_le_bytes());
        out.extend(8u32.to_le_bytes());
        out.extend(u16::try_from(entries.len()).expect("few").to_le_bytes());
        for (tag, kind, count, value) in entries {
            out.extend(tag.to_le_bytes());
            out.extend(kind.to_le_bytes());
            out.extend(count.to_le_bytes());
            out.extend(value.to_le_bytes());
        }
        out.extend(0u32.to_le_bytes());
        out.resize(out.len() + padding, 0);
        out
    }

    fn check(bytes: Vec<u8>) -> Result<(), RasterError> {
        let mut cursor = Cursor::new(bytes);
        let result = check_ifd0(&mut cursor);
        assert_eq!(cursor.position(), 0, "the pass rewinds");
        result
    }

    #[test]
    fn values_inside_the_file_pass() {
        // ImageWidth inline, StripOffsets: 4 LONGs at offset 26.
        let bytes = classic(&[(256, 3, 1, 64), (273, 4, 4, 26)], 16);
        assert!(check(bytes).is_ok());
    }

    #[test]
    fn a_value_past_the_end_is_refused() {
        let bytes = classic(&[(273, 4, 1_000_000, 26)], 16);
        assert!(matches!(
            check(bytes),
            Err(RasterError::TagValueOutOfBounds {
                tag: 273,
                bytes: 4_000_000,
                ..
            })
        ));
    }

    #[test]
    fn a_value_over_the_cap_is_refused_even_inside_the_file() {
        let count = u32::try_from(MAX_TAG_VALUE_BYTES + 1).expect("fits");
        let bytes = classic(&[(270, 2, count, 26)], MAX_TAG_VALUE_BYTES + 64);
        assert!(matches!(
            check(bytes),
            Err(RasterError::TagValueOutOfBounds { tag: 270, .. })
        ));
    }

    #[test]
    fn unknown_types_and_short_tables_are_left_to_the_decoder() {
        let unknown = classic(&[(273, 99, u32::MAX, 0)], 0);
        assert!(check(unknown).is_ok());
        let mut short = classic(&[(273, 4, 4, 26)], 0);
        short.truncate(12);
        assert!(check(short).is_ok());
        assert!(check(b"not a tiff".to_vec()).is_ok());
        assert!(check(Vec::new()).is_ok());
    }

    #[test]
    fn bigtiff_entries_are_checked() {
        let mut out = b"II".to_vec();
        out.extend(43u16.to_le_bytes());
        out.extend(8u16.to_le_bytes());
        out.extend(0u16.to_le_bytes());
        out.extend(16u64.to_le_bytes());
        out.extend(1u64.to_le_bytes());
        out.extend(273u16.to_le_bytes());
        out.extend(16u16.to_le_bytes());
        out.extend(u64::MAX.to_le_bytes());
        out.extend(0u64.to_le_bytes());
        out.extend(0u64.to_le_bytes());
        assert!(matches!(
            check(out),
            Err(RasterError::TagValueOutOfBounds { tag: 273, .. })
        ));
    }
}
