// SPDX-License-Identifier: Apache-2.0

//! Typed field-reader helpers shared by every variant in
//! [`super::from_messagepack`].
//!
//! Each variant payload is a MessagePack map of `{u8 key: value}` pairs.
//! The decoder dispatches on the variant tag and then calls one of these
//! helpers to pull the expected fields, skipping any extra fields a newer
//! producer might have added (forward compatibility).

use zerompk::{FromMessagePack, Read};

use crate::sync::compensation::CompensationHint;

/// Read the 2-element outer array and return `(tag, field_count)`.
#[inline]
pub(super) fn read_header<'a, R: Read<'a>>(reader: &mut R) -> zerompk::Result<(u16, usize)> {
    let outer = reader.read_array_len()?;
    if outer != 2 {
        return Err(zerompk::Error::ArrayLengthMismatch {
            expected: 2,
            actual: outer,
        });
    }
    let tag = reader.read_u16()?;
    let field_count = reader.read_map_len()?;
    Ok((tag, field_count))
}

/// Skip all remaining fields in a variant payload map.
#[inline]
pub(super) fn skip_fields<'a, R: Read<'a>>(reader: &mut R, count: usize) -> zerompk::Result<()> {
    for _ in 0..count {
        reader.read_u8()?;
        reader.skip_value()?;
    }
    Ok(())
}

/// Skip one arbitrary MessagePack value.
#[inline]
fn skip_one<'a, R: Read<'a>>(reader: &mut R) -> zerompk::Result<()> {
    reader.skip_value()
}

pub(super) fn read_u8_field<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<u8> {
    if field_count < 1 {
        return Err(zerompk::Error::InvalidMarker(0));
    }
    let _k = reader.read_u8()?;
    let v = reader.read_u8()?;
    for _ in 1..field_count {
        reader.read_u8()?;
        skip_one(reader)?;
    }
    Ok(v)
}

pub(super) fn read1_str<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<(String,)> {
    if field_count < 1 {
        return Err(zerompk::Error::InvalidMarker(0));
    }
    let _k = reader.read_u8()?;
    let v = reader.read_string()?.into_owned();
    for _ in 1..field_count {
        reader.read_u8()?;
        skip_one(reader)?;
    }
    Ok((v,))
}

pub(super) fn read2_str<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<(String, String)> {
    if field_count < 2 {
        return Err(zerompk::Error::InvalidMarker(0));
    }
    let _k1 = reader.read_u8()?;
    let v1 = reader.read_string()?.into_owned();
    let _k2 = reader.read_u8()?;
    let v2 = reader.read_string()?.into_owned();
    for _ in 2..field_count {
        reader.read_u8()?;
        skip_one(reader)?;
    }
    Ok((v1, v2))
}

pub(super) fn read_collection_deactivated<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<(String, u64, String)> {
    if field_count < 3 {
        return Err(zerompk::Error::InvalidMarker(0));
    }
    let _k1 = reader.read_u8()?;
    let collection = reader.read_string()?.into_owned();
    let _k2 = reader.read_u8()?;
    let retention_expires_at_ns = reader.read_u64()?;
    let _k3 = reader.read_u8()?;
    let undrop_hint = reader.read_string()?.into_owned();
    for _ in 3..field_count {
        reader.read_u8()?;
        skip_one(reader)?;
    }
    Ok((collection, retention_expires_at_ns, undrop_hint))
}

/// Read two `u32` fields, tolerating `field_count < 2` by substituting `0`.
pub(super) fn read2_u32<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<(u32, u32)> {
    let v1 = if field_count >= 1 {
        reader.read_u8()?;
        reader.read_u32()?
    } else {
        0
    };
    let v2 = if field_count >= 2 {
        reader.read_u8()?;
        reader.read_u32()?
    } else {
        0
    };
    for _ in 2..field_count {
        reader.read_u8()?;
        skip_one(reader)?;
    }
    Ok((v1, v2))
}

/// Read two `u64` fields, tolerating `field_count < 2` by substituting `0`.
pub(super) fn read2_u64<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<(u64, u64)> {
    let v1 = if field_count >= 1 {
        reader.read_u8()?;
        reader.read_u64()?
    } else {
        0
    };
    let v2 = if field_count >= 2 {
        reader.read_u8()?;
        reader.read_u64()?
    } else {
        0
    };
    for _ in 2..field_count {
        reader.read_u8()?;
        skip_one(reader)?;
    }
    Ok((v1, v2))
}

/// Read a single array field containing strings.
/// The field layout is: `{key: u8, value: [str, ...]}`.
pub(super) fn read_string_vec<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<Vec<String>> {
    if field_count < 1 {
        return Ok(Vec::new());
    }
    reader.read_u8()?; // key
    let len = reader.read_array_len()?;
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        out.push(reader.read_string()?.into_owned());
    }
    for _ in 1..field_count {
        reader.read_u8()?;
        skip_one(reader)?;
    }
    Ok(out)
}

/// Read 2 string fields, tolerating `field_count < 2` by filling missing
/// fields with `"unspecified"`.
pub(super) fn read2_str_tolerant<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<(String, String)> {
    let v1 = if field_count >= 1 {
        reader.read_u8()?;
        reader.read_string()?.into_owned()
    } else {
        "unspecified".into()
    };
    let v2 = if field_count >= 2 {
        reader.read_u8()?;
        reader.read_string()?.into_owned()
    } else {
        "unspecified".into()
    };
    for _ in 2..field_count {
        reader.read_u8()?;
        skip_one(reader)?;
    }
    Ok((v1, v2))
}

/// Read 3 string fields, tolerating `field_count < 3` by filling missing
/// fields with `"unspecified"`.
pub(super) fn read3_str_tolerant<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<(String, String, String)> {
    let v1 = if field_count >= 1 {
        reader.read_u8()?;
        reader.read_string()?.into_owned()
    } else {
        "unspecified".into()
    };
    let v2 = if field_count >= 2 {
        reader.read_u8()?;
        reader.read_string()?.into_owned()
    } else {
        "unspecified".into()
    };
    let v3 = if field_count >= 3 {
        reader.read_u8()?;
        reader.read_string()?.into_owned()
    } else {
        "unspecified".into()
    };
    for _ in 3..field_count {
        reader.read_u8()?;
        skip_one(reader)?;
    }
    Ok((v1, v2, v3))
}

/// Read `SegmentCorrupted` fields: (segment_id: u64, corruption: String, detail: String).
/// Tolerates `field_count < 3`; missing `segment_id` defaults to `0`.
pub(super) fn read_segment_corrupted_tolerant<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<(u64, String, String)> {
    let segment_id = if field_count >= 1 {
        reader.read_u8()?;
        reader.read_u64()?
    } else {
        0
    };
    let corruption = if field_count >= 2 {
        reader.read_u8()?;
        reader.read_string()?.into_owned()
    } else {
        "unspecified".into()
    };
    let detail = if field_count >= 3 {
        reader.read_u8()?;
        reader.read_string()?.into_owned()
    } else {
        "unspecified".into()
    };
    for _ in 3..field_count {
        reader.read_u8()?;
        skip_one(reader)?;
    }
    Ok((segment_id, corruption, detail))
}

pub(super) fn read_sync_delta_rejected<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<Option<CompensationHint>> {
    if field_count < 1 {
        return Ok(None);
    }
    let _k = reader.read_u8()?;
    let compensation = Option::<CompensationHint>::read(reader)?;
    for _ in 1..field_count {
        reader.read_u8()?;
        skip_one(reader)?;
    }
    Ok(compensation)
}
