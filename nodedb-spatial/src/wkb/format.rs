// SPDX-License-Identifier: Apache-2.0

//! WKB type codes and decoder limits shared by the reader and writers.

// WKB geometry type codes (ISO 13249 / OGC SFA).
pub(super) const WKB_POINT: u32 = 1;
pub(super) const WKB_LINESTRING: u32 = 2;
pub(super) const WKB_POLYGON: u32 = 3;
pub(super) const WKB_MULTIPOINT: u32 = 4;
pub(super) const WKB_MULTILINESTRING: u32 = 5;
pub(super) const WKB_MULTIPOLYGON: u32 = 6;
pub(super) const WKB_GEOMETRYCOLLECTION: u32 = 7;

pub(super) const BYTE_ORDER_LE: u8 = 1;

/// PostGIS EWKB flag: a 4-byte SRID follows the type word.
pub const EWKB_SRID_FLAG: u32 = 0x2000_0000;

// WKB count fields are attacker-controlled. The bound caps allocation and
// parsing work even when the encoded input itself is very large.
pub(super) const MAX_WKB_ELEMENTS: usize = 1_000_000;
pub(super) const MAX_WKB_BYTES: usize = 64 * 1024 * 1024;
// Nested geometry collections are recursive on the stack. Keep the budget
// explicit so hostile nesting cannot exhaust the decoder thread's stack.
pub(super) const MAX_WKB_DEPTH: usize = 64;
