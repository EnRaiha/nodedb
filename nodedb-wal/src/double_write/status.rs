// SPDX-License-Identifier: Apache-2.0

//! Observable torn-write protection state.
//!
//! A DWB problem never fails the WAL append that provoked it — the WAL write
//! itself is still correct and still acknowledged. What it does change is
//! whether a torn record at the tail of a segment can be reconstructed, and
//! that has to be legible to the caller instead of living in a log line.

/// Why a writer's double-write buffer stopped protecting it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DwbDegradation {
    /// The DWB file could not be opened when the writer was constructed.
    OpenFailed,
    /// Mirroring a record into the DWB failed; the buffer was detached.
    WriteFailed,
    /// The batch fsync of the DWB failed; the buffer was detached.
    FlushFailed,
}

impl std::fmt::Display for DwbDegradation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OpenFailed => f.write_str("DWB could not be opened"),
            Self::WriteFailed => f.write_str("DWB record write failed"),
            Self::FlushFailed => f.write_str("DWB batch fsync failed"),
        }
    }
}

/// Torn-write protection standing of a WAL writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DwbProtection {
    /// Every record that fits a slot is being mirrored.
    Active,
    /// No DWB was requested for this writer (`DwbMode::Off`). Intentional, so
    /// not a degradation.
    Off,
    /// A DWB was requested but is not mirroring.
    Degraded(DwbDegradation),
}

impl DwbProtection {
    /// Whether records are currently being mirrored.
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Active)
    }

    /// Whether protection was requested and then lost.
    pub fn is_degraded(&self) -> bool {
        matches!(self, Self::Degraded(_))
    }
}

/// Why a record could not be mirrored even though the buffer is healthy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DwbSkipReason {
    /// The record is larger than one slot. Multi-slot spanning would make a
    /// slot's own CRC insufficient to validate it, which is the property the
    /// whole recovery path rests on.
    RecordTooLarge { size: usize, max: usize },
}

impl std::fmt::Display for DwbSkipReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RecordTooLarge { size, max } => {
                write!(f, "record of {size} bytes exceeds the {max}-byte DWB slot")
            }
        }
    }
}

/// Outcome of mirroring one record into the double-write buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum DwbMirror {
    /// The record is in the buffer (durable once `flush` runs).
    Mirrored,
    /// The record was not mirrored and is therefore unprotected.
    Skipped(DwbSkipReason),
}
