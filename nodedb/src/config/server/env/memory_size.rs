// SPDX-License-Identifier: BUSL-1.1

//! Human-readable memory size parsing (`"512MiB"`, `"8G"`, raw byte counts).

/// Parse a human-readable memory size string into bytes.
///
/// Supported formats:
/// - `"512MiB"` / `"512M"` → mebibytes (base-1024)
/// - `"8GiB"` / `"8G"` → gibibytes (base-1024)
/// - `"1073741824"` → raw bytes (no suffix)
///
/// Matching is case-insensitive on the suffix.
pub fn parse_memory_size(s: &str) -> crate::Result<usize> {
    let s = s.trim();
    if s.is_empty() {
        return Err(crate::Error::Config {
            detail: "empty string".to_string(),
        });
    }

    let split_pos = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());

    let (num_part, suffix) = s.split_at(split_pos);
    let suffix = suffix.trim();

    let cfg = |detail: String| crate::Error::Config { detail };

    let base: u64 = num_part
        .parse()
        .map_err(|_| cfg(format!("invalid number: {num_part}")))?;

    let bytes: u64 = match suffix.to_ascii_uppercase().as_str() {
        "" => base,
        "B" => base,
        "K" | "KB" | "KIB" => base
            .checked_mul(1024)
            .ok_or_else(|| cfg(format!("overflow parsing memory size: {s}")))?,
        "M" | "MB" | "MIB" => base
            .checked_mul(1024 * 1024)
            .ok_or_else(|| cfg(format!("overflow parsing memory size: {s}")))?,
        "G" | "GB" | "GIB" => base
            .checked_mul(1024 * 1024 * 1024)
            .ok_or_else(|| cfg(format!("overflow parsing memory size: {s}")))?,
        "T" | "TB" | "TIB" => base
            .checked_mul(1024 * 1024 * 1024 * 1024)
            .ok_or_else(|| cfg(format!("overflow parsing memory size: {s}")))?,
        other => {
            return Err(cfg(format!("unknown memory size suffix: '{other}'")));
        }
    };

    usize::try_from(bytes).map_err(|_| cfg(format!("memory size too large for this platform: {s}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_raw_bytes() {
        assert_eq!(parse_memory_size("1073741824").unwrap(), 1_073_741_824);
        assert_eq!(parse_memory_size("0").unwrap(), 0);
        assert_eq!(parse_memory_size("1").unwrap(), 1);
    }

    #[test]
    fn parse_mib_suffix() {
        assert_eq!(parse_memory_size("512MiB").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_memory_size("512M").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_memory_size("512MB").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_memory_size("1MiB").unwrap(), 1024 * 1024);
    }

    #[test]
    fn parse_gib_suffix() {
        assert_eq!(parse_memory_size("8GiB").unwrap(), 8 * 1024 * 1024 * 1024);
        assert_eq!(parse_memory_size("8G").unwrap(), 8 * 1024 * 1024 * 1024);
        assert_eq!(parse_memory_size("8GB").unwrap(), 8 * 1024 * 1024 * 1024);
        assert_eq!(parse_memory_size("1GiB").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn parse_kib_suffix() {
        assert_eq!(parse_memory_size("64KiB").unwrap(), 64 * 1024);
        assert_eq!(parse_memory_size("64K").unwrap(), 64 * 1024);
        assert_eq!(parse_memory_size("64KB").unwrap(), 64 * 1024);
    }

    #[test]
    fn parse_bytes_suffix() {
        assert_eq!(parse_memory_size("100B").unwrap(), 100);
    }

    #[test]
    fn parse_case_insensitive() {
        assert_eq!(parse_memory_size("512mib").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_memory_size("8gib").unwrap(), 8 * 1024 * 1024 * 1024);
        assert_eq!(parse_memory_size("4g").unwrap(), 4 * 1024 * 1024 * 1024);
    }

    #[test]
    fn parse_trims_whitespace() {
        assert_eq!(parse_memory_size("  512MiB  ").unwrap(), 512 * 1024 * 1024);
        assert_eq!(
            parse_memory_size("  8GiB  ").unwrap(),
            8 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn parse_unknown_suffix_is_error() {
        assert!(parse_memory_size("512X").is_err());
        assert!(parse_memory_size("8ZiB").is_err());
    }

    #[test]
    fn parse_empty_is_error() {
        assert!(parse_memory_size("").is_err());
        assert!(parse_memory_size("   ").is_err());
    }

    #[test]
    fn parse_non_numeric_is_error() {
        assert!(parse_memory_size("abc").is_err());
        assert!(parse_memory_size("GiB").is_err());
    }
}
