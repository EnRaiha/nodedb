//! Per-engine reclaim pass summary.

/// Summary of a single engine's reclaim pass. Missing files count as zero;
/// actual I/O failures are returned to the lifecycle barrier.
#[derive(Debug, Default, Clone, Copy)]
pub struct ReclaimStats {
    pub files_unlinked: u32,
    pub bytes_freed: u64,
}

impl ReclaimStats {
    pub fn merge(&mut self, other: ReclaimStats) {
        self.files_unlinked = self.files_unlinked.saturating_add(other.files_unlinked);
        self.bytes_freed = self.bytes_freed.saturating_add(other.bytes_freed);
    }
}
