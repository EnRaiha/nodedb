// SPDX-License-Identifier: BUSL-1.1

//! Typed errors for surrogate allocation and mode promotion.

/// Allocation errors, wired into `crate::Error` via `From`.
#[derive(Debug, thiserror::Error)]
pub enum SurrogateAllocError {
    #[error("surrogate space exhausted (u32::MAX reached)")]
    Exhausted,
    #[error("surrogate batch size 0 is not allowed")]
    EmptyBatch,
    #[error("surrogate flush failed: {detail}")]
    FlushFailed { detail: String },
}

impl From<SurrogateAllocError> for crate::Error {
    fn from(e: SurrogateAllocError) -> Self {
        match e {
            SurrogateAllocError::Exhausted => crate::Error::Internal {
                detail: "surrogate space exhausted (u32::MAX reached)".into(),
            },
            SurrogateAllocError::EmptyBatch => crate::Error::BadRequest {
                detail: "surrogate batch size 0 is not allowed".into(),
            },
            SurrogateAllocError::FlushFailed { detail } => crate::Error::Storage {
                engine: "surrogate".into(),
                detail,
            },
        }
    }
}

/// Errors from `SurrogateRegistry::promote_to_cluster`.
#[derive(Debug, thiserror::Error)]
pub enum SurrogatePromotionError {
    /// Local → Cluster promotion needs the hwm-into-`G` join barrier;
    /// that barrier is not implemented yet.
    #[error("surrogate registry promotion requires the (not yet implemented) join barrier")]
    BarrierNotImplemented,
}

impl From<SurrogatePromotionError> for crate::Error {
    fn from(e: SurrogatePromotionError) -> Self {
        crate::Error::Internal {
            detail: e.to_string(),
        }
    }
}
