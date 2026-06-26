// SPDX-License-Identifier: BUSL-1.1

//! Shared HTTP route helpers: request ID extraction.
//!
//! Used by the CRDT endpoint and any future dedicated endpoints.

use axum::http::HeaderMap;

/// Extract X-Request-Id from headers, or generate one.
pub(super) fn extract_request_id(headers: &HeaderMap) -> u64 {
    headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64
        })
}
