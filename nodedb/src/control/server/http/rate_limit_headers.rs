// SPDX-License-Identifier: BUSL-1.1

//! `X-RateLimit-*` response headers for a successfully admitted HTTP request.
//!
//! Split out of `auth.rs` to keep that file under the file-size limit —
//! this is the HTTP-specific consumer of
//! [`RateLimiter::response_headers`](crate::control::security::ratelimit::limiter::RateLimiter::response_headers),
//! which `check_request_admission` returns as `Some(RateLimitResult)` for
//! every request that was not internal-service-exempt.

use axum::http::{HeaderMap, HeaderName, HeaderValue};

use crate::control::security::ratelimit::limiter::{RateLimitResult, RateLimiter};

/// Build the `X-RateLimit-Limit` / `X-RateLimit-Remaining` / `X-RateLimit-Reset`
/// headers for an admitted request. `None` (the internal-service exemption
/// short-circuit in `check_request_admission`) yields an empty header set.
pub(crate) fn rate_limit_headers(result: &Option<RateLimitResult>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    let Some(result) = result else {
        return headers;
    };
    for (name, value) in RateLimiter::response_headers(result) {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            headers.insert(name, value);
        }
    }
    headers
}
