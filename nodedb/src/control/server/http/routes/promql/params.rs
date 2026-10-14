// SPDX-License-Identifier: BUSL-1.1

//! Request parameters of the PromQL HTTP endpoints.
//!
//! Prometheus clients send parameters in the URL query string on GET and in
//! an `application/x-www-form-urlencoded` body on POST. Grafana uses POST by
//! default. Both sources are read, and keys repeat: `/series` takes several
//! `match[]` values.

use axum::http::HeaderMap;

use crate::control::promql::PromqlError;

/// Decoded `key=value` pairs from the query string and the form body.
#[derive(Debug, Default)]
pub struct PromParams {
    pairs: Vec<(String, String)>,
}

impl PromParams {
    /// Read the query string and, for a form-encoded body, the body.
    pub fn from_request(
        query: Option<&str>,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Self, PromqlError> {
        let mut pairs = Vec::new();
        if let Some(query) = query {
            decode_pairs(query.as_bytes(), &mut pairs)?;
        }
        let form_body = headers
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("application/x-www-form-urlencoded"));
        if form_body {
            decode_pairs(body, &mut pairs)?;
        }
        Ok(Self { pairs })
    }

    /// The first value of `key`.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.pairs
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// Every value of `key`, in request order.
    pub fn all(&self, key: &str) -> Vec<&str> {
        self.pairs
            .iter()
            .filter(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
            .collect()
    }

    /// The value of `key`, which the endpoint requires.
    pub fn required(&self, key: &str) -> Result<&str, PromqlError> {
        self.get(key)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| bad(key, "the parameter is required".to_owned()))
    }

    /// A time parameter in milliseconds: Unix seconds (with a fraction) or
    /// an RFC 3339 timestamp. `None` when the parameter is absent.
    pub fn time_ms(&self, key: &str) -> Result<Option<i64>, PromqlError> {
        match self.get(key).filter(|v| !v.is_empty()) {
            None => Ok(None),
            Some(raw) => parse_time_ms(raw)
                .map(Some)
                .ok_or_else(|| bad(key, format!("cannot parse {raw:?} as a timestamp"))),
        }
    }

    /// The `database` parameter, for the database resolver.
    pub fn database(&self) -> crate::control::server::http::routes::query::DatabaseQueryParam {
        crate::control::server::http::routes::query::DatabaseQueryParam {
            database: self.get("database").map(str::to_owned),
        }
    }
}

/// Parse Unix seconds or RFC 3339 into milliseconds since the epoch.
pub fn parse_time_ms(raw: &str) -> Option<i64> {
    if let Ok(secs) = raw.parse::<f64>() {
        if !secs.is_finite() {
            return None;
        }
        let ms = (secs * 1000.0).round();
        if ms < i64::MIN as f64 || ms > i64::MAX as f64 {
            return None;
        }
        return Some(ms as i64);
    }
    chrono::DateTime::parse_from_rfc3339(raw.trim())
        .ok()
        .map(|dt| dt.timestamp_millis())
}

fn bad(param: &str, detail: String) -> PromqlError {
    PromqlError::BadParameter {
        param: param.to_owned(),
        detail,
    }
}

/// Decode `a=b&c=d` with `+` as space and `%XX` escapes.
fn decode_pairs(raw: &[u8], out: &mut Vec<(String, String)>) -> Result<(), PromqlError> {
    for part in raw.split(|b| *b == b'&') {
        if part.is_empty() {
            continue;
        }
        let (key, value) = match part.iter().position(|b| *b == b'=') {
            Some(eq) => (&part[..eq], &part[eq + 1..]),
            None => (part, &[][..]),
        };
        out.push((percent_decode(key)?, percent_decode(value)?));
    }
    Ok(())
}

fn percent_decode(raw: &[u8]) -> Result<String, PromqlError> {
    let mut bytes = Vec::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        match raw[i] {
            b'+' => {
                bytes.push(b' ');
                i += 1;
            }
            b'%' => {
                let hex = raw
                    .get(i + 1..i + 3)
                    .and_then(|h| std::str::from_utf8(h).ok())
                    .and_then(|h| u8::from_str_radix(h, 16).ok())
                    .ok_or_else(|| {
                        bad(
                            "request",
                            "malformed percent escape in parameters".to_owned(),
                        )
                    })?;
                bytes.push(hex);
                i += 3;
            }
            other => {
                bytes.push(other);
                i += 1;
            }
        }
    }
    String::from_utf8(bytes)
        .map_err(|_| bad("request", "parameters are not valid UTF-8".to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(query: &str) -> PromParams {
        PromParams::from_request(Some(query), &HeaderMap::new(), &[]).unwrap()
    }

    #[test]
    fn repeated_keys_and_escapes_decode() {
        let p = params("match%5B%5D=up%7Bjob%3D%22a+b%22%7D&match[]=down&time=1.5");
        assert_eq!(p.all("match[]"), vec!["up{job=\"a b\"}", "down"]);
        assert_eq!(p.time_ms("time").unwrap(), Some(1500));
    }

    #[test]
    fn form_body_is_read_on_post() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::CONTENT_TYPE,
            "application/x-www-form-urlencoded".parse().unwrap(),
        );
        let p = PromParams::from_request(None, &headers, b"query=up&time=2").unwrap();
        assert_eq!(p.required("query").unwrap(), "up");
        assert_eq!(p.time_ms("time").unwrap(), Some(2000));
    }

    #[test]
    fn rfc3339_time_with_offset_parses() {
        assert_eq!(parse_time_ms("1970-01-01T01:00:00+01:00"), Some(0));
        assert_eq!(parse_time_ms("1970-01-01T00:00:01.250Z"), Some(1250));
        assert_eq!(parse_time_ms("not a time"), None);
        assert_eq!(parse_time_ms("NaN"), None);
    }

    #[test]
    fn bad_input_is_an_error() {
        assert!(PromParams::from_request(Some("a=%zz"), &HeaderMap::new(), &[]).is_err());
        assert!(params("time=x").time_ms("time").is_err());
        assert!(params("").required("query").is_err());
    }
}
