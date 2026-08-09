// SPDX-License-Identifier: BUSL-1.1

//! Last-stop wire safety for pgwire `ErrorResponse` field values.
//!
//! An `ErrorResponse` is a sequence of `(field_code, value)` pairs, each value a
//! NUL-terminated C string, followed by a zero terminator byte. The pgwire
//! encoder writes a value's bytes verbatim (`put_cstring` —
//! `buf.put_slice(input.as_bytes()); buf.put_u8(b'\0')`), so a value carrying an
//! *interior* NUL is written as-is and the reader cannot tell it from a field
//! terminator.
//!
//! The client walk (`postgres-protocol`'s `ErrorFields::next`) then splits that
//! field at the interior NUL and reads the rest of the SAME value as the next
//! field's type byte. What happens next depends on the bytes that follow:
//!
//! - The re-read lands a zero type byte while the frame still has bytes left →
//!   `invalid message length: error fields is not drained`. The client raises a
//!   protocol parse error, so the diagnostic is not garbled, it is *absent* —
//!   along with the SQLSTATE that would have told the client whether to retry.
//!   Two adjacent NULs, or a value ending in one, always land here.
//! - Otherwise the walk happens to re-synchronise on the next real field. No
//!   error, but the message is silently truncated at the NUL and a junk field is
//!   injected where the remainder used to be.
//!
//! Both come from the same interior NUL and both are closed by removing it. The
//! failure that exposed this took the first branch: a strict collection's stored
//! Binary Tuple reached the schemaless decoder, and sonic-rs's syntax error
//! quotes a 16-byte window of the offending input through
//! `String::from_utf8_lossy` — which passes NUL through untouched, because NUL
//! is valid UTF-8. The tuple header's `schema_version: u32 LE` puts a run of
//! them inside that window.
//!
//! NodeDB error text routinely interpolates values it does not control:
//! identifiers, stored field values, and the `Display` of third-party decoder
//! errors — several of which quote the offending input, and binary input
//! contains NUL. Sanitising at the ~130 individual `ErrorInfo::new` call sites
//! would be one omission away from reintroducing this, so it happens here
//! instead: every error, whatever built it, passes through `on_error` before
//! pgwire serialises it.

use pgwire::api::{ClientInfo, ErrorHandler};
use pgwire::error::{ErrorInfo, PgWireError};

/// Replacement for a byte that cannot travel in a protocol field value.
const REPLACEMENT: char = '\u{FFFD}';

/// True for characters that break, or are meaningless in, a C-string field.
///
/// NUL is the one that corrupts framing. The remaining C0 controls and DEL
/// cannot appear in a legible diagnostic and terminals render them
/// destructively, so they are folded into the same replacement rather than
/// being passed through as an inconsistent special case.
fn is_wire_hostile(c: char) -> bool {
    (c.is_control() && !matches!(c, '\t' | '\n' | '\r')) || c == '\u{7F}'
}

/// Make one field value safe to place in a NUL-terminated protocol field.
///
/// Borrows unchanged when there is nothing to replace, so the common case (a
/// literal, well-formed diagnostic) costs one scan and no allocation.
pub(crate) fn wire_safe(value: &str) -> std::borrow::Cow<'_, str> {
    if !value.chars().any(is_wire_hostile) {
        return std::borrow::Cow::Borrowed(value);
    }
    std::borrow::Cow::Owned(
        value
            .chars()
            .map(|c| if is_wire_hostile(c) { REPLACEMENT } else { c })
            .collect(),
    )
}

/// Rewrite `field` in place when it carries anything wire-hostile.
fn sanitize(field: &mut String) {
    if let std::borrow::Cow::Owned(clean) = wire_safe(field) {
        *field = clean;
    }
}

/// Rewrite an optional field in place.
fn sanitize_opt(field: &mut Option<String>) {
    if let Some(value) = field {
        sanitize(value);
    }
}

/// Sanitize every string field of an `ErrorInfo`.
///
/// Every one of these becomes its own `(code, value)` pair in `into_fields`, so
/// any of them can desynchronise the frame — not just `message`. The list
/// mirrors `into_fields` exactly; `line` is the only other field and it is a
/// `usize` rendered by `to_string`, which cannot produce a control byte.
fn sanitize_error_info(info: &mut ErrorInfo) {
    sanitize(&mut info.severity);
    sanitize(&mut info.code);
    sanitize(&mut info.message);
    sanitize_opt(&mut info.severity_nonlocalized);
    sanitize_opt(&mut info.detail);
    sanitize_opt(&mut info.hint);
    sanitize_opt(&mut info.position);
    sanitize_opt(&mut info.internal_position);
    sanitize_opt(&mut info.internal_query);
    sanitize_opt(&mut info.where_context);
    sanitize_opt(&mut info.file_name);
    sanitize_opt(&mut info.routine);
    sanitize_opt(&mut info.schema);
    sanitize_opt(&mut info.table);
    sanitize_opt(&mut info.column);
    sanitize_opt(&mut info.datatype);
    sanitize_opt(&mut info.constraint);
}

/// The connection's `ErrorHandler`: the last point NodeDB owns before pgwire
/// turns an error into wire bytes.
#[derive(Debug, Default)]
pub(crate) struct WireSafeErrorHandler;

impl ErrorHandler for WireSafeErrorHandler {
    fn on_error<C>(&self, _client: &C, error: &mut PgWireError)
    where
        C: ClientInfo,
    {
        // `UserError` is the only variant NodeDB constructs, and the only one
        // carrying an `ErrorInfo` whose fields it fills. pgwire's own variants
        // are rendered from its own `Display` impls, which emit no control
        // bytes.
        if let PgWireError::UserError(info) = error {
            sanitize_error_info(info);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The framing bug in one assertion: an interior NUL must not survive into
    /// a field value, because the length prefix counts it while the client's
    /// field walk stops at it.
    #[test]
    fn an_interior_nul_is_replaced() {
        let dirty = "decode failed: \u{0}\u{1}\u{2} at offset 3";
        let clean = wire_safe(dirty);
        assert!(
            !clean.contains('\u{0}'),
            "a NUL byte must never reach the encoder: {clean:?}"
        );
        assert!(clean.starts_with("decode failed: "));
        assert!(clean.ends_with(" at offset 3"));
    }

    #[test]
    fn clean_text_is_borrowed_unchanged() {
        let value = "collection \"users\" does not exist\n\thint: check the name";
        assert!(matches!(wire_safe(value), std::borrow::Cow::Borrowed(_)));
    }

    /// Tab, newline and carriage return are legible in a diagnostic and are not
    /// field terminators, so they must survive.
    #[test]
    fn ordinary_whitespace_survives() {
        assert_eq!(wire_safe("a\tb\nc\rd"), "a\tb\nc\rd");
    }

    /// Encode a real `ErrorResponse` and walk its fields byte-for-byte the way
    /// the client does.
    ///
    /// This is a transcription of `postgres-protocol`'s `ErrorFields::next`
    /// (`message/backend.rs`): read one type byte; a zero type byte ends the
    /// field list and MUST be the last byte, otherwise the iterator raises
    /// `"invalid message length: error fields is not drained"`; any other type
    /// byte is followed by the value's bytes up to the next NUL (`find_null`,
    /// which errors when the buffer holds no further NUL).
    ///
    /// Returns the fields the client would actually see, or the error it would
    /// actually raise.
    fn frame_fields(info: ErrorInfo) -> Result<Vec<(u8, String)>, String> {
        use bytes::{Buf, BytesMut};
        use pgwire::messages::Message;
        use pgwire::messages::response::ErrorResponse;

        let mut buf = BytesMut::new();
        ErrorResponse::from(info)
            .encode(&mut buf)
            .expect("error response must encode");

        // Strip the message-type byte, then the i32 length prefix. The prefix
        // covers itself plus the body, which is how the client bounds the walk.
        let mut body = buf.split_off(1);
        let declared = body.get_i32() as usize;
        assert_eq!(
            declared,
            body.len() + 4,
            "the length prefix must cover the body it precedes"
        );

        let bytes = &body[..];
        let mut pos = 0usize;
        let mut fields = Vec::new();
        loop {
            let Some(&type_) = bytes.get(pos) else {
                return Err("unexpected EOF reading a field type byte".to_string());
            };
            pos += 1;
            if type_ == 0 {
                if pos == bytes.len() {
                    return Ok(fields);
                }
                return Err("invalid message length: error fields is not drained".to_string());
            }
            let Some(end) = bytes[pos..].iter().position(|b| *b == 0) else {
                return Err("unexpected EOF looking for a field terminator".to_string());
            };
            fields.push((
                type_,
                String::from_utf8_lossy(&bytes[pos..pos + end]).into_owned(),
            ));
            pos += end + 1;
        }
    }

    /// The `M` (message) field the client would read back, if the walk succeeds.
    fn message_field(fields: &[(u8, String)]) -> Option<&str> {
        fields
            .iter()
            .find(|(code, _)| *code == b'M')
            .map(|(_, v)| v.as_str())
    }

    /// The message text the production chain actually produces for the failure
    /// that exposed this: a strict collection's stored body handed to the
    /// schemaless decoder.
    ///
    /// A Binary Tuple begins `"NDST"` (`4E 44 53 54`), which is not a
    /// MessagePack map header, so `decode_document` falls through to
    /// `sonic_rs::from_slice`. sonic-rs's syntax error embeds a 16-byte window
    /// of the offending input via `String::from_utf8_lossy` — and a NUL byte is
    /// VALID UTF-8, so it passes straight through into the message. The tuple
    /// header's `schema_version: u32 LE` supplies a run of them.
    fn message_from_a_real_strict_body_decode() -> String {
        // Binary Tuple header: magic "NDST", format_version 1, schema_version 1
        // little-endian — the last of which is `01 00 00 00`.
        let tuple: Vec<u8> = vec![
            0x4E, 0x44, 0x53, 0x54, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let err = sonic_rs::from_slice::<serde_json::Value>(&tuple)
            .expect_err("a Binary Tuple is not JSON");
        format!("serialization error (json): {err}")
    }

    /// The chain claim, checked rather than asserted from memory: the real error
    /// text carries raw NUL bytes out of the undecodable body.
    #[test]
    fn the_real_decode_failure_message_carries_interior_nuls() {
        let message = message_from_a_real_strict_body_decode();
        assert!(
            message.contains('\u{0}'),
            "the message must carry the raw input bytes, NULs included, or the \
             documented cause of the wire failure no longer holds: {message:?}"
        );
    }

    /// The defect, reproduced with the REAL message: the frame does not drain,
    /// which is the client's parse failure instead of the diagnostic.
    #[test]
    fn the_real_decode_failure_message_desynchronises_the_frame() {
        let message = message_from_a_real_strict_body_decode();
        let info = ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), message.clone());

        let walked = frame_fields(info);
        assert_eq!(
            walked.as_ref().err().map(String::as_str),
            Some("invalid message length: error fields is not drained"),
            "unsanitized, this is exactly the error the client reported"
        );

        let mut fixed = ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), message);
        sanitize_error_info(&mut fixed);
        let fields = frame_fields(fixed).expect("the sanitized frame must drain");
        let read_back = message_field(&fields).expect("the M field must survive");
        assert!(
            read_back.starts_with("serialization error (json): "),
            "the client must now read the whole diagnostic: {read_back:?}"
        );
        assert!(!read_back.contains('\u{0}'));
    }

    /// Two consecutive NULs is the minimal deterministic desync: the field walk
    /// splits at the first, then reads the second as a type byte with the frame
    /// not yet exhausted.
    #[test]
    fn consecutive_interior_nuls_desynchronise_the_frame() {
        let info = ErrorInfo::new(
            "ERROR".to_owned(),
            "XX000".to_owned(),
            "decode failed: NDST\u{1}\u{1}\u{0}\u{0}\u{0} at offset 0".to_owned(),
        );
        assert_eq!(
            frame_fields(info).err().as_deref(),
            Some("invalid message length: error fields is not drained")
        );
    }

    /// A SINGLE isolated NUL followed by non-zero bytes does NOT raise the
    /// client error — the walk re-synchronises on the next real field. It is
    /// still corruption: the message is silently truncated at the NUL and a
    /// bogus field is injected in its place.
    ///
    /// This alignment is what an earlier version of this test used, which is why
    /// it wrongly reported the defect as absent. Both symptoms come from the
    /// same interior NUL and both are closed by the sanitizer.
    #[test]
    fn a_single_interior_nul_silently_truncates_instead_of_erroring() {
        let message = "serialization error (json): expected value\u{0}\u{1}\u{2}";
        let info = ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), message.to_owned());

        let fields = frame_fields(info).expect("this alignment happens to re-synchronise");
        let read_back = message_field(&fields).expect("an M field is still produced");
        assert_ne!(
            read_back, message,
            "the client reads a TRUNCATED diagnostic, not the one the server sent"
        );
        assert_eq!(read_back, "serialization error (json): expected value");
        assert!(
            fields.iter().any(|(code, _)| *code == 0x01),
            "the bytes after the NUL are re-read as a bogus field: {fields:?}"
        );

        let mut fixed = ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), message.to_owned());
        sanitize_error_info(&mut fixed);
        let fields = frame_fields(fixed).expect("sanitized frame drains");
        let read_back = message_field(&fields).expect("M field");
        assert!(
            read_back.starts_with("serialization error (json): expected value"),
            "the whole diagnostic must survive: {read_back:?}"
        );
        assert!(fields.iter().all(|(code, _)| *code != 0x01));
    }

    /// A guard against the field list drifting: EVERY field pgwire turns into a
    /// wire pair is filled with a desyncing NUL pair, so the unsanitized frame
    /// breaks and the sanitized one drains. A pgwire upgrade that adds a field
    /// `sanitize_error_info` does not cover fails here rather than silently
    /// shipping the bug again.
    #[test]
    fn every_emitted_field_is_sanitized() {
        let dirty = || Some("value\u{0}\u{0}tail".to_owned());
        let build = || {
            let mut info = ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                "message\u{0}\u{0}tail".to_owned(),
            );
            info.severity_nonlocalized = dirty();
            info.detail = dirty();
            info.hint = dirty();
            info.position = dirty();
            info.internal_position = dirty();
            info.internal_query = dirty();
            info.where_context = dirty();
            info.file_name = dirty();
            info.line = Some(42);
            info.routine = dirty();
            info.schema = dirty();
            info.table = dirty();
            info.column = dirty();
            info.datatype = dirty();
            info.constraint = dirty();
            info
        };

        assert!(
            frame_fields(build()).is_err(),
            "the fixture must actually break the frame, or this proves nothing"
        );

        let mut info = build();
        sanitize_error_info(&mut info);
        let fields = frame_fields(info).expect("every emitted field must be sanitized");
        assert!(
            fields.iter().all(|(_, v)| !v.contains('\u{0}')),
            "no field value may carry a NUL onto the wire: {fields:?}"
        );
    }
}
