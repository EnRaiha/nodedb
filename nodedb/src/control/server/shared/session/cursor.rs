// SPDX-License-Identifier: BUSL-1.1

//! Server-side cursor methods on SessionStore.

use std::net::SocketAddr;

use super::state::CursorState;
use super::store::SessionStore;

impl SessionStore {
    /// Declare a cursor with pre-fetched results.
    pub fn declare_cursor(
        &self,
        addr: &SocketAddr,
        name: String,
        rows: Vec<String>,
        scrollable: bool,
        with_hold: bool,
    ) {
        self.write_session(addr, |session| {
            session.cursors.insert(
                name,
                CursorState {
                    rows,
                    position: 0,
                    scrollable,
                    with_hold,
                },
            );
        });
    }

    /// Fetch N rows forward from a cursor. Returns (rows, exhausted).
    pub fn fetch_cursor(
        &self,
        addr: &SocketAddr,
        name: &str,
        count: usize,
    ) -> crate::Result<(Vec<String>, bool)> {
        self.write_session(addr, |session| {
            let cursor = session
                .cursors
                .get_mut(name)
                .ok_or_else(|| crate::Error::BadRequest {
                    detail: format!("cursor \"{name}\" does not exist"),
                })?;

            let start = cursor.position;
            let end = (start + count).min(cursor.rows.len());
            let rows: Vec<String> = cursor.rows[start..end].to_vec();
            cursor.position = end;
            let exhausted = end >= cursor.rows.len();
            Ok((rows, exhausted))
        })
        .unwrap_or_else(|| {
            Err(crate::Error::BadRequest {
                detail: "no active session".to_string(),
            })
        })
    }

    /// Fetch all remaining rows from a cursor.
    pub fn fetch_cursor_all(&self, addr: &SocketAddr, name: &str) -> crate::Result<Vec<String>> {
        self.write_session(addr, |session| {
            let cursor = session
                .cursors
                .get_mut(name)
                .ok_or_else(|| crate::Error::BadRequest {
                    detail: format!("cursor \"{name}\" does not exist"),
                })?;

            let rows = cursor.rows[cursor.position..].to_vec();
            cursor.position = cursor.rows.len();
            Ok(rows)
        })
        .unwrap_or_else(|| {
            Err(crate::Error::BadRequest {
                detail: "no active session".to_string(),
            })
        })
    }

    /// Fetch N rows backward from a cursor (requires SCROLL).
    pub fn fetch_cursor_backward(
        &self,
        addr: &SocketAddr,
        name: &str,
        count: usize,
    ) -> crate::Result<Vec<String>> {
        self.write_session(addr, |session| {
            let cursor = session
                .cursors
                .get_mut(name)
                .ok_or_else(|| crate::Error::BadRequest {
                    detail: format!("cursor \"{name}\" does not exist"),
                })?;

            if !cursor.scrollable {
                return Err(crate::Error::BadRequest {
                    detail: format!(
                        "cursor \"{name}\" is not scrollable; use DECLARE ... SCROLL CURSOR"
                    ),
                });
            }

            let end = cursor.position;
            let start = end.saturating_sub(count);
            let mut rows: Vec<String> = cursor.rows[start..end].to_vec();
            rows.reverse(); // PostgreSQL FETCH BACKWARD returns rows in reverse order.
            cursor.position = start;
            Ok(rows)
        })
        .unwrap_or_else(|| {
            Err(crate::Error::BadRequest {
                detail: "no active session".to_string(),
            })
        })
    }

    /// Move the cursor position without fetching data.
    pub fn move_cursor(
        &self,
        addr: &SocketAddr,
        name: &str,
        forward: bool,
        count: usize,
    ) -> crate::Result<usize> {
        self.write_session(addr, |session| {
            let cursor = session
                .cursors
                .get_mut(name)
                .ok_or_else(|| crate::Error::BadRequest {
                    detail: format!("cursor \"{name}\" does not exist"),
                })?;

            if !forward && !cursor.scrollable {
                return Err(crate::Error::BadRequest {
                    detail: format!("cursor \"{name}\" is not scrollable; cannot MOVE BACKWARD"),
                });
            }

            let old_pos = cursor.position;
            if forward {
                cursor.position = (cursor.position + count).min(cursor.rows.len());
            } else {
                cursor.position = cursor.position.saturating_sub(count);
            }
            let moved = if forward {
                cursor.position - old_pos
            } else {
                old_pos - cursor.position
            };
            Ok(moved)
        })
        .unwrap_or_else(|| {
            Err(crate::Error::BadRequest {
                detail: "no active session".to_string(),
            })
        })
    }

    /// Close a cursor.
    pub fn close_cursor(&self, addr: &SocketAddr, name: &str) {
        self.write_session(addr, |session| {
            session.cursors.remove(name);
        });
    }

    /// Close all cursors that don't have WITH HOLD (called on transaction end).
    pub fn close_non_hold_cursors(&self, addr: &SocketAddr) {
        self.write_session(addr, |session| {
            session.cursors.retain(|_, c| c.with_hold);
        });
    }
}
