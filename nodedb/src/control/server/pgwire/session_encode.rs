// SPDX-License-Identifier: BUSL-1.1

//! PG-wire-format encoding of protocol-neutral session state.
//!
//! `TransactionState` itself lives in the protocol-neutral
//! `shared::session` module; this file owns the one bit of
//! wire-format knowledge (the PostgreSQL ReadyForQuery status byte)
//! so non-pgwire protocols never need to import it.

use crate::control::server::shared::session::TransactionState;

/// PostgreSQL ReadyForQuery status byte for a transaction state.
pub fn status_byte(state: &TransactionState) -> u8 {
    match state {
        TransactionState::Idle => b'I',
        TransactionState::InBlock => b'T',
        TransactionState::Failed => b'E',
    }
}
