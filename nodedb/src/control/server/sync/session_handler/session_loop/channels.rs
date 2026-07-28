// SPDX-License-Identifier: BUSL-1.1

//! Per-session server-push channels and their registration state.

use std::sync::Arc;

use nodedb_types::sync::wire::SyncFrame;

use crate::control::array_sync::OriginArrayInbound;
use crate::event::crdt_sync::types::OutboundDelta;

/// Outbound delivery channels a session registers for after authenticating,
/// plus the lazily-built inbound array engine.
///
/// Every registration is gated on the session having authenticated, so this
/// starts fully empty and fills in as the handshake completes.
#[derive(Default)]
pub(super) struct SessionChannels {
    pub crdt_delivery_rx: Option<tokio::sync::mpsc::Receiver<OutboundDelta>>,
    pub crdt_control_rx: Option<tokio::sync::mpsc::Receiver<SyncFrame>>,
    pub crdt_registered: bool,

    pub presence_rx: Option<tokio::sync::mpsc::Receiver<Arc<Vec<u8>>>>,
    pub presence_registered: bool,

    /// Built lazily after the handshake establishes the session tenant.
    pub array_inbound: Option<Arc<OriginArrayInbound>>,
    pub array_delivery_rx: Option<tokio::sync::mpsc::Receiver<Vec<u8>>>,
    pub array_delivery_registered: bool,

    pub definition_sync_rx: Option<tokio::sync::mpsc::Receiver<Vec<u8>>>,
    pub definition_sync_registered: bool,
}

/// Whether the session loop should keep running after a step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Flow {
    Continue,
    Break,
}

impl Flow {
    /// `Break` when the send failed, `Continue` otherwise.
    pub(super) fn from_send(sent: bool) -> Self {
        if sent { Self::Continue } else { Self::Break }
    }
}
