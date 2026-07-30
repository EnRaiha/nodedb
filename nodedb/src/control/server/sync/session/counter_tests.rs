// SPDX-License-Identifier: BUSL-1.1

//! What a session's close line says about the deltas it handled.
//!
//! These counters are the operator's only window onto a sync session, so the
//! distinctions they draw are the only ones anybody downstream can act on.
//! Every test here pins a pair of outcomes that produce the same client-visible
//! ack but opposite facts about the database.

use super::super::wire::*;
use super::state::SyncSession;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::types::TenantId;

fn make_authenticated_session() -> SyncSession {
    let mut session = SyncSession::new("counter-session".into());
    session.authenticated = true;
    session.tenant_id = Some(TenantId::new(1));
    session.username = Some("alice".into());
    session.identity = Some(AuthenticatedIdentity::new_regular(
        1,
        "alice",
        TenantId::new(1),
        crate::control::security::identity::AuthMethod::ApiKey,
        vec![crate::control::security::identity::Role::ReadWrite],
        None,
        AuthenticatedIdentity::default_database_set(false),
    ));
    session
}

/// The counters a session closes with have to distinguish a client whose
/// writes landed from one whose writes were absorbed. Both produce a
/// successful ack, so folding `Duplicate` into `applied` made the second
/// indistinguishable from the first — which is how a session that
/// materialized nothing closed reporting hundreds of applied mutations.
#[test]
fn a_deduplicated_delta_is_not_counted_as_applied() {
    let mut session = make_authenticated_session();

    let applied = SyncFrame::try_encode(
        SyncMessageType::DeltaAck,
        &DeltaAckMsg {
            mutation_id: 1,
            lsn: 0,
            clock_skew_warning_ms: None,
            applied_seq: 1,
            status: nodedb_types::sync::wire::AckStatus::Applied,
        },
    )
    .expect("ack encodes");
    let duplicate = SyncFrame::try_encode(
        SyncMessageType::DeltaAck,
        &DeltaAckMsg {
            mutation_id: 2,
            lsn: 0,
            clock_skew_warning_ms: None,
            applied_seq: 2,
            status: nodedb_types::sync::wire::AckStatus::Duplicate,
        },
    )
    .expect("ack encodes");

    session.record_delta_outcome(&applied);
    session.record_delta_outcome(&duplicate);

    assert_eq!(session.mutations_applied, 1);
    assert_eq!(session.mutations_deduplicated, 1);
    assert_eq!(session.mutations_rejected, 0);
}

/// The trim count is what turns "every delta was deduplicated" from an
/// indistinguishable state into a visible one, so it must accumulate across
/// the session rather than reporting only the last delta.
#[test]
fn trimmed_operations_accumulate_across_the_session() {
    let mut session = make_authenticated_session();
    assert_eq!(session.ops_trimmed, 0);
    session.record_delta_admission(3);
    session.record_delta_admission(4);
    assert_eq!(session.ops_trimmed, 7);
}

/// A delta that trimmed nothing must leave the counter alone: a resync that
/// re-sends known history is normal, and a counter that ticked on every delta
/// would say nothing about which sessions to look at.
#[test]
fn a_delta_that_trims_nothing_leaves_the_counter_untouched() {
    let mut session = make_authenticated_session();
    session.record_delta_admission(0);
    assert_eq!(session.ops_trimmed, 0);
}
