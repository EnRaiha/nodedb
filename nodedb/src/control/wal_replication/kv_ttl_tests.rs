// SPDX-License-Identifier: BUSL-1.1

//! Raft-replicated KV TTL determinism.
//!
//! These pin the fix for a real-world clock-divergence bug: every replica
//! applying a committed KV TTL write independently read its own wall clock at
//! apply time (`CoreLoop::kv_ttl_now_ms`'s wall-clock fallback), so a key's
//! `expire_at_ms` could differ across replicas by the replication latency.
//! `ReplicatedWrite`'s TTL-bearing `Kv*` variants now carry the instant the
//! proposing node resolved before proposing to Raft; `decode::from_replicated_entry`
//! must hand that exact instant back as `resolved_now_ms`, never a fresh clock
//! read, so every replica installs byte-identical `expire_at_ms`.
//!
//! A resolved instant of `1_000` is used deliberately: real wall-clock ms
//! since epoch is enormously larger, so a decoder that (re)computes `now_ms`
//! instead of decoding the wire value fails these assertions loudly rather
//! than passing by coincidence.

use super::*;
use crate::bridge::envelope::PhysicalPlan;
use crate::types::{DatabaseId, TenantId, VShardId};
use nodedb_physical::physical_plan::KvOp;

#[test]
fn kv_put_resolved_now_ms_roundtrips_verbatim_not_a_fresh_clock_read() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);
    let ttl_ms = 5_000u64;
    let resolved_now_ms = 1_000u64;

    let entry = ReplicatedEntry::new(
        tenant.as_u64(),
        DatabaseId::DEFAULT.as_u64(),
        vshard.as_u32(),
        ReplicatedWrite::KvPut {
            collection: "sessions".into(),
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
            ttl_ms,
            surrogate: 1,
            resolved_now_ms: Some(resolved_now_ms),
        },
    );
    let bytes = entry.to_bytes();

    let (_, _, plan, decoded_resolved_now_ms) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");

    assert_eq!(
        decoded_resolved_now_ms,
        Some(resolved_now_ms),
        "decoded resolved_now_ms must be the exact instant the proposing node carried, \
         not a fresh clock read"
    );
    match plan {
        PhysicalPlan::Kv(KvOp::Put {
            ttl_ms: decoded_ttl_ms,
            ..
        }) => {
            assert_eq!(decoded_ttl_ms, ttl_ms);
            let installed_expire_at_ms =
                decoded_resolved_now_ms.expect("resolved") + decoded_ttl_ms;
            assert_eq!(installed_expire_at_ms, resolved_now_ms + ttl_ms);
        }
        other => panic!("expected Kv(Put), got {other:?}"),
    }
}

#[test]
fn kv_put_ttl_zero_carries_no_resolved_now_ms() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);

    let entry = ReplicatedEntry::new(
        tenant.as_u64(),
        DatabaseId::DEFAULT.as_u64(),
        vshard.as_u32(),
        ReplicatedWrite::KvPut {
            collection: "sessions".into(),
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
            ttl_ms: 0,
            surrogate: 1,
            resolved_now_ms: None,
        },
    );
    let bytes = entry.to_bytes();

    let (_, _, plan, decoded_resolved_now_ms) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");

    assert_eq!(
        decoded_resolved_now_ms, None,
        "a ttl_ms == 0 write must carry no resolved instant on either side of the wire"
    );
    match plan {
        PhysicalPlan::Kv(KvOp::Put { ttl_ms, .. }) => assert_eq!(ttl_ms, 0),
        other => panic!("expected Kv(Put), got {other:?}"),
    }
}

#[test]
fn kv_expire_resolved_now_ms_is_always_present() {
    // `EXPIRE` has no "no TTL" sentinel: `ttl_ms == 0` is a legitimate
    // "expire now" request, so the resolved instant is always `Some`, unlike
    // the `Put` family's `None`-on-zero-TTL convention.
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);
    let ttl_ms = 0u64;
    let resolved_now_ms = 1_000u64;

    let entry = ReplicatedEntry::new(
        tenant.as_u64(),
        DatabaseId::DEFAULT.as_u64(),
        vshard.as_u32(),
        ReplicatedWrite::KvExpire {
            collection: "sessions".into(),
            key: b"k1".to_vec(),
            ttl_ms,
            resolved_now_ms: Some(resolved_now_ms),
        },
    );
    let bytes = entry.to_bytes();

    let (_, _, plan, decoded_resolved_now_ms) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");

    assert_eq!(
        decoded_resolved_now_ms,
        Some(resolved_now_ms),
        "KvExpire must always carry the proposing node's resolved instant verbatim"
    );
    match plan {
        PhysicalPlan::Kv(KvOp::Expire {
            ttl_ms: decoded_ttl_ms,
            ..
        }) => assert_eq!(decoded_ttl_ms, ttl_ms),
        other => panic!("expected Kv(Expire), got {other:?}"),
    }
}

#[test]
fn kv_incr_resolved_now_ms_only_when_ttl_positive() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);

    // ttl_ms > 0: resolved_now_ms must round-trip verbatim.
    let entry_with_ttl = ReplicatedEntry::new(
        tenant.as_u64(),
        DatabaseId::DEFAULT.as_u64(),
        vshard.as_u32(),
        ReplicatedWrite::KvIncr {
            collection: "counters".into(),
            key: b"c1".to_vec(),
            delta: 5,
            ttl_ms: 60_000,
            surrogate: 1,
            resolved_now_ms: Some(1_000),
        },
    );
    let bytes = entry_with_ttl.to_bytes();
    let (_, _, plan, decoded_resolved_now_ms) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    assert_eq!(decoded_resolved_now_ms, Some(1_000));
    match plan {
        PhysicalPlan::Kv(KvOp::Incr { ttl_ms, .. }) => assert_eq!(ttl_ms, 60_000),
        other => panic!("expected Kv(Incr), got {other:?}"),
    }

    // ttl_ms == 0 ("preserve existing TTL"): no instant to carry.
    let entry_no_ttl = ReplicatedEntry::new(
        tenant.as_u64(),
        DatabaseId::DEFAULT.as_u64(),
        vshard.as_u32(),
        ReplicatedWrite::KvIncr {
            collection: "counters".into(),
            key: b"c1".to_vec(),
            delta: 5,
            ttl_ms: 0,
            surrogate: 1,
            resolved_now_ms: None,
        },
    );
    let bytes_no_ttl = entry_no_ttl.to_bytes();
    let (_, _, _, decoded_resolved_now_ms_no_ttl) =
        decode::from_replicated_entry(&bytes_no_ttl, None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
    assert_eq!(decoded_resolved_now_ms_no_ttl, None);
}

#[test]
fn kv_encoders_resolve_the_instant_once_at_proposal_time() {
    // Exercises the real encode path (`encode::to_replicated_entry`), not a
    // hand-built `ReplicatedWrite`, so this pins that the leader-side encoder
    // itself populates `resolved_now_ms` for TTL-bearing writes -- the
    // preceding tests pin the wire round-trip / decode side.
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);

    let plan = PhysicalPlan::Kv(KvOp::Put {
        collection: "sessions".into(),
        key: b"k1".to_vec(),
        value: b"v1".to_vec(),
        ttl_ms: 5_000,
        surrogate: nodedb_types::Surrogate::new(1),
        returning: None,
        rls_filters: Vec::new(),
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("Kv(Put) should produce a ReplicatedEntry");
    match entry.write {
        ReplicatedWrite::KvPut {
            resolved_now_ms, ..
        } => assert!(
            resolved_now_ms.is_some(),
            "a ttl_ms > 0 Put must carry a resolved instant"
        ),
        other => panic!("expected KvPut, got {other:?}"),
    }

    let plan_no_ttl = PhysicalPlan::Kv(KvOp::Put {
        collection: "sessions".into(),
        key: b"k2".to_vec(),
        value: b"v2".to_vec(),
        ttl_ms: 0,
        surrogate: nodedb_types::Surrogate::new(2),
        returning: None,
        rls_filters: Vec::new(),
    });
    let entry_no_ttl = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan_no_ttl)
        .expect("Kv(Put) should produce a ReplicatedEntry");
    match entry_no_ttl.write {
        ReplicatedWrite::KvPut {
            resolved_now_ms, ..
        } => assert_eq!(
            resolved_now_ms, None,
            "a ttl_ms == 0 Put must carry no resolved instant"
        ),
        other => panic!("expected KvPut, got {other:?}"),
    }
}
