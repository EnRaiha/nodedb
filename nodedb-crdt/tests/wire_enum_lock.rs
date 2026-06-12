// SPDX-License-Identifier: Apache-2.0

//! Wire-enum stability snapshot tests for nodedb-crdt enums.

use serde_json::json;

// ─── ConflictPolicy ───────────────────────────────────────────────────────────

#[test]
fn conflict_policy_wire_forms() {
    use nodedb_crdt::policy::ConflictPolicy;

    let lww = ConflictPolicy::LastWriterWins;
    match &lww {
        ConflictPolicy::LastWriterWins => {}
        ConflictPolicy::RenameSuffix => {}
        ConflictPolicy::CascadeDefer { .. } => {}
        ConflictPolicy::Custom { .. } => {}
        ConflictPolicy::EscalateToDlq => {}
    }

    let v = serde_json::to_value(&lww).expect("serialize");
    assert_eq!(
        v,
        json!("LastWriterWins"),
        "ConflictPolicy::LastWriterWins wire form"
    );

    let rs = ConflictPolicy::RenameSuffix;
    let v = serde_json::to_value(&rs).expect("serialize");
    assert_eq!(
        v,
        json!("RenameSuffix"),
        "ConflictPolicy::RenameSuffix wire form"
    );

    let cd = ConflictPolicy::CascadeDefer {
        max_retries: 3,
        ttl_secs: 300,
    };
    let v = serde_json::to_value(&cd).expect("serialize");
    assert_eq!(
        v["CascadeDefer"]["max_retries"],
        json!(3),
        "ConflictPolicy::CascadeDefer wire form"
    );

    let cu = ConflictPolicy::Custom {
        webhook_url: "https://example.com/hook".into(),
        timeout_secs: 5,
    };
    let v = serde_json::to_value(&cu).expect("serialize");
    assert_eq!(
        v["Custom"]["webhook_url"],
        json!("https://example.com/hook"),
        "ConflictPolicy::Custom wire form"
    );

    let dlq = ConflictPolicy::EscalateToDlq;
    let v = serde_json::to_value(&dlq).expect("serialize");
    assert_eq!(
        v,
        json!("EscalateToDlq"),
        "ConflictPolicy::EscalateToDlq wire form"
    );
}
