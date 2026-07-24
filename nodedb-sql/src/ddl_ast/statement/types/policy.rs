// SPDX-License-Identifier: Apache-2.0

//! Policy DDL/DML statements.

#[derive(Debug, Clone, PartialEq)]
pub enum PolicyStmt {
    // ── RLS policy ───────────────────────────────────────────────
    CreateRlsPolicy {
        name: String,
        collection: String,
        policy_type: String,
        predicate_raw: String,
        is_restrictive: bool,
        on_deny_raw: Option<String>,
        tenant_id_override: Option<u64>,
    },
    DropRlsPolicy {
        name: String,
        collection: String,
        if_exists: bool,
        tenant_id_override: Option<u64>,
    },
    ShowRlsPolicies {
        collection: Option<String>,
        tenant_id_override: Option<u64>,
    },

    // ── Retention policy ─────────────────────────────────────────
    CreateRetentionPolicy {
        name: String,
        collection: String,
        body_raw: String,
        eval_interval_raw: Option<String>,
    },
    DropRetentionPolicy {
        name: String,
        if_exists: bool,
    },
    AlterRetentionPolicy {
        name: String,
        action: String,
        set_key: Option<String>,
        set_value: Option<String>,
    },
    ShowRetentionPolicies,

    // ── Custom types ─────────────────────────────────────────────
    /// `CREATE TYPE <name> AS ENUM ('label1', 'label2', ...)`
    CreateEnumType {
        name: String,
        labels: Vec<String>,
    },
    /// `CREATE TYPE <name> AS (<field1> <type1>, <field2> <type2>, ...)`
    CreateCompositeType {
        name: String,
        /// `(field_name, type_name)` pairs.
        fields: Vec<(String, String)>,
    },
    /// `DROP TYPE [IF EXISTS] <name>`
    DropType {
        name: String,
        if_exists: bool,
    },
    /// `ALTER TYPE <name> ADD VALUE 'label'`
    AlterTypeAddValue {
        type_name: String,
        label: String,
    },
    /// `SHOW TYPES`
    ShowTypes,

    // ── Synonym groups ───────────────────────────────────────────
    /// `CREATE SYNONYM GROUP <name> AS ('term1', 'term2', ...)`
    CreateSynonymGroup {
        name: String,
        terms: Vec<String>,
    },
    /// `DROP SYNONYM GROUP [IF EXISTS] <name>`
    DropSynonymGroup {
        name: String,
        if_exists: bool,
    },
    /// `SHOW SYNONYM GROUPS`
    ShowSynonymGroups,

    // ── CRDT conflict policy ─────────────────────────────────────
    /// `SHOW CONFLICT POLICY ON <collection>`
    ShowConflictPolicy {
        collection: String,
    },
}
