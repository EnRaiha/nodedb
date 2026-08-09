// SPDX-License-Identifier: Apache-2.0

//! Typed sub-operations for `ALTER COLLECTION`, `ALTER USER`, and `ALTER ROLE`.

/// Typed sub-operation for `ALTER COLLECTION <name> ...`.
///
/// Each variant corresponds to one ALTER sub-command parsed by
/// `nodedb-sql/src/ddl_ast/parse/collection.rs`. The handler in
/// `nodedb/src/control/server/pgwire/ddl/collection/alter/` matches
/// on this enum instead of rescanning raw SQL.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterCollectionOp {
    /// `ADD [COLUMN] <name> <type> [NOT NULL] [DEFAULT expr]`
    AddColumn {
        column_name: String,
        column_type: String,
        not_null: bool,
        default_expr: Option<String>,
    },
    /// `DROP COLUMN <name>`
    DropColumn { column_name: String },
    /// `RENAME COLUMN <old> TO <new>`
    RenameColumn { old_name: String, new_name: String },
    /// `ALTER COLUMN <name> TYPE <type>`
    AlterColumnType {
        column_name: String,
        new_type: String,
    },
    /// `OWNER TO <user>`
    OwnerTo { new_owner: String },
    /// `SET RETENTION = '<duration>'`
    SetRetention { value: String },
    /// `SET APPEND_ONLY`
    SetAppendOnly,
    /// `SET LAST_VALUE_CACHE = TRUE|FALSE`
    SetLastValueCache { enabled: bool },
    /// `SET LEGAL_HOLD = TRUE|FALSE TAG '<tag>'`
    SetLegalHold { enabled: bool, tag: String },
    /// `ADD [COLUMN] <target_column> ... AS MATERIALIZED_SUM SOURCE <source_collection>
    /// ON <join_column> VALUE <value_expr>` — fully parsed by
    /// `nodedb-sql`; the handler receives typed fields and never
    /// rescans raw SQL.
    AddMaterializedSum {
        /// Target collection name (lowercased).
        target_collection: String,
        /// Target column name to hold the sum (lowercased).
        target_column: String,
        /// Declared type of the target column, as written (e.g. `DECIMAL`).
        /// The statement declares a real column, so the handler adds it to the
        /// target's schema — a maintenance write into a strict target is a full
        /// document write and is rejected on a field the schema does not carry.
        target_column_type: String,
        /// Source collection name (lowercased).
        source_collection: String,
        /// Join column on the source side (lowercased).
        join_column: String,
        /// Value expression (column name or qualified `source.column`, lowercased).
        value_expr: String,
    },
    /// `SET ON CONFLICT <policy_keyword> FOR <constraint_kind_keyword>`
    ///
    /// Sets the per-collection, per-constraint-kind conflict resolution policy.
    SetOnConflict {
        /// Parsed conflict policy keyword.
        policy: ConflictPolicyKind,
        /// Which constraint kind this policy applies to.
        constraint_kind: ConstraintKindKeyword,
    },
}

/// Keyword representation of a conflict resolution policy for DDL.
#[derive(Debug, Clone, PartialEq)]
pub enum ConflictPolicyKind {
    LastWriterWins,
    RenameSuffix,
    CascadeDefer,
    EscalateToDlq,
}

/// Keyword representation of a constraint kind for DDL.
#[derive(Debug, Clone, PartialEq)]
pub enum ConstraintKindKeyword {
    Unique,
    ForeignKey,
    NotNull,
    Check,
}

/// Typed sub-operation for `ALTER USER <name> ...`.
///
/// Five forms are supported:
/// - `SET PASSWORD '<pw>'` — change password
/// - `SET ROLE <role>` — change role
/// - `MUST CHANGE PASSWORD` — require password change on next login
/// - `PASSWORD NEVER EXPIRES` — clear expiry date
/// - `PASSWORD EXPIRES '<iso8601>'` or `PASSWORD EXPIRES IN <N> DAYS` — set expiry
#[derive(Debug, Clone, PartialEq)]
pub enum AlterUserOp {
    /// `SET PASSWORD '<password>'`
    SetPassword { password: String },
    /// `SET ROLE <role>`
    SetRole { role: String },
    /// `MUST CHANGE PASSWORD`
    MustChangePassword,
    /// `PASSWORD NEVER EXPIRES`
    PasswordNeverExpires,
    /// `PASSWORD EXPIRES '<iso8601_datetime>'`
    PasswordExpiresAt { iso8601: String },
    /// `PASSWORD EXPIRES IN <n> DAYS`
    PasswordExpiresInDays { days: u32 },
    /// `SET DEFAULT DATABASE <db_name>`
    SetDefaultDatabase { db_name: String },
}

/// Typed sub-operation for `ALTER ROLE <name> ...`.
///
/// Three forms are supported:
/// - `GRANT <perm> ON [FUNCTION] <target>` — grant a permission to the role
/// - `REVOKE <perm> ON [FUNCTION] <target>` — revoke a permission from the role
/// - `SET INHERIT <parent>` — update role inheritance (original ALTER ROLE form)
#[derive(Debug, Clone, PartialEq)]
pub enum AlterRoleOp {
    /// `GRANT <perm> ON [FUNCTION] <target>`
    Grant {
        /// Permission token, e.g. "READ", "WRITE", "ALL".
        permission: String,
        /// "COLLECTION" or "FUNCTION".
        target_type: String,
        /// Collection or function name.
        target_name: String,
    },
    /// `REVOKE <perm> ON [FUNCTION] <target>`
    Revoke {
        /// Permission token, e.g. "READ", "WRITE", "ALL".
        permission: String,
        /// "COLLECTION" or "FUNCTION".
        target_type: String,
        /// Collection or function name.
        target_name: String,
    },
    /// `SET INHERIT <parent>`
    SetInherit {
        /// Parent role name.
        parent: String,
    },
}
