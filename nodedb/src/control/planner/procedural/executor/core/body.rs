// SPDX-License-Identifier: BUSL-1.1

//! Server-run bodies that execute as exactly one transaction.

/// A body the server runs on its own: its statements commit together on
/// success and are discarded on error, so a retry never repeats a part that
/// already applied. COMMIT and ROLLBACK are refused inside one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AtomicBody {
    /// A trigger body, named by its trigger.
    Trigger { name: String },
    /// A scheduled job body, named by its schedule.
    ScheduledJob { name: String },
    /// An alert's history write, named by its alert.
    Alert { name: String },
    /// Trigger DML shipped from another node and applied on this one. The
    /// body plans each statement with every write it derives, and they all
    /// commit in its one transaction, whichever vShards they span.
    CrossShardApply,
}

impl AtomicBody {
    pub fn trigger(name: &str) -> Self {
        Self::Trigger {
            name: name.to_owned(),
        }
    }

    pub fn scheduled_job(name: &str) -> Self {
        Self::ScheduledJob {
            name: name.to_owned(),
        }
    }

    pub fn alert(name: &str) -> Self {
        Self::Alert {
            name: name.to_owned(),
        }
    }

    /// Identity of this body within one source write. The cross-shard
    /// receiver deduplicates on it together with the source write's position.
    pub(super) fn origin_tag(&self, database_id: crate::types::DatabaseId) -> String {
        let database = database_id.as_u64();
        match self {
            Self::Trigger { name } => format!("trigger/{database}/{name}"),
            Self::ScheduledJob { name } => format!("schedule/{database}/{name}"),
            Self::Alert { name } => format!("alert/{database}/{name}"),
            Self::CrossShardApply => format!("cross-shard/{database}"),
        }
    }

    /// The error for a transaction-control statement inside this body.
    pub(super) fn refuse_transaction_control(&self, statement: &str) -> crate::Error {
        crate::Error::NotInTransactionBlock {
            statement: format!("{statement} in {self}"),
        }
    }
}

impl std::fmt::Display for AtomicBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Trigger { name } => write!(f, "the body of trigger '{name}'"),
            Self::ScheduledJob { name } => write!(f, "the body of schedule '{name}'"),
            Self::Alert { name } => write!(f, "the history write of alert '{name}'"),
            Self::CrossShardApply => f.write_str("a cross-shard trigger write"),
        }
    }
}
