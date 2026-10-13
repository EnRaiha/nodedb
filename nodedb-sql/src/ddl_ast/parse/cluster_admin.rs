// SPDX-License-Identifier: Apache-2.0

//! Parse cluster-admin: SHOW CLUSTER/NODES/RAFT/MIGRATIONS/RANGES/ROUTING,
//! REMOVE NODE, REBALANCE, ALTER RAFT GROUP.

use super::clause::{ClauseCursor, tokenize};
use crate::ddl_ast::statement::{ClusterStmt, NodedbStatement};
use crate::error::SqlError;

pub(super) fn try_parse(
    upper: &str,
    parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    if upper.starts_with("ALTER RAFT GROUP ") {
        return Some(parse_alter_raft_group(trimmed).map(NodedbStatement::Cluster));
    }
    (|| -> Option<NodedbStatement> {
        if upper.starts_with("SHOW CLUSTER") {
            return Some(NodedbStatement::Cluster(ClusterStmt::ShowCluster));
        }
        if upper.starts_with("SHOW MIGRATIONS") {
            return Some(NodedbStatement::Cluster(ClusterStmt::ShowMigrations));
        }
        if upper.starts_with("SHOW RANGES") {
            return Some(NodedbStatement::Cluster(ClusterStmt::ShowRanges));
        }
        if upper.starts_with("SHOW ROUTING") {
            return Some(NodedbStatement::Cluster(ClusterStmt::ShowRouting));
        }
        if upper.starts_with("SHOW SCHEMA VERSION") {
            return Some(NodedbStatement::Cluster(ClusterStmt::ShowSchemaVersion));
        }
        if upper.starts_with("SHOW PEER HEALTH") {
            return Some(NodedbStatement::Cluster(ClusterStmt::ShowPeerHealth));
        }
        if upper.starts_with("REBALANCE") {
            return Some(NodedbStatement::Cluster(ClusterStmt::Rebalance));
        }
        if upper.starts_with("SHOW RAFT GROUP ") {
            let id = parts.get(3)?.to_string();
            return Some(NodedbStatement::Cluster(ClusterStmt::ShowRaftGroup {
                group_id: id,
            }));
        }
        if upper.starts_with("SHOW RAFT GROUPS") || upper.starts_with("SHOW RAFT") {
            return Some(NodedbStatement::Cluster(ClusterStmt::ShowRaftGroups));
        }
        if upper.starts_with("REMOVE NODE ") {
            let id = parts.get(2)?.to_string();
            return Some(NodedbStatement::Cluster(ClusterStmt::RemoveNode {
                node_id: id,
            }));
        }
        if upper.starts_with("SHOW NODE ") {
            let id = parts.get(2)?.to_string();
            return Some(NodedbStatement::Cluster(ClusterStmt::ShowNode {
                node_id: id,
            }));
        }
        if upper.starts_with("SHOW NODES") {
            return Some(NodedbStatement::Cluster(ClusterStmt::ShowNodes));
        }
        None
    })()
    .map(Ok)
}

/// `ALTER RAFT GROUP <group_id> ADD|REMOVE NODE <node_id>`. Every token is
/// read at its position; a misspelled `NODE` or a trailing word is refused.
fn parse_alter_raft_group(sql: &str) -> Result<ClusterStmt, SqlError> {
    let toks = tokenize(sql);
    let mut c = ClauseCursor::new(sql, &toks, "ALTER RAFT GROUP");
    c.expect_keywords(&["ALTER", "RAFT", "GROUP"])?;
    let group_id = c.raw("group id")?.to_string();
    let action = c.one_of("action", &["ADD", "REMOVE"])?.to_string();
    c.expect_keyword("NODE")?;
    let node_id = c.raw("node id")?.to_string();
    c.finish()?;
    Ok(ClusterStmt::AlterRaftGroup {
        group_id,
        action,
        node_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alter_raft_group_reads_each_token_at_its_position() {
        assert_eq!(
            parse_alter_raft_group("ALTER RAFT GROUP 7 ADD NODE 3").expect("parses"),
            ClusterStmt::AlterRaftGroup {
                group_id: "7".into(),
                action: "ADD".into(),
                node_id: "3".into(),
            }
        );
        assert!(parse_alter_raft_group("ALTER RAFT GROUP 7 ADD NOODE 3").is_err());
        assert!(parse_alter_raft_group("ALTER RAFT GROUP 7 ADD NODE 3 FORCE").is_err());
    }
}
