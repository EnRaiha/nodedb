// SPDX-License-Identifier: BUSL-1.1
//! End-to-end cluster test: CREATE / INSERT / SELECT across 3 pgwire
//! clients, one per node.
//!
//! Acceptance gate for the replicated catalog path. Replays the
//! production failure mode that motivated it:
//!
//! > CREATE COLLECTION on node 1, SELECT on node 2 → "unknown table"
//!
//! Tests are split by concern in `sql_cluster_cross_node_dml_tests/`.

mod common;

#[path = "sql_cluster_cross_node_dml_tests/auth_objects.rs"]
mod auth_objects;
#[path = "sql_cluster_cross_node_dml_tests/cluster_boot.rs"]
mod cluster_boot;
#[path = "sql_cluster_cross_node_dml_tests/ddl_objects.rs"]
mod ddl_objects;
#[path = "sql_cluster_cross_node_dml_tests/gather_cross_node.rs"]
mod gather_cross_node;
#[path = "sql_cluster_cross_node_dml_tests/schema_objects.rs"]
mod schema_objects;
