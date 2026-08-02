// SPDX-License-Identifier: BUSL-1.1

//! Request routing: maps a decoded [`NativeRequest`](nodedb_types::protocol::NativeRequest)
//! to the appropriate handler by opcode.

use nodedb_types::protocol::{AuthMethod, NativeResponse, OpCode, RequestFields, TextFields};

use super::NativeSession;
use super::dispatch::{self, DispatchCtx};
use crate::config::auth::AuthMode;

impl NativeSession {
    /// Route a decoded request to the appropriate handler.
    ///
    /// Returns a [`SqlOutcome`](dispatch::SqlOutcome): every op produces a materialized
    /// `SqlOutcome::Response` except an eligible streamable SELECT on the
    /// `Sql`/`Ddl` path, which yields `SqlOutcome::Stream` for the run loop to
    /// emit as multiple frames.
    pub(super) async fn handle_request(
        &mut self,
        req: nodedb_types::protocol::NativeRequest,
    ) -> dispatch::SqlOutcome {
        use dispatch::SqlOutcome;
        let seq = req.seq;
        let op = req.op;

        // Auth handling.
        if op == OpCode::Auth {
            return SqlOutcome::Response(Box::new(self.handle_auth(seq, &req.fields).await));
        }

        // Ping requires no auth.
        if op == OpCode::Ping {
            return SqlOutcome::Response(Box::new(dispatch::handle_ping(seq)));
        }

        // Status requires no auth — returns current startup phase.
        if op == OpCode::Status {
            // A permanently wedged metadata applier happens after a clean boot,
            // so the startup gate still reads Ok. Report Failed anyway — both
            // status surfaces must agree that this node cannot serve.
            let native_status = if self.state.metadata_apply_wedge.is_wedged() {
                crate::control::startup::health::NativeStatus::Failed
            } else {
                let health = crate::control::startup::health::observe(&self.state.startup);
                crate::control::startup::health::to_native_status(&health)
            };
            return SqlOutcome::Response(Box::new(NativeResponse::status_row(
                seq,
                native_status.to_string(),
            )));
        }

        // All other ops require authentication.
        if self.identity.is_none() {
            if self.auth_mode == AuthMode::Trust {
                let Some(trust_id) =
                    super::super::super::session_auth::configured_trust_identity(&self.state)
                else {
                    return SqlOutcome::Response(Box::new(NativeResponse::error(
                        seq,
                        "28000",
                        "configured trust identity is unavailable",
                    )));
                };

                // Auto-authenticate through the normal Auth path so trust-mode
                // requests receive the same database selection, authorization,
                // admission permits, session binding, and AuthContext as an
                // explicit Auth frame. The successful internal response is not
                // sent: this request receives only its own operation response.
                let auth_fields = RequestFields::Text(TextFields {
                    auth: Some(AuthMethod::Trust {
                        username: trust_id.username,
                    }),
                    ..Default::default()
                });
                let auth_response = self.handle_auth(seq, &auth_fields).await;
                if !matches!(
                    auth_response.status,
                    nodedb_types::protocol::opcodes::ResponseStatus::Ok
                ) {
                    return SqlOutcome::Response(Box::new(auth_response));
                }
            } else {
                return SqlOutcome::Response(Box::new(NativeResponse::error(
                    seq,
                    "28000",
                    "not authenticated. Send Auth request first.",
                )));
            }
        }

        let identity = match self.identity.as_ref() {
            Some(id) => id,
            None => {
                return SqlOutcome::Response(Box::new(NativeResponse::error(
                    seq,
                    "28000",
                    "not authenticated",
                )));
            }
        };

        // Retain authenticated/session enrichment, but bind RLS to the database
        // selected for this request (including a later `USE DATABASE`).
        let mut auth_ctx = self
            .auth_context
            .clone()
            .unwrap_or_else(|| super::super::super::session_auth::build_auth_context(identity));
        auth_ctx.database_id = Some(
            self.sessions
                .get_current_database(self.peer_addr)
                .unwrap_or(crate::types::DatabaseId::DEFAULT),
        );

        let ctx = DispatchCtx {
            state: &self.state,
            identity,
            auth_context: &auth_ctx,
            query_ctx: &self.query_ctx,
            sessions: &self.sessions,
            peer_addr: &self.peer_addr,
        };

        let fields = match &req.fields {
            RequestFields::Text(f) => f,
            _ => {
                return SqlOutcome::Response(Box::new(NativeResponse::error(
                    seq,
                    "0A000",
                    "unsupported request field format for this server version",
                )));
            }
        };

        // SQL / DDL is the only path that can stream — handle it before the
        // materialized `match op` below so its `SqlOutcome` flows up unchanged.
        if matches!(op, OpCode::Sql | OpCode::Ddl) {
            let sql = match &fields.sql {
                Some(s) => s.as_str(),
                None => {
                    return SqlOutcome::Response(Box::new(NativeResponse::error(
                        seq,
                        "42601",
                        "missing 'sql' field",
                    )));
                }
            };
            return dispatch::handle_sql_streaming(&ctx, seq, sql, fields.sql_params.as_deref())
                .await;
        }

        let response = match op {
            // SQL handled above (streaming-capable).
            OpCode::Sql | OpCode::Ddl => unreachable!("SQL/DDL handled before this match"),

            // Session parameters.
            OpCode::Set => {
                let key = match &fields.key {
                    Some(k) => k.as_str(),
                    None => {
                        // Also support SET via sql field: "SET key = value"
                        if let Some(sql) = &fields.sql {
                            return SqlOutcome::Response(Box::new(
                                dispatch::handle_sql(&ctx, seq, sql, None).await,
                            ));
                        }
                        return SqlOutcome::Response(Box::new(NativeResponse::error(
                            seq,
                            "42601",
                            "missing 'key' field",
                        )));
                    }
                };
                let value = fields.value.as_deref().unwrap_or("");
                dispatch::handle_set(&ctx, seq, key, value)
            }
            OpCode::Show => {
                let key = match &fields.key {
                    Some(k) => k.as_str(),
                    None => {
                        if let Some(sql) = &fields.sql {
                            return SqlOutcome::Response(Box::new(
                                dispatch::handle_sql(&ctx, seq, sql, None).await,
                            ));
                        }
                        return SqlOutcome::Response(Box::new(NativeResponse::error(
                            seq,
                            "42601",
                            "missing 'key' field",
                        )));
                    }
                };
                dispatch::handle_show(&ctx, seq, key)
            }
            OpCode::Reset => {
                let key = match &fields.key {
                    Some(k) => k.as_str(),
                    None => {
                        return SqlOutcome::Response(Box::new(NativeResponse::error(
                            seq,
                            "42601",
                            "missing 'key' field",
                        )));
                    }
                };
                dispatch::handle_reset(&ctx, seq, key)
            }

            // Transaction control.
            OpCode::Begin => dispatch::handle_begin(&ctx, seq),
            OpCode::Commit => dispatch::handle_commit(&ctx, seq).await,
            OpCode::Rollback => dispatch::handle_rollback(&ctx, seq).await,

            // Explain.
            OpCode::Explain => {
                let sql = match &fields.sql {
                    Some(s) => s.as_str(),
                    None => {
                        return SqlOutcome::Response(Box::new(NativeResponse::error(
                            seq,
                            "42601",
                            "missing 'sql' field",
                        )));
                    }
                };
                dispatch::handle_sql(&ctx, seq, &format!("EXPLAIN {sql}"), None).await
            }

            // Direct Data Plane operations.
            OpCode::PointGet
            | OpCode::PointPut
            | OpCode::PointDelete
            | OpCode::VectorSearch
            | OpCode::RangeScan
            | OpCode::CrdtRead
            | OpCode::CrdtApply
            | OpCode::GraphRagFusion
            | OpCode::AlterCollectionPolicy
            | OpCode::GraphHop
            | OpCode::GraphNeighbors
            | OpCode::GraphPath
            | OpCode::GraphSubgraph
            | OpCode::EdgePut
            | OpCode::EdgeDelete
            | OpCode::TextSearch
            | OpCode::HybridSearch
            | OpCode::SpatialScan
            | OpCode::TimeseriesScan
            | OpCode::TimeseriesIngest
            | OpCode::KvScan
            | OpCode::KvExpire
            | OpCode::KvPersist
            | OpCode::KvGetTtl
            | OpCode::KvBatchGet
            | OpCode::KvBatchPut
            | OpCode::KvFieldGet
            | OpCode::KvFieldSet
            | OpCode::DocumentUpdate
            | OpCode::DocumentScan
            | OpCode::DocumentUpsert
            | OpCode::DocumentBulkUpdate
            | OpCode::DocumentBulkDelete
            | OpCode::VectorInsert
            | OpCode::VectorMultiSearch
            | OpCode::VectorDelete
            | OpCode::GraphAlgo
            | OpCode::ColumnarScan
            | OpCode::ColumnarInsert
            | OpCode::RecursiveScan
            | OpCode::DocumentTruncate
            | OpCode::DocumentEstimateCount
            | OpCode::DocumentInsertSelect
            | OpCode::DocumentRegister
            | OpCode::DocumentDropIndex
            | OpCode::KvRegisterIndex
            | OpCode::KvDropIndex
            | OpCode::KvTruncate
            | OpCode::VectorSetParams
            | OpCode::KvIncr
            | OpCode::KvIncrFloat
            | OpCode::KvCas
            | OpCode::KvGetSet
            | OpCode::KvRegisterSortedIndex
            | OpCode::KvDropSortedIndex
            | OpCode::KvSortedIndexRank
            | OpCode::KvSortedIndexTopK
            | OpCode::KvSortedIndexRange
            | OpCode::KvSortedIndexCount
            | OpCode::KvSortedIndexScore
            | OpCode::CrdtListInsert
            | OpCode::CrdtListDelete
            | OpCode::CrdtListMove => dispatch::handle_direct_op(&ctx, seq, op, fields).await,

            // MATCH: dedicated path that unwraps the DP `{rows, frontier}`
            // envelope into the bare rows array the native row decoder expects.
            OpCode::GraphMatch => dispatch::handle_graph_match(&ctx, seq, fields).await,

            // Batch ops: direct Data Plane dispatch.
            OpCode::VectorBatchInsert | OpCode::DocumentBatchInsert => {
                dispatch::handle_direct_op(&ctx, seq, op, fields).await
            }

            // Copy from file.
            OpCode::CopyFrom => {
                let sql = match &fields.sql {
                    Some(s) => s.as_str(),
                    None => {
                        return SqlOutcome::Response(Box::new(NativeResponse::error(
                            seq,
                            "42601",
                            "missing 'sql' field",
                        )));
                    }
                };
                dispatch::handle_sql(&ctx, seq, sql, None).await
            }

            // Auth/Ping/Status handled above.
            OpCode::Auth | OpCode::Ping | OpCode::Status => unreachable!(),
            // OpCode is #[non_exhaustive]; future opcodes that reach this
            // handler before session.rs is updated return a typed error.
            _ => NativeResponse::error(seq, "0A000", "opcode not supported by this server version"),
        };

        SqlOutcome::Response(Box::new(response))
    }
}
