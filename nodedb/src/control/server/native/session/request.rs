// SPDX-License-Identifier: BUSL-1.1

//! Request routing: maps a decoded [`NativeRequest`](nodedb_types::protocol::NativeRequest)
//! to the appropriate handler by opcode.

use nodedb_types::protocol::{NativeResponse, OpCode, RequestFields};

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
            let health = crate::control::startup::health::observe(&self.state.startup);
            let native_status = crate::control::startup::health::to_native_status(&health);
            return SqlOutcome::Response(Box::new(NativeResponse::status_row(
                seq,
                native_status.to_string(),
            )));
        }

        // All other ops require authentication.
        if self.identity.is_none() {
            if self.auth_mode == AuthMode::Trust {
                let trust_id =
                    super::super::super::session_auth::trust_identity(&self.state, "anonymous");
                self.auth_context = Some(super::super::super::session_auth::build_auth_context(
                    &trust_id,
                ));
                self.identity = Some(trust_id);
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

        // Build a default AuthContext if not yet set (shouldn't happen but be safe).
        let default_auth_ctx;
        let auth_ctx = match self.auth_context.as_ref() {
            Some(ctx) => ctx,
            None => {
                default_auth_ctx = super::super::super::session_auth::build_auth_context(identity);
                &default_auth_ctx
            }
        };

        let ctx = DispatchCtx {
            state: &self.state,
            identity,
            auth_context: auth_ctx,
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
