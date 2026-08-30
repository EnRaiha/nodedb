//! Session-level RPCs: auth, ping, SQL/DDL execution, transactions, parameters.

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::protocol::{AuthMethod, OpCode, ResponseStatus, TextFields};
use nodedb_types::result::QueryResult;

use super::NativeConnection;
use super::response::{check_error, response_to_query_result};

impl NativeConnection {
    /// Authenticate with the server.
    ///
    /// `database` — optional target database name. When set it is sent in
    /// the auth frame so the server can bind the connection's database
    /// context at handshake time (equivalent to `psql -d <name>`).
    pub async fn authenticate(
        &mut self,
        method: AuthMethod,
        database: Option<&str>,
    ) -> NodeDbResult<()> {
        let resp = self
            .send(
                OpCode::Auth,
                TextFields {
                    auth: Some(method),
                    database: database.map(|s| s.to_string()),
                    ..Default::default()
                },
            )
            .await?;

        if resp.status == ResponseStatus::Error {
            let msg = resp
                .error
                .map(|e| e.message)
                .unwrap_or_else(|| "auth failed".into());
            return Err(NodeDbError::authorization_denied(msg));
        }

        self.authenticated = true;
        Ok(())
    }

    /// Send a ping and await the pong.
    pub async fn ping(&mut self) -> NodeDbResult<()> {
        let resp = self.send(OpCode::Ping, TextFields::default()).await?;
        if resp.status == ResponseStatus::Error {
            return Err(NodeDbError::internal("ping failed"));
        }
        Ok(())
    }

    /// Execute a SQL query and return the result.
    pub async fn execute_sql(&mut self, sql: &str) -> NodeDbResult<QueryResult> {
        let resp = self
            .send(
                OpCode::Sql,
                TextFields {
                    sql: Some(sql.to_string()),
                    ..Default::default()
                },
            )
            .await?;
        response_to_query_result(resp)
    }

    /// Execute a SQL query with bound parameters and return the result.
    ///
    /// Bound parameters travel through `TextFields::sql_params` as a
    /// `Vec<Value>` — zerompk's generic array encoding serialises each
    /// element via `Value`'s hand-rolled `ToMessagePack` impl so the
    /// canonical scalar variants round-trip without a lossy JSON step.
    /// The server inlines each value as a SQL literal before planning,
    /// so `$1`, `$2`, … placeholders resolve to the caller's values.
    /// Empty `params` routes through the same opcode but omits the
    /// field, equivalent to `execute_sql`.
    pub async fn execute_sql_with_params(
        &mut self,
        sql: &str,
        params: &[nodedb_types::Value],
    ) -> NodeDbResult<QueryResult> {
        let sql_params = if params.is_empty() {
            None
        } else {
            Some(params.to_vec())
        };
        let resp = self
            .send(
                OpCode::Sql,
                TextFields {
                    sql: Some(sql.to_string()),
                    sql_params,
                    ..Default::default()
                },
            )
            .await?;
        response_to_query_result(resp)
    }

    /// Execute a DDL command.
    pub async fn execute_ddl(&mut self, sql: &str) -> NodeDbResult<QueryResult> {
        let resp = self
            .send(
                OpCode::Ddl,
                TextFields {
                    sql: Some(sql.to_string()),
                    ..Default::default()
                },
            )
            .await?;
        response_to_query_result(resp)
    }

    /// Begin a transaction.
    pub async fn begin(&mut self) -> NodeDbResult<()> {
        let resp = self.send(OpCode::Begin, TextFields::default()).await?;
        check_error(&resp)
    }

    /// Commit the current transaction.
    pub async fn commit(&mut self) -> NodeDbResult<()> {
        let resp = self.send(OpCode::Commit, TextFields::default()).await?;
        check_error(&resp)
    }

    /// Rollback the current transaction.
    pub async fn rollback(&mut self) -> NodeDbResult<()> {
        let resp = self.send(OpCode::Rollback, TextFields::default()).await?;
        check_error(&resp)
    }

    /// Set a session parameter.
    pub async fn set_parameter(&mut self, key: &str, value: &str) -> NodeDbResult<()> {
        let resp = self
            .send(
                OpCode::Set,
                TextFields {
                    key: Some(key.to_string()),
                    value: Some(value.to_string()),
                    ..Default::default()
                },
            )
            .await?;
        check_error(&resp)
    }

    /// Show a session parameter.
    pub async fn show_parameter(&mut self, key: &str) -> NodeDbResult<String> {
        let resp = self
            .send(
                OpCode::Show,
                TextFields {
                    key: Some(key.to_string()),
                    ..Default::default()
                },
            )
            .await?;
        if resp.status == ResponseStatus::Error {
            let msg = resp
                .error
                .map(|e| e.message)
                .unwrap_or_else(|| "show failed".into());
            return Err(NodeDbError::internal(msg));
        }
        let value = resp
            .rows
            .and_then(|rows| rows.into_iter().next())
            .and_then(|row| row.into_iter().next())
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_default();
        Ok(value)
    }
}
