//! Request framing, sequence correlation, and partial-frame reassembly.

use std::sync::atomic::Ordering;

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::protocol::{
    FRAME_HEADER_LEN, MAX_FRAME_SIZE, NativeRequest, NativeResponse, OpCode, RequestFields,
    ResponseStatus, TextFields,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::NativeConnection;
use super::response::io_err;

impl NativeConnection {
    pub(super) fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Send a request and read the response.
    pub(crate) async fn send(
        &mut self,
        op: OpCode,
        fields: TextFields,
    ) -> NodeDbResult<NativeResponse> {
        let req_seq = self.next_seq();
        let req = NativeRequest {
            op,
            seq: req_seq,
            fields: RequestFields::Text(fields),
        };

        let payload = zerompk::to_msgpack_vec(&req)
            .map_err(|e| NodeDbError::serialization("msgpack", format!("request encode: {e}")))?;

        let len = payload.len() as u32;
        self.stream
            .write_all(&len.to_be_bytes())
            .await
            .map_err(io_err)?;
        self.stream.write_all(&payload).await.map_err(io_err)?;
        self.stream.flush().await.map_err(io_err)?;

        let mut combined_rows: Vec<Vec<nodedb_types::Value>> = Vec::new();
        let mut partial_columns: Option<Vec<String>> = None;

        let final_resp = loop {
            let mut len_buf = [0u8; FRAME_HEADER_LEN];
            self.stream.read_exact(&mut len_buf).await.map_err(io_err)?;
            let resp_len = u32::from_be_bytes(len_buf);
            if resp_len > MAX_FRAME_SIZE {
                return Err(NodeDbError::internal(format!(
                    "response frame too large: {resp_len}"
                )));
            }

            let mut resp_buf = vec![0u8; resp_len as usize];
            self.stream
                .read_exact(&mut resp_buf)
                .await
                .map_err(io_err)?;

            let resp: NativeResponse = zerompk::from_msgpack(&resp_buf).map_err(|e| {
                NodeDbError::serialization("msgpack", format!("response decode: {e}"))
            })?;

            if resp.seq != req_seq {
                // A fan-out query for a preceding request can leave stale
                // trailing frames on the wire after that request's terminal
                // frame was already returned to its caller. Discard any
                // frame that doesn't belong to this request rather than
                // misattributing it — never surface another request's rows
                // or status as this request's response.
                tracing::warn!(
                    expected_seq = req_seq,
                    got_seq = resp.seq,
                    "native connection: discarding stale response frame"
                );
                continue;
            }

            if resp.status == ResponseStatus::Partial {
                if partial_columns.is_none() {
                    partial_columns = resp.columns;
                }
                if let Some(rows) = resp.rows {
                    combined_rows.extend(rows);
                }
                continue;
            }

            // The terminal frame owns status and all terminal metadata. In
            // particular, never turn a stream error into success merely
            // because earlier partial rows were received.
            if resp.status == ResponseStatus::Error {
                break resp;
            }
            let mut terminal = resp;
            if let Some(rows) = terminal.rows.take() {
                combined_rows.extend(rows);
            }
            if !combined_rows.is_empty() {
                terminal.rows = Some(combined_rows);
            }
            if terminal.columns.is_none() {
                terminal.columns = partial_columns;
            }
            break terminal;
        };

        Ok(final_resp)
    }
}
