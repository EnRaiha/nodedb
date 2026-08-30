//! Client-side native protocol handshake (HelloFrame / HelloAckFrame exchange).

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::protocol::{
    CAP_COLUMNAR, CAP_CRDT, CAP_FTS, CAP_GRAPHRAG, CAP_SPATIAL, CAP_STREAMING, CAP_TIMESERIES,
    HelloAckFrame, HelloFrame, PROTO_VERSION,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::NativeConnection;
use super::response::io_err;

impl NativeConnection {
    /// Perform the native protocol handshake.
    pub async fn perform_client_handshake(&mut self) -> NodeDbResult<()> {
        let client_caps = CAP_STREAMING
            | CAP_GRAPHRAG
            | CAP_FTS
            | CAP_CRDT
            | CAP_SPATIAL
            | CAP_TIMESERIES
            | CAP_COLUMNAR;

        let hello = HelloFrame {
            proto_min: 1,
            proto_max: PROTO_VERSION,
            capabilities: client_caps,
        };

        let payload = hello.encode();
        self.stream.write_all(&payload).await.map_err(io_err)?;
        self.stream.flush().await.map_err(io_err)?;

        let mut magic_buf = [0u8; 4];
        self.stream
            .read_exact(&mut magic_buf)
            .await
            .map_err(io_err)?;

        let magic = u32::from_be_bytes(magic_buf);

        if magic == nodedb_types::protocol::HELLO_ERROR_MAGIC_U32 {
            let mut header = [0u8; 2];
            self.stream.read_exact(&mut header).await.map_err(io_err)?;
            let msg_len = header[1] as usize;
            let mut msg_bytes = vec![0u8; msg_len];
            self.stream
                .read_exact(&mut msg_bytes)
                .await
                .map_err(io_err)?;

            let code = match header[0] {
                0 => nodedb_types::protocol::HelloErrorCode::BadMagic,
                1 => nodedb_types::protocol::HelloErrorCode::VersionMismatch,
                _ => nodedb_types::protocol::HelloErrorCode::Malformed,
            };
            let message = String::from_utf8_lossy(&msg_bytes).into_owned();
            return Err(NodeDbError::handshake_failed(code, message));
        }

        if magic != nodedb_types::protocol::HELLO_ACK_MAGIC {
            return Err(NodeDbError::internal(format!(
                "HelloAck magic mismatch: expected {:#010x}, got {:#010x}",
                nodedb_types::protocol::HELLO_ACK_MAGIC,
                magic,
            )));
        }

        let mut fixed_rest = [0u8; 11];
        self.stream
            .read_exact(&mut fixed_rest)
            .await
            .map_err(io_err)?;
        let sv_len = fixed_rest[10] as usize;
        let var_len = sv_len + 1 + 7 * 5;
        let mut var_buf = vec![0u8; var_len];
        self.stream.read_exact(&mut var_buf).await.map_err(io_err)?;

        let mut ack_buf = Vec::with_capacity(4 + 11 + var_len);
        ack_buf.extend_from_slice(&magic_buf);
        ack_buf.extend_from_slice(&fixed_rest);
        ack_buf.extend_from_slice(&var_buf);

        let ack = HelloAckFrame::decode(&ack_buf)
            .ok_or_else(|| NodeDbError::internal("failed to decode HelloAckFrame from server"))?;

        self.proto_version = ack.proto_version;
        self.capabilities = ack.capabilities;
        self.server_version = ack.server_version;
        self.limits = ack.limits;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::error::NodeDbResult;
    use nodedb_types::protocol::{
        CAP_MSGPACK, CAP_STREAMING, HELLO_ACK_MAGIC, HELLO_MAGIC, HelloAckFrame, HelloFrame,
        Limits, PROTO_VERSION,
    };

    use super::io_err;

    #[tokio::test]
    async fn client_handshake_succeeds_when_versions_match() {
        use tokio::io::{AsyncWriteExt, duplex};

        let (mut server_half, mut client_half) = duplex(4096);

        let server_task = tokio::spawn(async move {
            let mut hello_buf = [0u8; HelloFrame::WIRE_SIZE];
            tokio::io::AsyncReadExt::read_exact(&mut server_half, &mut hello_buf)
                .await
                .unwrap();
            let magic =
                u32::from_be_bytes([hello_buf[0], hello_buf[1], hello_buf[2], hello_buf[3]]);
            assert_eq!(magic, HELLO_MAGIC, "client sent correct HelloFrame magic");

            let ack = HelloAckFrame {
                proto_version: 1,
                capabilities: CAP_STREAMING | CAP_MSGPACK,
                server_version: "NodeDB/test".into(),
                limits: Limits::default(),
            };
            server_half.write_all(&ack.encode()).await.unwrap();
            server_half.flush().await.unwrap();
        });

        let result = handshake_on_duplex(&mut client_half).await;
        server_task.await.unwrap();

        assert!(result.is_ok(), "expected Ok, got {result:?}");
        let (proto_version, server_version) = result.unwrap();
        assert_eq!(proto_version, 1);
        assert!(server_version.contains("NodeDB"));
    }

    #[tokio::test]
    async fn client_handshake_returns_typed_error_on_version_mismatch() {
        use nodedb_types::protocol::{HelloErrorCode, HelloErrorFrame};
        use tokio::io::{AsyncWriteExt, duplex};

        let (mut server_half, mut client_half) = duplex(4096);

        let server_task = tokio::spawn(async move {
            let mut hello_buf = [0u8; HelloFrame::WIRE_SIZE];
            tokio::io::AsyncReadExt::read_exact(&mut server_half, &mut hello_buf)
                .await
                .unwrap();
            let err_frame = HelloErrorFrame {
                code: HelloErrorCode::VersionMismatch,
                message: "version mismatch".into(),
            };
            server_half.write_all(&err_frame.encode()).await.unwrap();
            server_half.flush().await.unwrap();
        });

        let result = handshake_on_duplex(&mut client_half).await;
        server_task.await.unwrap();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            format!("{err}").contains("version mismatch") || format!("{err}").contains("handshake")
        );
    }

    /// Drive the client-side handshake on a raw `AsyncRead + AsyncWrite` stream (for testing).
    async fn handshake_on_duplex<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
        stream: &mut S,
    ) -> NodeDbResult<(u16, String)> {
        use nodedb_types::error::NodeDbError;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let client_caps = CAP_STREAMING | CAP_MSGPACK;
        let hello = HelloFrame {
            proto_min: 1,
            proto_max: PROTO_VERSION,
            capabilities: client_caps,
        };

        let payload = hello.encode();
        stream.write_all(&payload).await.map_err(io_err)?;
        stream.flush().await.map_err(io_err)?;

        let mut magic_buf = [0u8; 4];
        stream.read_exact(&mut magic_buf).await.map_err(io_err)?;

        let magic = u32::from_be_bytes(magic_buf);

        if magic == nodedb_types::protocol::HELLO_ERROR_MAGIC_U32 {
            let mut header = [0u8; 2];
            stream.read_exact(&mut header).await.map_err(io_err)?;
            let msg_len = header[1] as usize;
            let mut msg_bytes = vec![0u8; msg_len];
            stream.read_exact(&mut msg_bytes).await.map_err(io_err)?;
            let code = match header[0] {
                0 => nodedb_types::protocol::HelloErrorCode::BadMagic,
                1 => nodedb_types::protocol::HelloErrorCode::VersionMismatch,
                _ => nodedb_types::protocol::HelloErrorCode::Malformed,
            };
            let message = String::from_utf8_lossy(&msg_bytes).into_owned();
            return Err(NodeDbError::handshake_failed(code, message));
        }

        if magic != HELLO_ACK_MAGIC {
            return Err(NodeDbError::internal(format!(
                "HelloAck magic mismatch: {magic:#010x}"
            )));
        }

        let mut fixed_rest = [0u8; 11];
        stream.read_exact(&mut fixed_rest).await.map_err(io_err)?;
        let sv_len = fixed_rest[10] as usize;
        let var_len = sv_len + 1 + 7 * 5;
        let mut var_buf = vec![0u8; var_len];
        stream.read_exact(&mut var_buf).await.map_err(io_err)?;

        let mut ack_buf = Vec::with_capacity(4 + 11 + var_len);
        ack_buf.extend_from_slice(&magic_buf);
        ack_buf.extend_from_slice(&fixed_rest);
        ack_buf.extend_from_slice(&var_buf);

        let ack = HelloAckFrame::decode(&ack_buf)
            .ok_or_else(|| NodeDbError::internal("failed to decode HelloAckFrame"))?;
        Ok((ack.proto_version, ack.server_version))
    }
}
