// SPDX-License-Identifier: BUSL-1.1

//! Vector clock sync + ping/pong.

use std::time::Instant;

use tracing::debug;

use super::super::wire::*;
use super::state::SyncSession;

impl SyncSession {
    /// Update the server's view of the client's clock and return the
    /// server's current clock.
    pub fn handle_vector_clock_sync(&mut self, msg: &VectorClockSyncMsg) -> Option<SyncFrame> {
        self.last_activity = Instant::now();
        for (collection, lsn) in &msg.clocks {
            self.server_clock
                .entry(collection.clone())
                .and_modify(|v| *v = (*v).max(*lsn))
                .or_insert(*lsn);
        }
        debug!(
            session = %self.session_id,
            collections = msg.clocks.len(),
            "vector clock sync"
        );
        let response = VectorClockSyncMsg {
            clocks: self.server_clock.clone(),
            sender_id: 0,
        };
        SyncFrame::try_encode(SyncMessageType::VectorClockSync, &response)
    }

    pub fn handle_ping(&mut self, msg: &PingPongMsg) -> Option<SyncFrame> {
        self.last_activity = Instant::now();
        let pong = PingPongMsg {
            timestamp_ms: msg.timestamp_ms,
            is_pong: true,
        };
        SyncFrame::try_encode(SyncMessageType::PingPong, &pong)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_session() -> SyncSession {
        SyncSession::new("test-session-1".into())
    }

    #[test]
    fn ping_pong() {
        let mut session = make_session();

        let ping = PingPongMsg {
            timestamp_ms: 99999,
            is_pong: false,
        };
        let response = session.handle_ping(&ping).expect("ping response");
        let pong: PingPongMsg = response.decode_body().unwrap();
        assert!(pong.is_pong);
        assert_eq!(pong.timestamp_ms, 99999);
    }

    #[test]
    fn vector_clock_sync() {
        let mut session = make_session();
        session.authenticated = true;

        let mut clocks = HashMap::new();
        clocks.insert("orders".into(), 42u64);

        let msg = VectorClockSyncMsg {
            clocks,
            sender_id: 5,
        };
        let response = session
            .handle_vector_clock_sync(&msg)
            .expect("clock sync response");
        let sync: VectorClockSyncMsg = response.decode_body().unwrap();
        assert_eq!(*sync.clocks.get("orders").unwrap(), 42);
    }
}
