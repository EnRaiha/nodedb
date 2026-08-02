// SPDX-License-Identifier: BUSL-1.1

//! Leader-side servicing of hot-key read reservations.

use tracing::{debug, warn};

use crate::calvin::sequencer::entry::SequencerEntry;
use crate::calvin::sequencer::reservation_inbox::ReservationRequest;
use crate::calvin::types::TxnIdWire;

use super::core::{RESERVATION_POSITION_BAND, SequencerService};

impl SequencerService {
    /// Service every pending hot-key read-reservation request.
    ///
    /// Leader-only: the caller gates on `is_leader` and passes the epoch seed —
    /// `None` while the sequencer group is still replaying. For a `Reserve` with
    /// no owner this mints a stable `R = (epoch, position)` in the
    /// reservation band and proposes a `ReserveRead` entry; for a `Reserve` with
    /// an existing owner it echoes that id (no mint) and proposes an additional
    /// `ReserveRead` under it. `Release` fires a `ReleaseReservation` entry.
    ///
    /// Only the fresh mint needs `epoch` — a minted id is stamped with it and is
    /// subject to the same no-collision invariant as batch positions. Echoes and
    /// releases carry an identity that was already assigned, so they run
    /// unchanged while the seed is pending: holding a release back would keep a
    /// hot key reserved until lease GC even though this node is the leader and
    /// nothing about the release is unsafe. A mint that cannot be served drops
    /// its `reply`, which degrades that one caller to plain OCC immediately
    /// instead of parking it for the length of the replay.
    ///
    /// Minting reads only `epoch` plus the local band counter and ships the
    /// resulting id on the wire entry — replicas never recompute it, exactly
    /// like the batch position path in `validate_batch_with_assignments`.
    /// No wall-clock, no per-replica divergence.
    pub(super) fn process_reservations(&mut self, epoch: Option<u64>) {
        let mut requests: Vec<ReservationRequest> = Vec::new();
        self.reservation_receiver.drain_into(&mut requests);

        for request in requests {
            match request {
                ReservationRequest::Reserve {
                    key,
                    vshard,
                    owner,
                    reply,
                } => {
                    let owner_id = match owner {
                        // Reserve an additional key under an existing R: echo it,
                        // no mint.
                        Some(existing) => existing,
                        // Mint a fresh R for a new interactive txn.
                        None => {
                            // No seeded epoch yet: refuse this one mint rather
                            // than stamping an id under an epoch that may
                            // collide with committed history. Dropping `reply`
                            // degrades the caller to OCC now; a later tick mints
                            // normally once the group has replayed.
                            let Some(epoch) = epoch else {
                                debug!(
                                    "sequencer epoch seed still pending; refusing to mint a \
                                     reservation id, caller falls back to OCC"
                                );
                                drop(reply);
                                continue;
                            };
                            // Reset the band counter when the epoch advances so
                            // positions stay small and unique within each epoch.
                            if self.reservation_epoch != epoch {
                                self.reservation_epoch = epoch;
                                self.next_reservation_position = RESERVATION_POSITION_BAND;
                            }
                            let position = self.next_reservation_position;
                            match self.next_reservation_position.checked_add(1) {
                                Some(n) => self.next_reservation_position = n,
                                None => {
                                    // Band exhausted within one epoch (pathological:
                                    // 2^31 reservations with no committed txn to
                                    // advance the epoch). Refuse rather than wrap
                                    // into the batch band; dropping `reply` degrades
                                    // the caller to OCC.
                                    warn!(
                                        epoch,
                                        "reservation band exhausted within epoch; \
                                         refusing reservation, caller falls back to OCC"
                                    );
                                    drop(reply);
                                    continue;
                                }
                            }
                            TxnIdWire { epoch, position }
                        }
                    };

                    match self.propose_entry(&SequencerEntry::ReserveRead {
                        owner: owner_id,
                        vshard,
                        key,
                    }) {
                        Ok(_) => {
                            let _ = reply.send(owner_id);
                        }
                        Err(e) => {
                            warn!(error = %e, "reservation propose failed; caller falls back to OCC");
                            // Drop `reply` implicitly at scope end → caller degrades.
                        }
                    }
                }
                ReservationRequest::Release {
                    owner,
                    vshard,
                    reason,
                } => {
                    if let Err(e) = self.propose_entry(&SequencerEntry::ReleaseReservation {
                        owner,
                        vshard,
                        reason,
                    }) {
                        warn!(error = %e, "reservation release propose failed");
                    }
                }
            }
        }
    }
}
