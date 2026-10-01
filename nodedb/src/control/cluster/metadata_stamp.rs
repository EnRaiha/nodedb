// SPDX-License-Identifier: BUSL-1.1

//! The floor of the stamps this node takes as metadata leader.
//!
//! A leader stamps each metadata entry as it appends it, above its HLC and
//! above every stamp its log holds (see
//! `MultiRaft::propose_stamped_metadata`). Entries compacted out of the log
//! no longer show there. The applier records the highest stamp it applied in
//! the catalog, and this folds it into the HLC wherever the in-memory clock
//! can have fallen behind it: at boot, and after a snapshot install replaced
//! the catalog.

use nodedb_types::Hlc;

use crate::control::state::SharedState;

/// Move the node HLC past the highest metadata stamp the catalog records.
pub fn fold_metadata_stamp_hwm(shared: &SharedState) -> crate::Result<()> {
    if let Some(hwm) = shared.credentials.catalog().load_metadata_stamp_hwm()? {
        shared.hlc_clock.update(Hlc::new(hwm, 0));
    }
    Ok(())
}
