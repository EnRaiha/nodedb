// SPDX-License-Identifier: BUSL-1.1

//! Seal the restore generation of a node's first boot after a cluster
//! restore, before its Raft groups start. A later restore then takes a newer
//! generation, above every term and epoch this cluster reaches.

use tracing::info;

use crate::control::state::SharedState;
use crate::storage::restore_generation::{
    read_generation_marker, remove_generation_marker, seal_generation,
};

/// Seal the generation the data directory was restored at, if any. Refuses
/// the boot when the node has no cold storage to seal it in.
pub async fn seal_restored_generation(state: &SharedState) -> crate::Result<()> {
    let data_dir = state.data_dir.clone();
    let Some(generation) = read_generation_marker(&data_dir)? else {
        return Ok(());
    };
    let cold = state
        .cold_storage
        .as_ref()
        .ok_or_else(|| crate::Error::Config {
            detail: format!(
                "{} was restored at generation {generation}, and its first boot seals the \
             generation in cold storage; configure [cold_storage]",
                data_dir.display()
            ),
        })?;
    seal_generation(&cold.object_store(), cold.prefix(), generation).await?;
    remove_generation_marker(&data_dir)?;
    info!(generation, "cluster restore generation sealed");
    Ok(())
}
