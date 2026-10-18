// SPDX-License-Identifier: Apache-2.0

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::id::NodeId;
use nodedb_types::protocol::ArrayCellWrite;

use super::super::trait_def::NodeDb;

pub(in crate::traits::core) fn array_put_cells_default(
    array: &str,
    cells: Vec<ArrayCellWrite>,
) -> NodeDbResult<u64> {
    let _ = cells;
    Err(NodeDbError::storage(format!(
        "array_put_cells is not implemented on this client; load array '{array}' \
         through the native client, or with COPY {array} FROM STDIN"
    )))
}

pub(in crate::traits::core) async fn batch_vector_insert_default<T: NodeDb + ?Sized>(
    this: &T,
    collection: &str,
    vectors: &[(&str, &[f32])],
) -> NodeDbResult<()> {
    for &(id, embedding) in vectors {
        this.vector_insert(collection, id, embedding, None).await?;
    }
    Ok(())
}

pub(in crate::traits::core) async fn batch_graph_insert_edges_default<T: NodeDb + ?Sized>(
    this: &T,
    collection: &str,
    edges: &[(&str, &str, &str)],
) -> NodeDbResult<()> {
    for &(from, to, label) in edges {
        let src = NodeId::try_new(from)
            .map_err(|e| NodeDbError::storage(format!("invalid node id: {e}")))?;
        let dst = NodeId::try_new(to)
            .map_err(|e| NodeDbError::storage(format!("invalid node id: {e}")))?;
        this.graph_insert_edge(collection, &src, &dst, label, None)
            .await?;
    }
    Ok(())
}
