// SPDX-License-Identifier: BUSL-1.1

//! Batched read of a write's source relation.

use futures::future::BoxFuture;
use nodedb_physical::physical_plan::{QuerySource, RowSource};

use super::context::SourceReadContext;
use super::pages::PageReader;

/// Rows per batch a query source yields: the page size of a page source, so
/// a write sized per batch sees the same batch size from either source.
const QUERY_BATCH_ROWS: usize = 4_096;

/// A batched reader over a [`RowSource`]. Each batch holds shaped msgpack
/// rows, ready for the write that consumes them.
pub(crate) enum SourceReader<'s> {
    Pages(PageReader),
    Query {
        source: &'s QuerySource,
        /// The rows not yet handed out. `None` until the query runs.
        pending: Option<std::vec::IntoIter<Vec<u8>>>,
    },
}

impl<'s> SourceReader<'s> {
    pub(crate) fn new(source: &'s RowSource) -> crate::Result<Self> {
        Ok(match source {
            RowSource::DocumentPages(page) => SourceReader::Pages(PageReader::new(page)?),
            RowSource::Query(query) => SourceReader::Query {
                source: query,
                pending: None,
            },
        })
    }

    /// The next batch of shaped rows. `None` once the source is exhausted. A
    /// batch can be empty while more follow.
    ///
    /// The read re-enters the gateway from inside a write that the gateway
    /// is already running, so the future is boxed here. That bounds the
    /// nested future type the compiler checks for `Send`.
    pub(crate) fn next_batch<'a>(
        &'a mut self,
        ctx: &'a SourceReadContext<'a>,
    ) -> BoxFuture<'a, crate::Result<Option<Vec<Vec<u8>>>>> {
        Box::pin(async move {
            match self {
                SourceReader::Pages(pages) => pages.next_page(ctx).await,
                SourceReader::Query { source, pending } => {
                    if pending.is_none() {
                        let rows = super::query::read_query_rows(ctx, source).await?;
                        *pending = Some(rows.into_iter());
                    }
                    let Some(rows) = pending.as_mut() else {
                        return Ok(None);
                    };
                    let batch: Vec<Vec<u8>> = rows.by_ref().take(QUERY_BATCH_ROWS).collect();
                    Ok((!batch.is_empty()).then_some(batch))
                }
            }
        })
    }
}

/// Read every row of `source`, for a write that needs the whole relation
/// before it starts (`MERGE`, `UPDATE ... FROM`).
///
/// The rows accumulate on the Control Plane, so they are held to the scan
/// byte budget a SELECT is held to. Past it, the read fails with
/// `MemoryExhausted` and names the source. It never returns a partial set.
pub(crate) async fn read_source_rows(
    ctx: &SourceReadContext<'_>,
    source: &RowSource,
) -> crate::Result<Vec<Vec<u8>>> {
    let budget = ctx.state.tuning.query.max_scan_result_bytes;
    let mut reader = SourceReader::new(source)?;
    let mut rows = Vec::new();
    let mut bytes = 0usize;
    while let Some(batch) = reader.next_batch(ctx).await? {
        for row in batch {
            bytes = bytes.saturating_add(row.len());
            if budget_exceeded(bytes, budget) {
                return Err(crate::Error::MemoryExhausted {
                    engine: format!(
                        "source '{}' (scan budget {budget} bytes; add a LIMIT or a more \
                         selective filter, or raise [tuning.query] max_scan_result_bytes)",
                        source.label()
                    ),
                });
            }
            rows.push(row);
        }
    }
    Ok(rows)
}

/// Whether `bytes` exceeds `budget`. A budget of 0 is unlimited.
fn budget_exceeded(bytes: usize, budget: usize) -> bool {
    budget != 0 && bytes > budget
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_zero_budget_is_unlimited() {
        assert!(!budget_exceeded(usize::MAX, 0));
    }

    #[test]
    fn the_budget_is_inclusive() {
        assert!(!budget_exceeded(100, 100));
        assert!(budget_exceeded(101, 100));
    }
}
