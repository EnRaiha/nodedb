// SPDX-License-Identifier: BUSL-1.1

//! `COPY <array> FROM '<path>' WITH (FORMAT geotiff)`: the decode task and
//! the load that drains it.
//!
//! One blocking task owns the file handle and the decoder. It reads the
//! tags, waits for the open-time checks, then decodes one block at a time
//! into a channel of two. The async side turns each block into rows and
//! feeds the array loader, so the load holds at most four decoded blocks
//! and the loader's few chunks. Decoding runs on the Control Plane's
//! blocking pool, never on a Data Plane core.

use std::io::BufReader;

use nodedb_raster::{GeoTiffReader, RasterBlock, RasterError, RasterLimits};
use nodedb_sql::ddl_ast::statement::CopyRasterOptions;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::control::server::shared::ddl::result::DdlError;

use super::super::super::source_file::CopySourceFile;
use super::super::decode::RowError;
use super::super::dispatch::{ArrayLoadCtx, input_cap};
use super::super::loader::{ArrayLoader, LoadInput};
use super::open::{RasterHeader, RasterPlan};
use super::pixels::BlockRows;
use super::refusal::raster_refusal;

/// The most bytes one block decodes to, before the memory headroom cap.
const MAX_BLOCK_BYTES: usize = 64 * 1024 * 1024;
/// Decoded blocks queued between the decoder and the load.
const BLOCK_QUEUE: usize = 2;

type HeaderResult = Result<RasterHeader, RasterError>;
type BlockResult = Result<RasterBlock, RasterError>;

/// Load the GeoTIFF at `path` into `array`. Returns the number of rows
/// loaded: every pixel but the all-nodata ones.
pub(crate) async fn copy_raster(
    ctx: &ArrayLoadCtx<'_>,
    array: &str,
    path: &str,
    options: &CopyRasterOptions,
) -> Result<u64, DdlError> {
    let mut loader = ArrayLoader::open(ctx, array, LoadInput::Raster)?;
    let georef = loader.view().georef.clone().ok_or_else(|| {
        loader.refuse(RowError::new(
            0,
            "55000",
            format!(
                "array '{array}' has no GEOREF, so no pixel has a cell; \
                 add one with ALTER ARRAY {array} SET GEOREF (...)"
            ),
        ))
    })?;
    let cap = MAX_BLOCK_BYTES.min(input_cap(ctx.state));
    let file = CopySourceFile::open(path).await?.into_std().await;
    let (header_tx, header_rx) = oneshot::channel();
    let (go_tx, go_rx) = oneshot::channel();
    let (block_tx, mut block_rx) = mpsc::channel(BLOCK_QUEUE);
    let decoder = tokio::task::spawn_blocking(move || {
        decode_file(file, cap, header_tx, go_rx, block_tx);
    });

    let header = match header_rx.await {
        Ok(Ok(header)) => header,
        Ok(Err(e)) => return Err(loader.refuse(raster_refusal(&e, path, cap))),
        Err(_) => return Err(decoder_gone(decoder).await),
    };
    let plan =
        RasterPlan::new(loader.view(), &georef, &header, options).map_err(|e| loader.refuse(e))?;
    if go_tx.send(()).is_err() {
        return Err(decoder_gone(decoder).await);
    }
    while let Some(next) = block_rx.recv().await {
        let block = next.map_err(|e| loader.refuse(raster_refusal(&e, path, cap)))?;
        let rows = BlockRows::new(&plan, &block).map_err(|e| loader.refuse(e))?;
        loader.feed_rows(ctx, rows).await?;
    }
    // The channel closes once the decoder returns. A panic closes it early,
    // so the join result tells a whole file from a cut-short one.
    decoder
        .await
        .map_err(|e| DdlError::internal(format!("COPY: GeoTIFF decoder failed: {e}")))?;
    loader.finish(ctx).await
}

/// The error of a decoder that ended before it answered.
async fn decoder_gone(decoder: JoinHandle<()>) -> DdlError {
    match decoder.await {
        Err(e) => DdlError::internal(format!("COPY: GeoTIFF decoder failed: {e}")),
        Ok(()) => DdlError::internal("COPY: GeoTIFF decoder ended before the file was read"),
    }
}

/// The decode task. It ends early once the load drops its side: a refused
/// file, a refused row, or a cancelled statement.
fn decode_file(
    file: std::fs::File,
    cap: usize,
    header_tx: oneshot::Sender<HeaderResult>,
    go: oneshot::Receiver<()>,
    blocks: mpsc::Sender<BlockResult>,
) {
    let opened = GeoTiffReader::open(BufReader::new(file), RasterLimits::new(cap))
        .and_then(|reader| header_of(&reader).map(|header| (reader, header)));
    let mut reader = match opened {
        Ok((reader, header)) => {
            if header_tx.send(Ok(header)).is_err() {
                return;
            }
            reader
        }
        Err(e) => {
            // A dropped receiver means the load already ended; nothing waits.
            let _ = header_tx.send(Err(e));
            return;
        }
    };
    if go.blocking_recv().is_err() {
        return;
    }
    for index in 0..reader.block_count() {
        let block = reader.read_block(index);
        let failed = block.is_err();
        if blocks.blocking_send(block).is_err() || failed {
            return;
        }
    }
}

fn header_of<R: std::io::Read + std::io::Seek>(
    reader: &GeoTiffReader<R>,
) -> Result<RasterHeader, RasterError> {
    Ok(RasterHeader {
        width: reader.width(),
        height: reader.height(),
        bands: reader.band_count(),
        sample_type: reader.sample_type(),
        transform: reader.transform().map(|gt| gt.0),
        srid: reader.srid()?,
        nodata: reader.nodata(),
    })
}
