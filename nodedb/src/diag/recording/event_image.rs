// SPDX-License-Identifier: BUSL-1.1

//! Capture sites for a stored row whose MessagePack image does not render
//! or decode.

use faultbox::{Capture, EventKind, error_chain_of};

use super::shared::error_class;
use crate::diag::context;

/// Report a stored strict row that does not render as MessagePack. Called
/// only from the Data Plane's image resolution, where the decode fails.
/// `fault` is the stable class of the failure.
pub fn strict_row_image_unrendered(collection: &str, fault: &'static str) {
    let ctx = context::StrictRowImageUnrendered { collection, fault };
    let _ = Capture::new(
        EventKind::Corruption,
        "stored strict row did not render as MessagePack: its image is withheld",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a landed timeseries row whose stored image does not decode as
/// MessagePack. Called only from the resolved timeseries ingest, where the
/// `RETURNING` decode fails. The caller returns the error alongside this
/// report.
pub fn timeseries_row_image_undecodable(err: &crate::Error, collection: &str, site: &'static str) {
    let class = error_class(err);
    let ctx = context::TimeseriesRowImageUndecodable {
        collection,
        site,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Corruption,
        "landed timeseries row image does not decode, so the RETURNING statement is refused",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
