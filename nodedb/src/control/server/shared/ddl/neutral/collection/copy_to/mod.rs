// SPDX-License-Identifier: BUSL-1.1

//! Handler for `COPY <collection> TO '<path>'` and
//! `COPY (SELECT ...) TO '<path>'` bulk export.

mod entry;
mod format;

pub use entry::copy_to_file;
