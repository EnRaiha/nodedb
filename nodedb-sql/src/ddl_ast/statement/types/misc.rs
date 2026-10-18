// SPDX-License-Identifier: Apache-2.0

//! Miscellaneous DDL/DML statements.

use crate::ddl_ast::statement::maintenance::{
    CopyFormat, CopyRasterOptions, CopyStdinFormat, CopyToSource,
};

#[derive(Debug, Clone, PartialEq)]
pub enum MiscStmt {
    // ── Miscellaneous ────────────────────────────────────────────
    ShowAuditLog,
    ShowConstraints {
        collection: String,
    },
    ShowTypeGuards {
        collection: String,
    },

    // ── Bulk import ──────────────────────────────────────────────
    /// `COPY <collection> FROM '<path>' [WITH (FORMAT ..., DELIMITER ..., HEADER ...)]`
    /// `COPY <array> FROM '<path>' WITH (FORMAT geotiff [, BANDS (...)] [, NODATA n] [, TIME '..'])`
    ///
    /// Server-side file-path bulk import. `CopyFromStdin` is the streaming
    /// form, and `CopyToFile` the export.
    CopyFromFile {
        collection: String,
        path: String,
        format: Option<CopyFormat>,
        delimiter: Option<char>,
        header: bool,
        /// GeoTIFF options. Empty unless `format` is `GeoTiff`.
        raster: CopyRasterOptions,
    },

    /// `COPY <name> FROM STDIN [WITH (FORMAT csv|text, DELIMITER '<c>', HEADER [bool])]`
    ///
    /// Streaming bulk import: the client sends the rows over the pgwire
    /// COPY protocol. `header` defaults to false, as in PostgreSQL.
    CopyFromStdin {
        collection: String,
        format: CopyStdinFormat,
        delimiter: Option<char>,
        header: bool,
    },

    // ── Bulk export ──────────────────────────────────────────────
    /// `COPY <collection> TO '<path>' [WITH (FORMAT ..., DELIMITER ..., HEADER ...)]`
    /// `COPY (SELECT ...) TO '<path>' [WITH (...)]`
    ///
    /// Server-side file-path bulk export. Streams scan results to a file.
    CopyToFile {
        /// The source: either a bare collection name or a SELECT query.
        source: CopyToSource,
        path: String,
        format: Option<CopyFormat>,
        delimiter: Option<char>,
        header: bool,
    },
}
