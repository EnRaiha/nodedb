// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral synonym group DDL — CREATE / DROP / SHOW.
//!
//! Ported from the pgwire `ddl::synonym_group` handlers. All non-return logic
//! (tenant-admin gate, duplicate / existence checks against the in-memory
//! `synonym_registry`, the `propose_catalog_entry` + `log_index == 0` manual
//! catalog write, the in-memory registry update, and the Data-Plane FTS
//! `PutSynonymGroup` / `DeleteSynonymGroup` dispatch) is preserved verbatim;
//! only the result construction changed from pgwire `Response` / `PgWireError`
//! to the protocol-neutral `DdlResult` / `DdlError`.

pub mod create;
pub mod drop;
pub mod show;

pub use create::create_synonym_group;
pub use drop::drop_synonym_group;
pub use show::show_synonym_groups;
