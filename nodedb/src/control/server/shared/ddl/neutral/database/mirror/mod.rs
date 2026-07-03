// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral mirror database DDL handlers.
//!
//! The mirror *runtime* helpers (`apply_mirror_ddl_entry`,
//! `check_mirror_read_consistency`) remain on the pgwire side
//! (`pgwire::ddl::database::mirror`) because they back the pgwire read path and
//! the observer applier, not the DDL router. Only the three router handlers
//! (`MIRROR DATABASE`, `ALTER DATABASE … PROMOTE`, `SHOW DATABASE MIRROR
//! STATUS`) are migrated here.

pub mod create;
pub mod promote;
pub mod show;
