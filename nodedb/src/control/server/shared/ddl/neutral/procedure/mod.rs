// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral stored-procedure DDL — CREATE / DROP / SHOW, plus the
//! `CALL <procedure>(...)` execution entry point.

pub mod call;
pub mod create;
pub mod drop;
mod parens;
pub mod show;

pub use call::call_procedure;
pub use create::create_procedure;
pub use drop::drop_procedure;
pub use show::show_procedures;
