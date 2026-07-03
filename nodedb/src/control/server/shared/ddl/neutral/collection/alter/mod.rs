// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `ALTER COLLECTION` handlers, one file per sub-command,
//! plus the total dispatcher over `AlterCollectionOp`.

mod add_column;
mod alter_type;
mod dispatch;
mod drop_column;
mod enforcement;
mod materialized_sum;
mod ownership;
mod rename_column;
mod strict_schema;
mod support;

pub use dispatch::dispatch_alter_collection;
