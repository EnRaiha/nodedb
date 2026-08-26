// SPDX-License-Identifier: BUSL-1.1

//! KV predicate DML: `UPDATE` / `DELETE` whose `WHERE` names no primary key.

pub(in crate::data::executor) mod apply;
mod matches;

pub(in crate::data::executor) use apply::KvPredicateCtx;
