// SPDX-License-Identifier: Apache-2.0

mod dispatch;
pub(super) mod legacy;
pub(crate) mod path;
pub(super) mod pg_ops;
pub(super) mod standard;

pub(super) use dispatch::try_eval;
