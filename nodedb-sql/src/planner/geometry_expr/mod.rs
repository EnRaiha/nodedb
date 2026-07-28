// SPDX-License-Identifier: Apache-2.0

mod resolve;

pub(crate) use resolve::{fold_geometry_function, resolve_geometry_expr};

#[cfg(test)]
mod tests;
