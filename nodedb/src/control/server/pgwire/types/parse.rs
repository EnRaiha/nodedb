// SPDX-License-Identifier: BUSL-1.1

//! Small parsing helpers used across pgwire DDL handlers: role-name parsing.

use crate::control::security::identity::Role;

/// Parse a role name string into a `Role`.
///
/// Known roles map to their enum variants; unknown names become `Role::Custom`.
pub fn parse_role(name: &str) -> Role {
    // Role::from_str is Infallible — unwrap is safe on Infallible.
    match name.parse() {
        Ok(role) => role,
        Err(e) => match e {},
    }
}
