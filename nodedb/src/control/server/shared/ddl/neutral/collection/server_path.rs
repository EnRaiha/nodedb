// SPDX-License-Identifier: BUSL-1.1

//! Checks a server-path `COPY` runs before it touches the filesystem.
//!
//! A server path names a file the server's OS user reads or writes. A
//! NodeDB grant carries no filesystem right, so only a superuser can name
//! one.

use std::path::{Component, Path};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::result::DdlError;

/// Refuse a non-superuser with `42501`. `statement` names the form, such
/// as `COPY ... FROM '<path>'`.
pub(crate) fn require_superuser(
    identity: &AuthenticatedIdentity,
    statement: &str,
) -> Result<(), DdlError> {
    if identity.is_superuser {
        return Ok(());
    }
    Err(DdlError::new(
        "42501",
        format!(
            "permission denied: {statement} accesses a file as the server's OS user \
             and requires superuser"
        ),
    ))
}

/// Refuse a relative path (`42601`) and a path with a `..` component
/// (`42501`). `statement` prefixes the message, such as `COPY TO`.
pub(crate) fn validate_path(path: &str, statement: &str) -> Result<(), DdlError> {
    if !path.starts_with('/') {
        return Err(DdlError::new(
            "42601",
            format!(
                "{statement}: path '{path}' is not absolute; \
                 only absolute server-side paths are accepted"
            ),
        ));
    }
    if Path::new(path)
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(DdlError::new(
            "42501",
            format!(
                "{statement}: path '{path}' contains '..'; \
                 directory traversal is not permitted"
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_and_traversal_paths_are_refused() {
        assert_eq!(
            validate_path("data/x.csv", "COPY")
                .expect_err("relative")
                .sqlstate,
            "42601"
        );
        assert_eq!(
            validate_path("/tmp/../etc/passwd", "COPY")
                .expect_err("traversal")
                .sqlstate,
            "42501"
        );
        assert!(validate_path("/srv/data/x.csv", "COPY").is_ok());
    }
}
