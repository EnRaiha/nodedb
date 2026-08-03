// SPDX-License-Identifier: BUSL-1.1

//! Superuser password resolution and auto-generation.

use super::config::AuthConfig;
use super::config::AuthMode;

impl AuthConfig {
    /// Resolve the superuser password from env var, config, or persisted
    /// auto-generated file. Returns None in trust mode (no password needed).
    ///
    /// Resolution order:
    /// 1. `NODEDB_SUPERUSER_PASSWORD` env var
    /// 2. `auth.superuser_password` config field
    /// 3. Persisted file at `<data_dir>/.superuser_password` (auto-generated
    ///    on first run when neither of the above is set)
    pub fn resolve_superuser_password(
        &self,
        data_dir: &std::path::Path,
    ) -> crate::Result<Option<String>> {
        if self.mode == AuthMode::Trust {
            return Ok(None);
        }

        if let Ok(env_pw) = std::env::var("NODEDB_SUPERUSER_PASSWORD")
            && !env_pw.is_empty()
        {
            return Ok(Some(env_pw));
        }

        if let Some(ref pw) = self.superuser_password
            && !pw.is_empty()
        {
            return Ok(Some(pw.clone()));
        }

        // Auto-generate on first run, persist to data dir for subsequent runs.
        let pw_path = data_dir.join(".superuser_password");
        if let Ok(existing) = std::fs::read_to_string(&pw_path) {
            let trimmed = existing.trim().to_string();
            if !trimmed.is_empty() {
                return Ok(Some(trimmed));
            }
        }

        let generated = generate_superuser_password();
        if let Some(parent) = pw_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| crate::Error::Config {
                detail: format!("failed to create data dir {parent:?}: {e}"),
            })?;
        }
        std::fs::write(&pw_path, &generated).map_err(|e| crate::Error::Config {
            detail: format!("failed to persist superuser password to {pw_path:?}: {e}"),
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&pw_path, std::fs::Permissions::from_mode(0o600));
        }

        eprintln!();
        eprintln!("  ╔══════════════════════════════════════════════════════════════╗");
        eprintln!("  ║         AUTO-GENERATED SUPERUSER PASSWORD (FIRST RUN)        ║");
        eprintln!("  ╠══════════════════════════════════════════════════════════════╣");
        eprintln!("  ║  user:     {:<50}║", self.superuser_name);
        eprintln!("  ║  password: {generated:<50}║");
        eprintln!("  ║  saved to: {:<50}║", pw_path.display().to_string());
        eprintln!("  ║                                                              ║");
        eprintln!("  ║  Override via NODEDB_SUPERUSER_PASSWORD or auth config.      ║");
        eprintln!("  ╚══════════════════════════════════════════════════════════════╝");
        eprintln!();

        Ok(Some(generated))
    }
}

fn generate_superuser_password() -> String {
    use rand::RngExt;
    const ALPHABET: &[u8] = b"abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789";
    let mut rng = rand::rng();
    (0..24)
        .map(|_| ALPHABET[rng.random_range(0..ALPHABET.len())] as char)
        .collect()
}
