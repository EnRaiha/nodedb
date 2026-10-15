// SPDX-License-Identifier: BUSL-1.1

use argon2::password_hash::{PasswordHasher, PasswordVerifier};
use argon2::{Algorithm, Argon2, Params, PasswordHash, Version};

use crate::config::auth::Argon2Config;

pub(super) fn generate_scram_salt() -> crate::Result<Vec<u8>> {
    let mut salt = vec![0u8; 16];
    getrandom::fill(&mut salt).map_err(|e| crate::Error::Internal {
        detail: format!("OS random source failed generating a SCRAM salt: {e}"),
    })?;
    Ok(salt)
}

pub(super) fn compute_scram_salted_password(password: &str, salt: &[u8]) -> Vec<u8> {
    pgwire::api::auth::sasl::scram::gen_salted_password(password, salt, 4096)
}

/// Build an `Argon2` instance from the supplied config.
fn build_argon2(cfg: &Argon2Config) -> crate::Result<Argon2<'static>> {
    let params = Params::new(
        cfg.memory_kib,
        cfg.time_cost,
        cfg.parallelism,
        Some(cfg.output_len),
    )
    .map_err(|e| crate::Error::Internal {
        detail: format!("invalid argon2 params: {e}"),
    })?;
    Ok(Argon2::new(Algorithm::Argon2id, Version::V0x13, params))
}

/// Hash `password` using the supplied Argon2 config.
pub(super) fn hash_password_argon2(password: &str, cfg: &Argon2Config) -> crate::Result<String> {
    let argon2 = build_argon2(cfg)?;
    let hash = argon2
        .hash_password(password.as_bytes())
        .map_err(|e| crate::Error::Internal {
            detail: format!("argon2 hashing failed: {e}"),
        })?;
    Ok(hash.to_string())
}

/// The outcome of a verify + rehash-check operation.
pub(super) enum VerifyOutcome {
    /// Password matched. `rehash` is the new PHC string if the stored hash
    /// was weaker than the configured params; `None` means no rehash needed.
    Ok { rehash: Option<String> },
    /// Password did not match.
    WrongPassword,
    /// The stored PHC string was unparseable — treat as auth failure.
    BadStoredHash,
}

/// Verify `password` against `stored_hash`.
///
/// On success, checks whether the stored params are weaker than `cfg`.
/// If so, returns a new PHC string in `VerifyOutcome::Ok { rehash: Some(...) }`.
///
/// **No-downgrade rule**: if the stored hash uses *stronger* params (operator
/// reduced the config dial), the stored hash is left intact — no rehash.
/// Equality also means no rehash (same params, no gain).
pub(super) fn verify_argon2_with_rehash(
    stored_hash: &str,
    password: &str,
    cfg: &Argon2Config,
) -> VerifyOutcome {
    let parsed = match PasswordHash::new(stored_hash) {
        Ok(h) => h,
        Err(_) => return VerifyOutcome::BadStoredHash,
    };

    // Verify with a default Argon2 that accepts any valid PHC params
    // (i.e. it reads params from the stored PHC string itself).
    let verify_ok = Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok();

    if !verify_ok {
        return VerifyOutcome::WrongPassword;
    }

    // Extract stored params from the PHC string.
    let stored_params = extract_params(&parsed);

    // Decide whether a rehash is needed: any stored param strictly weaker
    // than the configured param triggers a rehash.  If stored is equal or
    // stronger on all axes, skip (no downgrade).
    let needs_rehash = stored_params.is_some_and(|sp| params_are_weaker(&sp, cfg));

    if needs_rehash {
        match hash_password_argon2(password, cfg) {
            Ok(new_hash) => VerifyOutcome::Ok {
                rehash: Some(new_hash),
            },
            Err(e) => {
                // Hashing failure must not fail the login — warn and continue.
                tracing::warn!(error = %e, "argon2 rehash failed; continuing without rehash");
                VerifyOutcome::Ok { rehash: None }
            }
        }
    } else {
        VerifyOutcome::Ok { rehash: None }
    }
}

/// Extracted Argon2 memory/time/parallelism from a PHC `PasswordHash`.
struct StoredParams {
    memory_kib: u32,
    time_cost: u32,
    parallelism: u32,
}

/// Pull m/t/p out of the PHC parameter map.
///
/// Argon2 PHC strings look like:
/// `$argon2id$v=19$m=65536,t=2,p=1$<salt>$<hash>`
///
/// The `params` iterator yields `(name, decimal_value)` pairs.
fn extract_params(hash: &PasswordHash) -> Option<StoredParams> {
    let get = |name: &str| -> Option<u32> {
        // `password-hash` exposes Argon2 PHC params (m, t, p) as decimals;
        // `get_decimal` returns `Option<u32>` directly.
        hash.params.get_decimal(name)
    };

    Some(StoredParams {
        memory_kib: get("m")?,
        time_cost: get("t")?,
        parallelism: get("p")?,
    })
}

/// Return `true` if `stored` is strictly weaker than `cfg` on **any** axis.
///
/// Strictly weaker means the stored value is *less than* the configured value
/// for memory (higher = stronger) and time cost (higher = stronger).
/// For parallelism, more lanes alone does not make the hash *weaker* from a
/// security standpoint, but the OWASP recommended minimum is 1 and deviating
/// from the configured value wastes or under-uses resources.  We rehash when
/// stored parallelism != configured parallelism so all hashes converge to the
/// configured profile.
///
/// The no-downgrade invariant: if stored > configured on ALL axes, this
/// returns `false` (leave the stronger hash alone).  Only if *any* stored
/// value is less than configured do we rehash.
fn params_are_weaker(stored: &StoredParams, cfg: &Argon2Config) -> bool {
    stored.memory_kib < cfg.memory_kib
        || stored.time_cost < cfg.time_cost
        || stored.parallelism < cfg.parallelism
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::auth::Argon2Config;

    fn weak_cfg() -> Argon2Config {
        // Minimal params: fast for tests, clearly weaker than strong_cfg.
        Argon2Config {
            memory_kib: 8,
            time_cost: 1,
            parallelism: 1,
            output_len: 32,
        }
    }

    fn strong_cfg() -> Argon2Config {
        // Stronger on all axes than weak_cfg.
        Argon2Config {
            memory_kib: 16,
            time_cost: 2,
            parallelism: 1,
            output_len: 32,
        }
    }

    /// Hash with weak params, verify with strong-params config → succeeds and
    /// triggers a rehash whose stored hash embeds the strong params.
    #[test]
    fn weak_hash_triggers_rehash_with_strong_params() {
        let password = "correct_horse_battery";
        let stored = hash_password_argon2(password, &weak_cfg()).expect("hash");

        let outcome = verify_argon2_with_rehash(&stored, password, &strong_cfg());

        let new_hash = match outcome {
            VerifyOutcome::Ok { rehash: Some(h) } => h,
            VerifyOutcome::Ok { rehash: None } => {
                panic!("expected rehash but got Ok without rehash")
            }
            VerifyOutcome::WrongPassword => panic!("expected Ok, got WrongPassword"),
            VerifyOutcome::BadStoredHash => panic!("expected Ok, got BadStoredHash"),
        };

        // The new PHC string must embed the strong params.
        let new_parsed = PasswordHash::new(&new_hash).expect("parseable PHC");
        let sp = extract_params(&new_parsed).expect("params present");
        assert_eq!(sp.memory_kib, strong_cfg().memory_kib, "memory upgraded");
        assert_eq!(sp.time_cost, strong_cfg().time_cost, "time_cost upgraded");
        assert_eq!(sp.parallelism, strong_cfg().parallelism, "parallelism");
    }

    /// Hash with strong params, verify with same → succeeds, NO rehash.
    #[test]
    fn strong_hash_no_rehash_with_same_params() {
        let password = "correct_horse_battery";
        let stored = hash_password_argon2(password, &strong_cfg()).expect("hash");
        let stored_before = stored.clone();

        let outcome = verify_argon2_with_rehash(&stored, password, &strong_cfg());

        match outcome {
            VerifyOutcome::Ok { rehash: None } => {}
            VerifyOutcome::Ok { rehash: Some(_) } => {
                panic!("unexpected rehash when params are equal")
            }
            VerifyOutcome::WrongPassword => panic!("expected Ok, got WrongPassword"),
            VerifyOutcome::BadStoredHash => panic!("expected Ok, got BadStoredHash"),
        }

        // The stored hash is unchanged (byte-equal).
        assert_eq!(stored, stored_before);
    }

    /// Wrong password → no rehash, returns WrongPassword.
    #[test]
    fn wrong_password_no_rehash() {
        let stored = hash_password_argon2("correct", &weak_cfg()).expect("hash");
        let outcome = verify_argon2_with_rehash(&stored, "wrong", &strong_cfg());
        assert!(
            matches!(outcome, VerifyOutcome::WrongPassword),
            "expected WrongPassword"
        );
    }

    /// Garbage PHC string → BadStoredHash (not a panic).
    #[test]
    fn garbage_phc_returns_bad_stored_hash() {
        let outcome =
            verify_argon2_with_rehash("$notavalidphcstring$$$garbage", "password", &strong_cfg());
        assert!(
            matches!(outcome, VerifyOutcome::BadStoredHash),
            "expected BadStoredHash"
        );
    }

    /// Hash with strong params, verify with weak-params config → succeeds, NO
    /// rehash (no-downgrade rule: stored is stronger than config).
    #[test]
    fn strong_hash_no_downgrade_when_config_is_weaker() {
        let password = "no_downgrade_test";
        let stored = hash_password_argon2(password, &strong_cfg()).expect("hash");

        let outcome = verify_argon2_with_rehash(&stored, password, &weak_cfg());

        match outcome {
            VerifyOutcome::Ok { rehash: None } => {}
            VerifyOutcome::Ok { rehash: Some(_) } => {
                panic!("no-downgrade violated: rehash triggered with weaker config")
            }
            VerifyOutcome::WrongPassword => panic!("expected Ok"),
            VerifyOutcome::BadStoredHash => panic!("expected Ok, got BadStoredHash"),
        }
    }
}
