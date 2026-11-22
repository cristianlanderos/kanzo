//! Passwords, sessions and access tokens for a board with accounts.
//!
//! The database never holds a secret itself. A password is kept as a salted PBKDF2 hash,
//! and a session or access token as its SHA-256: both secrets are long and random, so a
//! plain hash is enough, and it lets the board find one with a single indexed lookup.

use sha2::{Digest, Sha256};

use crate::store::User;

/// Who sent a request, as the board's login layer found it. Handlers and MCP tools read it.
#[derive(Debug, Clone, Default)]
pub struct Who {
    /// False until the board has its first user: everyone may then do everything, as before accounts.
    pub accounts: bool,
    pub user: Option<User>,
    /// The digest of the browser's session, when that is how they signed in.
    pub session: Option<String>,
}

/// What OWASP asks of PBKDF2-HMAC-SHA256. Stored with each hash, so it can rise later.
const ROUNDS: u32 = if cfg!(test) { 1_000 } else { 600_000 };
const SCHEME: &str = "pbkdf2-sha256";
pub const MIN_PASSWORD: usize = 8;

/// What a browser sends back to stay signed in.
pub const COOKIE: &str = "kanzo_session";
/// Access tokens start with it, so one is easy to tell from other secrets.
pub const TOKEN_PREFIX: &str = "kz_";

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::with_capacity(bytes.len() * 2), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

fn unhex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) || !text.is_ascii() {
        return None;
    }
    (0..text.len()).step_by(2).map(|i| u8::from_str_radix(&text[i..i + 2], 16).ok()).collect()
}

fn random<const N: usize>() -> [u8; N] {
    let mut bytes = [0u8; N];
    // Without the system's random numbers there is no safe secret to hand out.
    getrandom::fill(&mut bytes).expect("the system has no random numbers");
    bytes
}

fn derive(password: &str, salt: &[u8], rounds: u32) -> [u8; 32] {
    let mut out = [0u8; 32];
    pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, rounds, &mut out);
    out
}

pub fn check_password(password: &str) -> Result<(), String> {
    if password.chars().count() < MIN_PASSWORD {
        return Err(format!("the password needs at least {MIN_PASSWORD} characters"));
    }
    Ok(())
}

/// Slow on purpose: call it off the async thread.
pub fn hash_password(password: &str) -> String {
    let salt = random::<16>();
    format!("{SCHEME}${ROUNDS}${}${}", hex(&salt), hex(&derive(password, &salt, ROUNDS)))
}

/// Slow on purpose, like `hash_password`. Takes the same time whether the hash matches or not.
pub fn verify_password(password: &str, stored: &str) -> bool {
    let mut parts = stored.split('$');
    let (Some(SCHEME), Some(rounds), Some(salt), Some(hash), None) =
        (parts.next(), parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    let (Ok(rounds), Some(salt), Some(hash)) = (rounds.parse::<u32>(), unhex(salt), unhex(hash)) else {
        return false;
    };
    let derived = derive(password, &salt, rounds);
    hash.len() == derived.len() && hash.iter().zip(derived).fold(0, |diff, (a, b)| diff | (a ^ b)) == 0
}

/// The work of a real check, for a name nobody has: a wrong name then answers as slowly as a wrong password.
pub fn waste_a_check(password: &str) {
    derive(password, &[0; 16], ROUNDS);
}

pub fn new_session() -> String {
    hex(&random::<32>())
}

pub fn new_token() -> String {
    format!("{TOKEN_PREFIX}{}", hex(&random::<20>()))
}

pub fn is_token(text: &str) -> bool {
    text.strip_prefix(TOKEN_PREFIX).is_some_and(|rest| rest.len() == 40 && rest.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// What the database keeps of a session or an access token.
pub fn digest(secret: &str) -> String {
    hex(&Sha256::digest(secret.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_password_checks_only_against_its_own_hash() {
        let stored = hash_password("correct horse");
        assert!(stored.starts_with("pbkdf2-sha256$"));
        assert!(verify_password("correct horse", &stored));
        assert!(!verify_password("correct horsf", &stored));
        assert_ne!(stored, hash_password("correct horse"), "each hash has its own salt");
        for broken in ["", "plain", "pbkdf2-sha256$x$00$00", "pbkdf2-sha256$1000$zz$00", "md5$1$00$00"] {
            assert!(!verify_password("correct horse", broken), "{broken}");
        }
    }

    #[test]
    fn secrets_are_long_random_and_told_apart() {
        let (token, session) = (new_token(), new_session());
        assert!(is_token(&token) && !is_token(&session) && !is_token("kz_short"));
        assert_eq!(session.len(), 64);
        assert_ne!(new_token(), token);
        assert_eq!(digest(&token).len(), 64);
        assert_ne!(digest(&token), token);
    }

    #[test]
    fn short_passwords_are_refused() {
        assert!(check_password("1234567").is_err());
        assert!(check_password("12345678").is_ok());
    }
}
