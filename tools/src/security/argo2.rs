use argon2::password_hash::{Error, Salt, SaltString};
use argon2::{Argon2, PasswordHash};
use rand_core::{OsRng, TryRngCore};

pub fn argo2_gen_salt_string() -> SaltString {
    let mut os_rng = OsRng;
    let mut bytes = [0u8; Salt::RECOMMENDED_LENGTH];
    match os_rng.try_fill_bytes(&mut bytes) {
        Ok(_) => SaltString::encode_b64(&bytes).expect("Failed to encode salt"),
        Err(_) => {
            panic!("Failed to generate random bytes for salt");
        }
    }
}

pub fn argo2_gen(value: impl Into<String>) -> Result<String, Error> {
    let value = value.into();
    let salt = argo2_gen_salt_string();
    let argon2 = Argon2::default();
    PasswordHash::generate(argon2, value.as_bytes(), &salt)
        .map(|password_hash| password_hash.to_string())
}

pub fn argo2_verify(hash: impl Into<String>, plain: impl Into<String>) -> Result<bool, Error> {
    let argon2 = Argon2::default();
    let hash = hash.into();
    let plain = plain.into();
    PasswordHash::new(&hash).map(|password_hash| {
        password_hash
            .verify_password(&[&argon2], plain.as_bytes())
            .is_ok()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_is_valid() {
        let password = "password";
        let hash = argo2_gen(password).unwrap();
        assert!(argo2_verify(hash, password).unwrap());
    }

    #[test]
    fn test_is_invalid() {
        let password = "password";
        let hash = argo2_gen(password).unwrap();
        assert!(!argo2_verify(hash, "invalid").unwrap());
    }
}
