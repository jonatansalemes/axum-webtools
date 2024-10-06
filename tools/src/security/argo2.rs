use argon2::password_hash::rand_core::OsRng;
use argon2::password_hash::{Error, SaltString};
use argon2::{Argon2, PasswordHash};

pub fn argo2_gen(value: impl Into<String>) -> Result<String, Error> {
    let value = value.into();
    let salt = SaltString::generate(&mut OsRng);
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
