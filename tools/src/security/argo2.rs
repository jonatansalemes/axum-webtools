use argon2::password_hash::Error;
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};

pub fn argo2_gen_salt_string() -> Result<[u8; 16], Error> {
    let mut buf = [0u8; 16];
    getrandom::fill(&mut buf)?;
    Ok(buf)
}

pub fn argo2_gen_password(password: &str) -> Result<String, Error> {
    let password = password.as_bytes();
    let argon2 = Argon2::default();
    let salt = argo2_gen_salt_string()?;
    let hashed = argon2.hash_password_with_salt(password, &salt)?;
    Ok(hashed.to_string())
}

pub fn argo2_verify_password(password_hash: &str, password: &str) -> Result<bool, Error> {
    let password = password.as_bytes();
    let pwd_hash = PasswordHash::new(password_hash)?;
    let argon2 = Argon2::default();
    match argon2.verify_password(password, &pwd_hash) {
        Ok(_) => Ok(true),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_is_valid() {
        let password = "password";
        let hash = argo2_gen_password(password).unwrap();
        assert!(argo2_verify_password(&hash, password).unwrap());
    }

    #[test]
    fn test_is_invalid() {
        let password = "password";
        let hash = argo2_gen_password(password).unwrap();
        assert!(argo2_verify_password(&hash, "invalid").is_err());
    }
}
