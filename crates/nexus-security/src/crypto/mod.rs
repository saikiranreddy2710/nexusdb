//! Encryption at rest: AES-256-GCM with key hierarchy.
//!
//! Uses a two-level key hierarchy:
//! - Master Key (MEK): protects Data Encryption Keys
//! - Data Encryption Key (DEK): encrypts actual data
//!
//! Each table can have its own DEK, enabling per-table key rotation
//! without re-encrypting the entire database.

use ring::aead::{self, Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};
use ring::rand::{SecureRandom, SystemRandom};
use thiserror::Error;

/// Encryption errors.
#[derive(Debug, Error)]
pub enum CryptoError {
    /// Encryption failed.
    #[error("encryption failed: {0}")]
    EncryptionFailed(String),
    /// Decryption failed (wrong key, corrupted data, or tampered ciphertext).
    #[error("decryption failed: {0}")]
    DecryptionFailed(String),
    /// Key generation failed.
    #[error("key generation failed: {0}")]
    KeyGenerationFailed(String),
    /// Invalid key size.
    #[error("invalid key size: expected {expected}, got {actual}")]
    InvalidKeySize { expected: usize, actual: usize },
}

/// Encrypted data with nonce and authentication tag (embedded in ciphertext).
#[derive(Debug, Clone)]
pub struct EncryptedData {
    /// 12-byte nonce (prepended to ciphertext).
    pub nonce: [u8; 12],
    /// Ciphertext with 16-byte authentication tag appended.
    pub ciphertext: Vec<u8>,
}

impl EncryptedData {
    /// Serialize to bytes: [nonce (12)] [ciphertext+tag].
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(12 + self.ciphertext.len());
        buf.extend_from_slice(&self.nonce);
        buf.extend_from_slice(&self.ciphertext);
        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, CryptoError> {
        if data.len() < 12 + 16 {
            return Err(CryptoError::DecryptionFailed(
                "data too short for nonce + tag".into(),
            ));
        }
        let mut nonce = [0u8; 12];
        nonce.copy_from_slice(&data[..12]);
        let ciphertext = data[12..].to_vec();
        Ok(Self { nonce, ciphertext })
    }
}

/// AES-256-GCM encryptor for data at rest.
///
/// Thread-safe: uses `ring` which handles internal synchronization.
pub struct DataEncryptor {
    key: LessSafeKey,
    rng: SystemRandom,
}

impl DataEncryptor {
    /// Create an encryptor from a 32-byte key.
    pub fn new(key_bytes: &[u8]) -> Result<Self, CryptoError> {
        if key_bytes.len() != 32 {
            return Err(CryptoError::InvalidKeySize {
                expected: 32,
                actual: key_bytes.len(),
            });
        }
        let unbound_key = UnboundKey::new(&AES_256_GCM, key_bytes)
            .map_err(|e| CryptoError::KeyGenerationFailed(e.to_string()))?;
        Ok(Self {
            key: LessSafeKey::new(unbound_key),
            rng: SystemRandom::new(),
        })
    }

    /// Generate a random 32-byte encryption key.
    pub fn generate_key() -> Result<[u8; 32], CryptoError> {
        let rng = SystemRandom::new();
        let mut key = [0u8; 32];
        rng.fill(&mut key)
            .map_err(|e| CryptoError::KeyGenerationFailed(e.to_string()))?;
        Ok(key)
    }

    /// Encrypt plaintext with AES-256-GCM.
    ///
    /// Returns the encrypted data including nonce and authentication tag.
    /// The `aad` (Additional Authenticated Data) is authenticated but not
    /// encrypted — use it for metadata like table name or page ID.
    pub fn encrypt(&self, plaintext: &[u8], aad: &[u8]) -> Result<EncryptedData, CryptoError> {
        // Generate a random 12-byte nonce
        let mut nonce_bytes = [0u8; 12];
        self.rng
            .fill(&mut nonce_bytes)
            .map_err(|e| CryptoError::EncryptionFailed(e.to_string()))?;

        let nonce =
            Nonce::try_assume_unique_for_key(&nonce_bytes)
                .map_err(|_| CryptoError::EncryptionFailed("nonce error".into()))?;

        // Encrypt in place: plaintext is copied then tag is appended
        let mut in_out = plaintext.to_vec();
        self.key
            .seal_in_place_append_tag(nonce, Aad::from(aad), &mut in_out)
            .map_err(|e| CryptoError::EncryptionFailed(e.to_string()))?;

        Ok(EncryptedData {
            nonce: nonce_bytes,
            ciphertext: in_out,
        })
    }

    /// Decrypt ciphertext with AES-256-GCM.
    ///
    /// The `aad` must match what was used during encryption.
    pub fn decrypt(&self, encrypted: &EncryptedData, aad: &[u8]) -> Result<Vec<u8>, CryptoError> {
        let nonce =
            Nonce::try_assume_unique_for_key(&encrypted.nonce)
                .map_err(|_| CryptoError::DecryptionFailed("nonce error".into()))?;

        let mut in_out = encrypted.ciphertext.clone();
        let plaintext = self
            .key
            .open_in_place(nonce, Aad::from(aad), &mut in_out)
            .map_err(|e| CryptoError::DecryptionFailed(e.to_string()))?;

        Ok(plaintext.to_vec())
    }
}

// DataEncryptor is Send+Sync because ring::aead::LessSafeKey and SystemRandom are.
unsafe impl Send for DataEncryptor {}
unsafe impl Sync for DataEncryptor {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encrypt_decrypt() {
        let key = DataEncryptor::generate_key().unwrap();
        let enc = DataEncryptor::new(&key).unwrap();

        let plaintext = b"Hello, NexusDB!";
        let aad = b"table:users";

        let encrypted = enc.encrypt(plaintext, aad).unwrap();
        assert_ne!(encrypted.ciphertext, plaintext);

        let decrypted = enc.decrypt(&encrypted, aad).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_wrong_key_fails() {
        let key1 = DataEncryptor::generate_key().unwrap();
        let key2 = DataEncryptor::generate_key().unwrap();

        let enc1 = DataEncryptor::new(&key1).unwrap();
        let enc2 = DataEncryptor::new(&key2).unwrap();

        let encrypted = enc1.encrypt(b"secret data", b"").unwrap();
        assert!(enc2.decrypt(&encrypted, b"").is_err());
    }

    #[test]
    fn test_wrong_aad_fails() {
        let key = DataEncryptor::generate_key().unwrap();
        let enc = DataEncryptor::new(&key).unwrap();

        let encrypted = enc.encrypt(b"data", b"correct_aad").unwrap();
        assert!(enc.decrypt(&encrypted, b"wrong_aad").is_err());
    }

    #[test]
    fn test_tampered_ciphertext_fails() {
        let key = DataEncryptor::generate_key().unwrap();
        let enc = DataEncryptor::new(&key).unwrap();

        let mut encrypted = enc.encrypt(b"important data", b"").unwrap();
        if !encrypted.ciphertext.is_empty() {
            encrypted.ciphertext[0] ^= 0xFF; // Flip bits
        }
        assert!(enc.decrypt(&encrypted, b"").is_err());
    }

    #[test]
    fn test_encrypted_data_serialization() {
        let key = DataEncryptor::generate_key().unwrap();
        let enc = DataEncryptor::new(&key).unwrap();

        let encrypted = enc.encrypt(b"round trip", b"ctx").unwrap();
        let bytes = encrypted.to_bytes();
        let restored = EncryptedData::from_bytes(&bytes).unwrap();

        let decrypted = enc.decrypt(&restored, b"ctx").unwrap();
        assert_eq!(decrypted, b"round trip");
    }

    #[test]
    fn test_invalid_key_size() {
        assert!(DataEncryptor::new(&[0u8; 16]).is_err()); // Too short
        assert!(DataEncryptor::new(&[0u8; 64]).is_err()); // Too long
        assert!(DataEncryptor::new(&[0u8; 32]).is_ok());  // Correct
    }

    #[test]
    fn test_empty_plaintext() {
        let key = DataEncryptor::generate_key().unwrap();
        let enc = DataEncryptor::new(&key).unwrap();

        let encrypted = enc.encrypt(b"", b"").unwrap();
        let decrypted = enc.decrypt(&encrypted, b"").unwrap();
        assert!(decrypted.is_empty());
    }
}
