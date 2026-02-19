//! TLS/mTLS configuration for transport security.
//!
//! Provides configuration types for enabling TLS 1.3 on the gRPC server
//! using rustls (pure Rust, no OpenSSL dependency). Supports both
//! server-side TLS and mutual TLS (mTLS) for client certificate auth.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// TLS configuration errors.
#[derive(Debug, Error)]
pub enum TlsError {
    /// Certificate file not found.
    #[error("certificate file not found: {0}")]
    CertNotFound(PathBuf),
    /// Key file not found.
    #[error("key file not found: {0}")]
    KeyNotFound(PathBuf),
    /// Invalid certificate format.
    #[error("invalid certificate: {0}")]
    InvalidCert(String),
    /// Invalid key format.
    #[error("invalid key: {0}")]
    InvalidKey(String),
    /// CA certificate not found (for mTLS).
    #[error("CA certificate not found: {0}")]
    CaNotFound(PathBuf),
    /// TLS configuration error.
    #[error("TLS configuration error: {0}")]
    ConfigError(String),
}

/// TLS configuration for the NexusDB server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Whether TLS is enabled.
    pub enabled: bool,
    /// Path to the server certificate (PEM format).
    pub cert_path: Option<PathBuf>,
    /// Path to the server private key (PEM format).
    pub key_path: Option<PathBuf>,
    /// Whether mutual TLS (client certificate verification) is required.
    pub mtls_enabled: bool,
    /// Path to the CA certificate for verifying client certs (mTLS).
    pub ca_cert_path: Option<PathBuf>,
    /// Minimum TLS version (default: TLS 1.3).
    pub min_tls_version: TlsVersion,
}

/// Supported TLS protocol versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TlsVersion {
    /// TLS 1.2 (minimum for compatibility).
    Tls12,
    /// TLS 1.3 (recommended, default).
    Tls13,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cert_path: None,
            key_path: None,
            mtls_enabled: false,
            ca_cert_path: None,
            min_tls_version: TlsVersion::Tls13,
        }
    }
}

impl TlsConfig {
    /// Create a TLS config for server-side TLS.
    pub fn server(cert_path: PathBuf, key_path: PathBuf) -> Self {
        Self {
            enabled: true,
            cert_path: Some(cert_path),
            key_path: Some(key_path),
            ..Default::default()
        }
    }

    /// Create a TLS config with mutual TLS.
    pub fn mtls(cert_path: PathBuf, key_path: PathBuf, ca_cert_path: PathBuf) -> Self {
        Self {
            enabled: true,
            cert_path: Some(cert_path),
            key_path: Some(key_path),
            mtls_enabled: true,
            ca_cert_path: Some(ca_cert_path),
            ..Default::default()
        }
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), TlsError> {
        if !self.enabled {
            return Ok(());
        }

        let cert = self
            .cert_path
            .as_ref()
            .ok_or_else(|| TlsError::ConfigError("cert_path required when TLS enabled".into()))?;

        let key = self
            .key_path
            .as_ref()
            .ok_or_else(|| TlsError::ConfigError("key_path required when TLS enabled".into()))?;

        if !cert.exists() {
            return Err(TlsError::CertNotFound(cert.clone()));
        }
        if !key.exists() {
            return Err(TlsError::KeyNotFound(key.clone()));
        }

        if self.mtls_enabled {
            let ca = self.ca_cert_path.as_ref().ok_or_else(|| {
                TlsError::ConfigError("ca_cert_path required when mTLS enabled".into())
            })?;
            if !ca.exists() {
                return Err(TlsError::CaNotFound(ca.clone()));
            }
        }

        Ok(())
    }

    /// Check if TLS is enabled and properly configured.
    pub fn is_active(&self) -> bool {
        self.enabled && self.cert_path.is_some() && self.key_path.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_tls_disabled_by_default() {
        let config = TlsConfig::default();
        assert!(!config.enabled);
        assert!(!config.is_active());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_tls_validation_missing_cert() {
        let config = TlsConfig {
            enabled: true,
            cert_path: Some(PathBuf::from("/nonexistent/cert.pem")),
            key_path: Some(PathBuf::from("/nonexistent/key.pem")),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_tls_validation_with_files() {
        let cert = NamedTempFile::new().unwrap();
        let key = NamedTempFile::new().unwrap();

        let config = TlsConfig::server(
            cert.path().to_path_buf(),
            key.path().to_path_buf(),
        );

        assert!(config.validate().is_ok());
        assert!(config.is_active());
    }

    #[test]
    fn test_mtls_validation() {
        let cert = NamedTempFile::new().unwrap();
        let key = NamedTempFile::new().unwrap();
        let ca = NamedTempFile::new().unwrap();

        let config = TlsConfig::mtls(
            cert.path().to_path_buf(),
            key.path().to_path_buf(),
            ca.path().to_path_buf(),
        );

        assert!(config.validate().is_ok());
        assert!(config.mtls_enabled);
    }

    #[test]
    fn test_mtls_missing_ca() {
        let cert = NamedTempFile::new().unwrap();
        let key = NamedTempFile::new().unwrap();

        let config = TlsConfig {
            enabled: true,
            cert_path: Some(cert.path().to_path_buf()),
            key_path: Some(key.path().to_path_buf()),
            mtls_enabled: true,
            ca_cert_path: None,
            ..Default::default()
        };

        assert!(config.validate().is_err());
    }
}
