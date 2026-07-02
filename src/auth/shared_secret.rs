use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::fmt;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SharedSecretError {
    #[error("HMAC error: {0}")]
    Hmac(String),
    #[error("system time error: {0}")]
    Time(String),
    #[error("invalid timestamp: {0}")]
    InvalidTimestamp(String),
}

/// Plug.Crypto MessageVerifier protocol header for HMAC-SHA256.
/// This is base64url("HS256") without padding.
const PROTOC_HS256: &str = "SFMyNTY";

/// Parameters for Shared Secret authentication.
#[derive(Clone)]
pub struct SharedSecretAuth {
    pub key: String,
    pub secret: String,
    pub digest: String,
    pub iterations: u32,
    pub key_length: usize,
}

impl fmt::Debug for SharedSecretAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedSecretAuth")
            .field("key", &self.key)
            .field("secret", &"<redacted>")
            .field("digest", &self.digest)
            .field("iterations", &self.iterations)
            .field("key_length", &self.key_length)
            .finish()
    }
}

impl SharedSecretAuth {
    pub fn new(key: String, secret: String) -> Self {
        Self {
            key,
            secret,
            digest: "sha256".to_string(),
            iterations: 1000,
            key_length: 32,
        }
    }

    /// The algorithm string sent in the x-nh-alg header.
    pub fn algorithm(&self) -> String {
        format!(
            "NH1-HMAC-{}-{}-{}",
            self.digest, self.iterations, self.key_length
        )
    }

    /// Generate auth headers for a WebSocket connection.
    /// Returns a list of (header_name, header_value) pairs.
    pub fn auth_headers(
        &self,
        identifier: &str,
    ) -> Result<Vec<(String, String)>, SharedSecretError> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| SharedSecretError::Time(e.to_string()))?
            .as_secs();

        self.auth_headers_at(identifier, timestamp)
    }

    /// Generate auth headers with a specific timestamp (for testing).
    pub fn auth_headers_at(
        &self,
        identifier: &str,
        timestamp: u64,
    ) -> Result<Vec<(String, String)>, SharedSecretError> {
        let alg = self.algorithm();
        let timestamp_str = timestamp.to_string();

        let signature = self.compute_signature(identifier, &alg, &timestamp_str)?;

        Ok(vec![
            ("x-nh-alg".to_string(), alg),
            ("x-nh-key".to_string(), self.key.clone()),
            ("x-nh-time".to_string(), timestamp_str),
            ("x-nh-signature".to_string(), signature),
        ])
    }

    fn compute_signature(
        &self,
        identifier: &str,
        alg: &str,
        timestamp: &str,
    ) -> Result<String, SharedSecretError> {
        // Build salt matching the Elixir heredoc (includes trailing newline)
        let salt = format!(
            "NH1:device-socket:shared-secret:connect\n\nx-nh-alg={}\nx-nh-key={}\nx-nh-time={}\n",
            alg, self.key, timestamp
        );

        // Derive key using PBKDF2 (matches Plug.Crypto.KeyGenerator)
        let mut derived_key = vec![0u8; self.key_length];
        pbkdf2::pbkdf2_hmac::<Sha256>(
            self.secret.as_bytes(),
            salt.as_bytes(),
            self.iterations,
            &mut derived_key,
        );

        // Build a Plug.Crypto token: SFMyNTY.{payload}.{signature}
        // Plug.Crypto.sign encodes: term_to_binary({data, signed_at_ms, max_age})
        let signed_at_secs: u64 = timestamp.parse().map_err(|e: std::num::ParseIntError| {
            SharedSecretError::InvalidTimestamp(e.to_string())
        })?;
        let signed_at_ms: u64 = signed_at_secs * 1000;
        let max_age: u64 = 86400; // Plug.Crypto default
        let term_binary = encode_token_term(identifier, signed_at_ms, max_age);
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&term_binary);

        // HMAC the "header.payload" string
        let signing_input = format!("{}.{}", PROTOC_HS256, payload);
        let mut mac = Hmac::<Sha256>::new_from_slice(&derived_key)
            .map_err(|e| SharedSecretError::Hmac(e.to_string()))?;
        mac.update(signing_input.as_bytes());
        let hmac_result = mac.finalize().into_bytes();
        let encoded_sig = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&hmac_result);

        Ok(format!("{}.{}.{}", PROTOC_HS256, payload, encoded_sig))
    }
}

/// Encode {identifier, signed_at_ms, max_age} in Erlang External Term Format.
///
/// This matches Plug.Crypto v2.x's encode/2:
///   :erlang.term_to_binary({data, signed_at_ms, max_age_in_seconds})
fn encode_token_term(identifier: &str, signed_at_ms: u64, max_age: u64) -> Vec<u8> {
    let mut buf = Vec::new();

    // Version byte
    buf.push(131);

    // Small tuple, 3 elements
    buf.push(104);
    buf.push(3);

    // Binary (Elixir string) for identifier
    let id_bytes = identifier.as_bytes();
    buf.push(109);
    buf.extend_from_slice(&(id_bytes.len() as u32).to_be_bytes());
    buf.extend_from_slice(id_bytes);

    // Integer for signed_at_ms (milliseconds - typically needs SMALL_BIG_EXT)
    encode_integer(&mut buf, signed_at_ms);

    // Integer for max_age
    encode_integer(&mut buf, max_age);

    buf
}

/// Encode an integer in Erlang External Term Format.
fn encode_integer(buf: &mut Vec<u8>, value: u64) {
    if value <= 255 {
        // SMALL_INTEGER_EXT
        buf.push(97);
        buf.push(value as u8);
    } else if value <= i32::MAX as u64 {
        // INTEGER_EXT (signed 32-bit big-endian)
        buf.push(98);
        buf.extend_from_slice(&(value as i32).to_be_bytes());
    } else {
        // SMALL_BIG_EXT for values that don't fit in 32-bit signed int
        // Unix timestamps after 2038 will need this
        let mut bytes = value.to_le_bytes().to_vec();
        // Trim trailing zeros
        while bytes.last() == Some(&0) && bytes.len() > 1 {
            bytes.pop();
        }
        buf.push(110); // SMALL_BIG_EXT
        buf.push(bytes.len() as u8);
        buf.push(0); // sign: 0 = positive
        buf.extend_from_slice(&bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn algorithm_string() {
        let auth = SharedSecretAuth::new("key".to_string(), "secret".to_string());
        assert_eq!(auth.algorithm(), "NH1-HMAC-sha256-1000-32");
    }

    #[test]
    fn debug_redacts_secret() {
        let auth = SharedSecretAuth::new("key".to_string(), "top-secret-value".to_string());
        let debug = format!("{:?}", auth);

        assert!(debug.contains("key"));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("top-secret-value"));
    }

    #[test]
    fn generates_headers() {
        let auth = SharedSecretAuth::new("device-key-1".to_string(), "my-secret".to_string());
        let headers = auth.auth_headers("device-serial-123").unwrap();

        assert_eq!(headers.len(), 4);
        assert_eq!(headers[0].0, "x-nh-alg");
        assert_eq!(headers[0].1, "NH1-HMAC-sha256-1000-32");
        assert_eq!(headers[1].0, "x-nh-key");
        assert_eq!(headers[1].1, "device-key-1");
        assert_eq!(headers[2].0, "x-nh-time");
        assert_eq!(headers[3].0, "x-nh-signature");
        // Plug.Crypto token format: SFMyNTY.{payload}.{sig}
        let sig = &headers[3].1;
        assert!(sig.starts_with("SFMyNTY."));
        let parts: Vec<&str> = sig.split('.').collect();
        assert_eq!(parts.len(), 3);
    }

    #[test]
    fn deterministic_with_same_timestamp() {
        let auth = SharedSecretAuth::new("key".to_string(), "secret".to_string());
        let h1 = auth.auth_headers_at("device-1", 1700000000).unwrap();
        let h2 = auth.auth_headers_at("device-1", 1700000000).unwrap();
        assert_eq!(h1[3].1, h2[3].1); // Same signature
    }

    #[test]
    fn different_timestamp_different_signature() {
        let auth = SharedSecretAuth::new("key".to_string(), "secret".to_string());
        let h1 = auth.auth_headers_at("device-1", 1700000000).unwrap();
        let h2 = auth.auth_headers_at("device-1", 1700000001).unwrap();
        assert_ne!(h1[3].1, h2[3].1);
    }

    #[test]
    fn different_identifier_different_signature() {
        let auth = SharedSecretAuth::new("key".to_string(), "secret".to_string());
        let h1 = auth.auth_headers_at("device-1", 1700000000).unwrap();
        let h2 = auth.auth_headers_at("device-2", 1700000000).unwrap();
        assert_ne!(h1[3].1, h2[3].1);
    }

    #[test]
    fn different_secret_different_signature() {
        let auth1 = SharedSecretAuth::new("key".to_string(), "secret-1".to_string());
        let auth2 = SharedSecretAuth::new("key".to_string(), "secret-2".to_string());
        let h1 = auth1.auth_headers_at("device-1", 1700000000).unwrap();
        let h2 = auth2.auth_headers_at("device-1", 1700000000).unwrap();
        assert_ne!(h1[3].1, h2[3].1);
    }

    #[test]
    fn token_payload_decodable() {
        // Verify the payload part is valid base64url containing our term
        let auth = SharedSecretAuth::new("key".to_string(), "secret".to_string());
        let headers = auth.auth_headers_at("test-serial", 1700000000).unwrap();
        let token = &headers[3].1;
        let parts: Vec<&str> = token.split('.').collect();

        // Decode the payload
        let payload_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(parts[1])
            .unwrap();

        // Should start with version 131, small tuple tag 104, arity 3
        assert_eq!(payload_bytes[0], 131); // ETF version
        assert_eq!(payload_bytes[1], 104); // SMALL_TUPLE_EXT
        assert_eq!(payload_bytes[2], 3); // 3 elements
                                         // First element is a binary (the identifier), not an atom
        assert_eq!(payload_bytes[3], 109); // BINARY_EXT
    }

    #[test]
    fn encode_small_integer() {
        let mut buf = Vec::new();
        encode_integer(&mut buf, 42);
        assert_eq!(buf, vec![97, 42]); // SMALL_INTEGER_EXT, value
    }

    #[test]
    fn encode_32bit_integer() {
        let mut buf = Vec::new();
        encode_integer(&mut buf, 1700000000);
        assert_eq!(buf[0], 98); // INTEGER_EXT
        let value = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
        assert_eq!(value, 1700000000);
    }

    #[test]
    fn encode_large_integer() {
        let mut buf = Vec::new();
        // Value larger than i32::MAX (2147483647) - like a millisecond timestamp
        encode_integer(&mut buf, 1700000000000);
        assert_eq!(buf[0], 110); // SMALL_BIG_EXT
        assert_eq!(buf[2], 0); // positive sign
    }

    #[test]
    fn term_binary_structure() {
        // {identifier, signed_at_ms, max_age} = {"hello", 1700000000000, 86400}
        let term = encode_token_term("hello", 1700000000000, 86400);
        assert_eq!(term[0], 131); // version
        assert_eq!(term[1], 104); // small tuple
        assert_eq!(term[2], 3); // 3 elements
                                // First element: binary "hello"
        assert_eq!(term[3], 109); // binary ext
        assert_eq!(&term[4..8], &5u32.to_be_bytes());
        assert_eq!(&term[8..13], b"hello");
        // Second element: large integer (1700000000000 > i32::MAX)
        assert_eq!(term[13], 110); // SMALL_BIG_EXT for ms timestamp
    }
}
