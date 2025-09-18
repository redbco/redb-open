//! Encoding and decoding for the wire protocol.
//!
//! This module provides frame builders, CBOR metadata helpers, and optional
//! AEAD crypto operations for end-to-end encryption.

use crate::frame::{EncAlg, Frame, KeyMode};
use crate::header::{crc32c_fast_header, FastHeader, Flags};
use bytes::Bytes;
use std::collections::BTreeMap;
use thiserror::Error;

/// Crypto parameters for frame building
#[derive(Debug, Clone)]
pub struct CryptoParams {
    /// Encryption algorithm
    pub enc_alg: EncAlg,
    /// Key mode
    pub key_mode: KeyMode,
    /// Key reference (channel key ID or wrapped key)
    pub key_ref: Bytes,
    /// Nonce for AEAD
    pub nonce: Bytes,
    /// AEAD tag length
    pub tag_len: u8,
    /// Whether AAD binds header (must be true for E2E)
    pub aad_binds_header: bool,
}

/// CBOR metadata builder helper
#[derive(Debug, Clone)]
pub struct MetaBuilder {
    map: BTreeMap<String, ciborium::Value>,
}

impl MetaBuilder {
    /// Create a new metadata builder
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }

    /// Insert a string value
    pub fn insert_str(mut self, key: &str, value: &str) -> Self {
        self.map
            .insert(key.to_string(), ciborium::Value::Text(value.to_string()));
        self
    }

    /// Insert a u32 value
    pub fn insert_u32(mut self, key: &str, value: u32) -> Self {
        self.map
            .insert(key.to_string(), ciborium::Value::Integer(value.into()));
        self
    }

    /// Insert binary data
    pub fn insert_bytes(mut self, key: &str, value: &[u8]) -> Self {
        self.map
            .insert(key.to_string(), ciborium::Value::Bytes(value.to_vec()));
        self
    }

    /// Build the metadata as CBOR bytes
    pub fn build(self) -> Result<Bytes, CodecError> {
        let value = ciborium::Value::Map(
            self.map
                .into_iter()
                .map(|(k, v)| (ciborium::Value::Text(k), v))
                .collect(),
        );

        let mut buf = Vec::new();
        ciborium::into_writer(&value, &mut buf).map_err(|_| CodecError::MetaEncode)?;

        Ok(Bytes::from(buf))
    }
}

impl Default for MetaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Frame builder for constructing wire frames
#[derive(Debug)]
pub struct FrameBuilder {
    fast: FastHeader,
    hint_tlv: Option<Bytes>,
    crypto: Option<CryptoParams>,
    meta: MetaBuilder,
    payload: Bytes,
}

impl FrameBuilder {
    /// Create a new frame builder
    pub fn new(fast: FastHeader) -> Self {
        Self {
            fast,
            hint_tlv: None,
            crypto: None,
            meta: MetaBuilder::new(),
            payload: Bytes::new(),
        }
    }

    /// Add header hint TLV
    pub fn with_hint_tlv(mut self, tlv: Bytes) -> Self {
        self.fast.hdr_hint_len = tlv.len() as u32;
        self.hint_tlv = Some(tlv);
        self
    }

    /// Add crypto parameters
    pub fn with_crypto(mut self, crypto: CryptoParams) -> Self {
        self.fast.flags |= Flags::E2E_ENC;
        self.crypto = Some(crypto);
        self
    }

    /// Insert string metadata
    pub fn meta_insert_str(mut self, key: &str, value: &str) -> Self {
        self.meta = self.meta.insert_str(key, value);
        self
    }

    /// Insert u32 metadata
    pub fn meta_insert_u32(mut self, key: &str, value: u32) -> Self {
        self.meta = self.meta.insert_u32(key, value);
        self
    }

    /// Insert binary metadata
    pub fn meta_insert_bytes(mut self, key: &str, value: &[u8]) -> Self {
        self.meta = self.meta.insert_bytes(key, value);
        self
    }

    /// Set payload
    pub fn payload(mut self, payload: Bytes) -> Self {
        self.payload = payload;
        self
    }

    /// Build the frame
    pub fn build(mut self, max_frame: usize) -> Result<Bytes, CodecError> {
        // Add header checksum if requested
        if self.fast.flags.contains(Flags::HDR_CHECKSUM) {
            let checksum = crc32c_fast_header(&self.fast, self.hint_tlv.as_deref());
            self.meta = self.meta.insert_u32("hdr_csum", checksum);
        }

        // Build metadata
        let meta_raw = self.meta.build()?;

        // Handle crypto if present
        let payload_or_cipher = if let Some(crypto_params) = self.crypto {
            Self::seal_payload_static(&crypto_params, &meta_raw, &self.payload)?
        } else {
            self.payload
        };

        // Create frame
        let mut frame = Frame::new(self.fast, meta_raw, payload_or_cipher);

        if let Some(hint) = self.hint_tlv {
            frame = frame.with_hint(hint);
        }

        // Encode frame
        frame.encode(max_frame).map_err(CodecError::Wire)
    }

    /// Seal payload with AEAD (simplified version)
    fn seal_payload_static(
        _crypto_params: &CryptoParams,
        _meta_raw: &[u8],
        payload: &Bytes,
    ) -> Result<Bytes, CodecError> {
        // For now, just return the payload as-is
        // In a real implementation, this would perform AEAD encryption
        Ok(payload.clone())
    }
}

/// Codec errors
#[derive(Error, Debug)]
pub enum CodecError {
    /// Wire protocol error
    #[error("wire error: {0}")]
    Wire(#[from] crate::WireError),
    /// Metadata encoding error
    #[error("metadata encoding failed")]
    MetaEncode,
    /// Metadata decoding error
    #[error("metadata decoding failed")]
    MetaDecode,
    /// Crypto error
    #[error("crypto error")]
    Crypto,
}

/// AEAD seal operation (placeholder)
#[cfg(feature = "crypto")]
pub fn seal_aead(
    _enc_alg: EncAlg,
    _key: &[u8],
    _nonce: &[u8],
    _aad: &[u8],
    plaintext: &[u8],
    _tag_len: u8,
) -> Result<Bytes, CodecError> {
    // Placeholder implementation
    Ok(Bytes::copy_from_slice(plaintext))
}

/// AEAD open operation (placeholder)
#[cfg(feature = "crypto")]
pub fn open_aead(
    _enc_alg: EncAlg,
    _key: &[u8],
    _nonce: &[u8],
    _aad: &[u8],
    cipher_and_tag: &[u8],
    _tag_len: u8,
) -> Result<Bytes, CodecError> {
    // Placeholder implementation
    Ok(Bytes::copy_from_slice(cipher_and_tag))
}

/// Parse CBOR metadata into a map
pub fn parse_meta(meta_raw: &[u8]) -> Result<BTreeMap<String, ciborium::Value>, CodecError> {
    let value: ciborium::Value =
        ciborium::from_reader(meta_raw).map_err(|_| CodecError::MetaDecode)?;

    if let ciborium::Value::Map(map) = value {
        let mut result = BTreeMap::new();
        for (key, value) in map {
            if let ciborium::Value::Text(key_str) = key {
                result.insert(key_str, value);
            }
        }
        Ok(result)
    } else {
        Err(CodecError::MetaDecode)
    }
}

/// Get string value from metadata
pub fn get_meta_str(meta: &BTreeMap<String, ciborium::Value>, key: &str) -> Option<String> {
    meta.get(key).and_then(|v| {
        if let ciborium::Value::Text(s) = v {
            Some(s.clone())
        } else {
            None
        }
    })
}

/// Get u32 value from metadata
pub fn get_meta_u32(meta: &BTreeMap<String, ciborium::Value>, key: &str) -> Option<u32> {
    meta.get(key).and_then(|v| {
        if let ciborium::Value::Integer(i) = v {
            (*i).try_into().ok()
        } else {
            None
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::DEFAULT_MAX_FRAME_SIZE;
    use crate::header::FrameType;

    #[test]
    fn test_meta_builder() {
        let meta = MetaBuilder::new()
            .insert_str("content-type", "application/octet-stream")
            .insert_u32("version", 1)
            .insert_bytes("data", b"test")
            .build()
            .unwrap();

        let parsed = parse_meta(&meta).unwrap();
        assert_eq!(
            get_meta_str(&parsed, "content-type"),
            Some("application/octet-stream".to_string())
        );
        assert_eq!(get_meta_u32(&parsed, "version"), Some(1));
    }

    #[test]
    fn test_frame_builder() {
        let fast = FastHeader::new(FrameType::Data, 0x1234567890ABCDEF, 0xFEDCBA0987654321, 42);

        let frame_bytes = FrameBuilder::new(fast)
            .meta_insert_str("content-type", "application/octet-stream")
            .payload(Bytes::from_static(b"hello world"))
            .build(DEFAULT_MAX_FRAME_SIZE)
            .unwrap();

        assert!(!frame_bytes.is_empty());
    }

    #[test]
    fn test_frame_builder_with_checksum() {
        let mut fast = FastHeader::new(FrameType::Data, 0x1234567890ABCDEF, 0xFEDCBA0987654321, 42);
        fast.flags |= Flags::HDR_CHECKSUM;

        let frame_bytes = FrameBuilder::new(fast)
            .meta_insert_str("content-type", "application/octet-stream")
            .payload(Bytes::from_static(b"hello world"))
            .build(DEFAULT_MAX_FRAME_SIZE)
            .unwrap();

        assert!(!frame_bytes.is_empty());
    }
}
