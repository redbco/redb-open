//! Message framing for the wire protocol.
//!
//! This module provides the complete frame structure including fast header,
//! optional crypto section, metadata, and payload handling.

use crate::header::{FastHeader, FAST_HEADER_SIZE};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

/// Maximum frame size (16 MiB default, 64 MiB hard limit)
pub const DEFAULT_MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;
/// Hard maximum frame size limit (64 MiB)
pub const HARD_MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;

/// Maximum metadata size (64 KiB)
pub const MAX_META_SIZE: usize = 64 * 1024;

/// Maximum header hint size (128 bytes)
pub const MAX_HINT_SIZE: usize = 128;

/// Encryption algorithms
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EncAlg {
    /// No encryption
    None = 0,
    /// ChaCha20-Poly1305
    Chacha20Poly1305 = 1,
    /// AES-128-GCM
    Aes128Gcm = 2,
    /// AES-256-GCM
    Aes256Gcm = 3,
}

/// Key modes for encryption
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyMode {
    /// Ephemeral wrapped key
    EphemeralWrapped = 1,
    /// Channel key ID
    ChannelKeyId = 2,
}

/// Complete wire frame
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// Fast header (48 bytes)
    pub fast: FastHeader,
    /// Optional header hint TLV
    pub hint: Option<Bytes>,
    /// Metadata as raw CBOR bytes
    pub meta_raw: Bytes,
    /// Payload or ciphertext
    pub payload_or_cipher: Bytes,
}

impl Frame {
    /// Create a new frame with minimal required fields
    pub fn new(fast: FastHeader, meta_raw: Bytes, payload_or_cipher: Bytes) -> Self {
        Self {
            fast,
            hint: None,
            meta_raw,
            payload_or_cipher,
        }
    }

    /// Set header hint TLV
    pub fn with_hint(mut self, hint: Bytes) -> Self {
        self.hint = Some(hint);
        self
    }

    /// Get the total frame size when encoded
    pub fn encoded_size(&self) -> usize {
        let mut size = 4; // frame_len u32
        size += FAST_HEADER_SIZE; // fast header

        if let Some(ref hint) = self.hint {
            size += hint.len();
        }

        size += 4; // meta_len u32
        size += self.meta_raw.len();
        size += self.payload_or_cipher.len();

        size
    }

    /// Encode frame to a contiguous buffer
    pub fn encode(&self, max_frame_size: usize) -> Result<Bytes, crate::WireError> {
        let total_size = self.encoded_size();
        if total_size > max_frame_size {
            return Err(crate::WireError::Size(total_size));
        }

        let mut buf = BytesMut::with_capacity(total_size);

        // Frame length (everything after this u32)
        let frame_len = total_size - 4;
        buf.put_u32(frame_len as u32);

        // Fast header
        self.fast.encode(&mut buf);

        // Header hint TLV
        if let Some(ref hint) = self.hint {
            buf.put_slice(hint);
        }

        // Metadata
        buf.put_u32(self.meta_raw.len() as u32);
        buf.put_slice(&self.meta_raw);

        // Payload
        buf.put_slice(&self.payload_or_cipher);

        Ok(buf.freeze())
    }
}

/// Frame decoder for parsing incoming frames
#[derive(Debug)]
pub struct FrameDecoder {
    max_frame_size: usize,
}

impl FrameDecoder {
    /// Create a new frame decoder
    pub fn new() -> Self {
        Self {
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        }
    }

    /// Decode one frame from a buffer
    pub fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Frame>, crate::WireError> {
        // Need at least 4 bytes for frame length
        if buf.len() < 4 {
            return Ok(None);
        }

        // Peek at frame length
        let frame_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;

        // Check frame size limits
        if frame_len > self.max_frame_size {
            return Err(crate::WireError::Size(frame_len));
        }

        // Check if we have the complete frame
        if buf.len() < 4 + frame_len {
            return Ok(None);
        }

        // Skip the frame length field
        buf.advance(4);

        // Decode fast header
        let mut frame_buf = buf.split_to(frame_len).freeze();
        let fast = FastHeader::decode(&mut frame_buf)?;

        // Decode header hint TLV if present
        let hint = if fast.hdr_hint_len > 0 {
            if frame_buf.len() < fast.hdr_hint_len as usize {
                return Err(crate::WireError::Malformed);
            }
            Some(frame_buf.split_to(fast.hdr_hint_len as usize))
        } else {
            None
        };

        // Decode metadata
        if frame_buf.len() < 4 {
            return Err(crate::WireError::Malformed);
        }

        let meta_len = frame_buf.get_u32() as usize;
        if meta_len > MAX_META_SIZE || frame_buf.len() < meta_len {
            return Err(crate::WireError::Meta);
        }

        let meta_raw = frame_buf.split_to(meta_len);

        // Remaining bytes are payload
        let payload_or_cipher = frame_buf;

        Ok(Some(Frame {
            fast,
            hint,
            meta_raw,
            payload_or_cipher,
        }))
    }
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self::new()
    }
}
