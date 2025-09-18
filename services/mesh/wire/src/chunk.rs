//! Message chunking for large payloads.
//!
//! This module provides chunking and reassembly capabilities for messages
//! that exceed the maximum frame size.

use crate::frame::{Frame, DEFAULT_MAX_FRAME_SIZE};
use crate::header::{FastHeader, Flags};
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Default chunk size (leave room for headers and metadata)
pub const DEFAULT_CHUNK_SIZE: usize = DEFAULT_MAX_FRAME_SIZE - 1024;

/// Chunk metadata for CBOR
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMeta {
    /// 0-based chunk index
    pub no: u32,
    /// Total number of chunks
    pub total: u32,
    /// Original message size in bytes
    pub size: u32,
}

/// Chunker for splitting large messages into frames
pub struct Chunker {
    chunk_size: usize,
}

impl Chunker {
    /// Create a new chunker with default chunk size
    pub fn new() -> Self {
        Self {
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    /// Split a large payload into chunked frames
    pub fn chunk_message(&self, fast_header: FastHeader, payload: Bytes) -> Vec<Frame> {
        if payload.is_empty() {
            return vec![];
        }

        let total_chunks = (payload.len() + self.chunk_size - 1) / self.chunk_size;
        let mut frames = Vec::with_capacity(total_chunks);
        let mut offset = 0;

        for chunk_no in 0..total_chunks {
            let chunk_end = std::cmp::min(offset + self.chunk_size, payload.len());
            let chunk_data = payload.slice(offset..chunk_end);

            let mut header = fast_header;
            header.flags |= Flags::CHUNKED;
            if chunk_no == total_chunks - 1 {
                header.flags |= Flags::CHUNK_END;
            }

            // Simple metadata for now
            let meta_raw = Bytes::from_static(b"{}");

            let frame = Frame::new(header, meta_raw, chunk_data);
            frames.push(frame);
            offset = chunk_end;
        }

        frames
    }
}

/// Reassembler for collecting chunks into complete messages
pub struct Reassembler {
    sessions: HashMap<u64, Vec<Bytes>>,
}

impl Reassembler {
    /// Create a new reassembler
    pub fn new() -> Self {
        Self {
            sessions: HashMap::new(),
        }
    }

    /// Add a chunk and potentially return a complete message
    pub fn add_chunk(&mut self, frame: Frame) -> Option<Bytes> {
        if !frame.fast.flags.contains(Flags::CHUNKED) {
            return None;
        }

        let msg_id = frame.fast.msg_id;
        let chunks = self.sessions.entry(msg_id).or_insert_with(Vec::new);
        chunks.push(frame.payload_or_cipher);

        if frame.fast.flags.contains(Flags::CHUNK_END) {
            // Reassemble complete message
            let mut result = BytesMut::new();
            for chunk in chunks {
                result.extend_from_slice(chunk);
            }
            self.sessions.remove(&msg_id);
            Some(result.freeze())
        } else {
            None
        }
    }
}

impl Default for Chunker {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for Reassembler {
    fn default() -> Self {
        Self::new()
    }
}
