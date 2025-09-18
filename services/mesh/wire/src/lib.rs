//! Wire protocol framing, encoding/decoding, fast headers, and chunking for mesh.
//!
//! This crate provides the low-level wire protocol implementation for the mesh network,
//! including message framing, efficient encoding/decoding, fast header processing,
//! and message chunking for large payloads.
//!
//! ## Features
//!
//! - **Fast Header Processing**: 48-byte fixed header for efficient routing
//! - **Zero-Copy I/O**: Uses `Bytes`/`BytesMut` for minimal allocations
//! - **Message Chunking**: Automatic splitting and reassembly of large messages
//! - **E2E Encryption**: Optional AEAD encryption with hybrid key exchange
//! - **CBOR Metadata**: Extensible metadata using canonical CBOR
//! - **Scatter-Gather I/O**: Efficient vectored writes
//!
//! ## Wire Format
//!
//! ```text
//! +----------------------+----------------------------+
//! | u32 frame_len        | length of bytes that follow|
//! +----------------------+----------------------------+
//! | Fast Header (48B)    | routing + control info     |
//! +----------------------+----------------------------+
//! | Header Hint (opt)    | variable (0..128B)         |
//! +----------------------+----------------------------+
//! | Crypto Section (opt) | variable (0..64KB)         |
//! +----------------------+----------------------------+
//! | u32 meta_len         | canonical CBOR map length  |
//! +----------------------+----------------------------+
//! | meta_bytes           | metadata (CBOR)            |
//! +----------------------+----------------------------+
//! | payload_or_cipher    | variable (0..N)            |
//! +----------------------+----------------------------+
//! ```

#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod chunk;
pub mod codec;
pub mod error;
pub mod frame;
pub mod header;
pub mod topology;

// Re-export main types
pub use chunk::{ChunkMeta, Chunker, Reassembler, DEFAULT_CHUNK_SIZE};
pub use codec::{
    get_meta_str, get_meta_u32, parse_meta, CodecError, CryptoParams, FrameBuilder, MetaBuilder,
};
pub use error::WireError;
pub use frame::{
    EncAlg, Frame, FrameDecoder, KeyMode, DEFAULT_MAX_FRAME_SIZE, HARD_MAX_FRAME_SIZE,
    MAX_HINT_SIZE, MAX_META_SIZE,
};
pub use header::{
    crc32c_fast_header, FastHeader, Flags, FrameType, Route, StatusCode, FAST_HEADER_SIZE,
    WIRE_VERSION,
};
pub use topology::{NeighborInfo, TopologyRequest, TopologyUpdate};

#[cfg(feature = "crypto")]
pub use codec::{open_aead, seal_aead};
