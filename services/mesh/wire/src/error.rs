//! Wire protocol error types.

use thiserror::Error;

/// Wire protocol errors
#[derive(Error, Debug)]
pub enum WireError {
    /// Incomplete frame (need more data)
    #[error("incomplete frame")]
    Incomplete,

    /// Unsupported protocol version
    #[error("version unsupported: {0}")]
    Version(u8),

    /// Invalid TTL
    #[error("invalid ttl")]
    Ttl,

    /// Size limit exceeded
    #[error("size limit exceeded: {0}")]
    Size(usize),

    /// Malformed crypto section
    #[error("malformed crypto section")]
    Crypto,

    /// Invalid CBOR metadata
    #[error("cbor meta invalid")]
    Meta,

    /// Header checksum mismatch
    #[error("hdr checksum mismatch")]
    HdrCsum,

    /// Reserved bits nonzero
    #[error("reserved bits nonzero")]
    Reserved,

    /// Unknown frame type
    #[error("unknown type {0}")]
    Type(u8),

    /// Unknown status code
    #[error("unknown code {0}")]
    Code(u8),

    /// Malformed frame structure
    #[error("malformed frame")]
    Malformed,
}
