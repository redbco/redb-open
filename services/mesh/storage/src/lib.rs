//! Reliability storage for mesh: WAL + Dedup with pluggable backends.
//!
//! This crate provides the storage layer for reliable message delivery in the mesh network,
//! including write-ahead logging for sender-side persistence, deduplication for receiver-side
//! idempotency, and pluggable backends (in-memory, file-based, Redis).

#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod backend;

use async_trait::async_trait;
use std::fmt;
use thiserror::Error;

/// Remote peer identifier (node ID)
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Peer(pub u64);

impl fmt::Display for Peer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Frame data for WAL storage
#[derive(Clone, Debug)]
pub struct WalFrame<'a> {
    /// Message ID (monotonic per peer)
    pub msg_id: u64,
    /// Serialized mesh-wire frame bytes
    pub bytes: &'a [u8],
    /// Cached length for credit accounting
    pub approx_len: usize,
}

/// ACK state tracking for sender-side flow control
#[derive(Clone, Debug, Default)]
pub struct AckState {
    /// Last contiguous ACK received from peer (sender-side view)
    pub cum_acked: u64,
}

/// Storage errors
#[derive(Error, Debug)]
pub enum StorageError {
    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// Data corruption detected
    #[error("Data corruption: {0}")]
    Corruption(String),
    /// Entry not found
    #[error("Entry not found")]
    NotFound,
    /// Invalid operation
    #[error("Invalid operation: {0}")]
    Invalid(String),
    /// Backend-specific error
    #[error("Backend error: {0}")]
    Backend(String),
    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

/// WAL entry for iteration
#[derive(Debug, Clone)]
pub struct WalEntry {
    /// Message ID
    pub msg_id: u64,
    /// Frame bytes
    pub bytes: Vec<u8>,
}

/// Write-Ahead Log trait for sender-side reliability
#[async_trait]
pub trait Wal: Send + Sync {
    /// Append a frame to the WAL for a peer
    async fn append(&self, peer: Peer, frame: WalFrame<'_>) -> Result<(), StorageError>;

    /// Get frames in range (from_exclusive, +∞) up to limit
    async fn range(
        &self,
        peer: Peer,
        from_exclusive: u64,
        limit: Option<usize>,
    ) -> Result<Vec<WalEntry>, StorageError>;

    /// Truncate WAL through msg_id (inclusive) - can delete these entries
    async fn truncate_through(&self, peer: Peer, up_to_inclusive: u64) -> Result<(), StorageError>;

    /// Get the last appended message ID for a peer
    async fn last_appended(&self, peer: Peer) -> Result<u64, StorageError>;

    /// Load ACK state for a peer
    async fn load_ack(&self, peer: Peer) -> Result<AckState, StorageError>;

    /// Store ACK state for a peer
    async fn store_ack(&self, peer: Peer, ack: AckState) -> Result<(), StorageError>;
}

/// Deduplication trait for receiver-side idempotency
#[async_trait]
pub trait Dedup: Send + Sync {
    /// Check if a message ID has been processed for a peer
    async fn is_processed(&self, peer: Peer, msg_id: u64) -> Result<bool, StorageError>;

    /// Mark a message ID as processed for a peer
    async fn mark_processed(&self, peer: Peer, msg_id: u64) -> Result<(), StorageError>;

    /// Get the cumulative processed watermark for a peer
    async fn cum_processed(&self, peer: Peer) -> Result<u64, StorageError>;

    /// Advance cumulative processed watermark for a peer
    async fn advance_cum(&self, peer: Peer, id: u64) -> Result<(), StorageError>;

    /// Periodic persistence snapshot (optional)
    async fn snapshot(&self) -> Result<(), StorageError>;
}

/// Combined storage interface
pub struct Storage {
    /// Write-ahead log for sender reliability
    pub wal: Box<dyn Wal>,
    /// Deduplication for receiver idempotency
    pub dedup: Box<dyn Dedup>,
}

/// Storage backend configuration
#[derive(Clone, Debug)]
pub enum StorageMode {
    /// In-memory storage (dev/tests only)
    InMemory,
    /// File-based storage with configurable segments
    File {
        /// Data directory path
        data_dir: String,
        /// Segment size in bytes
        segment_bytes: u64,
        /// Fsync frequency (1 = every write, N = every N writes)
        fsync_every: u32,
    },
    /// Redis cache over another backend
    RedisCache {
        /// Redis connection URL
        url: String,
        /// Wrapped backend
        wrap: Box<StorageMode>,
    },
    /// Redis as primary storage
    RedisPrimary {
        /// Redis connection URL
        url: String,
    },
}

impl Default for StorageMode {
    fn default() -> Self {
        StorageMode::InMemory
    }
}

// Re-export backend implementations
pub use backend::file::{FileDedup, FileWal, FileWalConfig};
pub use backend::mem::{MemoryDedup, MemoryWal};

impl Storage {
    /// Create storage from configuration
    pub async fn from_mode(mode: StorageMode) -> Result<Self, StorageError> {
        match mode {
            StorageMode::InMemory => Ok(Storage {
                wal: Box::new(MemoryWal::new()),
                dedup: Box::new(MemoryDedup::with_default_window()),
            }),
            StorageMode::File {
                data_dir,
                segment_bytes,
                fsync_every,
            } => {
                let config = FileWalConfig {
                    data_dir: data_dir.into(),
                    segment_bytes,
                    fsync_every,
                };
                Ok(Storage {
                    wal: Box::new(FileWal::new(config.clone()).await?),
                    dedup: Box::new(FileDedup::new(config, 65536).await?),
                })
            }
            StorageMode::RedisCache { .. } => {
                // TODO: Implement Redis cache wrapper
                Err(StorageError::Invalid(
                    "Redis cache not yet implemented".to_string(),
                ))
            }
            StorageMode::RedisPrimary { .. } => {
                // TODO: Implement Redis primary storage
                Err(StorageError::Invalid(
                    "Redis primary not yet implemented".to_string(),
                ))
            }
        }
    }
}
