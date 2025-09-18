//! In-memory storage backend for development and testing

use crate::{AckState, Dedup, Peer, StorageError, Wal, WalEntry, WalFrame};
use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use tracing::debug;

/// In-memory WAL implementation
pub struct MemoryWal {
    /// Per-peer WAL entries: peer -> VecDeque<(msg_id, frame_bytes)>
    entries: Arc<DashMap<Peer, VecDeque<(u64, Bytes)>>>,
    /// Per-peer ACK state: peer -> AckState
    ack_state: Arc<DashMap<Peer, AckState>>,
    /// Per-peer last appended message ID
    last_appended: Arc<DashMap<Peer, u64>>,
}

impl MemoryWal {
    /// Create a new in-memory WAL
    pub fn new() -> Self {
        Self {
            entries: Arc::new(DashMap::new()),
            ack_state: Arc::new(DashMap::new()),
            last_appended: Arc::new(DashMap::new()),
        }
    }
}

impl Default for MemoryWal {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Wal for MemoryWal {
    async fn append(&self, peer: Peer, frame: WalFrame<'_>) -> Result<(), StorageError> {
        debug!(
            "WAL append peer={} msg_id={} len={}",
            peer,
            frame.msg_id,
            frame.bytes.len()
        );

        let frame_bytes = Bytes::copy_from_slice(frame.bytes);

        // Get or create entry queue for peer
        let mut entries = self.entries.entry(peer).or_insert_with(VecDeque::new);
        entries.push_back((frame.msg_id, frame_bytes));

        // Update last appended
        self.last_appended.insert(peer, frame.msg_id);

        Ok(())
    }

    async fn range(
        &self,
        peer: Peer,
        from_exclusive: u64,
        limit: Option<usize>,
    ) -> Result<Vec<WalEntry>, StorageError> {
        debug!(
            "WAL range peer={} from_exclusive={} limit={:?}",
            peer, from_exclusive, limit
        );

        let mut results = Vec::new();

        if let Some(entries) = self.entries.get(&peer) {
            for (msg_id, frame_bytes) in entries.iter() {
                if *msg_id > from_exclusive {
                    results.push(WalEntry {
                        msg_id: *msg_id,
                        bytes: frame_bytes.to_vec(),
                    });

                    if let Some(limit) = limit {
                        if results.len() >= limit {
                            break;
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    async fn truncate_through(&self, peer: Peer, up_to_inclusive: u64) -> Result<(), StorageError> {
        debug!(
            "WAL truncate peer={} up_to_inclusive={}",
            peer, up_to_inclusive
        );

        if let Some(mut entries) = self.entries.get_mut(&peer) {
            // Remove entries with msg_id <= up_to_inclusive
            entries.retain(|(msg_id, _)| *msg_id > up_to_inclusive);
        }

        Ok(())
    }

    async fn last_appended(&self, peer: Peer) -> Result<u64, StorageError> {
        Ok(self.last_appended.get(&peer).map(|v| *v).unwrap_or(0))
    }

    async fn load_ack(&self, peer: Peer) -> Result<AckState, StorageError> {
        Ok(self
            .ack_state
            .get(&peer)
            .map(|v| v.clone())
            .unwrap_or_default())
    }

    async fn store_ack(&self, peer: Peer, ack: AckState) -> Result<(), StorageError> {
        debug!("WAL store_ack peer={} cum_acked={}", peer, ack.cum_acked);
        self.ack_state.insert(peer, ack);
        Ok(())
    }
}

/// In-memory deduplication implementation
pub struct MemoryDedup {
    /// Per-peer cumulative processed watermark
    cum_processed: Arc<DashMap<Peer, u64>>,
    /// Per-peer gap window for out-of-order messages
    gap_window: Arc<DashMap<Peer, HashSet<u64>>>,
    /// Window size for gap tracking
    window_size: u64,
}

impl MemoryDedup {
    /// Create a new in-memory dedup with specified window size
    pub fn new(window_size: u64) -> Self {
        Self {
            cum_processed: Arc::new(DashMap::new()),
            gap_window: Arc::new(DashMap::new()),
            window_size,
        }
    }

    /// Create with default window size (64K)
    pub fn with_default_window() -> Self {
        Self::new(65536)
    }
}

impl Default for MemoryDedup {
    fn default() -> Self {
        Self::with_default_window()
    }
}

#[async_trait]
impl Dedup for MemoryDedup {
    async fn is_processed(&self, peer: Peer, msg_id: u64) -> Result<bool, StorageError> {
        let cum = self.cum_processed.get(&peer).map(|v| *v).unwrap_or(0);

        if msg_id <= cum {
            // Already processed contiguously
            return Ok(true);
        }

        // Check gap window
        if let Some(gaps) = self.gap_window.get(&peer) {
            Ok(gaps.contains(&msg_id))
        } else {
            Ok(false)
        }
    }

    async fn mark_processed(&self, peer: Peer, msg_id: u64) -> Result<(), StorageError> {
        debug!("Dedup mark_processed peer={} msg_id={}", peer, msg_id);

        let mut cum = self.cum_processed.get(&peer).map(|v| *v).unwrap_or(0);

        if msg_id <= cum {
            // Already processed
            return Ok(());
        }

        if msg_id == cum + 1 {
            // Contiguous - advance watermark
            cum = msg_id;
            self.cum_processed.insert(peer, cum);

            // Check if we can advance further by processing gaps
            let mut gaps = self.gap_window.entry(peer).or_insert_with(HashSet::new);
            while gaps.remove(&(cum + 1)) {
                cum += 1;
            }

            if cum > msg_id {
                self.cum_processed.insert(peer, cum);
            }

            // Clean up old entries from gap window
            gaps.retain(|&id| id > cum && id <= cum + self.window_size);
        } else {
            // Out of order - add to gap window
            let mut gaps = self.gap_window.entry(peer).or_insert_with(HashSet::new);

            // Only track if within window
            if msg_id <= cum + self.window_size {
                gaps.insert(msg_id);
            }
        }

        Ok(())
    }

    async fn cum_processed(&self, peer: Peer) -> Result<u64, StorageError> {
        Ok(self.cum_processed.get(&peer).map(|v| *v).unwrap_or(0))
    }

    async fn advance_cum(&self, peer: Peer, id: u64) -> Result<(), StorageError> {
        debug!("Dedup advance_cum peer={} id={}", peer, id);

        let current = self.cum_processed.get(&peer).map(|v| *v).unwrap_or(0);
        if id > current {
            self.cum_processed.insert(peer, id);

            // Clean up gap window
            if let Some(mut gaps) = self.gap_window.get_mut(&peer) {
                gaps.retain(|&gap_id| gap_id > id);
            }
        }

        Ok(())
    }

    async fn snapshot(&self) -> Result<(), StorageError> {
        // No-op for in-memory backend
        debug!("Dedup snapshot (no-op for memory backend)");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_wal_basic() {
        let wal = MemoryWal::new();
        let peer = Peer(1001);

        // Test append
        let frame = WalFrame {
            msg_id: 1,
            bytes: b"test frame",
            approx_len: 10,
        };
        wal.append(peer, frame).await.unwrap();

        // Test last_appended
        assert_eq!(wal.last_appended(peer).await.unwrap(), 1);

        // Test range
        let results = wal.range(peer, 0, None).await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].msg_id, 1);
        assert_eq!(results[0].bytes, b"test frame");
    }

    #[tokio::test]
    async fn test_memory_dedup_basic() {
        let dedup = MemoryDedup::with_default_window();
        let peer = Peer(2002);

        // Test initial state
        assert_eq!(dedup.cum_processed(peer).await.unwrap(), 0);
        assert!(!dedup.is_processed(peer, 1).await.unwrap());

        // Test mark processed contiguous
        dedup.mark_processed(peer, 1).await.unwrap();
        assert_eq!(dedup.cum_processed(peer).await.unwrap(), 1);
        assert!(dedup.is_processed(peer, 1).await.unwrap());

        // Test out of order
        dedup.mark_processed(peer, 3).await.unwrap();
        assert_eq!(dedup.cum_processed(peer).await.unwrap(), 1);
        assert!(dedup.is_processed(peer, 3).await.unwrap());

        // Fill gap
        dedup.mark_processed(peer, 2).await.unwrap();
        assert_eq!(dedup.cum_processed(peer).await.unwrap(), 3);
    }
}
