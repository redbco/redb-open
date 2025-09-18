//! File-based storage backend with segments and recovery

use crate::{AckState, Dedup, Peer, StorageError, Wal, WalEntry, WalFrame};
use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc32fast::Hasher;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// Configuration for file-based WAL
#[derive(Clone, Debug)]
pub struct FileWalConfig {
    /// Base data directory
    pub data_dir: PathBuf,
    /// Segment size in bytes (default: 128 MiB)
    pub segment_bytes: u64,
    /// Fsync frequency (1 = every write, N = every N writes)
    pub fsync_every: u32,
}

impl Default for FileWalConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./meshdata"),
            segment_bytes: 128 * 1024 * 1024, // 128 MiB
            fsync_every: 1,
        }
    }
}

/// Per-peer state file content
#[derive(Serialize, Deserialize, Debug, Default)]
struct PeerState {
    last_appended: u64,
    cum_acked: u64,
    cum_processed: u64,
}

/// WAL segment header
#[derive(Debug)]
struct SegmentHeader {
    len: u32,    // Frame length (not including header)
    msg_id: u64, // Message ID
    crc32c: u32, // CRC32C over msg_id || frame_bytes
}

impl SegmentHeader {
    const SIZE: usize = 4 + 8 + 4; // u32 + u64 + u32

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.len);
        buf.put_u64_le(self.msg_id);
        buf.put_u32_le(self.crc32c);
    }

    fn decode(buf: &mut Bytes) -> Result<Self, StorageError> {
        if buf.remaining() < Self::SIZE {
            return Err(StorageError::Corruption(
                "Incomplete segment header".to_string(),
            ));
        }

        Ok(Self {
            len: buf.get_u32_le(),
            msg_id: buf.get_u64_le(),
            crc32c: buf.get_u32_le(),
        })
    }

    fn compute_crc(msg_id: u64, frame_bytes: &[u8]) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(&msg_id.to_le_bytes());
        hasher.update(frame_bytes);
        hasher.finalize()
    }
}

/// File-based WAL implementation
pub struct FileWal {
    config: FileWalConfig,
    /// Per-peer state cache
    peer_states: Arc<DashMap<Peer, PeerState>>,
    /// Per-peer active segment files
    active_segments: Arc<DashMap<Peer, File>>,
    /// Write counter for fsync batching
    write_counter: Arc<DashMap<Peer, u32>>,
}

impl FileWal {
    /// Create a new file-based WAL
    pub async fn new(config: FileWalConfig) -> Result<Self, StorageError> {
        // Ensure data directory exists
        std::fs::create_dir_all(&config.data_dir)?;

        let wal = Self {
            config,
            peer_states: Arc::new(DashMap::new()),
            active_segments: Arc::new(DashMap::new()),
            write_counter: Arc::new(DashMap::new()),
        };

        // Load existing peer states
        wal.load_peer_states().await?;

        Ok(wal)
    }

    async fn load_peer_states(&self) -> Result<(), StorageError> {
        let peers_dir = self.config.data_dir.join("peers");
        if !peers_dir.exists() {
            return Ok(());
        }

        for entry in std::fs::read_dir(&peers_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                if let Some(peer_name) = entry.file_name().to_str() {
                    if let Ok(peer_id) = peer_name.parse::<u64>() {
                        let peer = Peer(peer_id);
                        let state_file = entry.path().join("state.json");

                        if state_file.exists() {
                            match self.load_peer_state(peer, &state_file).await {
                                Ok(state) => {
                                    info!("Loaded state for peer {}: {:?}", peer, state);
                                    self.peer_states.insert(peer, state);
                                }
                                Err(e) => {
                                    warn!("Failed to load state for peer {}: {}", peer, e);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn load_peer_state(
        &self,
        peer: Peer,
        state_file: &Path,
    ) -> Result<PeerState, StorageError> {
        let content = std::fs::read_to_string(state_file)?;
        serde_json::from_str(&content).map_err(|e| {
            StorageError::Corruption(format!("Invalid state file for peer {}: {}", peer, e))
        })
    }

    async fn save_peer_state(&self, peer: Peer, state: &PeerState) -> Result<(), StorageError> {
        let peer_dir = self.config.data_dir.join("peers").join(peer.0.to_string());
        std::fs::create_dir_all(&peer_dir)?;

        let state_file = peer_dir.join("state.json");
        let content = serde_json::to_string_pretty(state)?;
        std::fs::write(state_file, content)?;

        Ok(())
    }

    fn get_peer_dir(&self, peer: Peer) -> PathBuf {
        self.config.data_dir.join("peers").join(peer.0.to_string())
    }

    fn get_wal_dir(&self, peer: Peer) -> PathBuf {
        self.get_peer_dir(peer).join("wal")
    }

    async fn get_or_create_active_segment(&self, peer: Peer) -> Result<(), StorageError> {
        if self.active_segments.contains_key(&peer) {
            return Ok(());
        }

        let wal_dir = self.get_wal_dir(peer);
        std::fs::create_dir_all(&wal_dir)?;

        // Find the latest segment or create first one
        let mut segment_files: Vec<_> = std::fs::read_dir(&wal_dir)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let file_name = entry.file_name();
                let name = file_name.to_str()?;
                if name.ends_with(".seg") {
                    let num_str = name.strip_suffix(".seg")?;
                    let num: u64 = num_str.parse().ok()?;
                    Some((num, entry.path()))
                } else {
                    None
                }
            })
            .collect();

        segment_files.sort_by_key(|(num, _)| *num);

        let segment_path = if let Some((latest_num, latest_path)) = segment_files.last() {
            // Check if latest segment is full
            let metadata = std::fs::metadata(latest_path)?;
            if metadata.len() >= self.config.segment_bytes {
                // Create new segment
                let new_num = latest_num + 1;
                wal_dir.join(format!("{:08}.seg", new_num))
            } else {
                latest_path.clone()
            }
        } else {
            // Create first segment
            wal_dir.join("00000001.seg")
        };

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&segment_path)?;

        self.active_segments.insert(peer, file);
        Ok(())
    }
}

#[async_trait]
impl Wal for FileWal {
    async fn append(&self, peer: Peer, frame: WalFrame<'_>) -> Result<(), StorageError> {
        debug!(
            "FileWAL append peer={} msg_id={} len={}",
            peer,
            frame.msg_id,
            frame.bytes.len()
        );

        self.get_or_create_active_segment(peer).await?;

        // Compute CRC
        let crc = SegmentHeader::compute_crc(frame.msg_id, frame.bytes);

        // Create header
        let header = SegmentHeader {
            len: frame.bytes.len() as u32,
            msg_id: frame.msg_id,
            crc32c: crc,
        };

        // Encode header + frame
        let mut buf = BytesMut::with_capacity(SegmentHeader::SIZE + frame.bytes.len());
        header.encode(&mut buf);
        buf.extend_from_slice(frame.bytes);

        // Write to active segment
        if let Some(mut file_ref) = self.active_segments.get_mut(&peer) {
            file_ref.write_all(&buf)?;

            // Update write counter and fsync if needed
            let mut counter = self.write_counter.entry(peer).or_insert(0);
            *counter += 1;

            if *counter >= self.config.fsync_every {
                file_ref.sync_all()?;
                *counter = 0;
            }
        }

        // Update peer state
        let mut state = self
            .peer_states
            .entry(peer)
            .or_insert_with(PeerState::default);
        state.last_appended = frame.msg_id;

        // Periodically save state (every 100 writes)
        if frame.msg_id % 100 == 0 {
            self.save_peer_state(peer, &state).await?;
        }

        Ok(())
    }

    async fn range(
        &self,
        peer: Peer,
        from_exclusive: u64,
        limit: Option<usize>,
    ) -> Result<Vec<WalEntry>, StorageError> {
        debug!(
            "FileWAL range peer={} from_exclusive={} limit={:?}",
            peer, from_exclusive, limit
        );

        let mut results = Vec::new();
        let wal_dir = self.get_wal_dir(peer);
        if !wal_dir.exists() {
            return Ok(results);
        }

        // Get all segment files in order
        let mut segment_files: Vec<_> = std::fs::read_dir(&wal_dir)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let file_name = entry.file_name();
                let name = file_name.to_str()?;
                if name.ends_with(".seg") {
                    let num_str = name.strip_suffix(".seg")?;
                    let num: u64 = num_str.parse().ok()?;
                    Some((num, entry.path()))
                } else {
                    None
                }
            })
            .collect();

        segment_files.sort_by_key(|(num, _)| *num);

        // Read through segments
        for (_, segment_path) in segment_files {
            let mut file = File::open(&segment_path)?;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf)?;

            let mut bytes = Bytes::from(buf);

            while bytes.remaining() >= SegmentHeader::SIZE {
                let header = SegmentHeader::decode(&mut bytes)?;

                if bytes.remaining() < header.len as usize {
                    warn!("Incomplete frame in segment {:?}", segment_path);
                    break;
                }

                let frame_bytes = bytes.split_to(header.len as usize);

                // Verify CRC
                let expected_crc = SegmentHeader::compute_crc(header.msg_id, &frame_bytes);
                if header.crc32c != expected_crc {
                    error!(
                        "CRC mismatch in segment {:?} msg_id={}",
                        segment_path, header.msg_id
                    );
                    return Err(StorageError::Corruption(format!(
                        "CRC mismatch for msg_id {}",
                        header.msg_id
                    )));
                }

                // Apply filter and collect results
                if header.msg_id > from_exclusive {
                    results.push(WalEntry {
                        msg_id: header.msg_id,
                        bytes: frame_bytes.to_vec(),
                    });

                    if let Some(limit) = limit {
                        if results.len() >= limit {
                            return Ok(results);
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    async fn truncate_through(&self, peer: Peer, up_to_inclusive: u64) -> Result<(), StorageError> {
        debug!(
            "FileWAL truncate peer={} up_to_inclusive={}",
            peer, up_to_inclusive
        );

        let wal_dir = self.get_wal_dir(peer);
        if !wal_dir.exists() {
            return Ok(());
        }

        // For simplicity in MVP, we'll just update the state
        // In production, we'd actually delete old segments
        let mut state = self
            .peer_states
            .entry(peer)
            .or_insert_with(PeerState::default);
        if up_to_inclusive > state.cum_acked {
            state.cum_acked = up_to_inclusive;
            self.save_peer_state(peer, &state).await?;
        }

        Ok(())
    }

    async fn last_appended(&self, peer: Peer) -> Result<u64, StorageError> {
        Ok(self
            .peer_states
            .get(&peer)
            .map(|s| s.last_appended)
            .unwrap_or(0))
    }

    async fn load_ack(&self, peer: Peer) -> Result<AckState, StorageError> {
        let cum_acked = self
            .peer_states
            .get(&peer)
            .map(|s| s.cum_acked)
            .unwrap_or(0);
        Ok(AckState { cum_acked })
    }

    async fn store_ack(&self, peer: Peer, ack: AckState) -> Result<(), StorageError> {
        debug!(
            "FileWAL store_ack peer={} cum_acked={}",
            peer, ack.cum_acked
        );

        let mut state = self
            .peer_states
            .entry(peer)
            .or_insert_with(PeerState::default);
        state.cum_acked = ack.cum_acked;
        self.save_peer_state(peer, &state).await?;

        Ok(())
    }
}

/// File-based deduplication implementation
pub struct FileDedup {
    config: FileWalConfig,
    /// Per-peer cumulative processed watermark
    cum_processed: Arc<DashMap<Peer, u64>>,
    /// Per-peer gap window for out-of-order messages
    gap_window: Arc<DashMap<Peer, HashSet<u64>>>,
    /// Window size for gap tracking
    window_size: u64,
}

impl FileDedup {
    /// Create a new file-based dedup
    pub async fn new(config: FileWalConfig, window_size: u64) -> Result<Self, StorageError> {
        let dedup = Self {
            config,
            cum_processed: Arc::new(DashMap::new()),
            gap_window: Arc::new(DashMap::new()),
            window_size,
        };

        // Load existing states
        dedup.load_states().await?;

        Ok(dedup)
    }

    async fn load_states(&self) -> Result<(), StorageError> {
        let peers_dir = self.config.data_dir.join("peers");
        if !peers_dir.exists() {
            return Ok(());
        }

        for entry in std::fs::read_dir(&peers_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                if let Some(peer_name) = entry.file_name().to_str() {
                    if let Ok(peer_id) = peer_name.parse::<u64>() {
                        let peer = Peer(peer_id);
                        let state_file = entry.path().join("state.json");

                        if state_file.exists() {
                            if let Ok(content) = std::fs::read_to_string(&state_file) {
                                if let Ok(state) = serde_json::from_str::<PeerState>(&content) {
                                    self.cum_processed.insert(peer, state.cum_processed);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn get_peer_dir(&self, peer: Peer) -> PathBuf {
        self.config.data_dir.join("peers").join(peer.0.to_string())
    }

    async fn save_peer_state(&self, peer: Peer, cum_processed: u64) -> Result<(), StorageError> {
        let peer_dir = self.get_peer_dir(peer);
        std::fs::create_dir_all(&peer_dir)?;

        let state_file = peer_dir.join("state.json");

        // Load existing state or create new
        let mut state = if state_file.exists() {
            let content = std::fs::read_to_string(&state_file)?;
            serde_json::from_str::<PeerState>(&content).unwrap_or_default()
        } else {
            PeerState::default()
        };

        state.cum_processed = cum_processed;

        let content = serde_json::to_string_pretty(&state)?;
        std::fs::write(state_file, content)?;

        Ok(())
    }
}

#[async_trait]
impl Dedup for FileDedup {
    async fn is_processed(&self, peer: Peer, msg_id: u64) -> Result<bool, StorageError> {
        let cum = self.cum_processed.get(&peer).map(|v| *v).unwrap_or(0);

        if msg_id <= cum {
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
        debug!("FileDedup mark_processed peer={} msg_id={}", peer, msg_id);

        let mut cum = self.cum_processed.get(&peer).map(|v| *v).unwrap_or(0);

        if msg_id <= cum {
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

            // Periodically save state
            if cum % 100 == 0 {
                self.save_peer_state(peer, cum).await?;
            }
        } else {
            // Out of order - add to gap window
            let mut gaps = self.gap_window.entry(peer).or_insert_with(HashSet::new);

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
        debug!("FileDedup advance_cum peer={} id={}", peer, id);

        let current = self.cum_processed.get(&peer).map(|v| *v).unwrap_or(0);
        if id > current {
            self.cum_processed.insert(peer, id);

            // Clean up gap window
            if let Some(mut gaps) = self.gap_window.get_mut(&peer) {
                gaps.retain(|&gap_id| gap_id > id);
            }

            self.save_peer_state(peer, id).await?;
        }

        Ok(())
    }

    async fn snapshot(&self) -> Result<(), StorageError> {
        debug!("FileDedup snapshot");

        // Save all current states
        for entry in self.cum_processed.iter() {
            let peer = *entry.key();
            let cum_processed = *entry.value();
            self.save_peer_state(peer, cum_processed).await?;
        }

        Ok(())
    }
}
