//! Reliability layer with WAL, dedup, ACK/CREDIT flow control

use anyhow::Result;
use bytes::Bytes;
use mesh_storage::{AckState, Peer, Storage, StorageError, WalFrame};
use mesh_wire::{FastHeader, FrameBuilder, FrameType};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Send state for reliability
#[derive(Debug, Clone)]
pub struct SendState {
    /// Next message ID to assign
    pub next_msg_id: u64,
    /// Last cumulatively ACKed message ID from peer
    pub cum_acked: u64,
    /// Available credit bytes from receiver
    pub credits_bytes: i64,
    /// Pending frames waiting for credits
    pub pending_frames: Vec<(u64, Bytes)>,
}

impl Default for SendState {
    fn default() -> Self {
        Self {
            next_msg_id: 1, // Start from 1, 0 is reserved
            cum_acked: 0,
            credits_bytes: 0,
            pending_frames: Vec::new(),
        }
    }
}

/// Receive state for reliability
#[derive(Debug, Clone)]
pub struct RecvState {
    /// Cumulative processed watermark
    pub cum_processed: u64,
    /// Maximum receive window in bytes
    pub credits_max: u32,
    /// Available credit bytes before backpressure
    pub credits_avail: i64,
    /// Whether ACK is pending to be sent
    pub ack_pending: bool,
    /// Last time ACK was sent
    pub last_ack_sent: Instant,
    /// Number of messages since last ACK
    pub msgs_since_ack: u32,
}

impl RecvState {
    /// Create a new receive state
    pub fn new(credits_max: u32) -> Self {
        Self {
            cum_processed: 0,
            credits_max,
            credits_avail: credits_max as i64,
            ack_pending: false,
            last_ack_sent: Instant::now(),
            msgs_since_ack: 0,
        }
    }
}

/// ACK metadata structure
#[derive(Debug, Clone)]
pub struct AckMeta {
    /// Last contiguous ACK received from peer (sender-side view)
    pub cum_ack: u64,
    /// Available credits from receiver
    pub credits: u32,
}

/// RESUME metadata structure
#[derive(Debug, Clone)]
pub struct ResumeMeta {
    /// Whether to resume the session
    pub resume: bool,
    /// Last contiguous ACK received from peer (sender-side view)
    pub sender_cum_ack: u64,
    /// Cumulative processed watermark from peer (receiver-side view)
    pub receiver_cum_proc: u64,
    /// Starting credits from peer
    pub starting_credits: Option<u32>,
}

/// Reliability manager for a session
pub struct ReliabilityManager {
    /// Storage backend
    storage: Arc<Storage>,
    /// Per-peer send state
    send_states: Arc<RwLock<HashMap<Peer, SendState>>>,
    /// Per-peer receive state
    recv_states: Arc<RwLock<HashMap<Peer, RecvState>>>,
    /// ACK flush configuration
    ack_interval: Duration,
    ack_batch_size: u32,
    /// Default receive window
    default_recv_window: u32,
}

impl ReliabilityManager {
    /// Create a new reliability manager
    pub fn new(
        storage: Arc<Storage>,
        ack_interval: Duration,
        ack_batch_size: u32,
        default_recv_window: u32,
    ) -> Self {
        Self {
            storage,
            send_states: Arc::new(RwLock::new(HashMap::new())),
            recv_states: Arc::new(RwLock::new(HashMap::new())),
            ack_interval,
            ack_batch_size,
            default_recv_window,
        }
    }

    /// Initialize send state for a peer
    pub async fn init_send_state(&self, peer: Peer) -> Result<(), StorageError> {
        let mut states = self.send_states.write().await;
        if !states.contains_key(&peer) {
            // Load from storage
            let last_appended = self.storage.wal.last_appended(peer).await?;
            let ack_state = self.storage.wal.load_ack(peer).await?;

            let send_state = SendState {
                next_msg_id: last_appended + 1,
                cum_acked: ack_state.cum_acked,
                credits_bytes: 0, // Will be set by initial HELLO/RESUME
                pending_frames: Vec::new(),
            };

            states.insert(peer, send_state);
            info!(
                "Initialized send state for peer {}: next_msg_id={}, cum_acked={}",
                peer,
                last_appended + 1,
                ack_state.cum_acked
            );
        }
        Ok(())
    }

    /// Initialize receive state for a peer
    pub async fn init_recv_state(&self, peer: Peer) -> Result<(), StorageError> {
        let mut states = self.recv_states.write().await;
        if !states.contains_key(&peer) {
            // Load from storage
            let cum_processed = self.storage.dedup.cum_processed(peer).await?;

            let recv_state = RecvState {
                cum_processed,
                credits_max: self.default_recv_window,
                credits_avail: self.default_recv_window as i64,
                ack_pending: false,
                last_ack_sent: Instant::now(),
                msgs_since_ack: 0,
            };

            states.insert(peer, recv_state);
            info!(
                "Initialized recv state for peer {}: cum_processed={}, credits_max={}",
                peer, cum_processed, self.default_recv_window
            );
        }
        Ok(())
    }

    /// Send a DATA frame with reliability
    pub async fn send_data<W: AsyncWriteExt + Unpin>(
        &self,
        peer: Peer,
        my_node_id: u64,
        payload: Bytes,
        writer: &mut W,
    ) -> Result<(), anyhow::Error> {
        self.init_send_state(peer).await?;

        let mut states = self.send_states.write().await;
        let send_state = states.get_mut(&peer).unwrap();

        let msg_id = send_state.next_msg_id;
        send_state.next_msg_id += 1;

        // Build DATA frame
        let fast_header = FastHeader::new(FrameType::Data, my_node_id, peer.0, msg_id);
        let frame_builder = FrameBuilder::new(fast_header)
            .meta_insert_str("content-type", "application/x-data")
            .payload(payload);

        let frame_bytes = frame_builder.build(16 * 1024 * 1024)?; // 16MB max frame

        // Store in WAL
        let wal_frame = WalFrame {
            msg_id,
            bytes: &frame_bytes,
            approx_len: frame_bytes.len(),
        };
        self.storage.wal.append(peer, wal_frame).await?;

        debug!(
            "Stored DATA frame in WAL: peer={} msg_id={} len={}",
            peer,
            msg_id,
            frame_bytes.len()
        );

        // Check credits and send or queue
        if send_state.credits_bytes >= frame_bytes.len() as i64 {
            // Send immediately
            writer.write_all(&frame_bytes).await?;
            send_state.credits_bytes -= frame_bytes.len() as i64;
            info!(
                "Sent DATA frame: peer={} msg_id={} len={} credits_remaining={}",
                peer,
                msg_id,
                frame_bytes.len(),
                send_state.credits_bytes
            );
        } else {
            // Queue for later
            let frame_len = frame_bytes.len();
            send_state.pending_frames.push((msg_id, frame_bytes));
            warn!("Queued DATA frame (insufficient credits): peer={} msg_id={} credits_needed={} credits_avail={}", 
                  peer, msg_id, frame_len, send_state.credits_bytes);
        }

        Ok(())
    }

    /// Process received DATA frame
    pub async fn process_data_frame(
        &self,
        peer: Peer,
        msg_id: u64,
        payload: &[u8],
    ) -> Result<bool, anyhow::Error> {
        self.init_recv_state(peer).await?;

        // Check if already processed
        if self.storage.dedup.is_processed(peer, msg_id).await? {
            debug!(
                "DATA frame already processed: peer={} msg_id={}",
                peer, msg_id
            );

            // Still need to ACK (idempotent re-ACK)
            let mut states = self.recv_states.write().await;
            if let Some(recv_state) = states.get_mut(&peer) {
                recv_state.ack_pending = true;
            }
            return Ok(false); // Not newly processed
        }

        // Mark as processed
        self.storage.dedup.mark_processed(peer, msg_id).await?;

        // Update receive state
        let mut states = self.recv_states.write().await;
        let recv_state = states.get_mut(&peer).unwrap();

        // Update cumulative processed from storage (dedup may have advanced it)
        recv_state.cum_processed = self.storage.dedup.cum_processed(peer).await?;

        // Consume credits
        recv_state.credits_avail -= payload.len() as i64;
        recv_state.ack_pending = true;
        recv_state.msgs_since_ack += 1;

        info!(
            "Processed DATA frame: peer={} msg_id={} len={} cum_processed={} credits_avail={}",
            peer,
            msg_id,
            payload.len(),
            recv_state.cum_processed,
            recv_state.credits_avail
        );

        Ok(true) // Newly processed
    }

    /// Process received ACK frame
    pub async fn process_ack_frame<W: AsyncWriteExt + Unpin>(
        &self,
        peer: Peer,
        ack_meta: AckMeta,
        writer: &mut W,
    ) -> Result<(), anyhow::Error> {
        self.init_send_state(peer).await?;

        let mut states = self.send_states.write().await;
        let send_state = states.get_mut(&peer).unwrap();

        // Update ACK state
        if ack_meta.cum_ack > send_state.cum_acked {
            send_state.cum_acked = ack_meta.cum_ack;

            // Store ACK state and truncate WAL
            let ack_state = AckState {
                cum_acked: ack_meta.cum_ack,
            };
            self.storage.wal.store_ack(peer, ack_state).await?;
            self.storage
                .wal
                .truncate_through(peer, ack_meta.cum_ack)
                .await?;

            info!(
                "Updated ACK state: peer={} cum_acked={}",
                peer, ack_meta.cum_ack
            );
        }

        // Update credits
        send_state.credits_bytes = ack_meta.credits as i64;

        // Try to send pending frames
        let mut sent_frames = Vec::new();
        for (i, (msg_id, frame_bytes)) in send_state.pending_frames.iter().enumerate() {
            if send_state.credits_bytes >= frame_bytes.len() as i64 {
                writer.write_all(frame_bytes).await?;
                send_state.credits_bytes -= frame_bytes.len() as i64;
                sent_frames.push(i);
                info!(
                    "Sent queued DATA frame: peer={} msg_id={} len={} credits_remaining={}",
                    peer,
                    msg_id,
                    frame_bytes.len(),
                    send_state.credits_bytes
                );
            } else {
                break; // Not enough credits for this frame
            }
        }

        // Remove sent frames from pending queue
        for &i in sent_frames.iter().rev() {
            send_state.pending_frames.remove(i);
        }

        Ok(())
    }

    /// Check if ACK should be sent and build ACK frame
    pub async fn maybe_build_ack(
        &self,
        peer: Peer,
        my_node_id: u64,
    ) -> Result<Option<Bytes>, anyhow::Error> {
        self.init_recv_state(peer).await?;

        let mut states = self.recv_states.write().await;
        let recv_state = states.get_mut(&peer).unwrap();

        let should_ack = recv_state.ack_pending
            && (
                recv_state.last_ack_sent.elapsed() >= self.ack_interval
                    || recv_state.msgs_since_ack >= self.ack_batch_size
                    || recv_state.credits_avail <= (recv_state.credits_max as i64) / 4
                // Low credits
            );

        if should_ack {
            // Refresh credits if low
            if recv_state.credits_avail <= (recv_state.credits_max as i64) / 2 {
                recv_state.credits_avail = recv_state.credits_max as i64;
            }

            let fast_header = FastHeader::new(FrameType::Ack, my_node_id, peer.0, 0);

            // Encode ACK metadata as CBOR
            let mut ack_meta = std::collections::BTreeMap::new();
            ack_meta.insert(
                serde_cbor::Value::Text("cum_ack".to_string()),
                serde_cbor::Value::Integer(recv_state.cum_processed as i128),
            );
            ack_meta.insert(
                serde_cbor::Value::Text("credits".to_string()),
                serde_cbor::Value::Integer(recv_state.credits_avail as i128),
            );
            let ack_meta_bytes = serde_cbor::to_vec(&serde_cbor::Value::Map(ack_meta))?;

            let frame_builder = FrameBuilder::new(fast_header)
                .meta_insert_str("content-type", "application/x-ack")
                .meta_insert_bytes("ack", &ack_meta_bytes)
                .payload(Bytes::new());

            let ack_frame = frame_builder.build(1024 * 1024)?; // 1MB max for ACK

            // Reset ACK state
            recv_state.ack_pending = false;
            recv_state.last_ack_sent = Instant::now();
            recv_state.msgs_since_ack = 0;

            info!(
                "Built ACK frame: peer={} cum_ack={} credits={}",
                peer, recv_state.cum_processed, recv_state.credits_avail
            );

            Ok(Some(ack_frame))
        } else {
            Ok(None)
        }
    }

    /// Parse ACK metadata from frame
    pub fn parse_ack_meta(meta_raw: &[u8]) -> Result<AckMeta, anyhow::Error> {
        let meta: serde_cbor::Value = serde_cbor::from_slice(meta_raw)?;

        let cum_ack = if let serde_cbor::Value::Map(map) = &meta {
            map.iter()
                .find(|(k, _)| matches!(k, serde_cbor::Value::Text(s) if s == "cum_ack"))
                .and_then(|(_, v)| match v {
                    serde_cbor::Value::Integer(i) => Some(*i as u64),
                    _ => None,
                })
                .unwrap_or(0)
        } else {
            0
        };

        let credits = if let serde_cbor::Value::Map(map) = &meta {
            map.iter()
                .find(|(k, _)| matches!(k, serde_cbor::Value::Text(s) if s == "credits"))
                .and_then(|(_, v)| match v {
                    serde_cbor::Value::Integer(i) => Some(*i as u32),
                    _ => None,
                })
                .unwrap_or(0)
        } else {
            0
        };

        Ok(AckMeta { cum_ack, credits })
    }

    /// Build RESUME frame for reconnection
    pub async fn build_resume(&self, peer: Peer, my_node_id: u64) -> Result<Bytes, anyhow::Error> {
        self.init_send_state(peer).await?;
        self.init_recv_state(peer).await?;

        let send_states = self.send_states.read().await;
        let recv_states = self.recv_states.read().await;

        let send_state = send_states.get(&peer).unwrap();
        let recv_state = recv_states.get(&peer).unwrap();

        let fast_header = FastHeader::new(FrameType::Resume, my_node_id, peer.0, 0);

        // Encode RESUME metadata as CBOR
        let mut resume_meta = std::collections::BTreeMap::new();
        resume_meta.insert(
            serde_cbor::Value::Text("resume".to_string()),
            serde_cbor::Value::Bool(true),
        );
        resume_meta.insert(
            serde_cbor::Value::Text("sender_cum_ack".to_string()),
            serde_cbor::Value::Integer(send_state.cum_acked as i128),
        );
        resume_meta.insert(
            serde_cbor::Value::Text("receiver_cum_proc".to_string()),
            serde_cbor::Value::Integer(recv_state.cum_processed as i128),
        );
        resume_meta.insert(
            serde_cbor::Value::Text("starting_credits".to_string()),
            serde_cbor::Value::Integer(recv_state.credits_max as i128),
        );
        let resume_meta_bytes = serde_cbor::to_vec(&serde_cbor::Value::Map(resume_meta))?;

        let frame_builder = FrameBuilder::new(fast_header)
            .meta_insert_str("content-type", "application/x-resume")
            .meta_insert_bytes("resume", &resume_meta_bytes)
            .payload(Bytes::new());

        let resume_frame = frame_builder.build(1024 * 1024)?; // 1MB max for RESUME

        info!("Built RESUME frame: peer={} sender_cum_ack={} receiver_cum_proc={} starting_credits={}", 
              peer, send_state.cum_acked, recv_state.cum_processed, recv_state.credits_max);

        Ok(resume_frame)
    }

    /// Parse RESUME metadata from frame
    pub fn parse_resume_meta(meta_raw: &[u8]) -> Result<ResumeMeta, anyhow::Error> {
        let meta: serde_cbor::Value = serde_cbor::from_slice(meta_raw)?;

        let resume = if let serde_cbor::Value::Map(map) = &meta {
            map.iter()
                .find(|(k, _)| matches!(k, serde_cbor::Value::Text(s) if s == "resume"))
                .and_then(|(_, v)| match v {
                    serde_cbor::Value::Bool(b) => Some(*b),
                    _ => None,
                })
                .unwrap_or(false)
        } else {
            false
        };

        let sender_cum_ack = if let serde_cbor::Value::Map(map) = &meta {
            map.iter()
                .find(|(k, _)| matches!(k, serde_cbor::Value::Text(s) if s == "sender_cum_ack"))
                .and_then(|(_, v)| match v {
                    serde_cbor::Value::Integer(i) => Some(*i as u64),
                    _ => None,
                })
                .unwrap_or(0)
        } else {
            0
        };

        let receiver_cum_proc = if let serde_cbor::Value::Map(map) = &meta {
            map.iter()
                .find(|(k, _)| matches!(k, serde_cbor::Value::Text(s) if s == "receiver_cum_proc"))
                .and_then(|(_, v)| match v {
                    serde_cbor::Value::Integer(i) => Some(*i as u64),
                    _ => None,
                })
                .unwrap_or(0)
        } else {
            0
        };

        let starting_credits = if let serde_cbor::Value::Map(map) = &meta {
            map.iter()
                .find(|(k, _)| matches!(k, serde_cbor::Value::Text(s) if s == "starting_credits"))
                .and_then(|(_, v)| match v {
                    serde_cbor::Value::Integer(i) => Some(*i as u32),
                    _ => None,
                })
        } else {
            None
        };

        Ok(ResumeMeta {
            resume,
            sender_cum_ack,
            receiver_cum_proc,
            starting_credits,
        })
    }

    /// Handle RESUME frame and retransmit if needed
    pub async fn handle_resume<W: AsyncWriteExt + Unpin>(
        &self,
        peer: Peer,
        resume_meta: ResumeMeta,
        writer: &mut W,
    ) -> Result<(), anyhow::Error> {
        self.init_send_state(peer).await?;

        let mut states = self.send_states.write().await;
        let send_state = states.get_mut(&peer).unwrap();

        // Set initial credits from RESUME
        if let Some(credits) = resume_meta.starting_credits {
            send_state.credits_bytes = credits as i64;
        }

        // Retransmit frames from peer's view
        let peer_view_cum_ack = resume_meta.receiver_cum_proc;

        info!(
            "Handling RESUME: peer={} peer_view_cum_ack={} our_cum_acked={} credits={}",
            peer, peer_view_cum_ack, send_state.cum_acked, send_state.credits_bytes
        );

        if peer_view_cum_ack < send_state.cum_acked {
            warn!("Peer's view is behind ours, this shouldn't happen in normal operation");
        }

        // Retransmit frames > peer_view_cum_ack
        let mut retransmit_count = 0;
        let entries = self
            .storage
            .wal
            .range(peer, peer_view_cum_ack, None)
            .await?;

        for entry in entries {
            if send_state.credits_bytes >= entry.bytes.len() as i64 {
                // Send frame
                writer.write_all(&entry.bytes).await?;
                send_state.credits_bytes -= entry.bytes.len() as i64;
                retransmit_count += 1;

                info!(
                    "Retransmitted frame: peer={} msg_id={} len={} credits_remaining={}",
                    peer,
                    entry.msg_id,
                    entry.bytes.len(),
                    send_state.credits_bytes
                );
            } else {
                info!(
                    "Stopping retransmission due to insufficient credits: peer={} msg_id={}",
                    peer, entry.msg_id
                );
                break;
            }
        }

        info!(
            "RESUME complete: peer={} retransmitted={} frames",
            peer, retransmit_count
        );

        Ok(())
    }
}
