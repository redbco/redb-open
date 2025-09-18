//! Keepalive protocol with PING/PONG and RTT measurement.
//!
//! This module provides functions to build PING/PONG frames and calculate
//! round-trip times using correlation IDs.

use bytes::Bytes;
use mesh_wire::{FastHeader, FrameBuilder, FrameType};
use std::time::{Duration, Instant};

/// Build a PING frame with correlation ID for RTT measurement
pub fn build_ping(my_node: u64, corr_id: u64) -> Bytes {
    let mut fast_header = FastHeader::new(
        FrameType::Ping,
        my_node, // src_node
        0,       // dst_node (broadcast/unknown)
        0,       // msg_id
    );
    fast_header.corr_id = corr_id;

    FrameBuilder::new(fast_header)
        .meta_insert_str("content-type", "application/x-ping")
        .payload(Bytes::new())
        .build(1024 * 1024)
        .expect("PING frame build should never fail")
}

/// Build a PONG frame in response to a PING
pub fn build_pong(my_node: u64, corr_id: u64) -> Bytes {
    let mut fast_header = FastHeader::new(
        FrameType::Pong,
        my_node, // src_node
        0,       // dst_node (broadcast/unknown)
        0,       // msg_id
    );
    fast_header.corr_id = corr_id;

    FrameBuilder::new(fast_header)
        .meta_insert_str("content-type", "application/x-pong")
        .payload(Bytes::new())
        .build(1024 * 1024)
        .expect("PONG frame build should never fail")
}

/// Generate a correlation ID based on monotonic time
pub fn now_corr_id() -> u64 {
    // Use monotonic nanoseconds packed into u64 (truncated if needed)
    static START: once_cell::sync::Lazy<Instant> = once_cell::sync::Lazy::new(Instant::now);
    let elapsed_ns = START.elapsed().as_nanos();
    (elapsed_ns & 0xFFFF_FFFF_FFFF_FFFF) as u64
}

/// Calculate RTT from a correlation ID in a PONG response
pub fn calc_rtt_from_corr(peer_corr_id: u64) -> Option<Duration> {
    static START: once_cell::sync::Lazy<Instant> = once_cell::sync::Lazy::new(Instant::now);
    let now_ns = START.elapsed().as_nanos() as u64;

    if now_ns >= peer_corr_id {
        let diff_ns = now_ns - peer_corr_id;
        Some(Duration::from_nanos(diff_ns))
    } else {
        // Clock skew or wraparound - invalid RTT
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use mesh_wire::FrameDecoder;

    #[test]
    fn test_ping_pong_frames() {
        let node_id = 0x1234567890ABCDEF;
        let corr_id = 0x9876543210FEDCBA;

        // Build PING
        let ping_bytes = build_ping(node_id, corr_id);
        assert!(!ping_bytes.is_empty());

        // Build PONG
        let pong_bytes = build_pong(node_id, corr_id);
        assert!(!pong_bytes.is_empty());

        // Decode and verify
        let mut decoder = FrameDecoder::new();
        let mut buf = BytesMut::from(ping_bytes.as_ref());

        let ping_frame = decoder.decode(&mut buf).unwrap().unwrap();
        assert_eq!(ping_frame.fast.typ, FrameType::Ping);
        assert_eq!(ping_frame.fast.src_node, node_id);
        assert_eq!(ping_frame.fast.corr_id, corr_id);
    }

    #[test]
    fn test_corr_id_generation() {
        let id1 = now_corr_id();
        std::thread::sleep(Duration::from_millis(1));
        let id2 = now_corr_id();

        assert!(
            id2 > id1,
            "Correlation IDs should be monotonically increasing"
        );
    }

    #[test]
    fn test_rtt_calculation() {
        let start_corr = now_corr_id();
        std::thread::sleep(Duration::from_millis(10));

        if let Some(rtt) = calc_rtt_from_corr(start_corr) {
            assert!(rtt >= Duration::from_millis(10));
            assert!(rtt < Duration::from_millis(100)); // Should be reasonable
        }
    }
}
