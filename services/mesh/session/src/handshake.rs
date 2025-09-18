//! Handshake protocol for mesh sessions.
//!
//! This module implements the HELLO handshake that occurs immediately
//! after connection establishment.

use bytes::{Bytes, BytesMut};
use mesh_wire::{FastHeader, FrameBuilder, FrameDecoder, FrameType, WIRE_VERSION};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, trace};

/// HELLO message data
#[derive(Debug, Clone)]
pub struct Hello {
    /// Node ID of the sender
    pub node_id: u64,
    /// Protocol version
    pub version: u8,
}

/// Send a HELLO frame to establish the session
pub async fn send_hello<W: AsyncWriteExt + Unpin>(
    mut writer: W,
    my_node_id: u64,
) -> Result<(), anyhow::Error> {
    let fast_header = FastHeader::new(
        FrameType::Hello,
        my_node_id, // src_node
        0,          // dst_node (unknown during handshake)
        0,          // msg_id
    );

    let frame_bytes = FrameBuilder::new(fast_header)
        .meta_insert_str("content-type", "application/x-hello")
        .meta_insert_u32("version", WIRE_VERSION as u32)
        .payload(Bytes::new())
        .build(16 * 1024 * 1024)?;

    writer.write_all(&frame_bytes).await?;
    debug!("Sent HELLO from node {}", my_node_id);
    Ok(())
}

/// Read any frame from the socket
pub async fn recv_any_frame<R: AsyncReadExt + Unpin>(
    mut reader: R,
    decoder: &mut FrameDecoder,
    buffer: &mut BytesMut,
) -> Result<mesh_wire::Frame, anyhow::Error> {
    // Simple read-more-then-parse loop
    // Production code should use proper framing with read_exact for length
    loop {
        let bytes_read = reader.read_buf(buffer).await?;
        if bytes_read == 0 {
            anyhow::bail!("EOF while reading frame");
        }

        trace!(
            "Read {} bytes, buffer now has {} bytes",
            bytes_read,
            buffer.len()
        );

        if let Some(frame) = decoder.decode(buffer)? {
            return Ok(frame);
        }
        // Need more data, continue reading
    }
}

/// Parse HELLO metadata to extract node information
pub fn parse_hello_meta(meta_raw: &[u8]) -> Result<Hello, anyhow::Error> {
    let meta = mesh_wire::parse_meta(meta_raw)?;

    let version = mesh_wire::get_meta_u32(&meta, "version").unwrap_or(1) as u8;

    // For now, we'll extract node_id from the fast header, not metadata
    // This is a placeholder for future extensions
    Ok(Hello {
        node_id: 0, // Will be filled from fast header
        version,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn test_hello_roundtrip() {
        let node_id = 0x1234567890ABCDEF;

        // Send HELLO to a buffer
        let mut buffer = Vec::new();
        send_hello(&mut buffer, node_id).await.unwrap();

        // Read it back
        let mut decoder = FrameDecoder::new();
        let mut read_buf = BytesMut::new();
        let cursor = Cursor::new(buffer);

        let frame = recv_any_frame(cursor, &mut decoder, &mut read_buf)
            .await
            .unwrap();

        assert_eq!(frame.fast.typ, FrameType::Hello);
        assert_eq!(frame.fast.src_node, node_id);

        let hello = parse_hello_meta(&frame.meta_raw).unwrap();
        assert_eq!(hello.version, WIRE_VERSION);
    }
}
