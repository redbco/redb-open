//! Basic usage example for the mesh wire protocol.

use bytes::Bytes;
use mesh_wire::{
    Chunker, FastHeader, FrameBuilder, FrameDecoder, FrameType, MetaBuilder, Reassembler,
    DEFAULT_MAX_FRAME_SIZE,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Mesh Wire Protocol Example ===\n");

    // 1. Create a basic data frame
    println!("1. Creating a basic DATA frame...");
    let fast_header = FastHeader::new(
        FrameType::Data,
        0x1234567890ABCDEF, // src_node
        0xFEDCBA0987654321, // dst_node
        42,                 // msg_id
    );

    let frame_bytes = FrameBuilder::new(fast_header)
        .meta_insert_str("content-type", "application/octet-stream")
        .meta_insert_u32("version", 1)
        .payload(Bytes::from_static(b"Hello, mesh network!"))
        .build(DEFAULT_MAX_FRAME_SIZE)?;

    println!("   Encoded frame size: {} bytes", frame_bytes.len());

    // 2. Decode the frame
    println!("\n2. Decoding the frame...");
    let mut decoder = FrameDecoder::new();
    let mut buf = bytes::BytesMut::from(frame_bytes.as_ref());

    if let Some(decoded_frame) = decoder.decode(&mut buf)? {
        println!("   Decoded successfully!");
        println!("   Source node: 0x{:016X}", decoded_frame.fast.src_node);
        println!(
            "   Destination node: 0x{:016X}",
            decoded_frame.fast.dst_node
        );
        println!("   Message ID: {}", decoded_frame.fast.msg_id);
        println!(
            "   Payload: {:?}",
            std::str::from_utf8(&decoded_frame.payload_or_cipher)
        );
    }

    // 3. Demonstrate chunking
    println!("\n3. Demonstrating message chunking...");
    let large_payload = Bytes::from(vec![0x42u8; 5000]); // 5KB payload
    let chunker = Chunker::new();

    let chunk_header =
        FastHeader::new(FrameType::Data, 0x1111111111111111, 0x2222222222222222, 100);

    let chunks = chunker.chunk_message(chunk_header, large_payload.clone());
    println!("   Split into {} chunks", chunks.len());

    // 4. Reassemble chunks
    println!("\n4. Reassembling chunks...");
    let mut reassembler = Reassembler::new();
    let mut reassembled = None;

    for chunk in chunks {
        if let Some(complete_message) = reassembler.add_chunk(chunk) {
            reassembled = Some(complete_message);
            break;
        }
    }

    if let Some(message) = reassembled {
        println!("   Reassembled message size: {} bytes", message.len());
        println!(
            "   Original matches reassembled: {}",
            message == large_payload
        );
    }

    // 5. Demonstrate metadata parsing
    println!("\n5. Working with CBOR metadata...");
    let meta = MetaBuilder::new()
        .insert_str("service", "mesh-example")
        .insert_str("version", "1.0.0")
        .insert_u32("priority", 5)
        .insert_bytes("trace-id", &[1, 2, 3, 4, 5, 6, 7, 8])
        .build()?;

    let parsed_meta = mesh_wire::parse_meta(&meta)?;
    if let Some(service) = mesh_wire::get_meta_str(&parsed_meta, "service") {
        println!("   Service: {}", service);
    }
    if let Some(priority) = mesh_wire::get_meta_u32(&parsed_meta, "priority") {
        println!("   Priority: {}", priority);
    }

    println!("\n=== Example completed successfully! ===");
    Ok(())
}
