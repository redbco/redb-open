//! Fast header processing for the wire protocol.
//!
//! This module defines the 48-byte fast header structure that enables efficient
//! routing without parsing the full frame payload or metadata.

use bitflags::bitflags;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

/// Wire protocol version
pub const WIRE_VERSION: u8 = 1;

/// Fast header size in bytes
pub const FAST_HEADER_SIZE: usize = 48;

/// Frame types as defined in the wire protocol
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FrameType {
    /// Data frame
    Data = 0x00,
    /// Acknowledgment frame
    Ack = 0x01,
    /// Credit frame for flow control
    Credit = 0x02,
    /// Ping frame
    Ping = 0x03,
    /// Pong frame
    Pong = 0x04,
    /// Hello frame for session establishment
    Hello = 0x05,
    /// Resume frame for session resumption
    Resume = 0x06,
    /// Drain frame for graceful shutdown
    Drain = 0x07,
    /// Bye frame for session termination
    Bye = 0x08,
    /// Topology update frame for link-state advertisement
    TopologyUpdate = 0x09,
    /// Topology request frame for requesting topology information
    TopologyRequest = 0x0A,
}

impl TryFrom<u8> for FrameType {
    type Error = crate::WireError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x00 => Ok(FrameType::Data),
            0x01 => Ok(FrameType::Ack),
            0x02 => Ok(FrameType::Credit),
            0x03 => Ok(FrameType::Ping),
            0x04 => Ok(FrameType::Pong),
            0x05 => Ok(FrameType::Hello),
            0x06 => Ok(FrameType::Resume),
            0x07 => Ok(FrameType::Drain),
            0x08 => Ok(FrameType::Bye),
            0x09 => Ok(FrameType::TopologyUpdate),
            0x0A => Ok(FrameType::TopologyRequest),
            _ => Err(crate::WireError::Type(value)),
        }
    }
}

bitflags! {
    /// Frame flags bitmask
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub struct Flags: u16 {
        /// Payload is a chunk of a larger message
        const CHUNKED = 1 << 0;
        /// Last chunk for this msg_id
        const CHUNK_END = 1 << 1;
        /// Payload is E2E encrypted (Crypto Section present)
        const E2E_ENC = 1 << 2;
        /// Payload is compressed (meta must declare codec)
        const COMPRESSED = 1 << 3;
        /// Do not ECMP rehash on forward (sticky routing)
        const ROUTE_LOCK = 1 << 4;
        /// Meta has "hdr_csum" u32; wire readers may validate
        const HDR_CHECKSUM = 1 << 5;
    }
}

/// Status codes for control frames and NACK semantics
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StatusCode {
    /// Success
    Ok = 0,
    /// Retryable error
    Retryable = 1,
    /// Fatal error
    Fatal = 2,
    /// Service busy
    Busy = 3,
    /// Unsupported operation
    Unsupported = 4,
}

impl TryFrom<u8> for StatusCode {
    type Error = crate::WireError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(StatusCode::Ok),
            1 => Ok(StatusCode::Retryable),
            2 => Ok(StatusCode::Fatal),
            3 => Ok(StatusCode::Busy),
            4 => Ok(StatusCode::Unsupported),
            _ => Err(crate::WireError::Code(value)),
        }
    }
}

/// Route bitfield components
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Route {
    /// Priority (0..7)
    pub prio: u8,
    /// Class of service (0..31)
    pub class: u8,
    /// Data/tenant partition hint (0..1023)
    pub partition: u16,
    /// Topology/policy epoch (0..16383)
    pub epoch: u16,
}

impl Route {
    /// Create a new route with the given components
    pub fn new(prio: u8, class: u8, partition: u16, epoch: u16) -> Self {
        Self {
            prio: prio & 0x07,
            class: class & 0x1F,
            partition: partition & 0x3FF,
            epoch: epoch & 0x3FFF,
        }
    }

    /// Pack route into a 32-bit value
    pub fn pack(self) -> u32 {
        ((self.prio as u32) << 29)
            | ((self.class as u32) << 24)
            | ((self.partition as u32) << 14)
            | (self.epoch as u32)
    }

    /// Unpack route from a 32-bit value
    pub fn unpack(value: u32) -> Self {
        Self {
            prio: ((value >> 29) & 0x07) as u8,
            class: ((value >> 24) & 0x1F) as u8,
            partition: ((value >> 14) & 0x3FF) as u16,
            epoch: (value & 0x3FFF) as u16,
        }
    }
}

/// Fast header structure (48 bytes)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FastHeader {
    /// Protocol version (must be 1)
    pub ver: u8,
    /// Frame type
    pub typ: FrameType,
    /// Frame flags
    pub flags: Flags,
    /// Status code for control frames
    pub code: StatusCode,
    /// TTL decremented at each forward
    pub ttl: u8,
    /// Reserved field (must be zero)
    pub reserved0: u16,
    /// Monotonically increasing per src_node
    pub msg_id: u64,
    /// Optional correlation ID (0 if none)
    pub corr_id: u64,
    /// Source node ID
    pub src_node: u64,
    /// Destination node ID
    pub dst_node: u64,
    /// Route bitfield
    pub route: u32,
    /// Optional hint length (bytes) or 0
    pub hdr_hint_len: u32,
}

impl FastHeader {
    /// Create a new fast header with default values
    pub fn new(typ: FrameType, src_node: u64, dst_node: u64, msg_id: u64) -> Self {
        Self {
            ver: WIRE_VERSION,
            typ,
            flags: Flags::empty(),
            code: StatusCode::Ok,
            ttl: 16, // Default TTL
            reserved0: 0,
            msg_id,
            corr_id: 0,
            src_node,
            dst_node,
            route: 0,
            hdr_hint_len: 0,
        }
    }

    /// Get the route components
    pub fn route_components(&self) -> Route {
        Route::unpack(self.route)
    }

    /// Set the route components
    pub fn set_route(&mut self, route: Route) {
        self.route = route.pack();
    }

    /// Encode the fast header to bytes (big-endian)
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.ver);
        buf.put_u8(self.typ as u8);
        buf.put_u16(self.flags.bits());
        buf.put_u8(self.code as u8);
        buf.put_u8(self.ttl);
        buf.put_u16(self.reserved0);
        buf.put_u64(self.msg_id);
        buf.put_u64(self.corr_id);
        buf.put_u64(self.src_node);
        buf.put_u64(self.dst_node);
        buf.put_u32(self.route);
        buf.put_u32(self.hdr_hint_len);
    }

    /// Decode the fast header from bytes (big-endian)
    pub fn decode(buf: &mut Bytes) -> Result<Self, crate::WireError> {
        if buf.len() < FAST_HEADER_SIZE {
            return Err(crate::WireError::Incomplete);
        }

        let ver = buf.get_u8();
        if ver != WIRE_VERSION {
            return Err(crate::WireError::Version(ver));
        }

        let typ = FrameType::try_from(buf.get_u8())?;
        let flags = Flags::from_bits(buf.get_u16()).ok_or(crate::WireError::Reserved)?;
        let code = StatusCode::try_from(buf.get_u8())?;
        let ttl = buf.get_u8();
        let reserved0 = buf.get_u16();

        if reserved0 != 0 {
            return Err(crate::WireError::Reserved);
        }

        if ttl == 0 {
            return Err(crate::WireError::Ttl);
        }

        let msg_id = buf.get_u64();
        let corr_id = buf.get_u64();
        let src_node = buf.get_u64();
        let dst_node = buf.get_u64();
        let route = buf.get_u32();
        let hdr_hint_len = buf.get_u32();

        Ok(Self {
            ver,
            typ,
            flags,
            code,
            ttl,
            reserved0,
            msg_id,
            corr_id,
            src_node,
            dst_node,
            route,
            hdr_hint_len,
        })
    }

    /// Validate the fast header
    pub fn validate(&self) -> Result<(), crate::WireError> {
        if self.ver != WIRE_VERSION {
            return Err(crate::WireError::Version(self.ver));
        }

        if self.reserved0 != 0 {
            return Err(crate::WireError::Reserved);
        }

        if self.ttl == 0 {
            return Err(crate::WireError::Ttl);
        }

        Ok(())
    }

    /// Decrement TTL for forwarding
    pub fn decrement_ttl(&mut self) -> Result<(), crate::WireError> {
        if self.ttl == 0 {
            return Err(crate::WireError::Ttl);
        }
        self.ttl -= 1;
        if self.ttl == 0 {
            return Err(crate::WireError::Ttl);
        }
        Ok(())
    }
}

/// Calculate CRC32C checksum for fast header and optional hint
pub fn crc32c_fast_header(fast: &FastHeader, hint: Option<&[u8]>) -> u32 {
    let mut hasher = crc32fast::Hasher::new();

    // Serialize fast header for checksum
    let mut buf = BytesMut::with_capacity(FAST_HEADER_SIZE);
    fast.encode(&mut buf);
    hasher.update(&buf);

    // Include hint if present
    if let Some(hint_bytes) = hint {
        hasher.update(hint_bytes);
    }

    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_type_conversion() {
        assert_eq!(FrameType::try_from(0x00).unwrap(), FrameType::Data);
        assert_eq!(FrameType::try_from(0x08).unwrap(), FrameType::Bye);
        assert!(FrameType::try_from(0xFF).is_err());
    }

    #[test]
    fn test_flags() {
        let flags = Flags::CHUNKED | Flags::E2E_ENC;
        assert!(flags.contains(Flags::CHUNKED));
        assert!(flags.contains(Flags::E2E_ENC));
        assert!(!flags.contains(Flags::COMPRESSED));
    }

    #[test]
    fn test_route_pack_unpack() {
        let route = Route::new(7, 31, 1023, 16383);
        let packed = route.pack();
        let unpacked = Route::unpack(packed);
        assert_eq!(route, unpacked);
    }

    #[test]
    fn test_fast_header_encode_decode() {
        let mut header =
            FastHeader::new(FrameType::Data, 0x1234567890ABCDEF, 0xFEDCBA0987654321, 42);
        header.flags = Flags::CHUNKED | Flags::E2E_ENC;
        header.corr_id = 123;
        header.set_route(Route::new(3, 15, 512, 8192));

        let mut buf = BytesMut::new();
        header.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = FastHeader::decode(&mut bytes).unwrap();

        assert_eq!(header, decoded);
    }

    #[test]
    fn test_ttl_decrement() {
        let mut header = FastHeader::new(FrameType::Data, 1, 2, 3);
        header.ttl = 2;

        assert!(header.decrement_ttl().is_ok());
        assert_eq!(header.ttl, 1);

        assert!(header.decrement_ttl().is_err()); // TTL would become 0
    }

    #[test]
    fn test_header_validation() {
        let header = FastHeader::new(FrameType::Data, 1, 2, 3);
        assert!(header.validate().is_ok());

        let mut bad_header = header;
        bad_header.ver = 2;
        assert!(bad_header.validate().is_err());

        let mut bad_header = header;
        bad_header.reserved0 = 1;
        assert!(bad_header.validate().is_err());

        let mut bad_header = header;
        bad_header.ttl = 0;
        assert!(bad_header.validate().is_err());
    }
}
