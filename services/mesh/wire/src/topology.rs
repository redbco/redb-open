//! Topology discovery message structures for the wire protocol.

use serde::{Deserialize, Serialize};

/// Information about a neighbor node
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NeighborInfo {
    /// Node ID of the neighbor
    pub node_id: u64,
    /// Cost to reach this neighbor (typically RTT in microseconds or hop count)
    pub cost: u32,
    /// Optional address information for the neighbor
    pub addr: Option<String>,
}

/// Topology update message for link-state advertisement
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopologyUpdate {
    /// Node ID that originated this update
    pub originator_node: u64,
    /// Sequence number for loop prevention and freshness
    pub sequence_number: u64,
    /// List of neighbors known to the originator
    pub neighbors: Vec<NeighborInfo>,
    /// Time-to-live for flooding control
    pub ttl: u8,
    /// Timestamp when this update was created (Unix timestamp in seconds)
    pub timestamp: u64,
}

/// Topology request message for requesting topology information
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopologyRequest {
    /// Node ID making the request
    pub requesting_node: u64,
    /// Optional specific node ID to request information about (None = all nodes)
    pub target_node: Option<u64>,
    /// Request ID for correlation
    pub request_id: u64,
}

impl TopologyUpdate {
    /// Create a new topology update
    pub fn new(
        originator_node: u64,
        sequence_number: u64,
        neighbors: Vec<NeighborInfo>,
        ttl: u8,
    ) -> Self {
        Self {
            originator_node,
            sequence_number,
            neighbors,
            ttl,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    /// Check if this update should be forwarded (TTL > 0)
    pub fn should_forward(&self) -> bool {
        self.ttl > 0
    }

    /// Decrement TTL for forwarding
    pub fn decrement_ttl(&mut self) {
        if self.ttl > 0 {
            self.ttl -= 1;
        }
    }

    /// Check if this update is newer than another based on sequence number
    pub fn is_newer_than(&self, other_seq: u64) -> bool {
        // Handle sequence number wraparound using signed comparison
        let diff = self.sequence_number.wrapping_sub(other_seq) as i64;
        diff > 0
    }
}

impl TopologyRequest {
    /// Create a new topology request
    pub fn new(requesting_node: u64, target_node: Option<u64>, request_id: u64) -> Self {
        Self {
            requesting_node,
            target_node,
            request_id,
        }
    }
}

impl NeighborInfo {
    /// Create a new neighbor info entry
    pub fn new(node_id: u64, cost: u32, addr: Option<String>) -> Self {
        Self {
            node_id,
            cost,
            addr,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topology_update_creation() {
        let neighbors = vec![
            NeighborInfo::new(2002, 100, Some("127.0.0.1:9002".to_string())),
            NeighborInfo::new(3003, 150, Some("127.0.0.1:9003".to_string())),
        ];

        let update = TopologyUpdate::new(1001, 1, neighbors.clone(), 5);

        assert_eq!(update.originator_node, 1001);
        assert_eq!(update.sequence_number, 1);
        assert_eq!(update.neighbors, neighbors);
        assert_eq!(update.ttl, 5);
        assert!(update.should_forward());
    }

    #[test]
    fn test_ttl_decrement() {
        let mut update = TopologyUpdate::new(1001, 1, vec![], 1);
        assert!(update.should_forward());

        update.decrement_ttl();
        assert!(!update.should_forward());
        assert_eq!(update.ttl, 0);

        // Should not go below 0
        update.decrement_ttl();
        assert_eq!(update.ttl, 0);
    }

    #[test]
    fn test_sequence_number_comparison() {
        let update1 = TopologyUpdate::new(1001, 1, vec![], 5);
        let update2 = TopologyUpdate::new(1001, 2, vec![], 5);

        assert!(update2.is_newer_than(update1.sequence_number));
        assert!(!update1.is_newer_than(update2.sequence_number));
    }

    #[test]
    fn test_topology_request() {
        let request = TopologyRequest::new(1001, Some(2002), 12345);

        assert_eq!(request.requesting_node, 1001);
        assert_eq!(request.target_node, Some(2002));
        assert_eq!(request.request_id, 12345);
    }
}
