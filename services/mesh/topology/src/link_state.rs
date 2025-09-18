//! Link-state topology database and shortest path computation.

use std::collections::HashMap;

/// Maximum age for topology entries (5 minutes)
const MAX_TOPOLOGY_AGE_SECS: u64 = 300;

/// Default TTL for topology updates
pub const DEFAULT_TOPOLOGY_TTL: u8 = 8;

/// Information about a node in the topology
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInfo {
    /// Node ID
    pub node_id: u64,
    /// Last known sequence number from this node
    pub sequence_number: u64,
    /// Timestamp when this information was last updated
    pub last_updated: u64,
    /// Direct neighbors of this node
    pub neighbors: HashMap<u64, LinkInfo>,
}

/// Information about a link between two nodes
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkInfo {
    /// Cost of this link (typically RTT in microseconds)
    pub cost: u32,
    /// Optional address information
    pub addr: Option<String>,
    /// Timestamp when this link was last seen
    pub last_seen: u64,
}

/// Computed route information
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComputedRoute {
    /// Destination node
    pub dst_node: u64,
    /// Next hop to reach destination
    pub next_hop: u64,
    /// Total cost to reach destination
    pub total_cost: u32,
    /// Number of hops to destination
    pub hop_count: u8,
}

/// Link-state topology database
#[derive(Debug)]
pub struct TopologyDatabase {
    /// Local node ID
    local_node_id: u64,
    /// Information about all known nodes
    nodes: HashMap<u64, NodeInfo>,
    /// Computed routes from local node to all destinations
    routes: HashMap<u64, ComputedRoute>,
    /// Local sequence number for our own updates
    local_sequence: u64,
}

// Include implementation
mod database;
pub use database::TopologyStats;
