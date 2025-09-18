//! TopologyDatabase implementation methods.

use super::{TopologyDatabase, NodeInfo, LinkInfo, ComputedRoute, MAX_TOPOLOGY_AGE_SECS, DEFAULT_TOPOLOGY_TTL};
use mesh_wire::{NeighborInfo, TopologyUpdate};
use std::collections::{HashMap, BinaryHeap};
use std::cmp::Reverse;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info};

impl TopologyDatabase {
    /// Create a new topology database
    pub fn new(local_node_id: u64) -> Self {
        Self {
            local_node_id,
            nodes: HashMap::new(),
            routes: HashMap::new(),
            local_sequence: 1,
        }
    }

    /// Get the next sequence number for local updates
    pub fn next_sequence_number(&mut self) -> u64 {
        let seq = self.local_sequence;
        self.local_sequence = self.local_sequence.wrapping_add(1);
        seq
    }

    /// Update local neighbors (when sessions connect/disconnect)
    pub fn update_local_neighbors(&mut self, neighbors: Vec<NeighborInfo>) -> TopologyUpdate {
        let sequence = self.next_sequence_number();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Update our own node info
        let mut neighbor_map = HashMap::new();
        for neighbor in &neighbors {
            neighbor_map.insert(
                neighbor.node_id,
                LinkInfo {
                    cost: neighbor.cost,
                    addr: neighbor.addr.clone(),
                    last_seen: now,
                },
            );
        }

        let node_info = NodeInfo {
            node_id: self.local_node_id,
            sequence_number: sequence,
            last_updated: now,
            neighbors: neighbor_map,
        };

        self.nodes.insert(self.local_node_id, node_info);

        // Recompute routes
        self.compute_routes();

        // Create topology update to broadcast
        TopologyUpdate::new(self.local_node_id, sequence, neighbors, DEFAULT_TOPOLOGY_TTL)
    }

    /// Process a received topology update
    pub fn process_topology_update(&mut self, update: TopologyUpdate) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Check if update is too old
        if now.saturating_sub(update.timestamp) > MAX_TOPOLOGY_AGE_SECS {
            debug!(
                "Ignoring old topology update from node {} (age: {}s)",
                update.originator_node,
                now.saturating_sub(update.timestamp)
            );
            return false;
        }

        // Check if we already have newer information
        if let Some(existing) = self.nodes.get(&update.originator_node) {
            if !update.is_newer_than(existing.sequence_number) {
                debug!(
                    "Ignoring old topology update from node {} (seq: {} vs {})",
                    update.originator_node, update.sequence_number, existing.sequence_number
                );
                return false;
            }
        }

        info!(
            "Processing topology update from node {} (seq: {}, {} neighbors)",
            update.originator_node,
            update.sequence_number,
            update.neighbors.len()
        );

        // Convert neighbors to link info
        let mut neighbor_map = HashMap::new();
        for neighbor in &update.neighbors {
            neighbor_map.insert(
                neighbor.node_id,
                LinkInfo {
                    cost: neighbor.cost,
                    addr: neighbor.addr.clone(),
                    last_seen: now,
                },
            );
        }

        // Update node info
        let node_info = NodeInfo {
            node_id: update.originator_node,
            sequence_number: update.sequence_number,
            last_updated: now,
            neighbors: neighbor_map,
        };

        self.nodes.insert(update.originator_node, node_info);

        // Recompute routes
        self.compute_routes();

        true
    }

    /// Compute shortest paths using Dijkstra's algorithm
    fn compute_routes(&mut self) {
        // Clear existing routes
        self.routes.clear();

        // Dijkstra's algorithm
        let mut distances: HashMap<u64, u32> = HashMap::new();
        let mut previous: HashMap<u64, u64> = HashMap::new();
        let mut unvisited: BinaryHeap<Reverse<(u32, u64)>> = BinaryHeap::new();

        // Initialize distances
        distances.insert(self.local_node_id, 0);
        unvisited.push(Reverse((0, self.local_node_id)));

        // Add all known nodes with infinite distance
        for &node_id in self.nodes.keys() {
            if node_id != self.local_node_id {
                distances.insert(node_id, u32::MAX);
                unvisited.push(Reverse((u32::MAX, node_id)));
            }
        }

        while let Some(Reverse((current_dist, current_node))) = unvisited.pop() {
            // Skip if we've already found a better path
            if current_dist > distances.get(&current_node).copied().unwrap_or(u32::MAX) {
                continue;
            }

            // Get neighbors of current node
            if let Some(node_info) = self.nodes.get(&current_node) {
                for (&neighbor_id, link_info) in &node_info.neighbors {
                    let new_dist = current_dist.saturating_add(link_info.cost);
                    let existing_dist = distances.get(&neighbor_id).copied().unwrap_or(u32::MAX);

                    if new_dist < existing_dist {
                        distances.insert(neighbor_id, new_dist);
                        previous.insert(neighbor_id, current_node);
                        unvisited.push(Reverse((new_dist, neighbor_id)));
                    }
                }
            }
        }

        // Build routes from computed paths
        for (&dst_node, &total_cost) in &distances {
            if dst_node != self.local_node_id && total_cost != u32::MAX {
                // Find next hop by walking back from destination
                let mut next_hop = dst_node;
                let mut hop_count = 0;

                while let Some(&prev_node) = previous.get(&next_hop) {
                    hop_count += 1;
                    if prev_node == self.local_node_id {
                        break;
                    }
                    next_hop = prev_node;
                }

                let route = ComputedRoute {
                    dst_node,
                    next_hop,
                    total_cost,
                    hop_count,
                };

                self.routes.insert(dst_node, route);
            }
        }

        debug!(
            "Computed {} routes from node {}",
            self.routes.len(),
            self.local_node_id
        );
    }

    /// Get all computed routes
    pub fn get_routes(&self) -> &HashMap<u64, ComputedRoute> {
        &self.routes
    }

    /// Get route to a specific destination
    pub fn get_route(&self, dst_node: u64) -> Option<&ComputedRoute> {
        self.routes.get(&dst_node)
    }

    /// Get all known nodes
    pub fn get_nodes(&self) -> &HashMap<u64, NodeInfo> {
        &self.nodes
    }

    /// Get topology updates for all known nodes (for synchronizing new neighbors)
    pub fn get_all_topology_updates(&self) -> Vec<TopologyUpdate> {
        let mut updates = Vec::new();
        
        for (node_id, node_info) in &self.nodes {
            // Skip our own node - we don't need to send our own topology to others
            if *node_id == self.local_node_id {
                continue;
            }
            
            // Convert neighbors back to NeighborInfo
            let neighbors: Vec<NeighborInfo> = node_info.neighbors
                .iter()
                .map(|(&neighbor_id, link_info)| NeighborInfo::new(
                    neighbor_id,
                    link_info.cost,
                    link_info.addr.clone(),
                ))
                .collect();
            
            // Create topology update for this node
            let update = TopologyUpdate::new(
                *node_id,
                node_info.sequence_number,
                neighbors,
                DEFAULT_TOPOLOGY_TTL - 1, // Reduce TTL since this is a retransmission
            );
            
            updates.push(update);
        }
        
        updates
    }

    /// Clean up old topology entries
    pub fn cleanup_old_entries(&mut self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut to_remove = Vec::new();

        for (&node_id, node_info) in &self.nodes {
            if node_id != self.local_node_id
                && now.saturating_sub(node_info.last_updated) > MAX_TOPOLOGY_AGE_SECS
            {
                to_remove.push(node_id);
            }
        }

        if !to_remove.is_empty() {
            info!("Cleaning up {} old topology entries", to_remove.len());
            for node_id in to_remove {
                self.nodes.remove(&node_id);
            }

            // Recompute routes after cleanup
            self.compute_routes();
        }
    }

    /// Get topology statistics
    pub fn get_stats(&self) -> TopologyStats {
        TopologyStats {
            total_nodes: self.nodes.len(),
            total_routes: self.routes.len(),
            local_sequence: self.local_sequence,
        }
    }
}

/// Topology database statistics
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopologyStats {
    /// Total number of known nodes
    pub total_nodes: usize,
    /// Total number of computed routes
    pub total_routes: usize,
    /// Current local sequence number
    pub local_sequence: u64,
}
