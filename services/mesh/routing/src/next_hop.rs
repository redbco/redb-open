//! Next hop definitions and utilities

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// A next hop for routing
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NextHop {
    /// Node ID of the next hop
    pub node_id: u64,
    /// Cost to reach this next hop
    pub cost: u32,
    /// Optional interface or connection identifier
    pub interface: Option<String>,
}

impl NextHop {
    /// Create a new next hop
    pub fn new(node_id: u64, cost: u32) -> Self {
        Self {
            node_id,
            cost,
            interface: None,
        }
    }
    
    /// Create a new next hop with interface
    pub fn with_interface(node_id: u64, cost: u32, interface: String) -> Self {
        Self {
            node_id,
            cost,
            interface: Some(interface),
        }
    }
}

/// A set of next hops for ECMP routing
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HopSet {
    /// Set of next hops with equal cost
    pub hops: HashSet<NextHop>,
    /// The cost of these hops
    pub cost: u32,
}

impl HopSet {
    /// Create a new hop set
    pub fn new(cost: u32) -> Self {
        Self {
            hops: HashSet::new(),
            cost,
        }
    }
    
    /// Create a hop set with a single hop
    pub fn single(hop: NextHop) -> Self {
        let cost = hop.cost;
        let mut hops = HashSet::new();
        hops.insert(hop);
        Self { hops, cost }
    }
    
    /// Add a hop to the set (only if cost matches)
    pub fn add_hop(&mut self, hop: NextHop) -> bool {
        if hop.cost == self.cost {
            self.hops.insert(hop);
            true
        } else {
            false
        }
    }
    
    /// Remove a hop from the set
    pub fn remove_hop(&mut self, node_id: u64) -> bool {
        self.hops.retain(|hop| hop.node_id != node_id);
        !self.hops.is_empty()
    }
    
    /// Check if the set is empty
    pub fn is_empty(&self) -> bool {
        self.hops.is_empty()
    }
    
    /// Get the number of hops in the set
    pub fn len(&self) -> usize {
        self.hops.len()
    }
    
    /// Get all node IDs in the hop set
    pub fn node_ids(&self) -> Vec<u64> {
        self.hops.iter().map(|hop| hop.node_id).collect()
    }
    
    /// Check if a node ID is in the hop set
    pub fn contains_node(&self, node_id: u64) -> bool {
        self.hops.iter().any(|hop| hop.node_id == node_id)
    }
}

impl FromIterator<NextHop> for HopSet {
    fn from_iter<T: IntoIterator<Item = NextHop>>(iter: T) -> Self {
        let hops: HashSet<NextHop> = iter.into_iter().collect();
        let cost = hops.iter().map(|h| h.cost).min().unwrap_or(0);
        
        // Filter to only include hops with minimum cost
        let equal_cost_hops: HashSet<NextHop> = hops
            .into_iter()
            .filter(|h| h.cost == cost)
            .collect();
        
        Self {
            hops: equal_cost_hops,
            cost,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_next_hop_creation() {
        let hop = NextHop::new(1001, 10);
        assert_eq!(hop.node_id, 1001);
        assert_eq!(hop.cost, 10);
        assert_eq!(hop.interface, None);
        
        let hop_with_iface = NextHop::with_interface(2002, 5, "eth0".to_string());
        assert_eq!(hop_with_iface.node_id, 2002);
        assert_eq!(hop_with_iface.cost, 5);
        assert_eq!(hop_with_iface.interface, Some("eth0".to_string()));
    }
    
    #[test]
    fn test_hop_set_operations() {
        let mut hop_set = HopSet::new(10);
        
        let hop1 = NextHop::new(1001, 10);
        let hop2 = NextHop::new(2002, 10);
        let hop3 = NextHop::new(3003, 15); // Different cost
        
        // Add hops with matching cost
        assert!(hop_set.add_hop(hop1.clone()));
        assert!(hop_set.add_hop(hop2.clone()));
        assert_eq!(hop_set.len(), 2);
        
        // Try to add hop with different cost
        assert!(!hop_set.add_hop(hop3));
        assert_eq!(hop_set.len(), 2);
        
        // Check contains
        assert!(hop_set.contains_node(1001));
        assert!(hop_set.contains_node(2002));
        assert!(!hop_set.contains_node(3003));
        
        // Remove hop
        assert!(hop_set.remove_hop(1001));
        assert_eq!(hop_set.len(), 1);
        assert!(!hop_set.contains_node(1001));
        
        // Remove last hop
        assert!(!hop_set.remove_hop(2002));
        assert!(hop_set.is_empty());
    }
    
    #[test]
    fn test_hop_set_from_iterator() {
        let hops = vec![
            NextHop::new(1001, 10),
            NextHop::new(2002, 10),
            NextHop::new(3003, 15), // Higher cost, should be filtered out
            NextHop::new(4004, 10),
        ];
        
        let hop_set: HopSet = hops.into_iter().collect();
        
        assert_eq!(hop_set.cost, 10);
        assert_eq!(hop_set.len(), 3);
        assert!(hop_set.contains_node(1001));
        assert!(hop_set.contains_node(2002));
        assert!(hop_set.contains_node(4004));
        assert!(!hop_set.contains_node(3003)); // Filtered out due to higher cost
    }
}
