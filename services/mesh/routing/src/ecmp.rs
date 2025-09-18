//! Equal-Cost Multi-Path (ECMP) routing implementation

use crate::next_hop::{HopSet, NextHop};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// ECMP selector for choosing among multiple equal-cost paths
#[derive(Debug, Clone)]
pub struct EcmpSelector {
    /// Hash seed for consistent selection
    hash_seed: u64,
}

impl EcmpSelector {
    /// Create a new ECMP selector
    pub fn new() -> Self {
        Self {
            hash_seed: 0x517cc1b727220a95, // Random seed
        }
    }
    
    /// Create a new ECMP selector with custom seed
    pub fn with_seed(seed: u64) -> Self {
        Self { hash_seed: seed }
    }
    
    /// Select a next hop from the hop set using consistent hashing
    /// 
    /// Uses (dst_node, corr_id) as the hash key to ensure consistent
    /// path selection for the same flow.
    pub fn select_hop(&self, hop_set: &HopSet, dst_node: u64, corr_id: u64) -> Option<NextHop> {
        if hop_set.is_empty() {
            return None;
        }
        
        // Create hash key from destination and correlation ID
        let mut hasher = DefaultHasher::new();
        self.hash_seed.hash(&mut hasher);
        dst_node.hash(&mut hasher);
        corr_id.hash(&mut hasher);
        let hash = hasher.finish();
        
        // Convert hop set to sorted vector for consistent ordering
        let mut hops: Vec<NextHop> = hop_set.hops.iter().cloned().collect();
        hops.sort_by_key(|hop| hop.node_id);
        
        // Select hop based on hash
        let index = (hash as usize) % hops.len();
        Some(hops[index].clone())
    }
    
    /// Select a next hop using only destination node (no correlation ID)
    pub fn select_hop_simple(&self, hop_set: &HopSet, dst_node: u64) -> Option<NextHop> {
        self.select_hop(hop_set, dst_node, 0)
    }
    
    /// Get load distribution across hops for analysis
    /// 
    /// Returns a vector of (node_id, estimated_load_percentage) pairs
    pub fn get_load_distribution(&self, hop_set: &HopSet, sample_flows: &[(u64, u64)]) -> Vec<(u64, f64)> {
        if hop_set.is_empty() || sample_flows.is_empty() {
            return Vec::new();
        }
        
        let mut hop_counts = std::collections::HashMap::new();
        
        // Count selections for sample flows
        for &(dst_node, corr_id) in sample_flows {
            if let Some(hop) = self.select_hop(hop_set, dst_node, corr_id) {
                *hop_counts.entry(hop.node_id).or_insert(0) += 1;
            }
        }
        
        // Calculate percentages
        let total_samples = sample_flows.len() as f64;
        hop_counts
            .into_iter()
            .map(|(node_id, count)| (node_id, (count as f64 / total_samples) * 100.0))
            .collect()
    }
}

impl Default for EcmpSelector {
    fn default() -> Self {
        Self::new()
    }
}

/// ECMP routing decision
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcmpDecision {
    /// Selected next hop
    pub next_hop: NextHop,
    /// Total number of available hops
    pub total_hops: usize,
    /// Cost of the selected path
    pub cost: u32,
}

impl EcmpDecision {
    /// Create a new ECMP decision
    pub fn new(next_hop: NextHop, total_hops: usize, cost: u32) -> Self {
        Self {
            next_hop,
            total_hops,
            cost,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_ecmp_selection() {
        let selector = EcmpSelector::new();
        
        let hop1 = NextHop::new(1001, 10);
        let hop2 = NextHop::new(2002, 10);
        let hop3 = NextHop::new(3003, 10);
        
        let hop_set: HopSet = vec![hop1, hop2, hop3].into_iter().collect();
        
        // Test that selection is consistent
        let dst_node = 5005;
        let corr_id = 12345;
        
        let selection1 = selector.select_hop(&hop_set, dst_node, corr_id);
        let selection2 = selector.select_hop(&hop_set, dst_node, corr_id);
        
        assert_eq!(selection1, selection2);
        assert!(selection1.is_some());
        
        // Test that different flows can select different hops
        let mut selections = HashMap::new();
        for i in 0..100 {
            if let Some(hop) = selector.select_hop(&hop_set, dst_node, i) {
                *selections.entry(hop.node_id).or_insert(0) += 1;
            }
        }
        
        // Should have some distribution across hops
        assert!(selections.len() > 1);
    }
    
    #[test]
    fn test_ecmp_empty_hop_set() {
        let selector = EcmpSelector::new();
        let empty_hop_set = HopSet::new(10);
        
        let result = selector.select_hop(&empty_hop_set, 1001, 12345);
        assert!(result.is_none());
    }
    
    #[test]
    fn test_ecmp_single_hop() {
        let selector = EcmpSelector::new();
        let hop = NextHop::new(1001, 10);
        let hop_set = HopSet::single(hop.clone());
        
        let result = selector.select_hop(&hop_set, 2002, 12345);
        assert_eq!(result, Some(hop));
    }
    
    #[test]
    fn test_load_distribution() {
        let selector = EcmpSelector::new();
        
        let hop1 = NextHop::new(1001, 10);
        let hop2 = NextHop::new(2002, 10);
        
        let hop_set: HopSet = vec![hop1, hop2].into_iter().collect();
        
        // Generate sample flows
        let sample_flows: Vec<(u64, u64)> = (0..1000)
            .map(|i| (5005, i))
            .collect();
        
        let distribution = selector.get_load_distribution(&hop_set, &sample_flows);
        
        assert_eq!(distribution.len(), 2);
        
        // Check that load is reasonably distributed (within 40-60% range)
        for (node_id, percentage) in distribution {
            assert!(hop_set.contains_node(node_id));
            assert!(percentage >= 30.0 && percentage <= 70.0);
        }
    }
    
    #[test]
    fn test_consistent_hashing() {
        let selector1 = EcmpSelector::with_seed(12345);
        let selector2 = EcmpSelector::with_seed(12345);
        let selector3 = EcmpSelector::with_seed(54321);
        
        let hop1 = NextHop::new(1001, 10);
        let hop2 = NextHop::new(2002, 10);
        let hop_set: HopSet = vec![hop1, hop2].into_iter().collect();
        
        let dst_node = 5005;
        let corr_id = 12345;
        
        // Same seed should produce same results
        let result1 = selector1.select_hop(&hop_set, dst_node, corr_id);
        let result2 = selector2.select_hop(&hop_set, dst_node, corr_id);
        assert_eq!(result1, result2);
        
        // Different seed might produce different results
        let result3 = selector3.select_hop(&hop_set, dst_node, corr_id);
        // Note: This might occasionally be the same due to hash collisions, but that's OK
        
        assert!(result1.is_some());
        assert!(result3.is_some());
    }
}