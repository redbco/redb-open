//! Router trait and routing decision logic

use crate::ecmp::{EcmpDecision};
use crate::next_hop::{HopSet};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Routing decision result
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutingDecision {
    /// Forward to next hop
    Forward(EcmpDecision),
    /// Deliver locally (we are the destination)
    Local,
    /// Drop packet (no route found, TTL expired, etc.)
    Drop(DropReason),
}

/// Reason for dropping a packet
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DropReason {
    /// No route to destination
    NoRoute,
    /// TTL expired
    TtlExpired,
    /// Invalid destination
    InvalidDestination,
    /// Routing loop detected
    RoutingLoop,
    /// Administrative prohibition
    AdminProhibited,
}

impl fmt::Display for DropReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DropReason::NoRoute => write!(f, "no route to destination"),
            DropReason::TtlExpired => write!(f, "TTL expired"),
            DropReason::InvalidDestination => write!(f, "invalid destination"),
            DropReason::RoutingLoop => write!(f, "routing loop detected"),
            DropReason::AdminProhibited => write!(f, "administratively prohibited"),
        }
    }
}

/// Routing context for making routing decisions
#[derive(Debug, Clone)]
pub struct RoutingContext {
    /// Source node ID
    pub src_node: u64,
    /// Destination node ID
    pub dst_node: u64,
    /// Current TTL value
    pub ttl: u8,
    /// Correlation ID for ECMP consistency
    pub corr_id: u64,
    /// Route class (for QoS/policy routing)
    pub route_class: u32,
    /// Partition ID
    pub partition: u32,
    /// Routing epoch
    pub epoch: u32,
}

impl RoutingContext {
    /// Create a new routing context
    pub fn new(src_node: u64, dst_node: u64, ttl: u8, corr_id: u64) -> Self {
        Self {
            src_node,
            dst_node,
            ttl,
            corr_id,
            route_class: 0,
            partition: 0,
            epoch: 0,
        }
    }
    
    /// Decrement TTL and return new context
    pub fn decrement_ttl(&self) -> Option<Self> {
        if self.ttl > 0 {
            let mut ctx = self.clone();
            ctx.ttl -= 1;
            Some(ctx)
        } else {
            None
        }
    }
    
    /// Check if TTL is expired
    pub fn is_ttl_expired(&self) -> bool {
        self.ttl == 0
    }
}

/// Router trait for making routing decisions
#[async_trait]
pub trait Router: Send + Sync {
    /// Make a routing decision for the given context
    async fn decide(&self, ctx: &RoutingContext) -> RoutingDecision;
    
    /// Get the local node ID
    fn local_node_id(&self) -> u64;
    
    /// Check if a destination is reachable
    async fn is_reachable(&self, dst_node: u64) -> bool;
    
    /// Get routing statistics
    async fn get_stats(&self) -> RouterStats;
    
    /// Update routing table (for topology changes)
    async fn update_routes(&self, updates: Vec<RouteUpdate>);
}

/// Router statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouterStats {
    /// Local node ID
    pub local_node_id: u64,
    /// Total number of routes
    pub total_routes: usize,
    /// Number of routing decisions made
    pub decisions_made: u64,
    /// Number of packets forwarded
    pub packets_forwarded: u64,
    /// Number of packets delivered locally
    pub packets_local: u64,
    /// Number of packets dropped
    pub packets_dropped: u64,
    /// Breakdown of drop reasons
    pub drop_reasons: std::collections::HashMap<String, u64>,
}

impl RouterStats {
    /// Create new router statistics
    pub fn new(local_node_id: u64) -> Self {
        Self {
            local_node_id,
            total_routes: 0,
            decisions_made: 0,
            packets_forwarded: 0,
            packets_local: 0,
            packets_dropped: 0,
            drop_reasons: std::collections::HashMap::new(),
        }
    }
}

/// Route update for dynamic routing
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteUpdate {
    /// Destination node or range
    pub destination: u64,
    /// New hop set (None means remove route)
    pub hop_set: Option<HopSet>,
    /// Route epoch
    pub epoch: u32,
}

impl RouteUpdate {
    /// Create a new route update
    pub fn new(destination: u64, hop_set: HopSet, epoch: u32) -> Self {
        Self {
            destination,
            hop_set: Some(hop_set),
            epoch,
        }
    }
    
    /// Create a route removal update
    pub fn remove(destination: u64, epoch: u32) -> Self {
        Self {
            destination,
            hop_set: None,
            epoch,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::next_hop::NextHop;

    #[test]
    fn test_routing_context() {
        let ctx = RoutingContext::new(1001, 2002, 64, 12345);
        assert_eq!(ctx.src_node, 1001);
        assert_eq!(ctx.dst_node, 2002);
        assert_eq!(ctx.ttl, 64);
        assert_eq!(ctx.corr_id, 12345);
        assert!(!ctx.is_ttl_expired());
        
        // Test TTL decrement
        let ctx2 = ctx.decrement_ttl().unwrap();
        assert_eq!(ctx2.ttl, 63);
        
        // Test TTL expiration
        let mut ctx3 = ctx.clone();
        ctx3.ttl = 1;
        let ctx4 = ctx3.decrement_ttl().unwrap();
        assert_eq!(ctx4.ttl, 0);
        assert!(ctx4.is_ttl_expired());
        
        // Test TTL already expired
        let ctx5 = ctx4.decrement_ttl();
        assert!(ctx5.is_none());
    }
    
    #[test]
    fn test_drop_reason_display() {
        assert_eq!(DropReason::NoRoute.to_string(), "no route to destination");
        assert_eq!(DropReason::TtlExpired.to_string(), "TTL expired");
        assert_eq!(DropReason::InvalidDestination.to_string(), "invalid destination");
        assert_eq!(DropReason::RoutingLoop.to_string(), "routing loop detected");
        assert_eq!(DropReason::AdminProhibited.to_string(), "administratively prohibited");
    }
    
    #[test]
    fn test_route_update() {
        let hop = NextHop::new(1001, 10);
        let hop_set = HopSet::single(hop);
        
        let update = RouteUpdate::new(2002, hop_set.clone(), 1);
        assert_eq!(update.destination, 2002);
        assert_eq!(update.hop_set, Some(hop_set));
        assert_eq!(update.epoch, 1);
        
        let remove_update = RouteUpdate::remove(2002, 2);
        assert_eq!(remove_update.destination, 2002);
        assert_eq!(remove_update.hop_set, None);
        assert_eq!(remove_update.epoch, 2);
    }
    
    #[test]
    fn test_router_stats() {
        let mut stats = RouterStats::new(1001);
        assert_eq!(stats.local_node_id, 1001);
        assert_eq!(stats.total_routes, 0);
        assert_eq!(stats.decisions_made, 0);
        
        stats.packets_dropped += 1;
        stats.drop_reasons.insert("no_route".to_string(), 1);
        
        assert_eq!(stats.packets_dropped, 1);
        assert_eq!(stats.drop_reasons.get("no_route"), Some(&1));
    }
}
