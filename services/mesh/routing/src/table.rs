//! Routing table implementation with ECMP support

use crate::ecmp::{EcmpDecision, EcmpSelector};
use crate::next_hop::{HopSet, NextHop};
use crate::router::{DropReason, RouteUpdate, Router, RouterStats, RoutingContext, RoutingDecision};
use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// In-memory routing table with ECMP support
#[derive(Debug)]
pub struct RoutingTable {
    /// Local node ID
    local_node_id: u64,
    /// Routing table: destination -> hop set
    routes: DashMap<u64, HopSet>,
    /// ECMP selector for load balancing
    ecmp_selector: EcmpSelector,
    /// Current routing epoch
    current_epoch: Arc<RwLock<u32>>,
    /// Router statistics
    stats: Arc<RwLock<RouterStats>>,
    /// Counters for statistics
    decisions_counter: AtomicU64,
    forwards_counter: AtomicU64,
    local_counter: AtomicU64,
    drops_counter: AtomicU64,
}

impl RoutingTable {
    /// Create a new routing table
    pub fn new(local_node_id: u64) -> Self {
        Self {
            local_node_id,
            routes: DashMap::new(),
            ecmp_selector: EcmpSelector::new(),
            current_epoch: Arc::new(RwLock::new(0)),
            stats: Arc::new(RwLock::new(RouterStats::new(local_node_id))),
            decisions_counter: AtomicU64::new(0),
            forwards_counter: AtomicU64::new(0),
            local_counter: AtomicU64::new(0),
            drops_counter: AtomicU64::new(0),
        }
    }
    
    /// Update routes from topology database
    pub async fn update_routes_from_topology(&self, computed_routes: &std::collections::HashMap<u64, mesh_topology::ComputedRoute>) {
        info!("Updating routing table with {} computed routes", computed_routes.len());
        
        // Clear existing routes (except local)
        self.routes.retain(|&dst, _| dst == self.local_node_id);
        
        // Add new routes from topology
        for (dst_node, computed_route) in computed_routes {
            let next_hop = NextHop::new(computed_route.next_hop, computed_route.total_cost);
            let hop_set = HopSet::single(next_hop);
            
            self.routes.insert(*dst_node, hop_set);
            debug!("Added route to node {} via {} (cost: {})", 
                   dst_node, computed_route.next_hop, computed_route.total_cost);
        }
        
        // Update epoch
        let mut epoch = self.current_epoch.write().await;
        *epoch = epoch.wrapping_add(1);
        
        info!("Routing table updated with epoch {}", *epoch);
    }

    /// Add or update a route
    pub async fn add_route(&self, dst_node: u64, hop_set: HopSet) {
        debug!(
            "Adding route to {} with {} hops (cost: {})",
            dst_node,
            hop_set.len(),
            hop_set.cost
        );
        
        self.routes.insert(dst_node, hop_set);
        
        // Update stats
        let mut stats = self.stats.write().await;
        stats.total_routes = self.routes.len();
    }
    
    /// Remove a route
    pub async fn remove_route(&self, dst_node: u64) {
        if self.routes.remove(&dst_node).is_some() {
            debug!("Removed route to {}", dst_node);
            
            // Update stats
            let mut stats = self.stats.write().await;
            stats.total_routes = self.routes.len();
        }
    }
    
    /// Get a route for a destination
    pub fn get_route(&self, dst_node: u64) -> Option<HopSet> {
        self.routes.get(&dst_node).map(|entry| entry.clone())
    }
    
    /// Get all routes
    pub fn get_all_routes(&self) -> Vec<(u64, HopSet)> {
        self.routes
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect()
    }
    
    /// Clear all routes
    pub async fn clear_routes(&self) {
        self.routes.clear();
        
        // Update stats
        let mut stats = self.stats.write().await;
        stats.total_routes = 0;
    }
    
    /// Get current epoch
    pub async fn get_epoch(&self) -> u32 {
        *self.current_epoch.read().await
    }
    
    /// Set current epoch
    pub async fn set_epoch(&self, epoch: u32) {
        *self.current_epoch.write().await = epoch;
        info!("Routing epoch updated to {}", epoch);
    }
    
    /// Increment epoch
    pub async fn increment_epoch(&self) -> u32 {
        let mut epoch = self.current_epoch.write().await;
        *epoch += 1;
        let new_epoch = *epoch;
        info!("Routing epoch incremented to {}", new_epoch);
        new_epoch
    }
    
    /// Update statistics for a routing decision
    async fn update_stats(&self, decision: &RoutingDecision) {
        self.decisions_counter.fetch_add(1, Ordering::Relaxed);
        
        match decision {
            RoutingDecision::Forward(_) => {
                self.forwards_counter.fetch_add(1, Ordering::Relaxed);
            }
            RoutingDecision::Local => {
                self.local_counter.fetch_add(1, Ordering::Relaxed);
            }
            RoutingDecision::Drop(reason) => {
                self.drops_counter.fetch_add(1, Ordering::Relaxed);
                
                let mut stats = self.stats.write().await;
                let reason_str = reason.to_string();
                *stats.drop_reasons.entry(reason_str).or_insert(0) += 1;
            }
        }
    }
}

#[async_trait]
impl Router for RoutingTable {
    async fn decide(&self, ctx: &RoutingContext) -> RoutingDecision {
        debug!(
            "Making routing decision: src={}, dst={}, ttl={}, corr_id={}",
            ctx.src_node, ctx.dst_node, ctx.ttl, ctx.corr_id
        );
        
        // Check if destination is local
        if ctx.dst_node == self.local_node_id {
            let decision = RoutingDecision::Local;
            self.update_stats(&decision).await;
            return decision;
        }
        
        // Check TTL
        if ctx.is_ttl_expired() {
            let decision = RoutingDecision::Drop(DropReason::TtlExpired);
            self.update_stats(&decision).await;
            return decision;
        }
        
        // Look up route
        match self.get_route(ctx.dst_node) {
            Some(hop_set) => {
                // Use ECMP to select next hop
                match self.ecmp_selector.select_hop(&hop_set, ctx.dst_node, ctx.corr_id) {
                    Some(next_hop) => {
                        let ecmp_decision = EcmpDecision::new(
                            next_hop,
                            hop_set.len(),
                            hop_set.cost,
                        );
                        let decision = RoutingDecision::Forward(ecmp_decision);
                        self.update_stats(&decision).await;
                        decision
                    }
                    None => {
                        warn!("Empty hop set for destination {}", ctx.dst_node);
                        let decision = RoutingDecision::Drop(DropReason::NoRoute);
                        self.update_stats(&decision).await;
                        decision
                    }
                }
            }
            None => {
                debug!("No route to destination {}", ctx.dst_node);
                let decision = RoutingDecision::Drop(DropReason::NoRoute);
                self.update_stats(&decision).await;
                decision
            }
        }
    }
    
    fn local_node_id(&self) -> u64 {
        self.local_node_id
    }
    
    async fn is_reachable(&self, dst_node: u64) -> bool {
        if dst_node == self.local_node_id {
            return true;
        }
        
        self.routes.contains_key(&dst_node)
    }
    
    async fn get_stats(&self) -> RouterStats {
        let mut stats = self.stats.write().await;
        
        // Update counters
        stats.decisions_made = self.decisions_counter.load(Ordering::Relaxed);
        stats.packets_forwarded = self.forwards_counter.load(Ordering::Relaxed);
        stats.packets_local = self.local_counter.load(Ordering::Relaxed);
        stats.packets_dropped = self.drops_counter.load(Ordering::Relaxed);
        stats.total_routes = self.routes.len();
        
        stats.clone()
    }
    
    async fn update_routes(&self, updates: Vec<RouteUpdate>) {
        let mut epoch_changed = false;
        let current_epoch = self.get_epoch().await;
        
        for update in updates {
            // Only apply updates from current or newer epoch
            if update.epoch >= current_epoch {
                if update.epoch > current_epoch {
                    self.set_epoch(update.epoch).await;
                    epoch_changed = true;
                }
                
                match update.hop_set {
                    Some(hop_set) => {
                        self.add_route(update.destination, hop_set).await;
                    }
                    None => {
                        self.remove_route(update.destination).await;
                    }
                }
            } else {
                debug!(
                    "Ignoring stale route update for {} (epoch {} < {})",
                    update.destination, update.epoch, current_epoch
                );
            }
        }
        
        if epoch_changed {
            info!("Routing table updated with new epoch");
        }
    }
}