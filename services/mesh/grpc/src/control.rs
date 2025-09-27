//! MeshControl gRPC service implementation

use crate::proto::mesh::v1::{
    mesh_control_server::MeshControl, AddSessionRequest, AddSessionResponse, DropSessionRequest, DropSessionResponse, GetMessageMetricsRequest,
    GetMessageMetricsResponse, GetRoutingTableRequest, GetRoutingTableResponse, GetSessionsRequest,
    GetSessionsResponse, GetTopologyRequest, GetTopologyResponse, InjectNeighborRequest,
    RouteEntry, SessionInfo, SetPolicyRequest, TopologySnapshot, NeighborInfo,
};
use crate::message_tracker::MessageTracker;
use mesh_routing::RoutingTable;
use mesh_session::manager::SessionInfo as SessionManagerInfo;
use mesh_topology::TopologyDatabase;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tonic::{Request, Response, Result, Status};
use tracing::{debug, info, warn};

/// Result of a session operation
#[derive(Debug, Clone)]
pub struct SessionOperationResult {
    /// Whether the operation was successful
    pub success: bool,
    /// Human-readable message describing the result
    pub message: String,
    /// Optional error code for failed operations
    pub error_code: Option<String>,
    /// Node ID of the peer (for successful AddSession operations)
    pub peer_node_id: Option<u64>,
    /// Remote address that was connected to (for successful AddSession operations)
    pub remote_addr: Option<String>,
}

/// Commands for session management with response channels
#[derive(Debug)]
pub enum SessionCommand {
    /// Add a new session to the specified address
    AddSession { 
        /// The socket address to connect to
        addr: SocketAddr,
        /// Connection timeout in seconds
        timeout_seconds: u32,
        /// Channel to send the result back
        response_tx: tokio::sync::oneshot::Sender<SessionOperationResult>,
    },
    /// Drop an existing session with the specified node
    DropSession { 
        /// The node ID of the peer to disconnect from
        peer_node_id: u64,
        /// Channel to send the result back
        response_tx: tokio::sync::oneshot::Sender<SessionOperationResult>,
    },
}

/// Session termination signal
#[derive(Debug, Clone)]
pub struct SessionTermination {
    /// Node ID to terminate
    pub peer_node_id: u64,
}

/// MeshControl service implementation
#[derive(Debug)]
pub struct MeshControlService {
    /// Local node ID
    node_id: u64,
    /// Policy store
    policies: Arc<RwLock<HashMap<String, String>>>,
    /// Routing table reference
    routing_table: Option<Arc<RoutingTable>>,
    /// Session registry reference
    session_registry: Option<Arc<RwLock<HashMap<u64, SessionManagerInfo>>>>,
    /// Topology database reference
    topology_db: Option<Arc<RwLock<TopologyDatabase>>>,
    /// Message tracker reference
    message_tracker: Option<Arc<MessageTracker>>,
    /// Channel for sending session management commands
    session_command_tx: Option<mpsc::UnboundedSender<SessionCommand>>,
}

impl MeshControlService {
    /// Create a new MeshControl service
    pub fn new(
        node_id: u64, 
        routing_table: Option<Arc<RoutingTable>>,
        session_registry: Option<Arc<RwLock<HashMap<u64, SessionManagerInfo>>>>,
        topology_db: Option<Arc<RwLock<TopologyDatabase>>>,
    ) -> Self {
        Self {
            node_id,
            policies: Arc::new(RwLock::new(HashMap::new())),
            routing_table,
            session_registry,
            topology_db,
            message_tracker: None,
            session_command_tx: None,
        }
    }
    
    /// Set the session command channel
    pub fn set_session_command_channel(&mut self, tx: mpsc::UnboundedSender<SessionCommand>) {
        self.session_command_tx = Some(tx);
    }
    
    /// Set the message tracker
    pub fn set_message_tracker(&mut self, tracker: Arc<MessageTracker>) {
        self.message_tracker = Some(tracker);
    }
    
    /// Get a policy value
    pub async fn get_policy(&self, key: &str) -> Option<String> {
        let policies = self.policies.read().await;
        policies.get(key).cloned()
    }
    
    /// List all policies
    pub async fn list_policies(&self) -> HashMap<String, String> {
        let policies = self.policies.read().await;
        policies.clone()
    }
}

#[tonic::async_trait]
impl MeshControl for MeshControlService {
    async fn get_sessions(
        &self,
        _request: Request<GetSessionsRequest>,
    ) -> Result<Response<GetSessionsResponse>> {
        debug!("GetSessions request received");
        
        let sessions = if let Some(session_registry) = &self.session_registry {
            let registry = session_registry.read().await;
            registry.iter().map(|(node_id, session_info)| {
                SessionInfo {
                    peer_node_id: *node_id,
                    remote_addr: session_info.remote_addr.to_string(),
                    state: "Connected".to_string(), // TODO: Add actual state tracking
                    rtt_microseconds: 0, // TODO: Add RTT tracking
                    bytes_sent: 0, // TODO: Add metrics tracking
                    bytes_received: 0, // TODO: Add metrics tracking
                    frames_sent: 0, // TODO: Add metrics tracking
                    frames_received: 0, // TODO: Add metrics tracking
                    is_tls: false, // TODO: Add TLS detection
                }
            }).collect()
        } else {
            vec![]
        };
        
        info!("Returning {} active sessions", sessions.len());
        Ok(Response::new(GetSessionsResponse { sessions }))
    }
    
    async fn get_routing_table(
        &self,
        _request: Request<GetRoutingTableRequest>,
    ) -> Result<Response<GetRoutingTableResponse>> {
        debug!("GetRoutingTable request received");
        
        let (routes, current_epoch) = if let Some(routing_table) = &self.routing_table {
            let current_epoch = routing_table.get_epoch().await;
            let all_routes = routing_table.get_all_routes();
            
            let routes: Vec<RouteEntry> = all_routes
                .into_iter()
                .map(|(dst_node, hop_set)| RouteEntry {
                    dst_node,
                    next_hops: hop_set.node_ids(),
                    cost: hop_set.cost,
                    epoch: current_epoch,
                })
                .collect();
                
            (routes, current_epoch)
        } else {
            (vec![], 0)
        };
        
        Ok(Response::new(GetRoutingTableResponse {
            routes,
            current_epoch,
        }))
    }
    
    async fn get_topology(
        &self,
        _request: Request<GetTopologyRequest>,
    ) -> Result<Response<GetTopologyResponse>> {
        debug!("GetTopology request received");
        
        let (current_epoch, neighbors, routes) = if let Some(topology_db) = &self.topology_db {
            let db = topology_db.read().await;
            let stats = db.get_stats();
            let current_epoch = stats.local_sequence as u32;
            
            // Get neighbors from our own node info
            let neighbors = if let Some(local_node) = db.get_nodes().get(&self.node_id) {
                local_node.neighbors.iter().map(|(node_id, link_info)| {
                    NeighborInfo {
                        node_id: *node_id,
                        addr: link_info.addr.clone().unwrap_or_default(),
                        connected: true, // If it's in our neighbor list, it's connected
                        epoch: current_epoch,
                    }
                }).collect()
            } else {
                vec![]
            };
            
            // Get routes from topology database
            let routes = db.get_routes().iter().map(|(dst_node, computed_route)| {
                RouteEntry {
                    dst_node: *dst_node,
                    next_hops: vec![computed_route.next_hop], // Single next hop from computed route
                    cost: computed_route.total_cost,
                    epoch: current_epoch,
                }
            }).collect();
            
            (current_epoch, neighbors, routes)
        } else if let Some(routing_table) = &self.routing_table {
            // Fallback to routing table if no topology database
            let current_epoch = routing_table.get_epoch().await;
            let all_routes = routing_table.get_all_routes();
            
            let routes: Vec<RouteEntry> = all_routes
                .into_iter()
                .map(|(dst_node, hop_set)| RouteEntry {
                    dst_node,
                    next_hops: hop_set.node_ids(),
                    cost: hop_set.cost,
                    epoch: current_epoch,
                })
                .collect();
                
            (current_epoch, vec![], routes)
        } else {
            (0, vec![], vec![])
        };
        
        let topology = TopologySnapshot {
            local_node_id: self.node_id,
            current_epoch,
            neighbors,
            routes,
        };
        
        info!("Returning topology with {} neighbors and {} routes", 
              topology.neighbors.len(), topology.routes.len());
        Ok(Response::new(GetTopologyResponse {
            topology: Some(topology),
        }))
    }
    
    async fn drop_session(
        &self,
        request: Request<DropSessionRequest>,
    ) -> Result<Response<DropSessionResponse>> {
        let req = request.into_inner();
        
        info!("Dropping session with peer node {}", req.peer_node_id);
        
        if let Some(ref session_command_tx) = self.session_command_tx {
            let (response_tx, response_rx) = tokio::sync::oneshot::channel();
            
            let command = SessionCommand::DropSession {
                peer_node_id: req.peer_node_id,
                response_tx,
            };
            
            if let Err(e) = session_command_tx.send(command) {
                warn!("Failed to send drop session command: {}", e);
                return Err(Status::internal("Failed to process drop session request"));
            }
            
            // Wait for the result with a timeout
            let result = match tokio::time::timeout(std::time::Duration::from_secs(30), response_rx).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => {
                    warn!("Drop session command response channel closed");
                    return Err(Status::internal("Internal communication error"));
                }
                Err(_) => {
                    warn!("Drop session command timed out");
                    return Err(Status::deadline_exceeded("Operation timed out"));
                }
            };
            
            info!("Drop session operation completed for peer node {}: success={}", req.peer_node_id, result.success);
            
            Ok(Response::new(DropSessionResponse {
                success: result.success,
                message: result.message,
                error_code: result.error_code.unwrap_or_default(),
            }))
        } else {
            warn!("Session management not available - no command channel configured");
            Err(Status::unavailable("Session management not available"))
        }
    }
    
    async fn add_session(
        &self,
        request: Request<AddSessionRequest>,
    ) -> Result<Response<AddSessionResponse>> {
        let req = request.into_inner();
        
        info!("Adding session to address: {}", req.addr);
        
        // Parse the address
        let addr: SocketAddr = req.addr.parse()
            .map_err(|e| Status::invalid_argument(format!("Invalid address format: {}", e)))?;
        
        // Use default timeout if not specified
        let timeout_seconds = if req.timeout_seconds == 0 { 30 } else { req.timeout_seconds };
        
        if let Some(ref session_command_tx) = self.session_command_tx {
            let (response_tx, response_rx) = tokio::sync::oneshot::channel();
            
            let command = SessionCommand::AddSession { 
                addr,
                timeout_seconds,
                response_tx,
            };
            
            if let Err(e) = session_command_tx.send(command) {
                warn!("Failed to send add session command: {}", e);
                return Err(Status::internal("Failed to process add session request"));
            }
            
            // Wait for the result with a timeout (add some buffer to the specified timeout)
            let wait_timeout = std::time::Duration::from_secs((timeout_seconds + 10) as u64);
            let result = match tokio::time::timeout(wait_timeout, response_rx).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => {
                    warn!("Add session command response channel closed");
                    return Err(Status::internal("Internal communication error"));
                }
                Err(_) => {
                    warn!("Add session command timed out");
                    return Err(Status::deadline_exceeded("Operation timed out"));
                }
            };
            
            info!("Add session operation completed for address {}: success={}", addr, result.success);
            
            Ok(Response::new(AddSessionResponse {
                success: result.success,
                message: result.message,
                error_code: result.error_code.unwrap_or_default(),
                peer_node_id: result.peer_node_id.unwrap_or(0),
                remote_addr: result.remote_addr.unwrap_or_default(),
            }))
        } else {
            warn!("Session management not available - no command channel configured");
            Err(Status::unavailable("Session management not available"))
        }
    }
    
    async fn inject_neighbor(
        &self,
        request: Request<InjectNeighborRequest>,
    ) -> Result<Response<()>> {
        let req = request.into_inner();
        
        info!("Injecting neighbor: {}", req.addr);
        
        // TODO: Integrate with actual topology manager
        warn!("Neighbor injection not yet implemented");
        
        Ok(Response::new(()))
    }
    
    async fn set_policy(
        &self,
        request: Request<SetPolicyRequest>,
    ) -> Result<Response<()>> {
        let req = request.into_inner();
        
        if req.key.is_empty() {
            return Err(Status::invalid_argument("Policy key cannot be empty"));
        }
        
        debug!("Setting policy: {} = {}", req.key, req.value);
        
        let mut policies = self.policies.write().await;
        policies.insert(req.key.clone(), req.value.clone());
        
        info!("Policy set: {} = {}", req.key, req.value);
        
        Ok(Response::new(()))
    }
    
    async fn get_message_metrics(
        &self,
        _request: Request<GetMessageMetricsRequest>,
    ) -> Result<Response<GetMessageMetricsResponse>> {
        debug!("GetMessageMetrics request received");
        
        if let Some(ref tracker) = self.message_tracker {
            let stats = tracker.get_stats();
            
            // Calculate rates
            let total = stats.total_messages as f64;
            let success_rate = if total > 0.0 {
                ((stats.delivered + stats.ack_success) as f64 / total) * 100.0
            } else {
                0.0
            };
            
            let failure_rate = if total > 0.0 {
                ((stats.undeliverable + stats.ack_failure) as f64 / total) * 100.0
            } else {
                0.0
            };
            
            let pending_rate = if total > 0.0 {
                ((stats.queued + stats.pending_node + stats.pending_client + stats.waiting_for_ack) as f64 / total) * 100.0
            } else {
                0.0
            };
            
            let response = GetMessageMetricsResponse {
                total_messages: stats.total_messages as u64,
                undeliverable: stats.undeliverable as u64,
                queued: stats.queued as u64,
                pending_node: stats.pending_node as u64,
                pending_client: stats.pending_client as u64,
                delivered: stats.delivered as u64,
                waiting_for_ack: stats.waiting_for_ack as u64,
                ack_success: stats.ack_success as u64,
                ack_failure: stats.ack_failure as u64,
                success_rate,
                failure_rate,
                pending_rate,
            };
            
            debug!("Returning message metrics: total={}, success_rate={:.1}%", stats.total_messages, success_rate);
            Ok(Response::new(response))
        } else {
            warn!("Message tracker not available");
            Err(Status::unavailable("Message tracking not enabled"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_set_and_get_policy() {
        let service = MeshControlService::new(1001, None, None, None);
        
        // Set a policy
        let set_req = SetPolicyRequest {
            key: "test_key".to_string(),
            value: "test_value".to_string(),
        };
        
        let response = service.set_policy(Request::new(set_req)).await.unwrap();
        assert_eq!(response.into_inner(), ());
        
        // Get the policy
        let value = service.get_policy("test_key").await;
        assert_eq!(value, Some("test_value".to_string()));
        
        // Get non-existent policy
        let no_value = service.get_policy("non_existent").await;
        assert_eq!(no_value, None);
    }
    
    #[tokio::test]
    async fn test_get_sessions() {
        let service = MeshControlService::new(1001, None, None, None);
        
        let request = GetSessionsRequest {};
        let response = service.get_sessions(Request::new(request)).await.unwrap();
        let sessions_response = response.into_inner();
        
        // Should return empty list when no session registry is provided
        assert_eq!(sessions_response.sessions.len(), 0);
    }
    
    #[tokio::test]
    async fn test_get_topology() {
        let service = MeshControlService::new(1001, None, None, None);
        
        let request = GetTopologyRequest {};
        let response = service.get_topology(Request::new(request)).await.unwrap();
        let topology_response = response.into_inner();
        
        let topology = topology_response.topology.unwrap();
        assert_eq!(topology.local_node_id, 1001);
        assert_eq!(topology.current_epoch, 0);
    }
}
