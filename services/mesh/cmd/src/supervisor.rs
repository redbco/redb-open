//! Supervisor integration for the mesh service.
//!
//! This module handles communication with the supervisor service, including
//! service registration, heartbeats, log streaming, and configuration updates.

use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, RwLock};
use tokio::time::{interval, sleep};
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};
use tracing::{info, warn, error, debug};
use uuid::Uuid;

// Import the generated protobuf types
use mesh_grpc::proto::supervisor::v1::{
    supervisor_service_client::SupervisorServiceClient,
    service_controller_service_server::{ServiceControllerService, ServiceControllerServiceServer},
    RegisterServiceRequest,
    UnregisterServiceRequest,
    HeartbeatRequest,
    StartRequest, StartResponse,
    StopRequest, StopResponse,
    GetHealthRequest, GetHealthResponse,
    ConfigureRequest, ConfigureResponse,
    ServiceCapabilities, ServiceMetrics, ServiceCommand,
};

use mesh_grpc::proto::common::v1::{
    ServiceInfo, HealthStatus,
};

use prost_types::Timestamp;

/// Configuration for supervisor integration
#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    /// Supervisor address (e.g., "localhost:50000")
    pub supervisor_addr: String,
    /// Service name
    pub service_name: String,
    /// Service version
    pub service_version: String,
    /// Service port (gRPC port)
    pub service_port: u16,
    /// Whether to run in standalone mode (no supervisor)
    pub standalone: bool,
}

/// Supervisor client for mesh service integration
pub struct SupervisorClient {
    config: SupervisorConfig,
    instance_id: String,
    service_id: Arc<RwLock<Option<String>>>,
    client: Arc<RwLock<Option<SupervisorServiceClient<Channel>>>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
    bind_address: Option<std::net::SocketAddr>,
}

impl SupervisorClient {
    /// Create a new supervisor client
    pub fn new(config: SupervisorConfig) -> Self {
        let instance_id = Uuid::new_v4().to_string();
        
        Self {
            config,
            instance_id,
            service_id: Arc::new(RwLock::new(None)),
            client: Arc::new(RwLock::new(None)),
            shutdown_tx: None,
            bind_address: None,
        }
    }
    
    /// Set the actual bind address for registration
    pub fn set_bind_address(&mut self, addr: std::net::SocketAddr) {
        self.bind_address = Some(addr);
    }

    /// Connect to supervisor and register the service
    pub async fn connect_and_register(&mut self) -> Result<()> {
        if self.config.standalone {
            info!("Running in standalone mode - supervisor integration disabled");
            return Ok(());
        }

        info!("Connecting to supervisor at {}", self.config.supervisor_addr);
        
        // Connect to supervisor with retry logic
        let client = self.connect_with_retry().await?;
        
        // Register the service
        let service_id = self.register_service(&client).await?;
        
        // Store the client and service ID
        {
            let mut client_guard = self.client.write().await;
            *client_guard = Some(client);
        }
        {
            let mut service_id_guard = self.service_id.write().await;
            *service_id_guard = Some(service_id);
        }
        
        info!("Successfully connected and registered with supervisor");
        Ok(())
    }

    /// Connect to supervisor with retry logic
    async fn connect_with_retry(&self) -> Result<SupervisorServiceClient<Channel>> {
        let mut retry_count = 0;
        let max_retries = 5;
        let mut retry_delay = Duration::from_secs(1);

        loop {
            match self.try_connect().await {
                Ok(client) => return Ok(client),
                Err(e) => {
                    retry_count += 1;
                    if retry_count >= max_retries {
                        return Err(anyhow::anyhow!(
                            "Failed to connect to supervisor after {} attempts: {}", 
                            max_retries, e
                        ));
                    }
                    
                    warn!(
                        "Failed to connect to supervisor (attempt {}/{}): {}. Retrying in {:?}",
                        retry_count, max_retries, e, retry_delay
                    );
                    
                    sleep(retry_delay).await;
                    retry_delay = std::cmp::min(retry_delay * 2, Duration::from_secs(30));
                }
            }
        }
    }

    /// Try to connect to supervisor once
    async fn try_connect(&self) -> Result<SupervisorServiceClient<Channel>> {
        let endpoint = Endpoint::from_shared(format!("http://{}", self.config.supervisor_addr))?
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(5));
            
        let channel = endpoint.connect().await?;
        Ok(SupervisorServiceClient::new(channel))
    }

    /// Register the service with supervisor
    async fn register_service(&self, client: &SupervisorServiceClient<Channel>) -> Result<String> {
        let mut client = client.clone();
        
        // Use the actual bind address if available, otherwise fall back to localhost
        let (host, port) = if let Some(bind_addr) = self.bind_address {
            (bind_addr.ip().to_string(), bind_addr.port() as i32)
        } else {
            ("127.0.0.1".to_string(), self.config.service_port as i32)
        };

        let service_info = ServiceInfo {
            name: self.config.service_name.clone(),
            version: self.config.service_version.clone(),
            instance_id: self.instance_id.clone(),
            host,
            port,
            metadata: HashMap::from([
                ("start_time".to_string(), chrono::Utc::now().to_rfc3339()),
                ("language".to_string(), "rust".to_string()),
            ]),
        };

        let capabilities = ServiceCapabilities {
            supports_hot_reload: false,
            supports_graceful_shutdown: true,
            dependencies: vec!["supervisor".to_string()],
            required_config: HashMap::new(),
        };

        let request = Request::new(RegisterServiceRequest {
            service: Some(service_info),
            capabilities: Some(capabilities),
        });

        let response = client.register_service(request).await?;
        let response = response.into_inner();

        if !response.success {
            return Err(anyhow::anyhow!("Registration rejected: {}", response.message));
        }

        info!("Registered with supervisor, service ID: {}", response.service_id);
        Ok(response.service_id)
    }

    /// Send heartbeat to supervisor
    #[allow(dead_code)]
    pub async fn send_heartbeat(&self) -> Result<()> {
        if self.config.standalone {
            return Ok(());
        }

        let client_guard = self.client.read().await;
        let service_id_guard = self.service_id.read().await;
        
        if let (Some(client), Some(service_id)) = (client_guard.as_ref(), service_id_guard.as_ref()) {
            let mut client = client.clone();
            
            let metrics = ServiceMetrics {
                memory_usage_bytes: 0, // TODO: Get actual metrics
                cpu_usage_percent: 0.0,
                goroutines: 0,
                custom_metrics: HashMap::new(),
            };

            let request = Request::new(HeartbeatRequest {
                service_id: service_id.clone(),
                health_status: HealthStatus::Healthy as i32,
                metrics: Some(metrics),
                timestamp: Some(Timestamp::from(SystemTime::now())),
            });

            match client.send_heartbeat(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    if response.acknowledged {
                        debug!("Heartbeat acknowledged by supervisor");
                        
                        // Process any commands from supervisor
                        for command in response.commands {
                            self.process_command(command).await;
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to send heartbeat: {}", e);
                    // Try to reconnect on next heartbeat
                }
            }
        }

        Ok(())
    }

    /// Process commands from supervisor
    #[allow(dead_code)]
    async fn process_command(&self, command: ServiceCommand) {
        info!("Received command from supervisor: {:?}", command.r#type);
        
        match command.r#type {
            1 => { // COMMAND_TYPE_RELOAD_CONFIG
                info!("Reloading configuration");
                // TODO: Implement config reload
            }
            2 => { // COMMAND_TYPE_ROTATE_LOGS
                info!("Rotating logs");
                // TODO: Implement log rotation
            }
            3 => { // COMMAND_TYPE_COLLECT_METRICS
                info!("Collecting detailed metrics");
                // TODO: Implement detailed metrics collection
            }
            4 => { // COMMAND_TYPE_CUSTOM
                info!("Processing custom command: {:?}", command.parameters);
                // TODO: Handle custom commands
            }
            _ => {
                warn!("Unknown command type: {}", command.r#type);
            }
        }
    }

    /// Start heartbeat loop
    pub async fn start_heartbeat_loop(&mut self) -> Result<()> {
        if self.config.standalone {
            return Ok(());
        }

        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
        self.shutdown_tx = Some(shutdown_tx);

        let client = self.client.clone();
        let service_id = self.service_id.clone();
        
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(5));
            
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Send heartbeat
                        let client_guard = client.read().await;
                        let service_id_guard = service_id.read().await;
                        
                        if let (Some(client_ref), Some(service_id_ref)) = (client_guard.as_ref(), service_id_guard.as_ref()) {
                            let mut client = client_ref.clone();
                            
                            let metrics = ServiceMetrics {
                                memory_usage_bytes: 0,
                                cpu_usage_percent: 0.0,
                                goroutines: 0,
                                custom_metrics: HashMap::new(),
                            };

                            let request = Request::new(HeartbeatRequest {
                                service_id: service_id_ref.clone(),
                                health_status: HealthStatus::Healthy as i32,
                                metrics: Some(metrics),
                                timestamp: Some(Timestamp::from(SystemTime::now())),
                            });

                            if let Err(e) = client.send_heartbeat(request).await {
                                error!("Failed to send heartbeat: {}", e);
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Heartbeat loop shutting down");
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    /// Unregister from supervisor
    pub async fn unregister(&self) -> Result<()> {
        if self.config.standalone {
            return Ok(());
        }

        // Stop heartbeat loop
        if let Some(shutdown_tx) = &self.shutdown_tx {
            let _ = shutdown_tx.send(()).await;
        }

        let client_guard = self.client.read().await;
        let service_id_guard = self.service_id.read().await;
        
        if let (Some(client), Some(service_id)) = (client_guard.as_ref(), service_id_guard.as_ref()) {
            let mut client = client.clone();
            
            let request = Request::new(UnregisterServiceRequest {
                service_id: service_id.clone(),
                reason: "Graceful shutdown".to_string(),
            });

            match client.unregister_service(request).await {
                Ok(_) => info!("Successfully unregistered from supervisor"),
                Err(e) => error!("Failed to unregister from supervisor: {}", e),
            }
        }

        Ok(())
    }
}

/// Service controller implementation for supervisor commands
pub struct MeshServiceController {
    shutdown_tx: Option<mpsc::Sender<()>>,
    shutdown_complete_rx: Arc<tokio::sync::Mutex<Option<mpsc::Receiver<()>>>>,
}

impl MeshServiceController {
    pub fn new() -> Self {
        Self {
            shutdown_tx: None,
            shutdown_complete_rx: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    pub async fn set_shutdown_channels(&mut self, shutdown_tx: mpsc::Sender<()>, shutdown_complete_rx: mpsc::Receiver<()>) {
        self.shutdown_tx = Some(shutdown_tx);
        *self.shutdown_complete_rx.lock().await = Some(shutdown_complete_rx);
    }
}

#[tonic::async_trait]
impl ServiceControllerService for MeshServiceController {
    async fn start(
        &self,
        request: Request<StartRequest>,
    ) -> Result<tonic::Response<StartResponse>, Status> {
        let _req = request.into_inner();
        info!("Received start request from supervisor");
        
        // The mesh service is already running, so we just acknowledge
        Ok(tonic::Response::new(StartResponse {
            success: true,
            message: "Service is already running".to_string(),
        }))
    }

    async fn stop(
        &self,
        request: Request<StopRequest>,
    ) -> Result<tonic::Response<StopResponse>, Status> {
        let req = request.into_inner();
        info!("Received stop request from supervisor (save_state: {})", req.save_state);
        
        // Get grace period from request (like Golang BaseService)
        let grace_period = if let Some(grace_period) = req.grace_period {
            std::time::Duration::from_secs(grace_period.seconds as u64)
        } else {
            std::time::Duration::from_secs(30)
        };
        
        // Signal shutdown immediately (like Golang BaseService)
        if let Some(shutdown_tx) = &self.shutdown_tx {
            if let Err(e) = shutdown_tx.send(()).await {
                error!("Failed to send shutdown signal: {}", e);
                return Ok(tonic::Response::new(StopResponse {
                    success: false,
                    message: format!("Failed to initiate shutdown: {}", e),
                    saved_state: vec![],
                }));
            }
        }
        
        // Wait for shutdown completion (like Golang BaseService does)
        let mut shutdown_complete_rx_guard = self.shutdown_complete_rx.lock().await;
        if let Some(shutdown_complete_rx) = shutdown_complete_rx_guard.as_mut() {
            // Wait for shutdown completion with timeout
            match tokio::time::timeout(grace_period, shutdown_complete_rx.recv()).await {
                Ok(Some(())) => {
                    info!("Shutdown completed successfully");
                    Ok(tonic::Response::new(StopResponse {
                        success: true,
                        message: "Service stopped successfully".to_string(),
                        saved_state: vec![],
                    }))
                }
                Ok(None) => {
                    warn!("Shutdown complete channel closed unexpectedly");
                    Ok(tonic::Response::new(StopResponse {
                        success: false,
                        message: "Shutdown channel closed unexpectedly".to_string(),
                        saved_state: vec![],
                    }))
                }
                Err(_) => {
                    error!("Shutdown timeout exceeded");
                    Ok(tonic::Response::new(StopResponse {
                        success: false,
                        message: "Shutdown timeout exceeded".to_string(),
                        saved_state: vec![],
                    }))
                }
            }
        } else {
            error!("No shutdown complete channel configured");
            Ok(tonic::Response::new(StopResponse {
                success: false,
                message: "No shutdown coordination configured".to_string(),
                saved_state: vec![],
            }))
        }
    }

    async fn get_health(
        &self,
        request: Request<GetHealthRequest>,
    ) -> Result<tonic::Response<GetHealthResponse>, Status> {
        let _req = request.into_inner();
        debug!("Received get health request from supervisor");
        
        // TODO: Get actual health status
        Ok(tonic::Response::new(GetHealthResponse {
            status: HealthStatus::Healthy as i32,
            checks: vec![], // TODO: Add actual health checks
            last_healthy: Some(Timestamp::from(SystemTime::now())),
        }))
    }

    async fn configure(
        &self,
        request: Request<ConfigureRequest>,
    ) -> Result<tonic::Response<ConfigureResponse>, Status> {
        let _req = request.into_inner();
        info!("Received configure request from supervisor");
        
        // TODO: Implement configuration updates
        // For now, we'll just acknowledge that we received the request
        Ok(tonic::Response::new(ConfigureResponse {
            success: true,
            message: "Configuration update not yet implemented".to_string(),
            restarting: false,
        }))
    }
}

/// Create and return the ServiceController gRPC service
pub async fn create_service_controller_service(
    shutdown_tx: mpsc::Sender<()>,
    shutdown_complete_rx: mpsc::Receiver<()>
) -> ServiceControllerServiceServer<MeshServiceController> {
    let mut controller = MeshServiceController::new();
    controller.set_shutdown_channels(shutdown_tx, shutdown_complete_rx).await;
    ServiceControllerServiceServer::new(controller)
}
