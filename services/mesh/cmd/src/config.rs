//! Configuration handling for the mesh service.
//!
//! This module handles reading configuration from the shared config file
//! and environment variables, providing a unified configuration interface.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use tracing::{info, warn};

/// Mesh service configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshConfig {
    /// Node ID for this mesh node
    pub node_id: u64,
    /// External port for mesh communication
    pub external_port: u16,
    /// gRPC server port
    pub grpc_port: u16,
    /// Supervisor address
    pub supervisor_addr: String,
    /// TLS configuration
    pub tls: TlsConfig,
    /// Mesh ID
    pub mesh_id: String,
    /// Mesh token for authentication
    pub mesh_token: String,
    /// Timeout for mesh operations (seconds)
    pub timeout: u32,
}

/// TLS configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Whether TLS is enabled
    pub enabled: bool,
    /// Path to certificate file
    pub cert_file: String,
    /// Path to private key file
    pub key_file: String,
    /// Path to CA certificate file
    pub ca_file: String,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cert_file: String::new(),
            key_file: String::new(),
            ca_file: String::new(),
        }
    }
}

impl Default for MeshConfig {
    fn default() -> Self {
        Self {
            node_id: 1001,
            external_port: 10001,
            grpc_port: 50056,
            supervisor_addr: "localhost:50000".to_string(),
            tls: TlsConfig::default(),
            mesh_id: "default-mesh".to_string(),
            mesh_token: String::new(),
            timeout: 30,
        }
    }
}

/// Root configuration structure (matches the YAML structure)
#[derive(Debug, Deserialize)]
struct RootConfig {
    supervisor: Option<SupervisorConfig>,
    services: Option<ServicesConfig>,
}

#[derive(Debug, Deserialize)]
struct SupervisorConfig {
    port: Option<u16>,
}

#[derive(Debug, Deserialize)]
struct ServicesConfig {
    mesh: Option<ServiceConfig>,
}

#[derive(Debug, Deserialize)]
struct ServiceConfig {
    args: Option<Vec<String>>,
    config: Option<HashMap<String, String>>,
}

impl MeshConfig {
    /// Load configuration from file and environment variables
    pub fn load_from_file<P: AsRef<Path>>(config_path: P) -> Result<Self> {
        let mut config = Self::default();
        
        // Try to read the config file
        if let Ok(content) = std::fs::read_to_string(&config_path) {
            if let Ok(root_config) = serde_yaml::from_str::<RootConfig>(&content) {
                config.apply_root_config(root_config)?;
                info!("Loaded configuration from {:?}", config_path.as_ref());
            } else {
                warn!("Failed to parse config file {:?}, using defaults", config_path.as_ref());
            }
        } else {
            warn!("Config file {:?} not found, using defaults", config_path.as_ref());
        }
        
        // Override with environment variables
        config.apply_environment_overrides();
        
        info!("Final mesh configuration: node_id={}, external_port={}, grpc_port={}, supervisor={}", 
              config.node_id, config.external_port, config.grpc_port, config.supervisor_addr);
        
        Ok(config)
    }
    
    /// Apply configuration from the root config structure
    fn apply_root_config(&mut self, root_config: RootConfig) -> Result<()> {
        // Set supervisor address
        if let Some(supervisor) = root_config.supervisor {
            if let Some(port) = supervisor.port {
                self.supervisor_addr = format!("localhost:{}", port);
            }
        }
        
        // Apply mesh service specific configuration
        if let Some(services) = root_config.services {
            if let Some(mesh_service) = services.mesh {
                self.apply_service_config(mesh_service)?;
            }
        }
        
        Ok(())
    }
    
    /// Apply mesh service specific configuration
    fn apply_service_config(&mut self, service_config: ServiceConfig) -> Result<()> {
        // Parse arguments to extract gRPC port and supervisor address
        if let Some(args) = service_config.args {
            for arg in args {
                if arg.starts_with("--port=") {
                    if let Ok(port) = arg.strip_prefix("--port=").unwrap().parse::<u16>() {
                        self.grpc_port = port;
                    }
                } else if arg.starts_with("--supervisor=") {
                    self.supervisor_addr = arg.strip_prefix("--supervisor=").unwrap().to_string();
                }
            }
        }
        
        // Apply configuration values
        if let Some(config_map) = service_config.config {
            for (key, value) in config_map {
                match key.as_str() {
                    "services.mesh.node_id" => {
                        if let Ok(node_id) = value.parse::<u64>() {
                            self.node_id = node_id;
                        }
                    }
                    "services.mesh.external_port" => {
                        if let Ok(port) = value.parse::<u16>() {
                            self.external_port = port;
                        }
                    }
                    "services.mesh.mesh_id" => {
                        self.mesh_id = value;
                    }
                    "services.mesh.mesh_token" => {
                        self.mesh_token = value;
                    }
                    "services.mesh.timeout" => {
                        if let Ok(timeout) = value.parse::<u32>() {
                            self.timeout = timeout;
                        }
                    }
                    "services.mesh.tls.enabled" => {
                        self.tls.enabled = value.to_lowercase() == "true";
                    }
                    "services.mesh.tls.cert_file" => {
                        self.tls.cert_file = value;
                    }
                    "services.mesh.tls.key_file" => {
                        self.tls.key_file = value;
                    }
                    "services.mesh.tls.ca_file" => {
                        self.tls.ca_file = value;
                    }
                    _ => {
                        // Ignore unknown configuration keys
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Apply environment variable overrides
    fn apply_environment_overrides(&mut self) {
        // Check for environment variable overrides
        if let Ok(node_id) = std::env::var("MESH_NODE_ID") {
            if let Ok(id) = node_id.parse::<u64>() {
                self.node_id = id;
                info!("Node ID overridden by environment: {}", id);
            }
        }
        
        if let Ok(external_port) = std::env::var("MESH_EXTERNAL_PORT") {
            if let Ok(port) = external_port.parse::<u16>() {
                self.external_port = port;
                info!("External port overridden by environment: {}", port);
            }
        }
        
        if let Ok(grpc_port) = std::env::var("MESH_GRPC_PORT") {
            if let Ok(port) = grpc_port.parse::<u16>() {
                self.grpc_port = port;
                info!("gRPC port overridden by environment: {}", port);
            }
        }
        
        if let Ok(supervisor_addr) = std::env::var("MESH_SUPERVISOR_ADDR") {
            self.supervisor_addr = supervisor_addr;
            info!("Supervisor address overridden by environment: {}", self.supervisor_addr);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_default_config() {
        let config = MeshConfig::default();
        assert_eq!(config.node_id, 1001);
        assert_eq!(config.external_port, 10001);
        assert_eq!(config.grpc_port, 50056);
        assert_eq!(config.supervisor_addr, "localhost:50000");
    }

    #[test]
    fn test_load_from_file() {
        let yaml_content = r#"
supervisor:
  port: 50000

services:
  mesh:
    enabled: true
    executable: ./redb-mesh
    args:
      - --port=50056
      - --supervisor=localhost:50000
    config:
      services.mesh.node_id: "2001"
      services.mesh.external_port: "20001"
      services.mesh.mesh_id: "test-mesh"
      services.mesh.tls.enabled: "true"
"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(yaml_content.as_bytes()).unwrap();
        
        let config = MeshConfig::load_from_file(temp_file.path()).unwrap();
        
        assert_eq!(config.node_id, 2001);
        assert_eq!(config.external_port, 20001);
        assert_eq!(config.grpc_port, 50056);
        assert_eq!(config.mesh_id, "test-mesh");
        assert_eq!(config.tls.enabled, true);
    }
}
