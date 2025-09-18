//! Tonic gRPC services for local Go clients and admin interface for mesh.
//!
//! This crate provides gRPC services using tonic for interfacing with local
//! Go clients and providing administrative interfaces for the mesh network.

#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod data;
pub mod control;
pub mod server;
pub mod delivery;
pub mod message_tracker;
pub mod message_queue;
pub mod metrics;

pub use data::*;
pub use control::*;
pub use server::*;
pub use delivery::*;
pub use message_tracker::*;
pub use message_queue::*;
pub use metrics::*;

/// Generated protobuf code and gRPC service definitions
pub mod proto {
    /// Mesh protocol definitions
    pub mod mesh {
        /// Version 1 of the mesh protocol
        #[allow(missing_docs)]
        pub mod v1 {
            tonic::include_proto!("redbco.redbopen.mesh.v1");
        }
    }
    
    /// Supervisor protocol definitions
    pub mod supervisor {
        /// Version 1 of the supervisor protocol
        #[allow(missing_docs)]
        pub mod v1 {
            tonic::include_proto!("redbco.redbopen.supervisor.v1");
        }
    }
    
    /// Common protocol definitions
    pub mod common {
        /// Version 1 of the common protocol
        #[allow(missing_docs)]
        pub mod v1 {
            tonic::include_proto!("redbco.redbopen.common.v1");
        }
    }
    
    /// File descriptor set for reflection
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("mesh_descriptor");
}

pub use proto::mesh::v1::*;
