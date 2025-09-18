//! Domain-aware routing tables, ECMP, BR policies, and path computation for mesh.
//!
//! This crate provides routing capabilities for the mesh network, including
//! domain-aware routing tables, Equal-Cost Multi-Path (ECMP) routing,
//! Border Router (BR) policies, and path computation algorithms.

#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod router;
pub mod table;
pub mod ecmp;
pub mod next_hop;

pub use router::*;
pub use table::*;
pub use ecmp::*;
pub use next_hop::*;
