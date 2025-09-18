//! Domain graph, epochs, gossip/link-state intra-domain, path-vector inter-domain for mesh.
//!
//! This crate provides topology management for the mesh network, including
//! domain graph representation, epoch-based updates, gossip and link-state
//! protocols for intra-domain routing, and path-vector protocols for inter-domain routing.

#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod link_state;

pub use link_state::*;