//! Storage backend implementations

pub mod file;
pub mod mem;

#[cfg(feature = "redis-backend")]
pub mod redis;
