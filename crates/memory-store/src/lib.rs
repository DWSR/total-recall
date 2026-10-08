//! Independent memory-store library boundary.

pub mod contracts;
pub mod database;

mod memory;

pub use memory::MemoryStore;
