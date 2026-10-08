//! High-level knowledge-graph store facade and public contracts.

pub mod contracts;
mod database;
mod facade;
mod graph;
pub mod normalization;

pub use database::GraphConfigurationError;
pub use facade::IiiKnowledgeGraphStore;

#[cfg_attr(not(test), allow(unused_imports))]
pub(crate) use graph::KnowledgeGraphStore;
