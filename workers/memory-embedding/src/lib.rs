pub mod config;
pub mod contracts;
mod event;
pub mod renderer;
mod repository;
mod router;
pub mod runtime;
mod service;
mod writer;

pub use event::EventAdapter;
pub use repository::IiiEmbeddingWorkRepository;
pub use router::IiiEmbeddingRouter;
pub use service::{EmbeddingCoordinator, QueueEventProcessor, ReconciliationProcessor};
pub use writer::IiiEmbeddingWriter;

#[doc(hidden)]
pub use repository::DatabaseExecutor;
#[doc(hidden)]
pub use router::RouterExecutor;
