//! Direct database row-change event adaptation.

use std::collections::HashSet;

use memory_store::contracts::DatabaseTarget;
use serde_json::Value;

use crate::{
    config::Config,
    contracts::{EmbeddingError, EventFailure, MemoryKey, parse_row_changed_event},
};

const MAX_EPOCH_MILLISECONDS: i64 = 253_402_300_799_999;

pub struct EventAdapter {
    database: DatabaseTarget,
    max_event_keys: usize,
}

impl EventAdapter {
    pub fn new(config: &Config) -> Self {
        Self {
            database: config.database.clone(),
            max_event_keys: config.max_event_keys as usize,
        }
    }

    pub fn adapt(&self, payload: Value) -> Result<Vec<MemoryKey>, EmbeddingError> {
        let event = parse_row_changed_event(payload)?;
        if event.db() != self.database.as_str()
            || !is_memory_table(event.table())
            || event.op() != "insert"
        {
            return Err(EmbeddingError::event(EventFailure::Unrelated));
        }
        if event.truncated() == Some(true) {
            return Err(EmbeddingError::event(EventFailure::Truncated));
        }
        // Keep externally supplied timestamps out of diagnostics.
        if !(0..=MAX_EPOCH_MILLISECONDS).contains(&event.at()) {
            return Err(EmbeddingError::event(EventFailure::Malformed));
        }

        let keys = event.returning();
        if keys.is_empty() {
            return Err(EmbeddingError::event(EventFailure::Malformed));
        }
        if keys.len() > self.max_event_keys {
            return Err(EmbeddingError::event(EventFailure::Oversized));
        }

        let mut unique_keys = HashSet::with_capacity(keys.len());
        if keys.iter().any(|key| !unique_keys.insert(key)) {
            return Err(EmbeddingError::event(EventFailure::Duplicate));
        }
        let key_count = u64::try_from(keys.len())
            .map_err(|_| EmbeddingError::event(EventFailure::Malformed))?;
        if event.affected_rows() == 0 || event.affected_rows() != key_count {
            return Err(EmbeddingError::event(EventFailure::Malformed));
        }

        Ok(keys.to_vec())
    }
}

fn is_memory_table(table: &str) -> bool {
    table.eq_ignore_ascii_case("memories") || table.eq_ignore_ascii_case("public.memories")
}
