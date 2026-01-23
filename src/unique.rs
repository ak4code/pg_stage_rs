use std::collections::HashSet;

use crate::error::{PgStageError, Result};

const MAX_RETRIES: u32 = 1000;

#[derive(Debug, Default)]
pub struct UniqueTracker {
    values: HashSet<String>,
}

impl UniqueTracker {
    pub fn new() -> Self {
        Self {
            values: HashSet::new(),
        }
    }

    /// Try to insert a value. Returns Ok(true) if inserted (unique),
    /// Ok(false) if already exists.
    pub fn try_insert(&mut self, value: &str) -> bool {
        self.values.insert(value.to_string())
    }

    /// Generate a unique value using the provided generator function.
    /// Retries up to MAX_RETRIES times.
    pub fn generate_unique<F>(&mut self, mut gen: F) -> Result<String>
    where
        F: FnMut() -> String,
    {
        for _ in 0..MAX_RETRIES {
            let value = gen();
            if self.try_insert(&value) {
                return Ok(value);
            }
        }
        Err(PgStageError::UniqueExhausted(MAX_RETRIES))
    }

    pub fn clear(&mut self) {
        self.values.clear();
    }
}
