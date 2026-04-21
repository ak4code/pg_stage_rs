use crate::error::{PgStageError, Result};
use crate::FastSet;

const MAX_RETRIES: u32 = 1000;

#[derive(Debug, Default)]
pub struct UniqueTracker {
    values: FastSet<Box<str>>,
}

impl UniqueTracker {
    pub fn new() -> Self {
        Self {
            values: FastSet::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Try to insert. Returns true if the value was new.
    /// Avoids allocating a `String` when the value is already present.
    pub fn try_insert(&mut self, value: &str) -> bool {
        if self.values.contains(value) {
            return false;
        }
        self.values.insert(Box::from(value));
        true
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
