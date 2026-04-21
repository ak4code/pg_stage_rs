use std::sync::Arc;

use crate::FastMap;

/// Tracks FK relationships to ensure consistent obfuscation across tables.
///
/// Layout: `by_table[table][column][fk_value] = obfuscated_value`.
///
/// `table` and `column` keys are `Arc<str>`, shared with `CompiledRelation` and
/// `MutationRegistry`, so a name that appears in thousands of schemas is stored
/// in memory exactly once per unique string.
///
/// `fk_value` and the stored obfuscated value are `Box<str>` (no String
/// capacity overhead).
#[derive(Debug, Default)]
pub struct RelationTracker {
    by_table: FastMap<Arc<str>, FastMap<Arc<str>, FastMap<Box<str>, Box<str>>>>,
    count: usize,
}

impl RelationTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn lookup(&self, table: &Arc<str>, column: &Arc<str>, fk_value: &str) -> Option<&str> {
        self.by_table
            .get(table.as_ref())?
            .get(column.as_ref())?
            .get(fk_value)
            .map(|v| v.as_ref())
    }

    pub fn store(
        &mut self,
        table: &Arc<str>,
        column: &Arc<str>,
        fk_value: &str,
        obfuscated: &str,
    ) {
        let outer = self.by_table.entry(Arc::clone(table)).or_default();
        let mid = outer.entry(Arc::clone(column)).or_default();
        let inserted = mid.insert(Box::from(fk_value), Box::from(obfuscated));
        if inserted.is_none() {
            self.count += 1;
        }
    }
}
