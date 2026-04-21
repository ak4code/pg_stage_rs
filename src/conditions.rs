use std::sync::Arc;

use crate::types::{CompiledCondition, CondOp};
use crate::FastMap;

/// Trait giving condition-evaluation access to a row's current (possibly already
/// mutated) values by column index.
pub trait RowRead {
    fn len(&self) -> usize;
    fn value_at(&self, idx: usize) -> &str;
}

/// Check if a compiled condition list matches the current row.
/// Returns true if the list is empty, or if at least one condition matches.
pub fn check_conditions(
    conditions: &[CompiledCondition],
    row: &dyn RowRead,
    column_indices: &FastMap<Arc<str>, usize>,
) -> bool {
    if conditions.is_empty() {
        return true;
    }
    for condition in conditions {
        let col_idx = match column_indices.get(condition.column_name.as_ref()) {
            Some(&idx) => idx,
            None => continue,
        };
        if col_idx >= row.len() {
            continue;
        }
        let col_value = row.value_at(col_idx);
        let matched = match &condition.op {
            CondOp::Equal(v) => col_value == v.as_str(),
            CondOp::NotEqual(v) => col_value != v.as_str(),
            CondOp::ByPattern(re) => re.is_match(col_value),
        };
        if matched {
            return true;
        }
    }
    false
}
