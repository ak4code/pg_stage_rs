use regex::Regex;

use crate::types::Condition;

/// Check if conditions are met for a given row.
/// Returns true if at least one condition matches.
/// Returns true if conditions list is empty.
pub fn check_conditions(
    conditions: &[Condition],
    values: &[&str],
    column_indices: &std::collections::HashMap<String, usize>,
) -> bool {
    if conditions.is_empty() {
        return true;
    }

    for condition in conditions {
        let col_idx = match column_indices.get(&condition.column_name) {
            Some(idx) => *idx,
            None => continue,
        };
        if col_idx >= values.len() {
            continue;
        }
        let col_value = values[col_idx];

        let matched = match condition.operation.as_str() {
            "equal" => col_value == condition.value,
            "not_equal" => col_value != condition.value,
            "by_pattern" => {
                if let Ok(re) = Regex::new(&condition.value) {
                    re.is_match(col_value)
                } else {
                    false
                }
            }
            _ => false,
        };

        if matched {
            return true;
        }
    }

    false
}
