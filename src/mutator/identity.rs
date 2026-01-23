use chrono::Utc;
use uuid::Uuid;

use crate::error::{PgStageError, Result};
use crate::mutator::MutationContext;

pub fn uuid4(_ctx: &mut MutationContext) -> Result<String> {
    Ok(Uuid::new_v4().to_string())
}

pub fn uuid5_by_source_value(ctx: &mut MutationContext) -> Result<String> {
    let namespace_str = ctx.get_str_kwarg("namespace").ok_or_else(|| {
        PgStageError::MissingParameter("namespace".to_string(), "uuid5_by_source_value".to_string())
    })?;
    let source_column = ctx.get_str_kwarg("source_column").ok_or_else(|| {
        PgStageError::MissingParameter(
            "source_column".to_string(),
            "uuid5_by_source_value".to_string(),
        )
    })?;

    let namespace = Uuid::parse_str(&namespace_str).map_err(|e| {
        PgStageError::InvalidParameter(format!("Invalid UUID namespace '{}': {}", namespace_str, e))
    })?;

    // Get source value from obfuscated_values
    let source_value = ctx
        .obfuscated_values
        .get(&source_column)
        .cloned()
        .unwrap_or_default();

    let today = Utc::now().format("%Y-%m-%d").to_string();
    let name = format!("{}{}", source_value, today);
    let uuid5 = Uuid::new_v5(&namespace, name.as_bytes());
    Ok(uuid5.to_string())
}
