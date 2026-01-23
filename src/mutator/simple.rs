use rand::Rng;

use crate::error::{PgStageError, Result};
use crate::mutator::MutationContext;

pub fn null(_ctx: &mut MutationContext) -> Result<String> {
    Ok("\\N".to_string())
}

pub fn empty_string(_ctx: &mut MutationContext) -> Result<String> {
    Ok(String::new())
}

pub fn fixed_value(ctx: &mut MutationContext) -> Result<String> {
    let value = ctx.kwargs.get("value").ok_or_else(|| {
        PgStageError::MissingParameter("value".to_string(), "fixed_value".to_string())
    })?;
    match value {
        serde_json::Value::String(s) => Ok(s.clone()),
        serde_json::Value::Null => Ok("\\N".to_string()),
        other => Ok(other.to_string()),
    }
}

pub fn random_choice(ctx: &mut MutationContext) -> Result<String> {
    let choices = ctx
        .kwargs
        .get("choices")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            PgStageError::MissingParameter("choices".to_string(), "random_choice".to_string())
        })?
        .clone();

    if choices.is_empty() {
        return Err(PgStageError::InvalidParameter(
            "choices list is empty".to_string(),
        ));
    }

    let idx = ctx.rng.gen_range(0..choices.len());
    match &choices[idx] {
        serde_json::Value::String(s) => Ok(s.clone()),
        serde_json::Value::Null => Ok("\\N".to_string()),
        other => Ok(other.to_string()),
    }
}
