use serde_json::{Map, Value};

use crate::error::{PgStageError, Result};
use crate::mutator::{resolve_mutation, MutationContext};
use crate::FastMap;

/// Partially mutates a JSON object value. `mutation_kwargs` maps JSON keys to
/// nested mutation specs: `{"mutation_name": "...", "mutation_kwargs": {...}}`.
/// The special `mutation_name: "delete"` clears the key's value (sets it to
/// an empty string) — it does NOT remove the key.
///
/// Missing keys: the mutation is skipped entirely (the key is NOT added).
/// The nested mutation receives the existing JSON value (stringified) as its
/// `current_value`; its output is inserted as a JSON string (or `null` if the
/// mutation returns the SQL null sentinel `\N`).
pub fn json_update(ctx: &mut MutationContext) -> Result<String> {
    let mut root: Value = if ctx.current_value == "\\N" || ctx.current_value.is_empty() {
        Value::Object(Map::new())
    } else {
        serde_json::from_str(ctx.current_value).map_err(|e| {
            PgStageError::MutationError(format!("json_update: failed to parse value as JSON: {}", e))
        })?
    };

    let obj = root.as_object_mut().ok_or_else(|| {
        PgStageError::MutationError("json_update: top-level value is not a JSON object".to_string())
    })?;

    // Rebind so the iterator below borrows the map directly, leaving `ctx`
    // free for split borrows of `rng` / `unique_tracker` inside the loop.
    let kwargs = ctx.kwargs;

    for (key, spec_val) in kwargs.iter() {
        let spec_obj = spec_val.as_object().ok_or_else(|| {
            PgStageError::InvalidParameter(format!(
                "json_update: expected object spec for key '{}'",
                key
            ))
        })?;

        let mutation_name = spec_obj
            .get("mutation_name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                PgStageError::InvalidParameter(format!(
                    "json_update: missing 'mutation_name' for key '{}'",
                    key
                ))
            })?;

        // Skip the mutation entirely if the key is not present in the JSON.
        if !obj.contains_key(key) {
            continue;
        }

        if mutation_name == "delete" {
            obj.insert(key.clone(), Value::String(String::new()));
            continue;
        }

        let mutation_fn = resolve_mutation(mutation_name)
            .ok_or_else(|| PgStageError::UnknownMutation(mutation_name.to_string()))?;

        let mut inner_kwargs: FastMap<String, Value> = FastMap::new();
        if let Some(kw) = spec_obj.get("mutation_kwargs").and_then(|v| v.as_object()) {
            for (k, v) in kw.iter() {
                inner_kwargs.insert(k.clone(), v.clone());
            }
        }

        let cur_value_str = match obj.get(key) {
            Some(Value::String(s)) => s.clone(),
            Some(v) => v.to_string(),
            None => String::new(),
        };

        let new_value = {
            let mut inner_ctx = MutationContext {
                kwargs: &inner_kwargs,
                current_value: &cur_value_str,
                rng: &mut *ctx.rng,
                unique_tracker: &mut *ctx.unique_tracker,
                locale: ctx.locale,
                secrets: ctx.secrets,
                obfuscated_values: ctx.obfuscated_values,
            };
            mutation_fn(&mut inner_ctx)?
        };

        let json_val = if new_value == "\\N" {
            Value::Null
        } else {
            Value::String(new_value)
        };

        obj.insert(key.clone(), json_val);
    }

    serde_json::to_string(&root).map_err(|e| {
        PgStageError::MutationError(format!("json_update: failed to serialize: {}", e))
    })
}
