use rand::Rng;

use crate::error::{PgStageError, Result};
use crate::mutator::MutationContext;

pub fn string_by_mask(ctx: &mut MutationContext) -> Result<String> {
    let mask = ctx.get_str_kwarg("mask").ok_or_else(|| {
        PgStageError::MissingParameter("mask".to_string(), "string_by_mask".to_string())
    })?;
    let char_placeholder = ctx
        .get_str_kwarg("char")
        .and_then(|s| s.chars().next())
        .unwrap_or('@');
    let digit_placeholder = ctx
        .get_str_kwarg("digit")
        .and_then(|s| s.chars().next())
        .unwrap_or('#');
    let unique = ctx.get_bool_kwarg("unique");

    let mut gen = || {
        let mut result = String::with_capacity(mask.len());
        for ch in mask.chars() {
            if ch == char_placeholder {
                let c = b'A' + ctx.rng.gen_range(0..26u8);
                result.push(c as char);
            } else if ch == digit_placeholder {
                let d = b'0' + ctx.rng.gen_range(0..10u8);
                result.push(d as char);
            } else {
                result.push(ch);
            }
        }
        result
    };

    if unique {
        ctx.unique_tracker.generate_unique(gen)
    } else {
        Ok(gen())
    }
}
