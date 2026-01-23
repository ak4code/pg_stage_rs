use rand::Rng;

use crate::error::Result;
use crate::mutator::MutationContext;

fn get_range_i64(ctx: &MutationContext, min: i64, max: i64) -> (i64, i64) {
    let start = ctx
        .kwargs
        .get("start")
        .and_then(|v| v.as_i64())
        .unwrap_or(min);
    let end = ctx
        .kwargs
        .get("end")
        .and_then(|v| v.as_i64())
        .unwrap_or(max);
    (start.max(min), end.min(max))
}

fn gen_int(ctx: &mut MutationContext, min: i64, max: i64) -> Result<String> {
    let (start, end) = get_range_i64(ctx, min, max);
    let unique = ctx.get_bool_kwarg("unique");
    let mut gen = || ctx.rng.gen_range(start..=end).to_string();
    if unique {
        ctx.unique_tracker.generate_unique(gen)
    } else {
        Ok(gen())
    }
}

pub fn smallint(ctx: &mut MutationContext) -> Result<String> {
    gen_int(ctx, -32768, 32767)
}

pub fn integer(ctx: &mut MutationContext) -> Result<String> {
    gen_int(ctx, -2147483648, 2147483647)
}

pub fn bigint(ctx: &mut MutationContext) -> Result<String> {
    gen_int(ctx, -9223372036854775808, 9223372036854775807)
}

pub fn smallserial(ctx: &mut MutationContext) -> Result<String> {
    gen_int(ctx, 1, 32767)
}

pub fn serial(ctx: &mut MutationContext) -> Result<String> {
    gen_int(ctx, 1, 2147483647)
}

pub fn bigserial(ctx: &mut MutationContext) -> Result<String> {
    gen_int(ctx, 1, 9223372036854775807)
}

pub fn decimal(ctx: &mut MutationContext) -> Result<String> {
    let start = ctx
        .kwargs
        .get("start")
        .and_then(|v| v.as_f64())
        .unwrap_or(-999999.0);
    let end = ctx
        .kwargs
        .get("end")
        .and_then(|v| v.as_f64())
        .unwrap_or(999999.0);
    let precision = ctx
        .kwargs
        .get("precision")
        .and_then(|v| v.as_u64())
        .unwrap_or(2) as usize;
    let unique = ctx.get_bool_kwarg("unique");
    let mut gen = || {
        let val: f64 = ctx.rng.gen_range(start..end);
        format!("{:.prec$}", val, prec = precision)
    };
    if unique {
        ctx.unique_tracker.generate_unique(gen)
    } else {
        Ok(gen())
    }
}

pub fn real(ctx: &mut MutationContext) -> Result<String> {
    let start = ctx
        .kwargs
        .get("start")
        .and_then(|v| v.as_f64())
        .unwrap_or(-999999.0);
    let end = ctx
        .kwargs
        .get("end")
        .and_then(|v| v.as_f64())
        .unwrap_or(999999.0);
    let unique = ctx.get_bool_kwarg("unique");
    let mut gen = || {
        let val: f64 = ctx.rng.gen_range(start..end);
        format!("{:.6}", val)
    };
    if unique {
        ctx.unique_tracker.generate_unique(gen)
    } else {
        Ok(gen())
    }
}

pub fn double_precision(ctx: &mut MutationContext) -> Result<String> {
    let start = ctx
        .kwargs
        .get("start")
        .and_then(|v| v.as_f64())
        .unwrap_or(-999999999.0);
    let end = ctx
        .kwargs
        .get("end")
        .and_then(|v| v.as_f64())
        .unwrap_or(999999999.0);
    let unique = ctx.get_bool_kwarg("unique");
    let mut gen = || {
        let val: f64 = ctx.rng.gen_range(start..end);
        format!("{:.15}", val)
    };
    if unique {
        ctx.unique_tracker.generate_unique(gen)
    } else {
        Ok(gen())
    }
}
