use crate::error::{PgStageError, Result};
use crate::mutator::locale;
use crate::mutator::MutationContext;

pub fn first_name(ctx: &mut MutationContext) -> Result<String> {
    let unique = ctx.get_bool_kwarg("unique");
    if unique {
        ctx.unique_tracker.generate_unique(|| {
            locale::get_first_name(ctx.locale, &mut ctx.rng)
        })
    } else {
        Ok(locale::get_first_name(ctx.locale, &mut ctx.rng))
    }
}

pub fn last_name(ctx: &mut MutationContext) -> Result<String> {
    let unique = ctx.get_bool_kwarg("unique");
    if unique {
        ctx.unique_tracker.generate_unique(|| {
            locale::get_last_name(ctx.locale, &mut ctx.rng)
        })
    } else {
        Ok(locale::get_last_name(ctx.locale, &mut ctx.rng))
    }
}

pub fn full_name(ctx: &mut MutationContext) -> Result<String> {
    let unique = ctx.get_bool_kwarg("unique");
    let mut gen = || {
        let last = locale::get_last_name(ctx.locale, &mut ctx.rng);
        let first = locale::get_first_name(ctx.locale, &mut ctx.rng);
        match ctx.locale {
            crate::types::Locale::Ru => {
                let patronymic = locale::get_patronymic(&mut ctx.rng);
                format!("{} {} {}", last, first, patronymic)
            }
            _ => format!("{} {}", last, first),
        }
    };
    if unique {
        ctx.unique_tracker.generate_unique(gen)
    } else {
        Ok(gen())
    }
}

pub fn middle_name(ctx: &mut MutationContext) -> Result<String> {
    if ctx.locale != crate::types::Locale::Ru {
        return Err(PgStageError::MutationError(
            "middle_name mutation is only available for Russian locale".to_string(),
        ));
    }
    let unique = ctx.get_bool_kwarg("unique");
    if unique {
        ctx.unique_tracker.generate_unique(|| {
            locale::get_patronymic(&mut ctx.rng)
        })
    } else {
        Ok(locale::get_patronymic(&mut ctx.rng))
    }
}
