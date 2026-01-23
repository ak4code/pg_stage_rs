use crate::error::{PgStageError, Result};
use crate::mutator::MutationContext;

pub fn first_name(ctx: &mut MutationContext) -> Result<String> {
    let unique = ctx.get_bool_kwarg("unique");
    if unique {
        ctx.unique_tracker.generate_unique(|| {
            ctx.provider.person.first_name(None).to_string()
        })
    } else {
        Ok(ctx.provider.person.first_name(None).to_string())
    }
}

pub fn last_name(ctx: &mut MutationContext) -> Result<String> {
    let unique = ctx.get_bool_kwarg("unique");
    if unique {
        ctx.unique_tracker.generate_unique(|| {
            ctx.provider.person.last_name(None)
        })
    } else {
        Ok(ctx.provider.person.last_name(None))
    }
}

pub fn full_name(ctx: &mut MutationContext) -> Result<String> {
    let unique = ctx.get_bool_kwarg("unique");
    let mut gen = || {
        let last = ctx.provider.person.last_name(None);
        let first = ctx.provider.person.first_name(None).to_string();
        match ctx.locale {
            crate::types::Locale::Ru => {
                let patronymic = crate::mutator::locale::get_patronymic(&mut ctx.rng);
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
            crate::mutator::locale::get_patronymic(&mut ctx.rng)
        })
    } else {
        Ok(crate::mutator::locale::get_patronymic(&mut ctx.rng))
    }
}
