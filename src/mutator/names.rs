use rand::Rng;

use crate::error::{PgStageError, Result};
use crate::mutator::locale::{en, ru};
use crate::mutator::MutationContext;
use crate::types::Locale;

pub fn first_name(ctx: &mut MutationContext) -> Result<String> {
    let unique = ctx.get_bool_kwarg("unique");
    let mut gen = || {
        match ctx.locale {
            Locale::En => en::FIRST_NAMES[ctx.rng.gen_range(0..en::FIRST_NAMES.len())].to_string(),
            Locale::Ru => {
                if ctx.rng.gen_bool(0.5) {
                    ru::FIRST_NAMES_MALE[ctx.rng.gen_range(0..ru::FIRST_NAMES_MALE.len())].to_string()
                } else {
                    ru::FIRST_NAMES_FEMALE[ctx.rng.gen_range(0..ru::FIRST_NAMES_FEMALE.len())].to_string()
                }
            }
        }
    };
    if unique {
        ctx.unique_tracker.generate_unique(gen)
    } else {
        Ok(gen())
    }
}

pub fn last_name(ctx: &mut MutationContext) -> Result<String> {
    let unique = ctx.get_bool_kwarg("unique");
    let mut gen = || {
        match ctx.locale {
            Locale::En => en::LAST_NAMES[ctx.rng.gen_range(0..en::LAST_NAMES.len())].to_string(),
            Locale::Ru => {
                if ctx.rng.gen_bool(0.5) {
                    ru::LAST_NAMES_MALE[ctx.rng.gen_range(0..ru::LAST_NAMES_MALE.len())].to_string()
                } else {
                    ru::LAST_NAMES_FEMALE[ctx.rng.gen_range(0..ru::LAST_NAMES_FEMALE.len())].to_string()
                }
            }
        }
    };
    if unique {
        ctx.unique_tracker.generate_unique(gen)
    } else {
        Ok(gen())
    }
}

pub fn full_name(ctx: &mut MutationContext) -> Result<String> {
    let unique = ctx.get_bool_kwarg("unique");
    let mut gen = || {
        match ctx.locale {
            Locale::En => {
                let first = en::FIRST_NAMES[ctx.rng.gen_range(0..en::FIRST_NAMES.len())];
                let last = en::LAST_NAMES[ctx.rng.gen_range(0..en::LAST_NAMES.len())];
                format!("{} {}", last, first)
            }
            Locale::Ru => {
                let (first, last) = if ctx.rng.gen_bool(0.5) {
                    (
                        ru::FIRST_NAMES_MALE[ctx.rng.gen_range(0..ru::FIRST_NAMES_MALE.len())],
                        ru::LAST_NAMES_MALE[ctx.rng.gen_range(0..ru::LAST_NAMES_MALE.len())],
                    )
                } else {
                    (
                        ru::FIRST_NAMES_FEMALE[ctx.rng.gen_range(0..ru::FIRST_NAMES_FEMALE.len())],
                        ru::LAST_NAMES_FEMALE[ctx.rng.gen_range(0..ru::LAST_NAMES_FEMALE.len())],
                    )
                };
                let patronymic = crate::mutator::locale::get_patronymic(&mut *ctx.rng);
                format!("{} {} {}", last, first, patronymic)
            }
        }
    };
    if unique {
        ctx.unique_tracker.generate_unique(gen)
    } else {
        Ok(gen())
    }
}

pub fn middle_name(ctx: &mut MutationContext) -> Result<String> {
    if ctx.locale != Locale::Ru {
        return Err(PgStageError::MutationError(
            "middle_name mutation is only available for Russian locale".to_string(),
        ));
    }
    let unique = ctx.get_bool_kwarg("unique");
    let mut gen = || crate::mutator::locale::get_patronymic(&mut *ctx.rng);
    if unique {
        ctx.unique_tracker.generate_unique(gen)
    } else {
        Ok(gen())
    }
}
