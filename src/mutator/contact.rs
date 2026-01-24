use hmac::{Hmac, Mac};
use rand::Rng;
use sha2::Sha256;

use crate::error::{PgStageError, Result};
use crate::mutator::locale::{en, ru};
use crate::mutator::MutationContext;
use crate::types::Locale;

pub fn email(ctx: &mut MutationContext) -> Result<String> {
    let unique = ctx.get_bool_kwarg("unique");
    let mut gen = || {
        let (first, last, domains) = match ctx.locale {
            Locale::En => (
                en::FIRST_NAMES[ctx.rng.gen_range(0..en::FIRST_NAMES.len())].to_lowercase(),
                en::LAST_NAMES[ctx.rng.gen_range(0..en::LAST_NAMES.len())].to_lowercase(),
                en::EMAIL_DOMAINS,
            ),
            Locale::Ru => (
                ru::FIRST_NAMES_MALE[ctx.rng.gen_range(0..ru::FIRST_NAMES_MALE.len())].to_lowercase(),
                ru::LAST_NAMES_MALE[ctx.rng.gen_range(0..ru::LAST_NAMES_MALE.len())].to_lowercase(),
                ru::EMAIL_DOMAINS,
            ),
        };
        let num: u32 = ctx.rng.gen_range(1..9999);
        let domain = domains[ctx.rng.gen_range(0..domains.len())];
        format!("{}.{}{}@{}", first, last, num, domain)
    };
    if unique {
        ctx.unique_tracker.generate_unique(gen)
    } else {
        Ok(gen())
    }
}

pub fn phone_number(ctx: &mut MutationContext) -> Result<String> {
    let mask: &str = ctx.get_str_kwarg("mask").ok_or_else(|| {
        PgStageError::MissingParameter("mask".to_string(), "phone_number".to_string())
    })?;
    let unique = ctx.get_bool_kwarg("unique");
    let mask_bytes = mask.as_bytes();
    let mut gen = || {
        let mut result = String::with_capacity(mask_bytes.len());
        for &b in mask_bytes {
            if b == b'X' || b == b'#' {
                result.push(char::from(b'0' + ctx.rng.gen_range(0..10u8)));
            } else {
                result.push(b as char);
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

pub fn address(ctx: &mut MutationContext) -> Result<String> {
    let unique = ctx.get_bool_kwarg("unique");
    let mut gen = || match ctx.locale {
        Locale::En => {
            let num = ctx.rng.gen_range(1..1400);
            let street = en::STREET_NAMES[ctx.rng.gen_range(0..en::STREET_NAMES.len())];
            let suffix = en::STREET_SUFFIXES[ctx.rng.gen_range(0..en::STREET_SUFFIXES.len())];
            let city = en::CITIES[ctx.rng.gen_range(0..en::CITIES.len())];
            let state = en::STATES[ctx.rng.gen_range(0..en::STATES.len())];
            format!("{} {} {}, {}, {}", num, street, suffix, city, state)
        }
        Locale::Ru => {
            let city = ru::CITIES[ctx.rng.gen_range(0..ru::CITIES.len())];
            let street_type = ru::STREET_TYPES[ctx.rng.gen_range(0..ru::STREET_TYPES.len())];
            let street = ru::STREETS[ctx.rng.gen_range(0..ru::STREETS.len())];
            let num = ctx.rng.gen_range(1..200);
            format!("{}, {} {}, {}", city, street_type, street, num)
        }
    };
    if unique {
        ctx.unique_tracker.generate_unique(gen)
    } else {
        Ok(gen())
    }
}

pub fn deterministic_phone(ctx: &mut MutationContext) -> Result<String> {
    let current_value = ctx.current_value.clone();
    let count = ctx
        .kwargs
        .get("obfuscated_numbers_count")
        .and_then(|v| v.as_u64())
        .unwrap_or(4) as usize;

    let secret_key = ctx
        .secrets
        .get("SECRET_KEY")
        .cloned()
        .unwrap_or_default();
    let nonce = ctx
        .secrets
        .get("SECRET_KEY_NONCE")
        .cloned()
        .unwrap_or_default();

    if secret_key.is_empty() {
        return Err(PgStageError::MutationError(
            "SECRET_KEY environment variable not set".to_string(),
        ));
    }

    let digits_only: String = current_value.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits_only.len() < count {
        return Err(PgStageError::MutationError(
            "Not enough digits to obfuscate".to_string(),
        ));
    }

    let message = format!("{}{}", current_value, nonce);
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(secret_key.as_bytes())
        .map_err(|e| PgStageError::MutationError(e.to_string()))?;
    mac.update(message.as_bytes());
    let result = mac.finalize();
    let hash_bytes = result.into_bytes();

    // Extract digits from hash into a Vec for O(1) indexing
    let new_digits: Vec<u8> = hash_bytes.iter()
        .take(count)
        .map(|byte| b'0' + (byte % 10))
        .collect();

    // Replace last `count` digits in original value
    let mut result_chars: Vec<char> = current_value.chars().collect();
    let mut replaced = 0;
    for i in (0..result_chars.len()).rev() {
        if result_chars[i].is_ascii_digit() && replaced < count {
            let digit_idx = count - 1 - replaced;
            if digit_idx < new_digits.len() {
                result_chars[i] = new_digits[digit_idx] as char;
            }
            replaced += 1;
        }
    }

    Ok(result_chars.into_iter().collect())
}
