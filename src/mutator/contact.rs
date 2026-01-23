use hmac::{Hmac, Mac};
use rand::Rng;
use sha2::Sha256;

use crate::error::{PgStageError, Result};
use crate::mutator::locale;
use crate::mutator::MutationContext;

pub fn email(ctx: &mut MutationContext) -> Result<String> {
    let unique = ctx.get_bool_kwarg("unique");
    let mut gen = || {
        let first = locale::get_first_name(ctx.locale, &mut ctx.rng).to_lowercase();
        let last = locale::get_last_name(ctx.locale, &mut ctx.rng).to_lowercase();
        let num: u32 = ctx.rng.gen_range(1..9999);
        let domain = locale::get_email_domain(ctx.locale, &mut ctx.rng);
        format!("{}.{}{}@{}", first, last, num, domain)
    };
    if unique {
        ctx.unique_tracker.generate_unique(gen)
    } else {
        Ok(gen())
    }
}

pub fn phone_number(ctx: &mut MutationContext) -> Result<String> {
    let mask = ctx.get_str_kwarg("mask").ok_or_else(|| {
        PgStageError::MissingParameter("mask".to_string(), "phone_number".to_string())
    })?;
    let unique = ctx.get_bool_kwarg("unique");
    let mut gen = || {
        let mut result = String::with_capacity(mask.len());
        for ch in mask.chars() {
            if ch == 'X' || ch == '#' {
                result.push(char::from(b'0' + ctx.rng.gen_range(0..10u8)));
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

pub fn address(ctx: &mut MutationContext) -> Result<String> {
    let unique = ctx.get_bool_kwarg("unique");
    if unique {
        ctx.unique_tracker.generate_unique(|| {
            locale::get_address(ctx.locale, &mut ctx.rng)
        })
    } else {
        Ok(locale::get_address(ctx.locale, &mut ctx.rng))
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

    // Extract digits from hash
    let mut new_digits = String::with_capacity(count);
    for byte in hash_bytes.iter() {
        if new_digits.len() >= count {
            break;
        }
        new_digits.push(char::from(b'0' + (byte % 10)));
    }

    // Replace last `count` digits in original value
    let mut result_chars: Vec<char> = current_value.chars().collect();
    let mut replaced = 0;
    for i in (0..result_chars.len()).rev() {
        if result_chars[i].is_ascii_digit() && replaced < count {
            let digit_idx = count - 1 - replaced;
            if digit_idx < new_digits.len() {
                result_chars[i] = new_digits.chars().nth(digit_idx).unwrap();
            }
            replaced += 1;
        }
    }

    Ok(result_chars.into_iter().collect())
}
