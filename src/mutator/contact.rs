use hmac::{Hmac, Mac};
use rand::Rng;
use sha2::Sha256;

use crate::error::{PgStageError, Result};
use crate::mutator::MutationContext;

pub fn email(ctx: &mut MutationContext) -> Result<String> {
    let unique = ctx.get_bool_kwarg("unique");
    let mut gen = || {
        let first = ctx.provider.person.first_name(None).to_string().to_lowercase();
        let last = ctx.provider.person.last_name(None).to_lowercase();
        let num: u32 = ctx.rng.gen_range(1..9999);
        let email = warlocks_cauldron::Person::email(None, false);
        let domain = email.split('@').nth(1).unwrap_or("example.com").to_string();
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
    if unique {
        ctx.unique_tracker.generate_unique(|| {
            ctx.provider.address.full_address()
        })
    } else {
        Ok(ctx.provider.address.full_address())
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
