use hmac::{Hmac, Mac};
use rand::Rng;
use sha2::Sha256;

use crate::error::{PgStageError, Result};
use crate::mutator::locale::{en, ru};
use crate::mutator::MutationContext;
use crate::types::Locale;

pub fn email(ctx: &mut MutationContext) -> Result<String> {
    let unique = ctx.get_bool_kwarg("unique");
    let domains: &[&str] = match ctx.locale {
        Locale::Ru => ru::EMAIL_DOMAINS,
        _ => en::EMAIL_DOMAINS,
    };
    let mut gen = || {
        let first = en::FIRST_NAMES[ctx.rng.gen_range(0..en::FIRST_NAMES.len())].to_lowercase();
        let last = en::LAST_NAMES[ctx.rng.gen_range(0..en::LAST_NAMES.len())].to_lowercase();
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
        .ok_or_else(|| {
            PgStageError::MissingParameter(
                "obfuscated_numbers_count".to_string(),
                "deterministic_phone_number".to_string(),
            )
        })? as usize;

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
    if nonce.is_empty() {
        return Err(PgStageError::MutationError(
            "SECRET_KEY_NONCE environment variable not set".to_string(),
        ));
    }

    // Find digit positions in the original string
    let chars: Vec<char> = current_value.chars().collect();
    let digit_positions: Vec<usize> = chars
        .iter()
        .enumerate()
        .filter(|(_, c)| c.is_ascii_digit())
        .map(|(i, _)| i)
        .collect();

    if digit_positions.len() < count {
        return Err(PgStageError::MutationError(
            "Not enough digits to obfuscate".to_string(),
        ));
    }

    // Compute seed: HMAC(key=nonce+secret_key, msg="digits_permutation")
    type HmacSha256 = Hmac<Sha256>;
    let hmac_key = format!("{}{}", nonce, secret_key);
    let mut mac = HmacSha256::new_from_slice(hmac_key.as_bytes())
        .map_err(|e| PgStageError::MutationError(e.to_string()))?;
    mac.update(b"digits_permutation");
    let hash_bytes = mac.finalize().into_bytes();

    // Use hash as seed for deterministic RNG
    use rand::seq::SliceRandom;
    use rand::SeedableRng;
    let mut seed_bytes = [0u8; 32];
    seed_bytes.copy_from_slice(&hash_bytes[..32]);
    let mut rng = rand::rngs::StdRng::from_seed(seed_bytes);

    // Collect last N digits and shuffle them deterministically
    let start_idx = digit_positions.len() - count;
    let positions_to_shuffle = &digit_positions[start_idx..];
    let mut digits_to_shuffle: Vec<char> = positions_to_shuffle
        .iter()
        .map(|&pos| chars[pos])
        .collect();
    digits_to_shuffle.shuffle(&mut rng);

    // Put shuffled digits back, preserving formatting
    let mut result_chars = chars;
    for (i, &pos) in positions_to_shuffle.iter().enumerate() {
        result_chars[pos] = digits_to_shuffle[i];
    }

    Ok(result_chars.into_iter().collect())
}
