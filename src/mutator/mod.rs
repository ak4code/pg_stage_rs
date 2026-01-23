pub mod contact;
pub mod datetime;
pub mod identity;
pub mod locale;
pub mod mask;
pub mod names;
pub mod network;
pub mod numeric;
pub mod simple;

use std::collections::HashMap;

use rand::rngs::ThreadRng;

use crate::error::{PgStageError, Result};
use crate::types::Locale;
use crate::unique::UniqueTracker;

pub struct MutationContext<'a> {
    pub kwargs: &'a HashMap<String, serde_json::Value>,
    pub current_value: String,
    pub rng: &'a mut ThreadRng,
    pub unique_tracker: &'a mut UniqueTracker,
    pub locale: Locale,
    pub secrets: &'a HashMap<String, String>,
    pub obfuscated_values: &'a HashMap<String, String>,
}

impl<'a> MutationContext<'a> {
    pub fn get_bool_kwarg(&self, key: &str) -> bool {
        self.kwargs
            .get(key)
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    pub fn get_str_kwarg(&self, key: &str) -> Option<String> {
        self.kwargs.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
    }
}

pub fn dispatch_mutation(name: &str, ctx: &mut MutationContext) -> Result<String> {
    match name {
        // Names
        "first_name" => names::first_name(ctx),
        "last_name" => names::last_name(ctx),
        "full_name" => names::full_name(ctx),
        "middle_name" => names::middle_name(ctx),

        // Contact
        "email" => contact::email(ctx),
        "phone_number" => contact::phone_number(ctx),
        "address" => contact::address(ctx),
        "deterministic_phone_number" => contact::deterministic_phone(ctx),

        // Numeric
        "numeric_smallint" => numeric::smallint(ctx),
        "numeric_integer" => numeric::integer(ctx),
        "numeric_bigint" => numeric::bigint(ctx),
        "numeric_decimal" => numeric::decimal(ctx),
        "numeric_real" => numeric::real(ctx),
        "numeric_double_precision" => numeric::double_precision(ctx),
        "numeric_smallserial" => numeric::smallserial(ctx),
        "numeric_serial" => numeric::serial(ctx),
        "numeric_bigserial" => numeric::bigserial(ctx),

        // DateTime
        "date" => datetime::date(ctx),

        // Network
        "uri" => network::uri(ctx),
        "ipv4" => network::ipv4(ctx),
        "ipv6" => network::ipv6(ctx),

        // Identity
        "uuid4" => identity::uuid4(ctx),
        "uuid5_by_source_value" => identity::uuid5_by_source_value(ctx),

        // Simple
        "null" => simple::null(ctx),
        "empty_string" => simple::empty_string(ctx),
        "fixed_value" => simple::fixed_value(ctx),
        "random_choice" => simple::random_choice(ctx),

        // Mask
        "string_by_mask" => mask::string_by_mask(ctx),

        _ => Err(PgStageError::UnknownMutation(name.to_string())),
    }
}
