pub mod contact;
pub mod datetime;
pub mod identity;
pub mod json_update;
pub mod locale;
pub mod mask;
pub mod names;
pub mod network;
pub mod numeric;
pub mod simple;

use rand::rngs::ThreadRng;

use crate::error::Result;
use crate::types::Locale;
use crate::unique::UniqueTracker;
use crate::FastMap;

/// Monomorphic function pointer type used by the dispatch table.
pub type MutationFn = fn(&mut MutationContext) -> Result<String>;

/// Read-only accessor for already-obfuscated values in the current row.
/// Used by mutations like `uuid5_by_source_value` that derive their output
/// from another column's (already obfuscated) value.
pub trait ObfuscatedLookup {
    fn get(&self, column: &str) -> Option<&str>;
}

pub struct MutationContext<'a> {
    pub kwargs: &'a FastMap<String, serde_json::Value>,
    pub current_value: &'a str,
    pub rng: &'a mut ThreadRng,
    pub unique_tracker: &'a mut UniqueTracker,
    pub locale: Locale,
    pub secrets: &'a FastMap<String, String>,
    pub obfuscated_values: &'a dyn ObfuscatedLookup,
}

impl<'a> MutationContext<'a> {
    pub fn get_bool_kwarg(&self, key: &str) -> bool {
        self.kwargs
            .get(key)
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    pub fn get_str_kwarg(&self, key: &str) -> Option<&'a str> {
        self.kwargs.get(key).and_then(|v| v.as_str())
    }
}

/// Resolve a mutation name to its function pointer at parse time (once).
/// Returns `None` for unknown names — callers turn that into an error.
pub fn resolve_mutation(name: &str) -> Option<MutationFn> {
    Some(match name {
        "first_name" => names::first_name,
        "last_name" => names::last_name,
        "full_name" => names::full_name,
        "middle_name" => names::middle_name,

        "email" => contact::email,
        "phone_number" => contact::phone_number,
        "address" => contact::address,
        "deterministic_phone_number" => contact::deterministic_phone,

        "numeric_smallint" => numeric::smallint,
        "numeric_integer" => numeric::integer,
        "numeric_bigint" => numeric::bigint,
        "numeric_decimal" => numeric::decimal,
        "numeric_real" => numeric::real,
        "numeric_double_precision" => numeric::double_precision,
        "numeric_smallserial" => numeric::smallserial,
        "numeric_serial" => numeric::serial,
        "numeric_bigserial" => numeric::bigserial,

        "date" => datetime::date,

        "uri" => network::uri,
        "ipv4" => network::ipv4,
        "ipv6" => network::ipv6,

        "uuid4" => identity::uuid4,
        "uuid5_by_source_value" => identity::uuid5_by_source_value,

        "null" => simple::null,
        "empty_string" => simple::empty_string,
        "fixed_value" => simple::fixed_value,
        "random_choice" => simple::random_choice,

        "string_by_mask" => mask::string_by_mask,

        "json_update" => json_update::json_update,

        _ => return None,
    })
}
