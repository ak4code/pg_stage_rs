pub mod conditions;
pub mod error;
pub mod format;
pub mod mutator;
pub mod processor;
pub mod relations;
pub mod types;
pub mod unique;

/// Project-wide hashmap/set type aliases.
///
/// pg_stage_rs hashes keys it fully controls (table/column names, column values
/// already seen during this very dump), so HashDoS protection is unnecessary.
/// aHash is 3-5x faster than the std SipHash on short string keys that dominate
/// this workload.
pub type FastMap<K, V> = ahash::AHashMap<K, V>;
pub type FastSet<T> = ahash::AHashSet<T>;
