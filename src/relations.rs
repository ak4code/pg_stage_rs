use std::collections::HashMap;

use uuid::Uuid;

/// Tracks FK relationships to ensure consistent obfuscation across tables.
///
/// Maps: table_name -> to_column_name -> fk_value -> relation_key (UUID)
/// And:  relation_key -> obfuscated_value
#[derive(Debug, Default)]
pub struct RelationTracker {
    /// table_name -> column_name -> fk_value -> relation_uuid
    fk_map: HashMap<String, HashMap<String, HashMap<String, String>>>,
    /// relation_uuid -> obfuscated_value
    values: HashMap<String, String>,
}

impl RelationTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Look up if a relation already has an obfuscated value.
    /// Returns the existing obfuscated value if found.
    pub fn lookup(
        &self,
        table_name: &str,
        to_column_name: &str,
        fk_value: &str,
    ) -> Option<&String> {
        self.fk_map
            .get(table_name)
            .and_then(|cols| cols.get(to_column_name))
            .and_then(|fks| fks.get(fk_value))
            .and_then(|key| self.values.get(key))
    }

    /// Store a new relation mapping.
    pub fn store(
        &mut self,
        table_name: &str,
        to_column_name: &str,
        fk_value: &str,
        obfuscated_value: &str,
    ) {
        let key = Uuid::new_v4().to_string();
        self.fk_map
            .entry(table_name.to_string())
            .or_default()
            .entry(to_column_name.to_string())
            .or_default()
            .insert(fk_value.to_string(), key.clone());
        self.values.insert(key, obfuscated_value.to_string());
    }
}
