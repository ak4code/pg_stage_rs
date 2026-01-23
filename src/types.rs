use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Locale {
    En,
    Ru,
}

impl FromStr for Locale {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(match s.to_lowercase().as_str() {
            "ru" | "russian" => Locale::Ru,
            _ => Locale::En,
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Condition {
    pub column_name: String,
    pub operation: String,
    pub value: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Relation {
    pub table_name: String,
    pub column_name: String,
    pub from_column_name: String,
    pub to_column_name: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MutationSpec {
    pub mutation_name: String,
    #[serde(default)]
    pub mutation_kwargs: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub conditions: Vec<Condition>,
    #[serde(default)]
    pub relations: Vec<Relation>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TableMutationSpec {
    pub mutation_name: String,
}

/// Maps table_name -> column_name -> Vec<MutationSpec>
pub type MutationMap = HashMap<String, HashMap<String, Vec<MutationSpec>>>;

/// Maps table_name -> TableMutationSpec (e.g., delete)
pub type TableMutationMap = HashMap<String, TableMutationSpec>;
