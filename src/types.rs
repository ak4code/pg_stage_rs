use regex::Regex;
use serde::Deserialize;
use std::str::FromStr;
use std::sync::Arc;

use crate::error::{PgStageError, Result};
use crate::mutator::{resolve_mutation, MutationFn, MutationContext};
use crate::FastMap;

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
    pub mutation_kwargs: FastMap<String, serde_json::Value>,
    #[serde(default)]
    pub conditions: Vec<Condition>,
    #[serde(default)]
    pub relations: Vec<Relation>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TableMutationSpec {
    pub mutation_name: String,
}

/// Condition operation resolved at parse time.
#[derive(Debug, Clone)]
pub enum CondOp {
    Equal(String),
    NotEqual(String),
    ByPattern(Regex),
}

#[derive(Debug, Clone)]
pub struct CompiledCondition {
    pub column_name: Arc<str>,
    pub op: CondOp,
}

#[derive(Debug, Clone)]
pub struct CompiledRelation {
    pub table_name: Arc<str>,
    pub to_column_name: Arc<str>,
    pub from_column_name: Arc<str>,
}

/// Mutation spec with the function resolved and regex/op compiled.
#[derive(Clone)]
pub struct CompiledMutationSpec {
    pub mutation_name: Arc<str>,
    pub mutation_fn: MutationFn,
    pub mutation_kwargs: Arc<FastMap<String, serde_json::Value>>,
    pub conditions: Vec<CompiledCondition>,
    pub relations: Vec<CompiledRelation>,
}

impl std::fmt::Debug for CompiledMutationSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledMutationSpec")
            .field("mutation_name", &self.mutation_name)
            .field("conditions", &self.conditions)
            .field("relations", &self.relations)
            .finish()
    }
}

impl CompiledMutationSpec {
    pub fn compile(spec: MutationSpec) -> Result<Self> {
        let mutation_fn: MutationFn = resolve_mutation(&spec.mutation_name)
            .ok_or_else(|| PgStageError::UnknownMutation(spec.mutation_name.clone()))?;
        let conditions = spec
            .conditions
            .into_iter()
            .map(|c| {
                let op = match c.operation.as_str() {
                    "equal" => CondOp::Equal(c.value),
                    "not_equal" => CondOp::NotEqual(c.value),
                    "by_pattern" => {
                        let re = Regex::new(&c.value).map_err(|e| {
                            PgStageError::InvalidParameter(format!(
                                "invalid regex in condition.by_pattern '{}': {}",
                                c.value, e
                            ))
                        })?;
                        CondOp::ByPattern(re)
                    }
                    other => {
                        return Err(PgStageError::InvalidParameter(format!(
                            "unknown condition operation '{}'",
                            other
                        )))
                    }
                };
                Ok(CompiledCondition {
                    column_name: Arc::from(c.column_name.as_str()),
                    op,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let relations = spec
            .relations
            .into_iter()
            .map(|r| CompiledRelation {
                table_name: Arc::from(r.table_name.as_str()),
                to_column_name: Arc::from(r.to_column_name.as_str()),
                from_column_name: Arc::from(r.from_column_name.as_str()),
            })
            .collect();
        Ok(Self {
            mutation_name: Arc::from(spec.mutation_name.as_str()),
            mutation_fn,
            mutation_kwargs: Arc::new(spec.mutation_kwargs),
            conditions,
            relations,
        })
    }

    pub fn has_source_column(&self) -> bool {
        self.mutation_kwargs.contains_key("source_column")
    }

    /// Run the compiled mutation.
    pub fn call(&self, ctx: &mut MutationContext) -> Result<String> {
        (self.mutation_fn)(ctx)
    }
}

/// Maps table_name -> column_name -> Vec<CompiledMutationSpec>
pub type MutationMap = FastMap<Arc<str>, FastMap<Arc<str>, Vec<CompiledMutationSpec>>>;

/// Maps table_name -> TableMutationSpec (e.g., delete)
pub type TableMutationMap = FastMap<Arc<str>, TableMutationSpec>;

/// File format for --rules-file: pattern-based mutations that apply to many schemas.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct RulesFile {
    #[serde(default)]
    pub table_patterns: Vec<TablePatternRule>,
    #[serde(default)]
    pub column_patterns: Vec<ColumnPatternRule>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TablePatternRule {
    /// Regex matched against fully-qualified "schema.table".
    pub table: String,
    pub mutation: TableMutationSpec,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ColumnPatternRule {
    /// Regex matched against fully-qualified "schema.table".
    pub table: String,
    /// Regex matched against the column name.
    pub column: String,
    pub mutations: Vec<MutationSpec>,
}
