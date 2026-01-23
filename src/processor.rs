use std::collections::HashMap;

use rand::rngs::ThreadRng;
use rand::thread_rng;
use regex::Regex;

use crate::conditions::check_conditions;
use crate::mutator::{dispatch_mutation, MutationContext};
use crate::relations::RelationTracker;
use crate::types::{Locale, MutationMap, MutationSpec, TableMutationMap, TableMutationSpec};
use crate::unique::UniqueTracker;

pub struct DataProcessor {
    pub mutation_map: MutationMap,
    pub table_mutations: TableMutationMap,
    pub locale: Locale,
    pub delimiter: u8,
    pub delete_patterns: Vec<Regex>,

    // Current table state
    current_table: String,
    current_columns: Vec<String>,
    column_indices: HashMap<String, usize>,
    current_mutations: HashMap<String, Vec<MutationSpec>>,
    is_delete_table: bool,
    sorted_columns: Vec<String>,

    // Shared state
    rng: ThreadRng,
    unique_tracker: UniqueTracker,
    relation_tracker: RelationTracker,
    secrets: HashMap<String, String>,

    // Regex patterns
    comment_column_re: Regex,
    comment_table_re: Regex,
    copy_re: Regex,
}

impl DataProcessor {
    pub fn new(locale: Locale, delimiter: u8, delete_patterns: Vec<Regex>) -> Self {
        let secrets = {
            let mut m = HashMap::new();
            if let Ok(v) = std::env::var("SECRET_KEY") {
                m.insert("SECRET_KEY".to_string(), v);
            }
            if let Ok(v) = std::env::var("SECRET_KEY_NONCE") {
                m.insert("SECRET_KEY_NONCE".to_string(), v);
            }
            m
        };

        Self {
            mutation_map: HashMap::new(),
            table_mutations: HashMap::new(),
            locale,
            delimiter,
            delete_patterns,
            current_table: String::new(),
            current_columns: Vec::new(),
            column_indices: HashMap::new(),
            current_mutations: HashMap::new(),
            is_delete_table: false,
            sorted_columns: Vec::new(),
            rng: thread_rng(),
            unique_tracker: UniqueTracker::new(),
            relation_tracker: RelationTracker::new(),
            secrets,
            comment_column_re: Regex::new(
                r"COMMENT ON COLUMN ([\d\w_\.]+) IS 'anon: ([\s\S]*)';",
            )
            .unwrap(),
            comment_table_re: Regex::new(
                r"COMMENT ON TABLE ([\d\w_\.]*) IS 'anon: ([\s\S]*)';",
            )
            .unwrap(),
            copy_re: Regex::new(r"COPY ([\d\w_\.]+) \(([\w\W]+)\) FROM stdin;").unwrap(),
        }
    }

    /// Parse a COMMENT ON COLUMN or COMMENT ON TABLE line.
    /// Returns true if a comment was parsed.
    pub fn parse_comment(&mut self, line: &str) -> bool {
        if let Some(caps) = self.comment_column_re.captures(line) {
            let full_name = caps.get(1).unwrap().as_str();
            let json_str = caps.get(2).unwrap().as_str();

            // Parse table.column from full_name (e.g., "public.users.email")
            let parts: Vec<&str> = full_name.rsplitn(2, '.').collect();
            if parts.len() < 2 {
                return false;
            }
            let column_name = parts[0].to_string();
            let table_name = parts[1].to_string();

            if let Ok(specs) = serde_json::from_str::<Vec<MutationSpec>>(json_str) {
                self.mutation_map
                    .entry(table_name)
                    .or_default()
                    .insert(column_name, specs);
            }
            return true;
        }

        if let Some(caps) = self.comment_table_re.captures(line) {
            let table_name = caps.get(1).unwrap().as_str().to_string();
            let json_str = caps.get(2).unwrap().as_str();

            if let Ok(spec) = serde_json::from_str::<TableMutationSpec>(json_str) {
                self.table_mutations.insert(table_name, spec);
            }
            return true;
        }

        false
    }

    /// Set up the processor for a new table based on COPY statement.
    /// Returns true if line was a COPY statement.
    pub fn setup_table(&mut self, line: &str) -> bool {
        if let Some(caps) = self.copy_re.captures(line) {
            let table_name = caps.get(1).unwrap().as_str().to_string();
            let columns_str = caps.get(2).unwrap().as_str();

            self.current_columns = columns_str
                .split(", ")
                .map(|s| s.trim().to_string())
                .collect();

            self.column_indices.clear();
            for (i, col) in self.current_columns.iter().enumerate() {
                self.column_indices.insert(col.clone(), i);
            }

            // Check if table should be deleted
            self.is_delete_table = self.should_delete_table(&table_name);

            // Get mutations for this table
            self.current_mutations = self
                .mutation_map
                .get(&table_name)
                .cloned()
                .unwrap_or_default();

            // Sort columns: non-source-dependent first
            self.sorted_columns = self.sort_columns_by_dependency();

            self.current_table = table_name;
            self.unique_tracker.clear();
            return true;
        }
        false
    }

    /// Process a single data line (tab-separated values).
    /// Returns None if the line should be deleted.
    /// Returns Some(mutated_line) otherwise.
    pub fn process_line(&mut self, line: &[u8]) -> Option<Vec<u8>> {
        if self.is_delete_table {
            return None;
        }

        if self.current_mutations.is_empty() {
            return Some(line.to_vec());
        }

        // Split line by delimiter
        let line_str = match std::str::from_utf8(line) {
            Ok(s) => s,
            Err(_) => return Some(line.to_vec()),
        };

        let values: Vec<&str> = line_str.split(self.delimiter as char).collect();
        if values.len() != self.current_columns.len() {
            return Some(line.to_vec());
        }

        let mut result_values: Vec<String> = values.iter().map(|s| s.to_string()).collect();
        let mut obfuscated_values: HashMap<String, String> = HashMap::new();

        for col_name in &self.sorted_columns.clone() {
            let specs = match self.current_mutations.get(col_name) {
                Some(s) => s.clone(),
                None => continue,
            };

            let col_idx = match self.column_indices.get(col_name) {
                Some(idx) => *idx,
                None => continue,
            };

            let current_value = result_values[col_idx].clone();

            // Try each mutation spec in order
            for spec in specs.iter() {
                // Check conditions
                let values_refs: Vec<&str> = result_values.iter().map(|s| s.as_str()).collect();
                if !check_conditions(&spec.conditions, &values_refs, &self.column_indices) {
                    continue;
                }

                // Check relations
                if !spec.relations.is_empty() {
                    if let Some(new_val) = self.try_relation_lookup(spec, &result_values) {
                        result_values[col_idx] = new_val.clone();
                        obfuscated_values.insert(col_name.clone(), new_val);
                        break;
                    }
                }

                // Dispatch mutation
                let mut ctx = MutationContext {
                    kwargs: &spec.mutation_kwargs,
                    current_value: current_value.clone(),
                    rng: &mut self.rng,
                    unique_tracker: &mut self.unique_tracker,
                    locale: self.locale,
                    secrets: &self.secrets,
                    obfuscated_values: &obfuscated_values,
                };

                match dispatch_mutation(&spec.mutation_name, &mut ctx) {
                    Ok(new_val) => {
                        // Store relation if needed
                        if !spec.relations.is_empty() {
                            self.store_relation(spec, &result_values, &new_val);
                        }
                        result_values[col_idx] = new_val.clone();
                        obfuscated_values.insert(col_name.clone(), new_val);
                        break;
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }
        }

        let delimiter_char = self.delimiter as char;
        let output = result_values.join(&delimiter_char.to_string());
        Some(output.into_bytes())
    }

    /// Reset table state (called when COPY data ends)
    pub fn reset_table(&mut self) {
        self.current_table.clear();
        self.current_columns.clear();
        self.current_mutations.clear();
        self.is_delete_table = false;
    }

    /// Check if current table has any mutations configured
    pub fn has_mutations(&self) -> bool {
        !self.current_mutations.is_empty()
    }

    /// Check if current table should be deleted
    pub fn is_delete(&self) -> bool {
        self.is_delete_table
    }

    fn should_delete_table(&self, table_name: &str) -> bool {
        // Check table mutations
        if let Some(spec) = self.table_mutations.get(table_name) {
            if spec.mutation_name == "delete" {
                return true;
            }
        }

        // Check delete patterns
        for pattern in &self.delete_patterns {
            if pattern.is_match(table_name) {
                return true;
            }
        }

        false
    }

    fn sort_columns_by_dependency(&self) -> Vec<String> {
        let mut independent = Vec::new();
        let mut dependent = Vec::new();

        for col_name in self.current_columns.iter() {
            if let Some(specs) = self.current_mutations.get(col_name) {
                let has_source = specs.iter().any(|s| {
                    s.mutation_kwargs.contains_key("source_column")
                });
                if has_source {
                    dependent.push(col_name.clone());
                } else {
                    independent.push(col_name.clone());
                }
            } else {
                independent.push(col_name.clone());
            }
        }

        independent.extend(dependent);
        independent
    }

    fn try_relation_lookup(
        &self,
        spec: &MutationSpec,
        values: &[String],
    ) -> Option<String> {
        for relation in &spec.relations {
            let from_col_idx = match self.column_indices.get(&relation.from_column_name) {
                Some(idx) => *idx,
                None => continue,
            };
            let fk_value = &values[from_col_idx];
            if let Some(existing) = self.relation_tracker.lookup(
                &relation.table_name,
                &relation.to_column_name,
                fk_value,
            ) {
                return Some(existing.clone());
            }
        }
        None
    }

    fn store_relation(
        &mut self,
        spec: &MutationSpec,
        values: &[String],
        new_val: &str,
    ) {
        for relation in &spec.relations {
            let from_col_idx = match self.column_indices.get(&relation.from_column_name) {
                Some(idx) => *idx,
                None => continue,
            };
            let fk_value = &values[from_col_idx];
            self.relation_tracker.store(
                &relation.table_name,
                &relation.to_column_name,
                fk_value,
                new_val,
            );
        }
    }
}
