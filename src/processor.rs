use std::sync::Arc;

use rand::rngs::ThreadRng;
use rand::thread_rng;
use regex::Regex;

use crate::conditions::{check_conditions, RowRead};
use crate::error::{PgStageError, Result};
use crate::mutator::{MutationContext, ObfuscatedLookup};
use crate::relations::RelationTracker;
use crate::types::{
    ColumnPatternRule, CompiledMutationSpec, Locale, MutationMap, MutationSpec, RulesFile,
    TableMutationMap, TableMutationSpec, TablePatternRule,
};
use crate::unique::UniqueTracker;
use crate::FastMap;

/// Compiled mutation registry, filled during parse-time.
/// Separated from per-row runtime state so it can, in the future, be shared
/// read-only between worker threads.
#[derive(Default)]
pub struct MutationRegistry {
    pub mutation_map: MutationMap,
    pub table_mutations: TableMutationMap,
    pub table_pattern_rules: Vec<(Regex, TableMutationSpec)>,
    pub column_pattern_rules: Vec<(Regex, Regex, Vec<CompiledMutationSpec>)>,
}

impl MutationRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn table_delete(&self, table: &str, extra_patterns: &[Regex]) -> bool {
        if let Some(spec) = self.table_mutations.get(table) {
            if spec.mutation_name == "delete" {
                return true;
            }
        }
        for (re, spec) in &self.table_pattern_rules {
            if spec.mutation_name == "delete" && re.is_match(table) {
                return true;
            }
        }
        for re in extra_patterns {
            if re.is_match(table) {
                return true;
            }
        }
        false
    }
}

pub struct DataProcessor {
    pub registry: MutationRegistry,
    pub locale: Locale,
    pub delimiter: u8,
    pub delete_patterns: Vec<Regex>,

    strict: bool,
    verbose: bool,

    pub rows_processed: u64,
    pub mutations_applied: u64,

    current_table: Arc<str>,
    current_columns: Vec<Arc<str>>,
    column_indices: FastMap<Arc<str>, usize>,
    current_mutations: FastMap<Arc<str>, Vec<CompiledMutationSpec>>,
    sorted_col_indices: Vec<usize>,
    is_delete_table: bool,

    // Per-row scratch — cleared, not reallocated, each row.
    scratch_spans: Vec<(u32, u32)>,
    scratch_replacements: Vec<Option<Box<str>>>,
    scratch_output: Vec<u8>,

    rng: ThreadRng,
    unique_tracker: UniqueTracker,
    relation_tracker: RelationTracker,
    secrets: FastMap<String, String>,

    comment_column_re: Regex,
    comment_table_re: Regex,
    copy_re: Regex,

    json_errors: u64,
    unknown_mutation_errors: u64,
}

impl DataProcessor {
    pub fn new(locale: Locale, delimiter: u8, delete_patterns: Vec<Regex>) -> Self {
        let mut secrets = FastMap::new();
        if let Ok(v) = std::env::var("SECRET_KEY") {
            secrets.insert("SECRET_KEY".to_string(), v);
        }
        if let Ok(v) = std::env::var("SECRET_KEY_NONCE") {
            secrets.insert("SECRET_KEY_NONCE".to_string(), v);
        }
        Self {
            registry: MutationRegistry::new(),
            locale,
            delimiter,
            delete_patterns,
            strict: false,
            verbose: false,
            rows_processed: 0,
            mutations_applied: 0,
            current_table: Arc::from(""),
            current_columns: Vec::new(),
            column_indices: FastMap::new(),
            current_mutations: FastMap::new(),
            sorted_col_indices: Vec::new(),
            is_delete_table: false,
            scratch_spans: Vec::new(),
            scratch_replacements: Vec::new(),
            scratch_output: Vec::new(),
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
            copy_re: Regex::new(r"COPY ([\d\w_\.]+) \(([#\w\W]+)\) FROM stdin;").unwrap(),
            json_errors: 0,
            unknown_mutation_errors: 0,
        }
    }

    pub fn set_strict(&mut self, strict: bool) {
        self.strict = strict;
    }

    pub fn set_verbose(&mut self, verbose: bool) {
        self.verbose = verbose;
    }

    pub fn load_rules(&mut self, text: &str) -> Result<()> {
        let file: RulesFile = serde_json::from_str(text)
            .map_err(|e| PgStageError::InvalidParameter(format!("invalid rules file: {}", e)))?;
        for TablePatternRule { table, mutation } in file.table_patterns {
            let re = Regex::new(&table).map_err(|e| {
                PgStageError::InvalidParameter(format!("invalid table pattern '{}': {}", table, e))
            })?;
            self.registry.table_pattern_rules.push((re, mutation));
        }
        for ColumnPatternRule { table, column, mutations } in file.column_patterns {
            let table_re = Regex::new(&table).map_err(|e| {
                PgStageError::InvalidParameter(format!(
                    "invalid rule table pattern '{}': {}",
                    table, e
                ))
            })?;
            let col_re = Regex::new(&column).map_err(|e| {
                PgStageError::InvalidParameter(format!(
                    "invalid rule column pattern '{}': {}",
                    column, e
                ))
            })?;
            let compiled: Vec<CompiledMutationSpec> = mutations
                .into_iter()
                .map(CompiledMutationSpec::compile)
                .collect::<Result<Vec<_>>>()?;
            self.registry
                .column_pattern_rules
                .push((table_re, col_re, compiled));
        }
        Ok(())
    }

    pub fn parse_warnings(&self) -> (u64, u64) {
        (self.json_errors, self.unknown_mutation_errors)
    }

    /// Parse a COMMENT ON COLUMN / COMMENT ON TABLE line. Returns true if a
    /// comment was recognized (even if its JSON failed to parse — that error
    /// is reported via stderr based on `strict`/`verbose`).
    pub fn parse_comment(&mut self, line: &str) -> bool {
        if let Some(caps) = self.comment_column_re.captures(line) {
            let full_name = caps.get(1).unwrap().as_str();
            let json_str = caps.get(2).unwrap().as_str();

            let parts: Vec<&str> = full_name.rsplitn(2, '.').collect();
            if parts.len() < 2 {
                return false;
            }
            let column_name: Arc<str> = Arc::from(parts[0]);
            let table_name: Arc<str> = Arc::from(parts[1]);

            match serde_json::from_str::<Vec<MutationSpec>>(json_str) {
                Ok(specs) => {
                    let mut compiled = Vec::with_capacity(specs.len());
                    for spec in specs {
                        match CompiledMutationSpec::compile(spec) {
                            Ok(c) => compiled.push(c),
                            Err(e) => {
                                self.unknown_mutation_errors += 1;
                                if self.strict {
                                    eprintln!(
                                        "pg_stage_rs error: compile failed for {}: {}",
                                        full_name, e
                                    );
                                } else if self.verbose {
                                    eprintln!(
                                        "pg_stage_rs warning: compile failed for {}: {}",
                                        full_name, e
                                    );
                                }
                            }
                        }
                    }
                    self.registry
                        .mutation_map
                        .entry(table_name)
                        .or_default()
                        .insert(column_name, compiled);
                }
                Err(e) => {
                    self.json_errors += 1;
                    if self.strict {
                        eprintln!(
                            "pg_stage_rs error: invalid JSON in COMMENT ON COLUMN {}: {}",
                            full_name, e
                        );
                    } else if self.verbose {
                        eprintln!(
                            "pg_stage_rs warning: invalid JSON in COMMENT ON COLUMN {}: {}",
                            full_name, e
                        );
                    }
                }
            }
            return true;
        }

        if let Some(caps) = self.comment_table_re.captures(line) {
            let table_name: Arc<str> = Arc::from(caps.get(1).unwrap().as_str());
            let json_str = caps.get(2).unwrap().as_str();
            match serde_json::from_str::<TableMutationSpec>(json_str) {
                Ok(spec) => {
                    self.registry.table_mutations.insert(table_name, spec);
                }
                Err(e) => {
                    self.json_errors += 1;
                    if self.strict || self.verbose {
                        eprintln!(
                            "pg_stage_rs warning: invalid JSON in COMMENT ON TABLE {}: {}",
                            table_name, e
                        );
                    }
                }
            }
            return true;
        }

        false
    }

    pub fn setup_table(&mut self, line: &str) -> bool {
        let Some(caps) = self.copy_re.captures(line) else {
            return false;
        };
        let table_name_str = caps.get(1).unwrap().as_str();
        let columns_str = caps.get(2).unwrap().as_str();

        self.current_columns.clear();
        self.column_indices.clear();
        self.current_mutations.clear();

        for (i, raw) in columns_str.split(", ").enumerate() {
            let col: Arc<str> = Arc::from(raw.trim());
            self.column_indices.insert(Arc::clone(&col), i);
            self.current_columns.push(col);
        }

        let table_name: Arc<str> = Arc::from(table_name_str);
        self.current_table = Arc::clone(&table_name);

        self.is_delete_table = self
            .registry
            .table_delete(&table_name, &self.delete_patterns);

        if let Some(cols) = self.registry.mutation_map.get(&table_name) {
            for (col, specs) in cols.iter() {
                self.current_mutations
                    .entry(Arc::clone(col))
                    .or_default()
                    .extend(specs.iter().cloned());
            }
        }

        for (table_re, col_re, specs) in &self.registry.column_pattern_rules {
            if !table_re.is_match(&table_name) {
                continue;
            }
            for col in self.current_columns.iter() {
                if col_re.is_match(col) {
                    self.current_mutations
                        .entry(Arc::clone(col))
                        .or_default()
                        .extend(specs.iter().cloned());
                }
            }
        }

        self.build_sorted_indices();
        self.unique_tracker.clear();
        true
    }

    fn build_sorted_indices(&mut self) {
        self.sorted_col_indices.clear();
        let mut dependent = Vec::new();
        for (i, col) in self.current_columns.iter().enumerate() {
            if let Some(specs) = self.current_mutations.get(col) {
                let has_source = specs.iter().any(|s| s.has_source_column());
                if has_source {
                    dependent.push(i);
                } else {
                    self.sorted_col_indices.push(i);
                }
            }
        }
        self.sorted_col_indices.extend(dependent);
    }

    pub fn reset_table(&mut self) {
        self.current_table = Arc::from("");
        self.current_columns.clear();
        self.column_indices.clear();
        self.current_mutations.clear();
        self.sorted_col_indices.clear();
        self.is_delete_table = false;
    }

    pub fn has_mutations(&self) -> bool {
        !self.current_mutations.is_empty()
    }

    pub fn is_delete(&self) -> bool {
        self.is_delete_table
    }

    pub fn relation_tracker_size(&self) -> usize {
        self.relation_tracker.len()
    }

    pub fn unique_tracker_size(&self) -> usize {
        self.unique_tracker.len()
    }

    /// Process a single data line. Returns `None` if the table is being
    /// deleted, else `Some(bytes)` where `bytes` is valid until the next call
    /// to any `&mut self` method.
    pub fn process_line<'a>(&'a mut self, line: &'a [u8]) -> Option<&'a [u8]> {
        if self.is_delete_table {
            return None;
        }
        self.rows_processed = self.rows_processed.wrapping_add(1);

        if self.current_mutations.is_empty() {
            return Some(line);
        }

        if std::str::from_utf8(line).is_err() {
            return Some(line);
        }

        self.scratch_spans.clear();
        self.scratch_replacements.clear();
        self.scratch_replacements
            .resize_with(self.current_columns.len(), || None);

        let delim = self.delimiter;
        let mut start: u32 = 0;
        for (i, &b) in line.iter().enumerate() {
            if b == delim {
                self.scratch_spans.push((start, i as u32));
                start = i as u32 + 1;
            }
        }
        self.scratch_spans.push((start, line.len() as u32));

        if self.scratch_spans.len() != self.current_columns.len() {
            return Some(line);
        }

        self.run_mutations(line);
        self.build_output(line);
        Some(&self.scratch_output)
    }

    fn run_mutations(&mut self, line: &[u8]) {
        let Self {
            current_columns,
            column_indices,
            current_mutations,
            sorted_col_indices,
            scratch_spans,
            scratch_replacements,
            rng,
            unique_tracker,
            relation_tracker,
            secrets,
            locale,
            mutations_applied,
            ..
        } = self;

        for &col_idx in sorted_col_indices.iter() {
            let col_name: &Arc<str> = &current_columns[col_idx];
            let Some(specs) = current_mutations.get(col_name) else {
                continue;
            };

            for spec in specs.iter() {
                let row = ScratchRow {
                    line,
                    spans: scratch_spans,
                    replacements: scratch_replacements,
                };
                if !check_conditions(&spec.conditions, &row, column_indices) {
                    continue;
                }

                if !spec.relations.is_empty() {
                    let mut found: Option<String> = None;
                    for rel in &spec.relations {
                        let from_idx = match column_indices.get(rel.from_column_name.as_ref()) {
                            Some(&i) => i,
                            None => continue,
                        };
                        let fk_view = current_value(line, scratch_spans, scratch_replacements, from_idx);
                        if let Some(existing) =
                            relation_tracker.lookup(&rel.table_name, &rel.to_column_name, fk_view)
                        {
                            found = Some(existing.to_string());
                            break;
                        }
                    }
                    if let Some(val) = found {
                        scratch_replacements[col_idx] = Some(Box::from(val.as_str()));
                        break;
                    }
                }

                let cur = current_value(line, scratch_spans, scratch_replacements, col_idx);
                let lookup = ScratchLookup {
                    column_indices,
                    replacements: scratch_replacements,
                };
                let mut ctx = MutationContext {
                    kwargs: spec.mutation_kwargs.as_ref(),
                    current_value: cur,
                    rng,
                    unique_tracker,
                    locale: *locale,
                    secrets,
                    obfuscated_values: &lookup,
                };

                match spec.call(&mut ctx) {
                    Ok(new_val) => {
                        if !spec.relations.is_empty() {
                            for rel in &spec.relations {
                                if let Some(&from_idx) =
                                    column_indices.get(rel.from_column_name.as_ref())
                                {
                                    let fk_view = current_value(
                                        line,
                                        scratch_spans,
                                        scratch_replacements,
                                        from_idx,
                                    );
                                    relation_tracker.store(
                                        &rel.table_name,
                                        &rel.to_column_name,
                                        fk_view,
                                        &new_val,
                                    );
                                }
                            }
                        }
                        scratch_replacements[col_idx] = Some(Box::from(new_val.as_str()));
                        *mutations_applied = mutations_applied.wrapping_add(1);
                        break;
                    }
                    Err(e) => {
                        eprintln!(
                            "pg_stage_rs warning: mutation '{}' failed for column '{}': {}",
                            spec.mutation_name, col_name, e
                        );
                        continue;
                    }
                }
            }
        }
    }

    fn build_output(&mut self, line: &[u8]) {
        self.scratch_output.clear();
        self.scratch_output.reserve(line.len() + 16);
        for (i, span) in self.scratch_spans.iter().enumerate() {
            if i > 0 {
                self.scratch_output.push(self.delimiter);
            }
            match &self.scratch_replacements[i] {
                Some(s) => self.scratch_output.extend_from_slice(s.as_bytes()),
                None => self
                    .scratch_output
                    .extend_from_slice(&line[span.0 as usize..span.1 as usize]),
            }
        }
    }

    pub fn emit_summary(&self) {
        if !self.verbose {
            return;
        }
        eprintln!(
            "[INFO] processed rows: {}, mutations applied: {}, unique values tracked: {}, relations tracked: {}",
            self.rows_processed,
            self.mutations_applied,
            self.unique_tracker.len(),
            self.relation_tracker.len(),
        );
        if self.json_errors > 0 || self.unknown_mutation_errors > 0 {
            eprintln!(
                "[WARN] parse warnings: {} invalid JSON comments, {} unknown mutations",
                self.json_errors, self.unknown_mutation_errors
            );
        }
    }
}

#[inline]
fn current_value<'a>(
    line: &'a [u8],
    spans: &[(u32, u32)],
    replacements: &'a [Option<Box<str>>],
    idx: usize,
) -> &'a str {
    match &replacements[idx] {
        Some(b) => b.as_ref(),
        None => {
            let (s, e) = spans[idx];
            // SAFETY: caller verified the whole line is valid UTF-8 and the
            // delimiter byte is ASCII, so each span is a valid UTF-8 slice.
            unsafe { std::str::from_utf8_unchecked(&line[s as usize..e as usize]) }
        }
    }
}

struct ScratchRow<'a> {
    line: &'a [u8],
    spans: &'a [(u32, u32)],
    replacements: &'a [Option<Box<str>>],
}

impl<'a> RowRead for ScratchRow<'a> {
    fn len(&self) -> usize {
        self.spans.len()
    }
    fn value_at(&self, idx: usize) -> &str {
        current_value(self.line, self.spans, self.replacements, idx)
    }
}

struct ScratchLookup<'a> {
    column_indices: &'a FastMap<Arc<str>, usize>,
    replacements: &'a [Option<Box<str>>],
}

impl<'a> ObfuscatedLookup for ScratchLookup<'a> {
    fn get(&self, column: &str) -> Option<&str> {
        let idx = *self.column_indices.get(column)?;
        self.replacements.get(idx)?.as_deref()
    }
}
