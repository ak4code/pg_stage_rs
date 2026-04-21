use std::io::{self, Read};

use clap::Parser;
use regex::Regex;

use pg_stage_rs::error::{PgStageError, Result};
use pg_stage_rs::format::custom::CustomHandler;
use pg_stage_rs::format::plain::PlainHandler;
use pg_stage_rs::format::{detect_format, DumpFormat};
use pg_stage_rs::processor::DataProcessor;
use pg_stage_rs::types::Locale;

#[cfg(feature = "mimalloc-allocator")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser, Debug)]
#[command(name = "pg_stage_rs", version, about = "PostgreSQL dump anonymizer")]
struct Args {
    /// Locale for generated data (en, ru)
    #[arg(short, long, default_value = "en")]
    locale: String,

    /// Column delimiter character
    #[arg(short, long, default_value = "\t")]
    delimiter: String,

    /// Force format (plain, custom). Auto-detected if not specified.
    #[arg(short, long)]
    format: Option<String>,

    /// Regex patterns for tables to delete (can be specified multiple times)
    #[arg(long = "delete-table-pattern")]
    delete_table_patterns: Vec<String>,

    /// Optional rules file (JSON) with pattern-based mutations for many schemas.
    /// See README §"Pattern rules".
    #[arg(long = "rules-file")]
    rules_file: Option<String>,

    /// Zstd compression level for the output dump (1-22). Lower is faster.
    #[arg(long = "zstd-level", default_value_t = 1)]
    zstd_level: i32,

    /// Zstd compression threads (0 = auto-detect CPU count).
    #[arg(long = "zstd-threads", default_value_t = 0)]
    zstd_threads: u32,

    /// Fail fast on invalid JSON in COMMENT mutations instead of logging a warning.
    #[arg(long)]
    strict: bool,

    /// Enable verbose output (dump version, TOC count, compression info, progress)
    #[arg(short, long)]
    verbose: bool,
}

fn main() {
    if let Err(e) = run() {
        eprintln!("pg_stage_rs error: {}", e);
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = Args::parse();

    if !matches!(args.locale.to_lowercase().as_str(), "en" | "ru" | "russian" | "english") {
        eprintln!(
            "pg_stage_rs warning: unknown locale '{}', falling back to 'en'",
            args.locale
        );
    }
    let locale: Locale = args.locale.parse().unwrap_or(Locale::En);

    let delimiter = args.delimiter.bytes().next().ok_or_else(|| {
        PgStageError::InvalidParameter("--delimiter must be a non-empty string".to_string())
    })?;

    let delete_patterns: Vec<Regex> = args
        .delete_table_patterns
        .iter()
        .map(|p| {
            Regex::new(p).map_err(|e| {
                PgStageError::InvalidParameter(format!(
                    "invalid --delete-table-pattern regex '{}': {}",
                    p, e
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut reader = stdin.lock();
    let writer = stdout.lock();

    // Peek first 5 bytes for format detection
    let mut peek_buf = [0u8; 5];
    let peek_n = reader.read(&mut peek_buf)?;
    let peeked = &peek_buf[..peek_n];

    let format = if let Some(ref fmt) = args.format {
        match fmt.as_str() {
            "plain" | "p" => DumpFormat::Plain,
            "custom" | "c" => DumpFormat::Custom,
            other => {
                return Err(PgStageError::InvalidParameter(format!(
                    "unknown --format '{}', expected plain|p|custom|c",
                    other
                )))
            }
        }
    } else {
        detect_format(peeked)?
    };

    let mut processor = DataProcessor::new(locale, delimiter, delete_patterns);
    processor.set_strict(args.strict);
    processor.set_verbose(args.verbose);

    if let Some(rules_path) = &args.rules_file {
        let text = std::fs::read_to_string(rules_path).map_err(|e| {
            PgStageError::InvalidParameter(format!("cannot read --rules-file '{}': {}", rules_path, e))
        })?;
        processor.load_rules(&text)?;
    }

    match format {
        DumpFormat::Plain => {
            let mut handler = PlainHandler::new(processor);
            handler.process(reader, writer, peeked)?;
        }
        DumpFormat::Custom => {
            let mut handler = CustomHandler::new(processor)
                .verbose(args.verbose)
                .zstd_level(args.zstd_level)
                .zstd_threads(args.zstd_threads);
            handler.process(reader, writer, peeked)?;
        }
    }

    Ok(())
}
