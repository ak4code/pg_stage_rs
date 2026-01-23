use std::io::{self, Read};

use clap::Parser;
use regex::Regex;

use pg_stage::error::Result;
use pg_stage::format::custom::CustomHandler;
use pg_stage::format::plain::PlainHandler;
use pg_stage::format::{detect_format, DumpFormat};
use pg_stage::processor::DataProcessor;
use pg_stage::types::Locale;

#[derive(Parser, Debug)]
#[command(name = "pg_stage", version, about = "PostgreSQL dump anonymizer")]
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
}

fn main() {
    if let Err(e) = run() {
        eprintln!("pg_stage error: {}", e);
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = Args::parse();

    let locale: Locale = args.locale.parse().unwrap();
    let delimiter = args.delimiter.bytes().next().unwrap_or(b'\t');

    let delete_patterns: Vec<Regex> = args
        .delete_table_patterns
        .iter()
        .filter_map(|p| Regex::new(p).ok())
        .collect();

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
            _ => detect_format(peeked)?,
        }
    } else {
        detect_format(peeked)?
    };

    let processor = DataProcessor::new(locale, delimiter, delete_patterns);

    match format {
        DumpFormat::Plain => {
            let mut handler = PlainHandler::new(processor);
            handler.process(reader, writer, peeked)?;
        }
        DumpFormat::Custom => {
            let mut handler = CustomHandler::new(processor);
            handler.process(reader, writer, peeked)?;
        }
    }

    Ok(())
}
