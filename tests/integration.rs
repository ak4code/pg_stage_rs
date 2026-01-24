use std::io::Cursor;

use pg_stage_rs::format::plain::PlainHandler;
use pg_stage_rs::format::{detect_format, DumpFormat};
use pg_stage_rs::processor::DataProcessor;
use pg_stage_rs::types::Locale;

fn make_processor() -> DataProcessor {
    DataProcessor::new(Locale::En, b'\t', vec![])
}

fn make_ru_processor() -> DataProcessor {
    DataProcessor::new(Locale::Ru, b'\t', vec![])
}

#[test]
fn test_detect_format_plain() {
    let data = b"-- PostgreSQL dump\n";
    assert_eq!(detect_format(data).unwrap(), DumpFormat::Plain);
}

#[test]
fn test_detect_format_custom() {
    let data = b"PGDMP\x01\x0e\x00\x04\x08";
    assert_eq!(detect_format(data).unwrap(), DumpFormat::Custom);
}

#[test]
fn test_plain_passthrough_no_mutations() {
    let input = b"-- Comment line\nSET statement_timeout = 0;\nSELECT 1;\n";
    let mut output = Vec::new();
    let mut handler = PlainHandler::new(make_processor());
    handler.process(Cursor::new(b""), &mut output, input).unwrap();
    assert_eq!(output, input);
}

#[test]
fn test_plain_copy_passthrough_no_mutations() {
    let input = concat!(
        "COPY public.users (id, name, email) FROM stdin;\n",
        "1\tJohn\tjohn@example.com\n",
        "2\tJane\tjane@example.com\n",
        "\\.\n",
    );
    let mut output = Vec::new();
    let mut handler = PlainHandler::new(make_processor());
    handler.process(Cursor::new(b""), &mut output, input.as_bytes()).unwrap();
    assert_eq!(String::from_utf8(output).unwrap(), input);
}

#[test]
fn test_plain_mutation_null() {
    let input = concat!(
        "COMMENT ON COLUMN public.users.email IS 'anon: [{\"mutation_name\": \"null\"}]';\n",
        "COPY public.users (id, name, email) FROM stdin;\n",
        "1\tJohn\tjohn@example.com\n",
        "\\.\n",
    );
    let mut output = Vec::new();
    let mut handler = PlainHandler::new(make_processor());
    handler.process(Cursor::new(b""), &mut output, input.as_bytes()).unwrap();
    let result = String::from_utf8(output).unwrap();
    assert!(result.contains("1\tJohn\t\\N\n"));
}

#[test]
fn test_plain_mutation_empty_string() {
    let input = concat!(
        "COMMENT ON COLUMN public.users.email IS 'anon: [{\"mutation_name\": \"empty_string\"}]';\n",
        "COPY public.users (id, name, email) FROM stdin;\n",
        "1\tJohn\tjohn@example.com\n",
        "\\.\n",
    );
    let mut output = Vec::new();
    let mut handler = PlainHandler::new(make_processor());
    handler.process(Cursor::new(b""), &mut output, input.as_bytes()).unwrap();
    let result = String::from_utf8(output).unwrap();
    assert!(result.contains("1\tJohn\t\n"));
}

#[test]
fn test_plain_mutation_fixed_value() {
    let input = concat!(
        "COMMENT ON COLUMN public.users.email IS 'anon: [{\"mutation_name\": \"fixed_value\", \"mutation_kwargs\": {\"value\": \"REDACTED\"}}]';\n",
        "COPY public.users (id, name, email) FROM stdin;\n",
        "1\tJohn\tjohn@example.com\n",
        "2\tJane\tjane@example.com\n",
        "\\.\n",
    );
    let mut output = Vec::new();
    let mut handler = PlainHandler::new(make_processor());
    handler.process(Cursor::new(b""), &mut output, input.as_bytes()).unwrap();
    let result = String::from_utf8(output).unwrap();
    assert!(result.contains("1\tJohn\tREDACTED\n"));
    assert!(result.contains("2\tJane\tREDACTED\n"));
}

#[test]
fn test_plain_mutation_email() {
    let input = concat!(
        "COMMENT ON COLUMN public.users.email IS 'anon: [{\"mutation_name\": \"email\"}]';\n",
        "COPY public.users (id, name, email) FROM stdin;\n",
        "1\tJohn\tjohn@example.com\n",
        "\\.\n",
    );
    let mut output = Vec::new();
    let mut handler = PlainHandler::new(make_processor());
    handler.process(Cursor::new(b""), &mut output, input.as_bytes()).unwrap();
    let result = String::from_utf8(output).unwrap();
    // Email should be changed
    assert!(!result.contains("john@example.com"));
    // Should contain @ and a domain
    let lines: Vec<&str> = result.lines().collect();
    let data_line = lines.iter().find(|l| l.starts_with("1\t")).unwrap();
    let parts: Vec<&str> = data_line.split('\t').collect();
    assert!(parts[2].contains('@'));
}

#[test]
fn test_plain_mutation_first_name() {
    let input = concat!(
        "COMMENT ON COLUMN public.users.name IS 'anon: [{\"mutation_name\": \"first_name\"}]';\n",
        "COPY public.users (id, name) FROM stdin;\n",
        "1\tOriginalName\n",
        "\\.\n",
    );
    let mut output = Vec::new();
    let mut handler = PlainHandler::new(make_processor());
    handler.process(Cursor::new(b""), &mut output, input.as_bytes()).unwrap();
    let result = String::from_utf8(output).unwrap();
    assert!(!result.contains("OriginalName"));
}

#[test]
fn test_plain_mutation_phone_number() {
    let input = concat!(
        "COMMENT ON COLUMN public.users.phone IS 'anon: [{\"mutation_name\": \"phone_number\", \"mutation_kwargs\": {\"mask\": \"+1 (###) ###-####\"}}]';\n",
        "COPY public.users (id, phone) FROM stdin;\n",
        "1\t+1 (555) 123-4567\n",
        "\\.\n",
    );
    let mut output = Vec::new();
    let mut handler = PlainHandler::new(make_processor());
    handler.process(Cursor::new(b""), &mut output, input.as_bytes()).unwrap();
    let result = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = result.lines().collect();
    let data_line = lines.iter().find(|l| l.starts_with("1\t")).unwrap();
    let parts: Vec<&str> = data_line.split('\t').collect();
    // Verify format: +1 (###) ###-####
    assert!(parts[1].starts_with("+1 ("));
    assert_eq!(parts[1].len(), "+1 (###) ###-####".len());
}

#[test]
fn test_plain_mutation_uuid4() {
    let input = concat!(
        "COMMENT ON COLUMN public.users.uid IS 'anon: [{\"mutation_name\": \"uuid4\"}]';\n",
        "COPY public.users (id, uid) FROM stdin;\n",
        "1\told-uuid\n",
        "\\.\n",
    );
    let mut output = Vec::new();
    let mut handler = PlainHandler::new(make_processor());
    handler.process(Cursor::new(b""), &mut output, input.as_bytes()).unwrap();
    let result = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = result.lines().collect();
    let data_line = lines.iter().find(|l| l.starts_with("1\t")).unwrap();
    let parts: Vec<&str> = data_line.split('\t').collect();
    // UUID4 format: 8-4-4-4-12
    assert_eq!(parts[1].len(), 36);
    assert_eq!(parts[1].chars().filter(|c| *c == '-').count(), 4);
}

#[test]
fn test_plain_mutation_numeric_integer() {
    let input = concat!(
        "COMMENT ON COLUMN public.data.value IS 'anon: [{\"mutation_name\": \"numeric_integer\", \"mutation_kwargs\": {\"start\": 100, \"end\": 200}}]';\n",
        "COPY public.data (id, value) FROM stdin;\n",
        "1\t42\n",
        "\\.\n",
    );
    let mut output = Vec::new();
    let mut handler = PlainHandler::new(make_processor());
    handler.process(Cursor::new(b""), &mut output, input.as_bytes()).unwrap();
    let result = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = result.lines().collect();
    let data_line = lines.iter().find(|l| l.starts_with("1\t")).unwrap();
    let parts: Vec<&str> = data_line.split('\t').collect();
    let val: i64 = parts[1].parse().unwrap();
    assert!(val >= 100 && val <= 200);
}

#[test]
fn test_plain_mutation_date() {
    let input = concat!(
        "COMMENT ON COLUMN public.data.created IS 'anon: [{\"mutation_name\": \"date\", \"mutation_kwargs\": {\"start\": 2020, \"end\": 2023}}]';\n",
        "COPY public.data (id, created) FROM stdin;\n",
        "1\t2021-06-15\n",
        "\\.\n",
    );
    let mut output = Vec::new();
    let mut handler = PlainHandler::new(make_processor());
    handler.process(Cursor::new(b""), &mut output, input.as_bytes()).unwrap();
    let result = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = result.lines().collect();
    let data_line = lines.iter().find(|l| l.starts_with("1\t")).unwrap();
    let parts: Vec<&str> = data_line.split('\t').collect();
    // Should be a date in YYYY-MM-DD format
    assert_eq!(parts[1].len(), 10);
    let year: i32 = parts[1][..4].parse().unwrap();
    assert!(year >= 2020 && year <= 2023);
}

#[test]
fn test_plain_mutation_ipv4() {
    let input = concat!(
        "COMMENT ON COLUMN public.logs.ip IS 'anon: [{\"mutation_name\": \"ipv4\"}]';\n",
        "COPY public.logs (id, ip) FROM stdin;\n",
        "1\t192.168.1.1\n",
        "\\.\n",
    );
    let mut output = Vec::new();
    let mut handler = PlainHandler::new(make_processor());
    handler.process(Cursor::new(b""), &mut output, input.as_bytes()).unwrap();
    let result = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = result.lines().collect();
    let data_line = lines.iter().find(|l| l.starts_with("1\t")).unwrap();
    let parts: Vec<&str> = data_line.split('\t').collect();
    let octets: Vec<&str> = parts[1].split('.').collect();
    assert_eq!(octets.len(), 4);
}

#[test]
fn test_plain_mutation_string_by_mask() {
    let input = concat!(
        "COMMENT ON COLUMN public.data.code IS 'anon: [{\"mutation_name\": \"string_by_mask\", \"mutation_kwargs\": {\"mask\": \"@@-###\"}}]';\n",
        "COPY public.data (id, code) FROM stdin;\n",
        "1\tAB-123\n",
        "\\.\n",
    );
    let mut output = Vec::new();
    let mut handler = PlainHandler::new(make_processor());
    handler.process(Cursor::new(b""), &mut output, input.as_bytes()).unwrap();
    let result = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = result.lines().collect();
    let data_line = lines.iter().find(|l| l.starts_with("1\t")).unwrap();
    let parts: Vec<&str> = data_line.split('\t').collect();
    let code = parts[1];
    assert_eq!(code.len(), 6);
    assert!(code.chars().nth(0).unwrap().is_ascii_alphabetic());
    assert!(code.chars().nth(1).unwrap().is_ascii_alphabetic());
    assert_eq!(code.chars().nth(2).unwrap(), '-');
    assert!(code.chars().nth(3).unwrap().is_ascii_digit());
    assert!(code.chars().nth(4).unwrap().is_ascii_digit());
    assert!(code.chars().nth(5).unwrap().is_ascii_digit());
}

#[test]
fn test_plain_mutation_random_choice() {
    let input = concat!(
        "COMMENT ON COLUMN public.data.status IS 'anon: [{\"mutation_name\": \"random_choice\", \"mutation_kwargs\": {\"choices\": [\"active\", \"inactive\"]}}]';\n",
        "COPY public.data (id, status) FROM stdin;\n",
        "1\tpending\n",
        "\\.\n",
    );
    let mut output = Vec::new();
    let mut handler = PlainHandler::new(make_processor());
    handler.process(Cursor::new(b""), &mut output, input.as_bytes()).unwrap();
    let result = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = result.lines().collect();
    let data_line = lines.iter().find(|l| l.starts_with("1\t")).unwrap();
    let parts: Vec<&str> = data_line.split('\t').collect();
    assert!(parts[1] == "active" || parts[1] == "inactive");
}

#[test]
fn test_plain_condition_equal() {
    let input = concat!(
        "COMMENT ON COLUMN public.users.email IS 'anon: [{\"mutation_name\": \"null\", \"conditions\": [{\"column_name\": \"role\", \"operation\": \"equal\", \"value\": \"admin\"}]}, {\"mutation_name\": \"email\"}]';\n",
        "COPY public.users (id, role, email) FROM stdin;\n",
        "1\tadmin\tadmin@example.com\n",
        "2\tuser\tuser@example.com\n",
        "\\.\n",
    );
    let mut output = Vec::new();
    let mut handler = PlainHandler::new(make_processor());
    handler.process(Cursor::new(b""), &mut output, input.as_bytes()).unwrap();
    let result = String::from_utf8(output).unwrap();
    // Admin should get null
    assert!(result.contains("1\tadmin\t\\N\n"));
    // User should get a generated email
    assert!(!result.contains("user@example.com"));
}

#[test]
fn test_plain_condition_not_equal() {
    let input = concat!(
        "COMMENT ON COLUMN public.users.email IS 'anon: [{\"mutation_name\": \"null\", \"conditions\": [{\"column_name\": \"role\", \"operation\": \"not_equal\", \"value\": \"admin\"}]}]';\n",
        "COPY public.users (id, role, email) FROM stdin;\n",
        "1\tadmin\tadmin@example.com\n",
        "2\tuser\tuser@example.com\n",
        "\\.\n",
    );
    let mut output = Vec::new();
    let mut handler = PlainHandler::new(make_processor());
    handler.process(Cursor::new(b""), &mut output, input.as_bytes()).unwrap();
    let result = String::from_utf8(output).unwrap();
    // Admin keeps original (condition not met)
    assert!(result.contains("1\tadmin\tadmin@example.com\n"));
    // User gets null
    assert!(result.contains("2\tuser\t\\N\n"));
}

#[test]
fn test_plain_delete_table() {
    let input = concat!(
        "COMMENT ON TABLE public.logs IS 'anon: {\"mutation_name\": \"delete\"}';\n",
        "COPY public.logs (id, message) FROM stdin;\n",
        "1\tlog message 1\n",
        "2\tlog message 2\n",
        "\\.\n",
        "SELECT 1;\n",
    );
    let mut output = Vec::new();
    let mut handler = PlainHandler::new(make_processor());
    handler.process(Cursor::new(b""), &mut output, input.as_bytes()).unwrap();
    let result = String::from_utf8(output).unwrap();
    // Data and COPY/end markers should be removed
    assert!(!result.contains("log message"));
    assert!(!result.contains("COPY public.logs"));
    // Other content preserved
    assert!(result.contains("SELECT 1;"));
}

#[test]
fn test_plain_multiple_tables() {
    let input = concat!(
        "COMMENT ON COLUMN public.users.name IS 'anon: [{\"mutation_name\": \"first_name\"}]';\n",
        "COMMENT ON COLUMN public.orders.total IS 'anon: [{\"mutation_name\": \"numeric_integer\", \"mutation_kwargs\": {\"start\": 1, \"end\": 100}}]';\n",
        "COPY public.users (id, name) FROM stdin;\n",
        "1\tAlice\n",
        "\\.\n",
        "COPY public.orders (id, total) FROM stdin;\n",
        "1\t999\n",
        "\\.\n",
    );
    let mut output = Vec::new();
    let mut handler = PlainHandler::new(make_processor());
    handler.process(Cursor::new(b""), &mut output, input.as_bytes()).unwrap();
    let result = String::from_utf8(output).unwrap();
    assert!(!result.contains("Alice"));
    assert!(!result.contains("999"));
}

#[test]
fn test_plain_russian_locale_full_name() {
    let input = concat!(
        "COMMENT ON COLUMN public.users.name IS 'anon: [{\"mutation_name\": \"full_name\"}]';\n",
        "COPY public.users (id, name) FROM stdin;\n",
        "1\tOriginal Name\n",
        "\\.\n",
    );
    let mut output = Vec::new();
    let mut handler = PlainHandler::new(make_ru_processor());
    handler.process(Cursor::new(b""), &mut output, input.as_bytes()).unwrap();
    let result = String::from_utf8(output).unwrap();
    assert!(!result.contains("Original Name"));
    // Russian full name has 3 parts (last + first + patronymic)
    let lines: Vec<&str> = result.lines().collect();
    let data_line = lines.iter().find(|l| l.starts_with("1\t")).unwrap();
    let parts: Vec<&str> = data_line.split('\t').collect();
    let name_parts: Vec<&str> = parts[1].split(' ').collect();
    assert_eq!(name_parts.len(), 3);
}

#[test]
fn test_plain_mutation_address_en() {
    let input = concat!(
        "COMMENT ON COLUMN public.users.addr IS 'anon: [{\"mutation_name\": \"address\"}]';\n",
        "COPY public.users (id, addr) FROM stdin;\n",
        "1\t123 Old Street\n",
        "\\.\n",
    );
    let mut output = Vec::new();
    let mut handler = PlainHandler::new(make_processor());
    handler.process(Cursor::new(b""), &mut output, input.as_bytes()).unwrap();
    let result = String::from_utf8(output).unwrap();
    assert!(!result.contains("123 Old Street"));
}

#[test]
fn test_plain_mutation_uri() {
    let input = concat!(
        "COMMENT ON COLUMN public.data.url IS 'anon: [{\"mutation_name\": \"uri\"}]';\n",
        "COPY public.data (id, url) FROM stdin;\n",
        "1\thttps://original.com/page\n",
        "\\.\n",
    );
    let mut output = Vec::new();
    let mut handler = PlainHandler::new(make_processor());
    handler.process(Cursor::new(b""), &mut output, input.as_bytes()).unwrap();
    let result = String::from_utf8(output).unwrap();
    assert!(!result.contains("https://original.com/page"));
    let lines: Vec<&str> = result.lines().collect();
    let data_line = lines.iter().find(|l| l.starts_with("1\t")).unwrap();
    let parts: Vec<&str> = data_line.split('\t').collect();
    assert!(parts[1].starts_with("https://"));
}

#[test]
fn test_processor_parse_comment() {
    let mut proc = make_processor();
    let comment = "COMMENT ON COLUMN public.users.email IS 'anon: [{\"mutation_name\": \"email\", \"mutation_kwargs\": {\"unique\": true}}]';";
    assert!(proc.parse_comment(comment));
    assert!(proc.mutation_map.contains_key("public.users"));
    assert!(proc.mutation_map["public.users"].contains_key("email"));
}

#[test]
fn test_processor_parse_table_comment() {
    let mut proc = make_processor();
    let comment = "COMMENT ON TABLE public.logs IS 'anon: {\"mutation_name\": \"delete\"}';";
    assert!(proc.parse_comment(comment));
    assert!(proc.table_mutations.contains_key("public.logs"));
    assert_eq!(proc.table_mutations["public.logs"].mutation_name, "delete");
}

#[test]
fn test_processor_setup_table() {
    let mut proc = make_processor();
    let copy = "COPY public.users (id, name, email) FROM stdin;";
    assert!(proc.setup_table(copy));
}

#[test]
fn test_delete_table_pattern() {
    let patterns = vec![regex::Regex::new(r"_log$").unwrap()];
    let proc = DataProcessor::new(Locale::En, b'\t', patterns);
    let input = concat!(
        "COPY public.audit_log (id, message) FROM stdin;\n",
        "1\tlog entry\n",
        "\\.\n",
    );
    let mut output = Vec::new();
    let mut handler = PlainHandler::new(proc);
    handler.process(Cursor::new(b""), &mut output, input.as_bytes()).unwrap();
    let result = String::from_utf8(output).unwrap();
    assert!(!result.contains("log entry"));
    assert!(!result.contains("COPY public.audit_log"));
}
