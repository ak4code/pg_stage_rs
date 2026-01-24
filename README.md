# pg_stage_rs

Streaming anonymizer for PostgreSQL dumps. Supports both plain text (`-Fp`) and custom binary (`-Fc`) formats.

Reads a `pg_dump` output from stdin, applies data mutations defined via `COMMENT ON COLUMN/TABLE` statements, and writes the anonymized dump to stdout.

## Features

- **Streaming architecture** -- processes data line-by-line without loading the entire dump into memory
- **Plain (`-Fp`) and Custom (`-Fc`) format** support with auto-detection
- **25+ mutation types**: names, emails, phones, addresses, UUIDs, numerics, dates, IPs, masks
- **Referential integrity** via relation tracking across tables
- **Conditions** -- apply mutations only when column values match specified criteria
- **Unique value generation** with configurable retry limits
- **Deterministic obfuscation** for phone numbers (HMAC-SHA256)
- **Locale support**: English and Russian (names, patronymics, addresses)
- **Table deletion** by name or regex pattern

## Installation

```bash
cargo install --git https://github.com/ak4code/pg_stage_rs
```

## Usage

```bash
# Plain format (auto-detected)
pg_dump -Fp mydb | pg_stage_rs > anonymized.sql

# Custom format (auto-detected)
pg_dump -Fc mydb | pg_stage_rs > anonymized.dump

# Explicit format, Russian locale
pg_dump -Fp mydb | pg_stage_rs --locale ru --format plain > anonymized.sql

# Delete specific tables by regex
pg_dump -Fp mydb | pg_stage_rs --delete-table-pattern "^audit_.*" > anonymized.sql
```

### CLI Options

| Option | Default | Description |
|--------|---------|-------------|
| `-l, --locale` | `en` | Locale for generated data (`en`, `ru`) |
| `-d, --delimiter` | `\t` | Column delimiter character |
| `-f, --format` | auto | Force format: `plain`/`p`, `custom`/`c` |
| `--delete-table-pattern` | -- | Regex pattern for tables to remove (repeatable) |

## Defining Mutations

Mutations are configured as JSON embedded in PostgreSQL column/table comments. Add them to your schema before dumping:

### Column-level mutations

```sql
COMMENT ON COLUMN public.users.email IS 'anon: [
  {
    "mutation_name": "email",
    "mutation_kwargs": {"unique": true},
    "conditions": [],
    "relations": []
  }
]';
```

### Conditional mutations

Apply different mutations based on column values:

```sql
COMMENT ON COLUMN public.users.email IS 'anon: [
  {
    "mutation_name": "email",
    "mutation_kwargs": {"unique": true},
    "conditions": [
      {"column_name": "role", "operation": "equal", "value": "user"}
    ],
    "relations": []
  },
  {
    "mutation_name": "fixed_value",
    "mutation_kwargs": {"value": "admin@company.com"},
    "conditions": [
      {"column_name": "role", "operation": "equal", "value": "admin"}
    ],
    "relations": []
  }
]';
```

### Relation tracking (FK consistency)

Ensure the same FK value always maps to the same obfuscated value:

```sql
COMMENT ON COLUMN public.orders.customer_email IS 'anon: [
  {
    "mutation_name": "email",
    "mutation_kwargs": {"unique": true},
    "conditions": [],
    "relations": [
      {
        "table_name": "users",
        "column_name": "email",
        "from_column_name": "user_id",
        "to_column_name": "id"
      }
    ]
  }
]';
```

### Table-level deletion

```sql
COMMENT ON TABLE public.audit_log IS 'anon: {"mutation_name": "delete"}';
```

## Available Mutations

### Names

| Mutation | Parameters | Description |
|----------|-----------|-------------|
| `first_name` | `unique` | Random first name |
| `last_name` | `unique` | Random last name |
| `full_name` | `unique` | Full name (RU: last + first + patronymic) |
| `middle_name` | `unique` | Patronymic (Russian locale only) |

### Contact

| Mutation | Parameters | Description |
|----------|-----------|-------------|
| `email` | `unique` | Generated email address |
| `phone_number` | `mask`, `unique` | Phone by mask (`X`/`#` = digit) |
| `address` | `unique` | Full postal address |
| `deterministic_phone_number` | `obfuscated_numbers_count` | HMAC-based phone obfuscation |

### Numeric

| Mutation | Parameters | Description |
|----------|-----------|-------------|
| `numeric_smallint` | `start`, `end`, `unique` | i16 range |
| `numeric_integer` | `start`, `end`, `unique` | i32 range |
| `numeric_bigint` | `start`, `end`, `unique` | i64 range |
| `numeric_smallserial` | `start`, `end`, `unique` | 1..i16 |
| `numeric_serial` | `start`, `end`, `unique` | 1..i32 |
| `numeric_bigserial` | `start`, `end`, `unique` | 1..i64 |
| `numeric_decimal` | `start`, `end`, `precision`, `unique` | Float with precision |
| `numeric_real` | `start`, `end`, `unique` | Float, 6 decimal places |
| `numeric_double_precision` | `start`, `end`, `unique` | Float, 15 decimal places |

### DateTime

| Mutation | Parameters | Description |
|----------|-----------|-------------|
| `date` | `start`, `end`, `date_format`, `unique` | Random date in year range |

### Network

| Mutation | Parameters | Description |
|----------|-----------|-------------|
| `uri` | `max_length`, `unique` | Random HTTPS URI |
| `ipv4` | `unique` | Random IPv4 address |
| `ipv6` | `unique` | Random IPv6 address |

### Identity

| Mutation | Parameters | Description |
|----------|-----------|-------------|
| `uuid4` | -- | Random UUID v4 |
| `uuid5_by_source_value` | `namespace`, `source_column` | Deterministic UUID v5 |

### Simple

| Mutation | Parameters | Description |
|----------|-----------|-------------|
| `null` | -- | PostgreSQL NULL (`\N`) |
| `empty_string` | -- | Empty string |
| `fixed_value` | `value` | Static value |
| `random_choice` | `choices` | Random pick from array |

### Mask

| Mutation | Parameters | Description |
|----------|-----------|-------------|
| `string_by_mask` | `mask`, `char`, `digit`, `unique` | Template: `@`=letter, `#`=digit |

## Condition Operations

| Operation | Description |
|-----------|-------------|
| `equal` | Exact string match |
| `not_equal` | String inequality |
| `by_pattern` | Regex match |

## Environment Variables

| Variable | Used by | Description |
|----------|---------|-------------|
| `SECRET_KEY` | `deterministic_phone_number` | HMAC key for deterministic obfuscation |
| `SECRET_KEY_NONCE` | `deterministic_phone_number` | Nonce appended to input before hashing |

## Supported PostgreSQL Versions

Custom format (`-Fc`) support covers pg_dump format versions **1.12.0 -- 1.16.0**.

## Running Tests

```bash
cargo test
```

## License

MIT