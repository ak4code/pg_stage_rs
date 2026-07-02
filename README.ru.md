# pg_stage_rs

[![Rust](https://github.com/ak4code/pg_stage_rs/actions/workflows/rust.yml/badge.svg)](https://github.com/ak4code/pg_stage_rs/actions/workflows/rust.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/Rust-1.70%2B-orange.svg)](https://www.rust-lang.org/)

Потоковый анонимизатор дампов PostgreSQL. Поддерживает текстовый (`-Fp`) и бинарный (`-Fc`) форматы.

Читает вывод `pg_dump` из stdin, применяет мутации данных, объявленные через `COMMENT ON COLUMN/TABLE`, и записывает анонимизированный дамп в stdout.

## Возможности

- **Потоковая архитектура** — обрабатывает данные построчно без загрузки всего дампа в память
- **Поддержка форматов plain (`-Fp`) и custom (`-Fc`)** с автоопределением
- **25+ типов мутаций**: имена, email, телефоны, адреса, UUID, числа, даты, IP, маски
- **Ссылочная целостность** через отслеживание связей между таблицами
- **Условия** — применять мутации только при совпадении значений столбцов с заданными критериями
- **Генерация уникальных значений** с настраиваемым лимитом повторов
- **Детерминированная обфускация** телефонных номеров (HMAC-SHA256)
- **Поддержка локалей**: английская и русская (имена, отчества, адреса)
- **Удаление таблиц** по имени или регулярному выражению

## Установка

Установите Rust и cargo:
```bash
curl https://sh.rustup.rs -sSf | sh
```

Затем установите pg_stage_rs через cargo:
```bash
cargo install --git https://github.com/ak4code/pg_stage_rs
```

## Использование

```bash
# Текстовый формат (автоопределение)
pg_dump -Fp mydb | pg_stage_rs > anonymized.sql

# Бинарный формат (автоопределение)
pg_dump -Fc mydb | pg_stage_rs > anonymized.dump

# Явное указание формата, русская локаль
pg_dump -Fp mydb | pg_stage_rs --locale ru --format plain > anonymized.sql

# Удаление таблиц по регулярному выражению
pg_dump -Fp mydb | pg_stage_rs --delete-table-pattern "^audit_.*" > anonymized.sql

# Подробный режим (показать метаданные дампа)
pg_dump -Fc mydb | pg_stage_rs --verbose > anonymized.dump
# Вывод в stderr:
#   [INFO] pg_dump format version: 1.16.0
#   [INFO] Compression: Zlib
#   [INFO] Database: "mydb"
#   [INFO] TOC entries: 1234
```

### Параметры CLI

| Параметр | По умолчанию | Описание |
|----------|-------------|----------|
| `-l, --locale` | `en` | Локаль для генерируемых данных (`en`, `ru`) |
| `-d, --delimiter` | `\t` | Символ-разделитель столбцов |
| `-f, --format` | auto | Принудительный формат: `plain`/`p`, `custom`/`c` |
| `-v, --verbose` | off | Показывать информацию о дампе: версию формата, сжатие, количество TOC, предупреждения |
| `--delete-table-pattern` | -- | Регулярное выражение для таблиц, которые нужно удалить (можно указывать несколько раз) |
| `--rules-file` | -- | Путь к JSON-файлу с правилами на основе регулярных выражений (см. «Файл правил») |
| `--zstd-level` | `1` | Уровень сжатия zstd для выходного дампа (1–22) |
| `--zstd-threads` | `0` | Количество потоков zstd (0 = автоопределение по числу CPU) |
| `--strict` | off | Режим жёстких ошибок: `error:` вместо `warning:` при невалидном `anon:` JSON в COMMENT |

## Определение мутаций

Мутации задаются в виде JSON, встроенного в комментарии к столбцам/таблицам PostgreSQL. Добавьте их в схему до создания дампа:

### Мутации на уровне столбца

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

### Условные мутации

Применять разные мутации в зависимости от значения столбца:

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

### Отслеживание связей (консистентность FK)

Гарантирует, что одно и то же значение FK всегда отображается в одно и то же обфусцированное значение:

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

### Удаление на уровне таблицы

```sql
COMMENT ON TABLE public.audit_log IS 'anon: {"mutation_name": "delete"}';
```

## Файл правил (`--rules-file`)

Альтернатива `COMMENT ON COLUMN/TABLE`: JSON-файл с правилами на основе регулярных выражений. Полезен, когда нельзя или нежелательно изменять исходную схему, либо когда одни и те же правила должны применяться к нескольким базам данных.

Формат файла правил:

```json
{
  "table_patterns": [
    { "table": "<регулярное выражение для schema.table>", "mutation": { "mutation_name": "delete" } }
  ],
  "column_patterns": [
    {
      "table":  "<регулярное выражение для schema.table>",
      "column": "<регулярное выражение для имени столбца>",
      "mutations": [ /* тот же массив MutationSpec, что и в COMMENT */ ]
    }
  ]
}
```

- `table_patterns` — правила на уровне таблицы. В настоящее время значимо только `delete` (эквивалент `--delete-table-pattern`, но в JSON).
- `column_patterns` — та же структура `MutationSpec`, что и в `COMMENT ON COLUMN`, применяется к столбцам, чьи `schema.table` и имя столбца совпадают с заданными регулярными выражениями. Правила из файла **дополняют** правила из COMMENT, а не заменяют их.

Сравниваемое имя всегда имеет вид `schema.table` (с префиксом схемы). Используйте якоря (`^...$`) — шаблон `users` совпадёт и с `users_archive`.

Ошибки в файле правил (невалидный JSON, некорректное регулярное выражение, неизвестное имя мутации) прерывают выполнение вне зависимости от `--strict`/`--verbose`.

Пример:

```json
{
  "table_patterns": [
    { "table": "^public\\.(audit_log|temp_.*)$",
      "mutation": { "mutation_name": "delete" } }
  ],
  "column_patterns": [
    {
      "table":  "^public\\.users$",
      "column": "^email$",
      "mutations": [{
        "mutation_name": "email",
        "mutation_kwargs": {"unique": true},
        "conditions": [], "relations": []
      }]
    },
    {
      "table":  "^public\\..*$",
      "column": "^(phone|mobile|.*_phone)$",
      "mutations": [{
        "mutation_name": "phone_number",
        "mutation_kwargs": {"mask": "+7XXXXXXXXXX"},
        "conditions": [], "relations": []
      }]
    }
  ]
}
```

```bash
pg_dump -Fc mydb | pg_stage_rs --rules-file rules.json > out.dump
```

## Доступные мутации

### Имена

| Мутация | Параметры | Описание |
|---------|----------|----------|
| `first_name` | `unique` | Случайное имя |
| `last_name` | `unique` | Случайная фамилия |
| `full_name` | `unique` | Полное имя (RU: фамилия + имя + отчество) |
| `middle_name` | `unique` | Отчество (только русская локаль) |

### Контактные данные

| Мутация | Параметры | Описание |
|---------|----------|----------|
| `email` | `unique` | Сгенерированный email-адрес |
| `phone_number` | `mask`, `unique` | Телефон по маске (`X`/`#` = цифра) |
| `address` | `unique` | Полный почтовый адрес |
| `deterministic_phone_number` | `obfuscated_numbers_count` | Детерминированная обфускация телефона на основе HMAC |

### Числовые

| Мутация | Параметры | Описание |
|---------|----------|----------|
| `numeric_smallint` | `start`, `end`, `unique` | Диапазон i16 |
| `numeric_integer` | `start`, `end`, `unique` | Диапазон i32 |
| `numeric_bigint` | `start`, `end`, `unique` | Диапазон i64 |
| `numeric_smallserial` | `start`, `end`, `unique` | 1..i16 |
| `numeric_serial` | `start`, `end`, `unique` | 1..i32 |
| `numeric_bigserial` | `start`, `end`, `unique` | 1..i64 |
| `numeric_decimal` | `start`, `end`, `precision`, `unique` | Число с плавающей точкой и точностью |
| `numeric_real` | `start`, `end`, `unique` | Float, 6 знаков после запятой |
| `numeric_double_precision` | `start`, `end`, `unique` | Float, 15 знаков после запятой |

### Дата и время

| Мутация | Параметры | Описание |
|---------|----------|----------|
| `date` | `start`, `end`, `date_format`, `unique` | Случайная дата в диапазоне лет |

### Сеть

| Мутация | Параметры | Описание |
|---------|----------|----------|
| `uri` | `max_length`, `unique` | Случайный HTTPS URI |
| `ipv4` | `unique` | Случайный IPv4-адрес |
| `ipv6` | `unique` | Случайный IPv6-адрес |

### Идентификаторы

| Мутация | Параметры | Описание |
|---------|----------|----------|
| `uuid4` | -- | Случайный UUID v4 |
| `uuid5_by_source_value` | `namespace`, `source_column` | Детерминированный UUID v5 |

### Простые

| Мутация | Параметры | Описание |
|---------|----------|----------|
| `null` | -- | PostgreSQL NULL (`\N`) |
| `empty_string` | -- | Пустая строка |
| `fixed_value` | `value` | Фиксированное значение |
| `random_choice` | `choices` | Случайный выбор из массива |

### Маска

| Мутация | Параметры | Описание |
|---------|----------|----------|
| `string_by_mask` | `mask`, `char`, `digit`, `unique` | Шаблон: `@`=буква, `#`=цифра |

### JSON

| Мутация | Параметры | Описание |
|---------|----------|----------|
| `json_update` | словарь `ключ → вложенная спецификация мутации` | Частично обновляет столбец типа JSON. Каждое значение — `{"mutation_name": ..., "mutation_kwargs": ...}`. `mutation_name: "delete"` очищает значение (устанавливает `""`) — ключ остаётся. Отсутствующие ключи пропускаются — мутация не применяется и ключ не добавляется. Результат мутации вставляется как JSON-строка (или `null`, если возвращается `\N`). |

Пример:

```sql
COMMENT ON COLUMN public.users.meta IS 'anon: [{
    "mutation_name": "json_update",
    "mutation_kwargs": {
        "name":   {"mutation_name": "first_name"},
        "secret": {"mutation_name": "delete"}
    }
}]';
```

## Операции условий

| Операция | Описание |
|----------|----------|
| `equal` | Точное совпадение строк |
| `not_equal` | Несовпадение строк |
| `by_pattern` | Совпадение по регулярному выражению |

## Переменные окружения

| Переменная | Используется в | Описание |
|-----------|---------------|----------|
| `SECRET_KEY` | `deterministic_phone_number` | HMAC-ключ для детерминированной обфускации |
| `SECRET_KEY_NONCE` | `deterministic_phone_number` | Nonce, добавляемый к входным данным перед хешированием |

## Поддерживаемые версии PostgreSQL

Поддержка бинарного формата (`-Fc`) охватывает версии формата pg_dump **1.12.0 — 1.16.0**.

## Запуск тестов

```bash
cargo test
```

## Лицензия

MIT