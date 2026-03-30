# `minigate` Test Fixture Bundle

This directory contains fixtures for debugging and regression testing of `minigate`.

## Layout

- `inputs/csv/`
  - CSV input fixtures
- `inputs/tsv/`
  - TSV input fixtures
- `inputs/varlists/`
  - `format varlists` input fixtures

## Fixture Index

### CSV

| File | Purpose |
|---|---|
| `inputs/csv/basic_header.csv` | Basic CSV with a header |
| `inputs/csv/basic_no_header.csv` | Headerless CSV specifically for `{#N}` references |
| `inputs/csv/duplicate_header.csv` | Duplicate header warning and rightmost precedence check |
| `inputs/csv/header_only.csv` | Header-only file resulting in 0 pipelines |
| `inputs/csv/empty.csv` | Empty file resulting in 0 pipelines |
| `inputs/csv/short_row.csv` | Missing columns in some rows |
| `inputs/csv/unicode_header.csv` | Non-identifier header for `COLUMN("...")` |
| `inputs/csv/quoted_commas.csv` | Fields containing commas via CSV quoting |
| `inputs/csv/spaces_in_fields.csv` | Verification of whitespace preservation within fields |

### TSV

| File | Purpose |
|---|---|
| `inputs/tsv/basic_header.tsv` | Basic TSV with a header |
| `inputs/tsv/basic_no_header.tsv` | Headerless TSV specifically for `{#N}` references |
| `inputs/tsv/duplicate_header.tsv` | Duplicate header warning and rightmost precedence check |
| `inputs/tsv/header_only.tsv` | Header-only file resulting in 0 pipelines |
| `inputs/tsv/empty.tsv` | Empty file resulting in 0 pipelines |
| `inputs/tsv/short_row.tsv` | Missing columns in some rows |
| `inputs/tsv/unicode_header.tsv` | Non-identifier header for `COLUMN("...")` |
| `inputs/tsv/embedded_spaces.tsv` | Verification of whitespace preservation within tab-separated fields |

### VARLISTS

| File | Purpose |
|---|---|
| `inputs/varlists/basic_whitespace.txt` | Normal case for whitespace delimiters |
| `inputs/varlists/basic_comma.txt` | Normal case for comma delimiters |
| `inputs/varlists/ragged_lengths.txt` | Normal case with varying column counts per row (ragged) |
| `inputs/varlists/empty.txt` | Empty file resulting in 0 pipelines |
| `inputs/varlists/blank_lines.txt` | Verification of ignoring blank lines |
| `inputs/varlists/leading_comma.txt` | Empty element error caused by a leading comma |
| `inputs/varlists/trailing_comma.txt` | Empty element error caused by a trailing comma |
| `inputs/varlists/double_comma.txt` | Empty element error caused by consecutive commas |
| `inputs/varlists/comma_with_spaces.txt` | Verification of whitespace trimming around commas |
| `inputs/varlists/whitespace_many_spaces.txt` | Verification that multiple spaces are treated as a single delimiter |
| `inputs/varlists/whitespace_tabs.txt` | Verification of mixed tabs and spaces |
| `inputs/varlists/fullwidth_space.txt` | Verification that full-width spaces are not treated as delimiters |
| `inputs/varlists/hash_literal.txt` | Verification that leading `#` has no comment meaning (pure data) |
| `inputs/varlists/utf8_nonascii.txt` | Verification of non-ASCII strings and codepages |

## Coverage Map

| Behavior | Fixtures |
|---|---|
| Basic loading with headers | `csv/basic_header.csv`, `tsv/basic_header.tsv` |
| Reference using `{#N}` without headers | `csv/basic_no_header.csv`, `tsv/basic_no_header.tsv` |
| Duplicate header warning | `csv/duplicate_header.csv`, `tsv/duplicate_header.tsv` |
| Empty / header-only -> 0 pipelines | `csv/empty.csv`, `csv/header_only.csv`, `tsv/empty.tsv`, `tsv/header_only.tsv`, `varlists/empty.txt` |
| Row-dependent column shortage | `csv/short_row.csv`, `tsv/short_row.tsv` |
| Headers for `COLUMN("...")` | `csv/unicode_header.csv`, `tsv/unicode_header.tsv` |
| CSV quote handling | `csv/quoted_commas.csv` |
| Whitespace preservation within fields | `csv/spaces_in_fields.csv`, `tsv/embedded_spaces.tsv` |
| `varlists` whitespace delimiter | `varlists/basic_whitespace.txt`, `varlists/whitespace_many_spaces.txt`, `varlists/whitespace_tabs.txt` |
| `varlists` comma delimiter | `varlists/basic_comma.txt`, `varlists/comma_with_spaces.txt` |
| Variable-length (ragged) rows | `varlists/ragged_lengths.txt` |
| Ignoring blank lines in `varlists` | `varlists/blank_lines.txt` |
| Empty element errors in `varlists` | `varlists/leading_comma.txt`, `varlists/trailing_comma.txt`, `varlists/double_comma.txt` |
| Full-width space as a non-delimiter | `varlists/fullwidth_space.txt` |
| Leading `#` treated as pure data | `varlists/hash_literal.txt` |
| Non-ASCII data | `varlists/utf8_nonascii.txt` |