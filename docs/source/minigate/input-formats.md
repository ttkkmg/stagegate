# Input Formats

`minigate` supports four input formats:

- `csv`
- `tsv`
- `args`
- `varlists`

Each input row becomes one `minigate` pipeline instance.

## Choosing a Format

Use:

- `args`
  - when each pipeline is driven directly by a positional command-line value
- `csv`
  - when you want ordinary CSV parsing, including quoted fields
- `tsv`
  - when your data is tab-separated and may contain spaces inside fields
- `varlists`
  - when row widths vary and the tail of each row is naturally a variable list

## `format args`

`format args` is the simplest mode.

Each positional argument becomes one row with a single named column:

- `arg`

Example:

```bash
minigate -f docs/source/minigate/examples/quickstart_args.pipeline alpha.txt beta.txt
```

Inside the pipeline file:

- `{arg}` is valid
- `header`, `codepage`, and `delimiter` are invalid

This format is usually the best starting point for shell-oriented batch use.

## `format csv`

`format csv` reads each positional input as a CSV file using Python's standard
CSV handling.

Example:

```text
format csv
header true
codepage "utf-8-sig"
```

Behavior:

- quoted CSV fields are supported
- `header true` makes the first row available by column name
- `header false` means direct column access must use `{#N}`
- duplicate headers emit a warning and the rightmost matching column wins
- empty files and header-only files produce zero pipelines

`delimiter` is invalid for `csv`.

## `format tsv`

`format tsv` is the tab-separated counterpart to `csv`.

Example:

```text
format tsv
header true
codepage "utf-8-sig"
```

Behavior:

- fields are separated by tabs
- spaces inside a field are preserved
- `header true` and `header false` follow the same rules as `csv`
- duplicate headers warn and resolve to the rightmost column
- empty files and header-only files produce zero pipelines

`delimiter` is invalid for `tsv`.

## `format varlists`

`format varlists` is intended for ragged rows where the number of fields may
change from row to row.

Example:

```text
format varlists
delimiter whitespace
```

Typical use case:

```text
cat cat1.jpg cat2.jpg cat3.jpg
dog dog1.jpg dog2.jpg
```

Key behavior:

- row widths may vary
- direct column access is `{#N}` only
- named columns are not available
- `COLUMN("...")` is invalid
- blank lines are ignored
- lines beginning with `#` are plain data, not comments
- empty files produce zero pipelines

This format is what enables patterns such as:

```text
[list]
target_images {#2}, ...
```

## `varlists` Delimiters

`format varlists` requires a `delimiter` statement.

### `delimiter whitespace`

```text
format varlists
delimiter whitespace
```

Rules:

- one or more `U+0020` spaces or `U+0009` tabs form one delimiter
- leading and trailing spaces/tabs are ignored
- no empty fields are created
- delimiter behavior is independent of `codepage`

### `delimiter comma`

```text
format varlists
delimiter comma
```

Rules:

- the delimiter is `U+002C` comma
- spaces and tabs around each comma-separated field are trimmed
- empty fields are invalid
- leading commas, trailing commas, and repeated commas cause runtime row
  errors
- delimiter behavior is independent of `codepage`

Unlike `csv`, this mode does **not** support quoted fields.

## Header Handling

Header behavior depends on format:

- `args`
  - no header setting exists
  - the only named column is `arg`
- `csv` / `tsv`
  - `header true` or `header false`
- `varlists`
  - `header true` is invalid
  - `header false` may be written explicitly, but is also the default

If `header false` is in effect, named references such as `{sample}` are not
valid. Use `{#N}` instead.

## Codepages

`codepage "..."` applies to:

- `csv`
- `tsv`
- `varlists`

It is invalid for:

- `args`

The default is:

```text
codepage "utf-8-sig"
```

For `varlists`, `codepage` affects file decoding only. It does not change what
counts as a delimiter.

## Reading from Standard Input

For file-based formats, a positional input of `-` means standard input.

Examples:

```bash
cat samples.tsv | minigate -f pipeline.pipeline -
```

```bash
printf 'cat a.jpg b.jpg\n' | minigate -f docs/source/minigate/examples/varlists.pipeline -
```

`args` is different:

- its positional inputs are already the data rows
- `-` is just another argument value there unless you choose to interpret it in
  your own commands

## Warnings and Errors by Format

Common cases:

- duplicate header in `csv` / `tsv`
  - warning
- column-name reference under `header false`
  - static validation error
- out-of-range `{#N}` on a specific row
  - runtime row error
- empty comma field in `varlists`
  - runtime row error
