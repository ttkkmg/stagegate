# Pipeline Files

A `minigate` pipeline file is a small forward-only DSL for launching shell
commands through `stagegate`.

This page explains the structure of the file itself. For template syntax such
as `{...}` or `[list]`, see the templates page.

## File Structure

A pipeline file contains:

- top-level statements
- zero or one `[list]` sections
- zero or one `[alias]` sections
- one or more `[stage.N]` sections

The minimal useful file is:

```text
format args

[stage.1]
run "echo {arg}"
```

## Top-Level Statements

Valid top-level statements are:

- `pipelines INT`
- `tasks INT`
- `format csv|tsv|varlists|args`
- `header true|false`
- `codepage "ENCODING_NAME"`
- `delimiter comma|whitespace`
- `limit IDENTIFIER, INT`

Meaning of each statement:

- `pipelines INT`
  - sets how many pipelines may be active at once
  - this is the pipeline-level parallelism seen by `minigate`
- `tasks INT`
  - sets the scheduler task parallelism
  - this is the global limit on concurrently admitted tasks
- `format ...`
  - selects how positional CLI inputs are interpreted
  - this controls whether inputs are loaded as `args`, `csv`, `tsv`, or
    `varlists`
- `header true|false`
  - controls whether the first row of `csv`/`tsv` input is treated as column
    names
- `codepage "ENCODING_NAME"`
  - selects text decoding for file-based input formats
- `delimiter comma|whitespace`
  - selects how `varlists` rows are split into fields
- `limit IDENTIFIER, INT`
  - declares one abstract resource capacity such as `cpu`
  - later `use ...` statements consume from these capacities

Notes:

- `format` is required
- `=` is never used
- statements are whitespace-separated
- `limit` may appear multiple times
- all other top-level statements may appear at most once

Defaults:

- `pipelines`
  - defaults to `1`
- `tasks`
  - defaults to `1`
- `header`
  - defaults to `false`
- `codepage`
  - defaults to `"utf-8-sig"`
- `limit`
  - omitted means no abstract resource labels are configured

Example:

```text
pipelines 4
tasks 2
format csv
header true
codepage "utf-8-sig"
limit cpu, 4
limit mem, 32
```

## Format-Specific Top-Level Rules

Some top-level statements are valid only for specific input formats.

### `format args`

- `header` is invalid
- `codepage` is invalid
- `delimiter` is invalid

### `format csv` and `format tsv`

- `delimiter` is invalid

### `format varlists`

- `delimiter` is required
- `header false` may be written explicitly
- `header true` is invalid

## `[list]`

`[list]` defines a reusable list of columns from the current input row.

Example:

```text
[list]
target_images {#2}, ...
```

Rules:

- the left-hand side is an identifier such as `target_images`
- `=` is not used
- the right-hand side contains one or more column references
- each element is either `{col}` or `{#N}`
- alias references are not allowed here
- `...` may appear at most once in one definition

Supported `...` shapes are:

- `A, ..., B`
  - columns from `A` through `B`
- `..., B`
  - columns `1` through `B`
- `A, ...`
  - columns from `A` to the last column in the row
- `...`
  - the whole row

Additional rules:

- you must not mix named references and `{#N}` references around the same
  `...`
- if the input has duplicate headers, named endpoints resolve to the rightmost
  matching column

## `[alias]`

`[alias]` defines a reusable name for either:

- a scalar string
- a list of strings

The right-hand side must be one of:

- a template string
- a single `COLUMN("...")`

Example:

```text
[alias]
sample "{arg:stem}"
tmp "{sample}.tmp"
dat_files "[target_images:stem].dat"
special COLUMN("sample id")
```

Key rules:

- aliases may reference earlier aliases
- forward references are invalid
- scalar aliases are referenced as `{alias_name}`
- list aliases are referenced as `[alias_name]` or `{[alias_name]}`
- `COLUMN("...")` is only valid in alias definitions

`COLUMN("...")` is mainly for header names that are not valid DSL identifiers.
It is invalid when:

- `header false`
- `format varlists`

## `[stage.N]`

Each `[stage.N]` section defines one stage of the pipeline.

Example:

```text
[stage.1]
use cpu, 1
run "preprocess {arg} > {tmp}"

[stage.2]
use cpu, 1
run "summarize {tmp} > {sample}.summary.txt"
```

Rules:

- `N` must be a positive integer
- stages execute in ascending `N` order
- duplicate stage numbers are invalid
- a stage must contain exactly one of `run` or `run_list`
- `accept_retvals` may appear at most once
- `use` may appear multiple times
- duplicate `use` labels in one stage are invalid

Meaning:

- a stage is one barriered wave of task submission
- `minigate` submits the commands from that stage
- waits for the whole stage to finish
- then advances to the next stage number

## `run`

`run` launches one or more fixed shell-command templates.

Example:

```text
run "cmd1", "cmd2", "cmd3"
```

Rules:

- each item is one template string
- templates are comma-separated
- trailing commas are invalid
- `[]` list-parallel expansion is not allowed inside `run`

Use `run` when the number of commands is fixed at the stage-definition level.
If a `run` statement contains multiple templates, they are launched as one
parallel wave inside that stage and then waited on together.

## `run_list`

`run_list` launches a variable number of commands from one template.

Example:

```text
run_list "preprocess {#1} --in [target_images] --out [dat_files]"
```

Rules:

- exactly one template string is allowed
- comma-separated multiple templates are invalid
- one or more `[]` expansions may appear
- if multiple `[]` expansions appear, they must all resolve to the same length
- broadcasting is not performed

Use `run_list` when one input row should fan out into a task wave.
This is the main statement for per-file or per-item batch work derived from one
row.

## `use`

`use` adds abstract resource requirements to every task launched from that
stage.

Example:

```text
use cpu, 4
use mem, 16
```

These resource labels are scheduler admission labels, not OS-enforced quotas.
If a stage contains two `run` commands and also `use cpu, 2`, then each task in
that wave requests `cpu=2`.

## `accept_retvals`

By default, `minigate` accepts only shell exit code `0`.

You can override that for one stage:

```text
accept_retvals 0, 10-15, 255
```

Rules:

- a single value must be a non-negative integer
- a range must be `left-right`
- `left` and `right` must be positive integers
- `left < right` is required

If a command in the stage returns a disallowed code, `minigate` treats that as
failure and enters fail-fast shutdown.
This is useful when an external tool uses non-zero codes for domain-specific
states that you still want to accept.

## Comments and Continuations

The DSL supports line comments:

```text
format csv  # input format
```

`#` begins a comment only outside strings and outside brace references. That is
why `{#2}` still means a column-index reference.

Indented continuation lines are joined to the previous logical line. This is
mainly useful for long statements, though most pipeline files are easier to
read when kept one statement per line.
