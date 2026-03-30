# CLI

`minigate` runs a validated `.pipeline` file against one or more input sources.

## Usage

```text
minigate -f PIPELINE_FILE [options] {- | input1 [input2 ...]}
```

- `-f` / `--file` is required
- at least one input source or `-` is required
- `-` means stdin for file-based formats such as `csv`, `tsv`, and `varlists`
- `PIPELINE_FILE` itself must be a real file path

## Options

### Runtime Overrides

These options override values from the pipeline file:

- `-pp N`, `--pipelines N`
  - override `pipelines`
- `-tp N`, `--tasks N`
  - override `tasks`
- `-l key=value`, `--limit key=value`
  - override or add one resource limit
  - may be specified multiple times

### Output Control

- `-n`, `--dry-run`
  - render expanded command lines without executing `run_shell(...)`
- `-v`, `--verbose`
  - print expanded command lines while executing
  - repeated flags such as `-v -v` are accepted, although the current output
    level is the same for any value greater than zero

## Input Interpretation

The meaning of positional inputs depends on `format`:

- `format args`
  - each positional input becomes one pipeline row
  - direct scalar reference is `{arg}`
- `format csv`
  - each positional input is a CSV file
- `format tsv`
  - each positional input is a TSV file
- `format varlists`
  - each positional input is a varlists file

Multiple input files are consumed in argument order.

## Dry-Run and Verbose Output

Both `-n` and `-v` render fully expanded command lines.

Example:

```text
[1]run(2) : command arg1 arg2
           command arg3 arg4
```

The left-hand prefix shows:

- stage number
- statement kind (`run` or `run_list`)
- number of concrete commands launched from that statement

## Exit Codes

- `0`
  - successful completion
- `1`
  - general failure

This includes:

- parse errors
- static validation errors
- runtime row errors
- task or pipeline failures
- `accept_retvals` violations

## Fail-Fast Behavior

`minigate` is intentionally fail-fast.

If any of the following occurs:

- runtime row error
- task failure
- pipeline failure
- `accept_retvals` violation

then `minigate`:

- stops submitting new pipelines
- lets the surrounding `Scheduler(..., exception_exit_policy="fail_fast")`
  start fail-fast cleanup
- closes the scheduler before returning to the CLI
- exits with a non-zero status
