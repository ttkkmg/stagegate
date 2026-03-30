# minigate Quickstart

`minigate` is installed together with `stagegate` and exposes a small DSL for
shell-command pipelines.

If `stagegate` is installed, the `minigate` command should be available:

```bash
minigate --help
```

This page walks through the smallest useful setup with `format args`.

## A Runnable First Pipeline

The sample file
[quickstart_args.pipeline](./examples/quickstart_args.pipeline)
contains:

```text
pipelines 2
tasks 2
format args
limit cpu, 2

[alias]
sample "{arg:stem}"
text_file "{sample}.txt"

[stage.1]
use cpu, 1
run "printf '%s\\n' '{arg}' > {text_file}"

[stage.2]
use cpu, 1
run "wc -c {text_file} > {sample}.count.txt"
```

What it does:

- each positional CLI argument becomes one pipeline row
- `{arg}` is the only direct column name in `format args`
- stage 1 writes one text file per argument
- stage 2 counts the resulting file size

Note:

- the DSL string literal uses `\\n`, not `\n`
- `minigate` string literals only support a small escape set, so the backslash
  itself must be escaped there

## Dry-Run First

Before running real commands, inspect the expanded command lines:

```bash
minigate -f docs/source/minigate/examples/quickstart_args.pipeline -n alpha.txt beta.txt
```

Typical output:

```text
[1]run(1) : printf '%s\n' 'alpha.txt' > alpha.txt
[2]run(1) : wc -c alpha.txt > alpha.count.txt
[1]run(1) : printf '%s\n' 'beta.txt' > beta.txt
[2]run(1) : wc -c beta.txt > beta.count.txt
```

`-n` renders the final command lines but does not call `run_shell(...)`.

## Run It

When the dry-run output looks correct, run the same pipeline without `-n`:

```bash
minigate -f docs/source/minigate/examples/quickstart_args.pipeline alpha.txt beta.txt
```

This creates:

- `alpha.txt`
- `alpha.count.txt`
- `beta.txt`
- `beta.count.txt`
