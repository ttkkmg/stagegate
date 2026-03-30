# Templates

`minigate` templates are the string expressions used in:

- `[alias]`
- `run`
- `run_list`

They are how input rows, aliases, and list expansions become concrete shell
command lines.

## Template Building Blocks

`minigate` supports four template forms:

- literal text
- scalar interpolation: `{...}`
- list expansion: `[...]`
- list concatenation: `{[...]}`

Example:

```text
run_list "preprocess {#1} --in [target_images] --out [dat_files]"
```

In that one template:

- `{#1}` is a scalar interpolation
- `[target_images]` is a list expansion
- `[dat_files]` is also a list expansion

## Scalar References: `{...}`

Use `{...}` when the result must be one string.

Allowed targets:

- a column name, such as `{sample}`
- a column index, such as `{#2}`
- a scalar alias, such as `{tmp}`

Examples:

```text
"{sample}.txt"
"{#2}"
"{arg:stem}"
```

Notes:

- `format args` only allows `{arg}` as a direct named column
- `{}` and `{:stem}` are not valid
- `#` inside `{#N}` is part of the reference, not a comment marker

## List Expansion: `[...]`

Use `[...]` when one template should expand into multiple concrete commands.

Example:

```text
run_list "preprocess [inputs] --out [outputs]"
```

If:

- `[inputs]` resolves to `("a.jpg", "b.jpg")`
- `[outputs]` resolves to `("a.dat", "b.dat")`

then the template becomes:

```text
preprocess a.jpg --out a.dat
preprocess b.jpg --out b.dat
```

Rules:

- `[]` is allowed in `run_list`
- `[]` is not allowed in `run`
- multiple `[]` forms in one template must resolve to the same length
- no broadcasting is performed

## List Concatenation: `{[...]}`

Use `{[...]}` when you want a list value joined into one scalar string with
spaces.

Example:

```text
run "makestat {label} {[dat_files]} > {label}.stat.txt"
```

If `dat_files` resolves to:

```text
("cat1.dat", "cat2.dat", "cat3.dat")
```

then `{[dat_files]}` becomes:

```text
cat1.dat cat2.dat cat3.dat
```

This form is scalar, so it is valid in `run`.

`{[list_name:modifier]}` is not supported.

## Alias Types

Aliases are either:

- scalar
- list

Examples:

```text
[alias]
sample "{arg:stem}"
tmp "{sample}.tmp"
dat_files "[target_images:stem].dat"
joined "X {[dat_files]} Y"
```

Type rules:

- an alias containing `[]` is list-typed
- an alias containing `{[]}` is scalar
- `{alias_name}` requires a scalar alias
- `[alias_name]` and `{[alias_name]}` require a list alias

Type mismatches are static validation errors.

## `COLUMN("...")`

`COLUMN("...")` is a special alias-only form for header names that are not
valid DSL identifiers.

Example:

```text
[alias]
sample_id COLUMN("sample id")

[stage.1]
run "echo {sample_id}"
```

Use this when the input header contains spaces or punctuation and cannot be
written as `{sample_id}` directly.

Restrictions:

- only valid on the right-hand side of an alias definition
- invalid under `header false`
- invalid under `format varlists`

## Modifiers

Modifiers transform one scalar string.

They are applied from left to right.

Supported modifiers:

- `name`
- `stem`
- `parent`
- `slice(i,j)`
- `removesuffix(...)`

Examples:

```text
"{arg:stem}"
"{path:parent}"
"{name:slice(0,3)}"
"[images:stem]"
"{#2:removesuffix(.nii.gz)}"
```

### `name`

Returns the final path component.

Example:

```text
"/tmp/data/sample.txt" -> "sample.txt"
```

### `stem`

Returns the final path component without its last suffix.

Example:

```text
"/tmp/data/sample.txt" -> "sample"
```

### `parent`

Returns the parent path as a string.

Example:

```text
"/tmp/data/sample.txt" -> "/tmp/data"
```

### `slice(i,j)`

Applies Python-style string slicing.

Example:

```text
"abcdef" -> "bcd"   # slice(1,4)
```

### `removesuffix(...)`

Removes a suffix if present.

Example:

```text
"image.nii.gz" -> "image"   # removesuffix(.nii.gz)
```

Inside `removesuffix(...)`, `)` and `\` may be escaped.

## Escapes in Template Literals

Inside a string literal, `minigate` supports only these escapes:

- `\\`
- `\"`
- `\{`
- `\}`

These escapes affect literal text in the template. They do not create new
template forms.

## Common Patterns

### Derive an Output Name from One Input

```text
[alias]
sample "{arg:stem}"
tmp "{sample}.tmp"
```

### Expand One List into a Task Wave

```text
[list]
imgs {#2}, ...

[alias]
outs "[imgs:stem].dat"

[stage.1]
run_list "prep [imgs] --out [outs]"
```

### Join a List Back into One Command

```text
[stage.2]
run "merge {[outs]} > result.txt"
```

## What to Use Where

Use:

- `{...}`
  - for one scalar value
- `[...]`
  - for fan-out in `run_list`
- `{[...]}`
  - for joining a list back into one command

If you find yourself trying to use `[]` inside `run`, or `{list}` for a
list-typed alias, the problem is usually a type mismatch between scalar and
list forms.
