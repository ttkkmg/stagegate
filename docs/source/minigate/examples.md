# Examples

This page collects the public sample files shipped with the documentation.

## Quickstart Example

The smallest runnable example is:

- [quickstart_args.pipeline](./examples/quickstart_args.pipeline)

It uses:

- `format args`
- two stages
- simple filename derivation through aliases

## `args` Example

The more representative `args`-based sample is:

- [basic_args.pipeline](./examples/basic_args.pipeline)

It demonstrates:

- `pipelines` and `tasks`
- resource limits with `limit cpu, ...`
- scalar aliases such as `{sample}` and `{tmp}`

## `varlists` Example

The varlists sample set is:

- [varlists.pipeline](./examples/varlists.pipeline)
- [varlists.txt](./examples/varlists.txt)

These files demonstrate:

- `format varlists`
- `[list]` with `{#2}, ...`
- list-typed alias generation
- `run_list` fan-out
- `{[list_alias]}` join-back in a later stage

## Log Management Example

- [log_archive.pipeline](./examples/log_archive.pipeline)
- [log_archive.csv](./examples/log_archive.csv)

These files demonstrate:

- `[alias]` for filename construction
- parallel upload on `[stage.3]`

## Media Processing Example

- [media_transcode.pipeline](./examples/media_transcode.pipeline)
- [media_transcode.csv](./examples/media_transcode.csv)

These files demonstrate:

- one stage that runs two `ffmpeg` commands in parallel
- one transcription stage
- one cleanup stage

## Machine Learning Examples

### Batch Inference from CSV

The batch inference sample set is:

- [batch_inference.pipeline](./examples/batch_inference.pipeline)
- [batch_inference.csv](./examples/batch_inference.csv)

This example fits workloads where each row describes one sample, one model, and
one prompt or config file.

### Feature Extraction with `varlists` and `run_list`

The feature-pack sample set is:

- [ml_feature_pack.pipeline](./examples/ml_feature_pack.pipeline)
- [ml_feature_pack.txt](./examples/ml_feature_pack.txt)

These files demonstrate:

- `format varlists`
- `[list]` with `{#2}, ...`
- `run_list` fan-out for one file per image
- `{[feature_files]}` join-back for later packing
- a cleanup stage that removes intermediates

This is the recommended sample to read if you want a machine-learning-flavored
example of `varlists` plus `run_list`.

## RNA-seq Paired-End Example

A bioinformatics domain-specific sample is:

- [rnaseq.pipeline](./examples/rnaseq.pipeline)

This example models one paired-end RNA-seq library per pipeline.

It assumes that each input row is a read-1 filename such as:

```text
LIB001_1.fastq.gz
```

Recommended invocation:

```bash
minigate -f docs/source/minigate/examples/rnaseq.pipeline *_1.fastq.gz
```

- if you pass `*.fastq.gz`, both read-1 and read-2 files become independent
  input rows
- `*_1.fastq.gz` is the intended shell pattern for this example
