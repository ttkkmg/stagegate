# Welcome to Stagegate

`stagegate` is a small, explicit, single-process Python library for running
many forward-only pipelines under stage-aware priority and abstract resource
quotas.

It is intended for local batch workloads that need more structure than a plain
thread pool, but less machinery than a workflow engine.

- **User's Guide** explains the design background, mental model, and scheduling
  approach.
- **Use Cases** shows the intended API patterns for barriers, incremental
  collection, cancellation, cooperative terminate, and long-lived outer loops.
- **API Reference** provides generated reference documentation for the public
  API.

[![GitHub Repo](https://img.shields.io/badge/GitHub-Repository-blue?logo=github)](https://github.com/ttkkmg/stagegate)
[![License](https://img.shields.io/github/license/ttkkmg/stagegate)](https://github.com/ttkkmg/stagegate/blob/main/LICENSE)