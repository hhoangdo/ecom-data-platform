# `configs`

This folder contains YAML configuration for local generation and pipeline behavior. Use it to understand runtime defaults before reading the implementation code.

## What Is Here

| Path | Purpose |
| --- | --- |
| `generator/base.yaml` | Default generator scale, outputs, seeds, schema behavior, quality settings, and topic options. |
| `pipelines/local.yaml` | Local platform defaults for batch, streaming, SQL, catalog, object storage, and data output paths. |
| `pipelines/flink_streaming.yaml` | Flink streaming job and derived-topic configuration. |
| `scenarios/` | Scenario documentation and placeholders for drift/change simulations. See `scenarios/README.md`. |

## How To Read It

Start with `generator/base.yaml` for source data behavior, then `pipelines/local.yaml` for platform-wide defaults. Use `pipelines/flink_streaming.yaml` when reviewing the streaming deliverable and Flink modules.

## How It Differs From Similar Folders

| Folder | Role |
| --- | --- |
| `configs/` | Project-level YAML settings and defaults. |
| `infra/` | Service-specific runtime assets, schemas, models, Dockerfiles, and recipes. |
| `src/vina_bim_shop/` | Python code that reads config values and implements behavior. |

Configuration here should describe supported local behavior. Generated data belongs in `../data/`, and captured proof belongs in `../evidence/`.
