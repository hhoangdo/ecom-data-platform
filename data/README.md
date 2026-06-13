# `data`

This folder is the local data workspace for generated outputs and committed reference inputs. Most runtime data here is intentionally gitignored so reviewers can regenerate it without committing large local artifacts.

## What Is Here

| Path | Purpose | Git behavior |
| --- | --- | --- |
| `raw/` | Generator-managed raw Parquet snapshots, Kafka-topic JSONL files, and bad-record examples. | Gitignored local output, preserved by nested `.gitignore`. |
| `gold/` | Local DuckDB parity database and exported executive mart outputs. | Gitignored local output, preserved by nested `.gitignore`. |
| `reference/taxonomy/taxonomy_snapshot.yaml` | Committed category taxonomy reference input used by generation/configuration. | Tracked source data. |

## How To Read It

For committed inputs, inspect `reference/`. For generated runtime outputs, run the generator or batch/export commands described in the root README and then inspect `raw/` or `gold/` locally.

## How It Differs From Similar Folders

| Folder | Role |
| --- | --- |
| `data/` | Local generated datasets and committed reference inputs. |
| `evidence/` | Reviewable proof artifacts that are committed for coursework assessment. |
| `configs/` | YAML settings that control how data and pipelines are generated or run. |

The final submitted raw dataset package is committed under `../evidence/final_dataset/`, not directly under this folder.
