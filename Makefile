# =============================================================================
# Vina Bim Shop Data Platform - Convenience Makefile
# =============================================================================
#
# Purpose
# -------
# A thin, discoverable, `make + verb` wrapper around the project's official
# command surface. Every target here maps to an existing script, `uv` command,
# or `docker compose` invocation documented in README.md, scripts/README.md,
# and the per-stage deliverables. Compose lifecycle targets delegate to
# scripts/ctl.py, which expands documented profile bundles and prebuilds shared
# images where needed.
#
# Conventions
# -----------
# - Targets follow `make + verb` (e.g. `make install`, `make generate`).
# - Compose profile lifecycle uses `make up-<profile>` / `make down-<profile>`.
# - All recipes are shell-agnostic: they delegate to `uv run python` (using
#   the thin wrapper `scripts/ctl.py` for docker compose operations), so the
#   Makefile behaves the same way from bash, zsh, PowerShell, Git Bash, or WSL.
# - Destructive targets print a clear warning and rely on explicit invocation.
#
# Usage
# -----
#   make help                  # print the full target catalog with descriptions
#   make install               # install Python dependencies
#   make generate              # run the data generator with default options
#   make generate SCALE=smoke MODE=streaming SEED=7
#   make build-dbt             # run dbt build against the local DuckDB profile
#   make test                  # run pytest
#   make finalize              # produce the final Section 01/02 evidence package
#   make reset                 # reset local runtime (prints warning; see below)
#   make up-ingestion          # start the Kafka ingestion profile
#   make down-ingestion        # stop and remove the Kafka ingestion profile
#
# Notes
# -----
# - The `reset` target intentionally stops Docker services, removes volumes,
#   and can clean gitignored local data. It is the Makefile equivalent of
#   `uv run python scripts/qa/reset_all.py`. Forward flags via RESET_FLAGS,
#   e.g. `make reset RESET_FLAGS="--dry-run"` or
#   `make reset RESET_FLAGS="--force --clean-local-data"`.
# - The `up-all` / `down-all` targets are best-effort; staged profile startup
#   remains the supported evidence workflow.
# =============================================================================

# Use the system default shell. Recipes only call `uv run python ...`, so the
# underlying shell does not matter; the wrapper handles cross-platform details.
SHELL := /bin/sh

# Default target: print the catalog so `make` with no arguments is useful.
.DEFAULT_GOAL := help

# Mark all targets as phony (they are commands, not files).
.PHONY: help install generate build-dbt test finalize reset \
        up-ingestion down-ingestion \
        up-lakehouse down-lakehouse \
        up-batch down-batch \
        up-streaming down-streaming \
        up-serving down-serving \
        up-orchestration down-orchestration \
        up-governance down-governance \
        up-all down-all

# -----------------------------------------------------------------------------
# Configurable variables (override on the command line, e.g. `make generate SCALE=smoke`)
# -----------------------------------------------------------------------------

# Generator defaults match the official README quick-start invocation.
SCALE ?= medium
MODE  ?= full
SEED  ?= 42

# Pass-through flags for the reset target. Default is empty (interactive prompt).
RESET_FLAGS ?=

# Path to the thin Python wrapper used for docker compose lifecycle commands.
CTL := scripts/ctl.py

# -----------------------------------------------------------------------------
# Meta
# -----------------------------------------------------------------------------

# Print the full target catalog. Self-documenting on purpose.
help:
	@echo "Vina Bim Shop - common Make targets"
	@echo ""
	@echo "Setup and fast local path:"
	@echo "  make install                 # uv sync"
	@echo "  make generate [SCALE=...] [MODE=...] [SEED=...]"
	@echo "  make build-dbt               # dbt build against local DuckDB"
	@echo "  make test                    # pytest"
	@echo "  make finalize                # scripts/qa/finalize_sections_01_02.py"
	@echo "  make reset [RESET_FLAGS=...] # scripts/qa/reset_all.py (destructive)"
	@echo ""
	@echo "Compose profile lifecycle (use make up-<profile> / make down-<profile>):"
	@echo "  ingestion lakehouse batch streaming serving orchestration governance all"
	@echo ""
	@echo "Run 'make <target>' to execute. See README.md and scripts/README.md for"
	@echo "the authoritative command surface and stage-by-stage documentation."

# -----------------------------------------------------------------------------
# Setup and fast local path
# -----------------------------------------------------------------------------

# Install Python dependencies into the project's virtual environment.
install:
	uv sync

# Run the data generator. Defaults match the README quick-start invocation.
# Override with: make generate SCALE=smoke MODE=streaming SEED=7
generate:
	uv run python scripts/generate/run_generator.py \
		--scale $(SCALE) --mode $(MODE) --clean --seed $(SEED)

# Run dbt build against the local DuckDB profile (fast local parity path).
build-dbt:
	uv run dbt build --project-dir infra/analytics/dbt --profiles-dir infra/analytics/dbt

# Run the local test suite.
test:
	uv run pytest

# Produce the final Section 01/02 evidence package.
finalize:
	uv run python scripts/qa/finalize_sections_01_02.py

# WARNING: destructive. Stops services, removes Docker volumes, and can clean
# gitignored local data. Forward flags via RESET_FLAGS, e.g.:
#   make reset                          # interactive
#   make reset RESET_FLAGS="--dry-run"  # preview
#   make reset RESET_FLAGS="--force"    # skip prompt
#   make reset RESET_FLAGS="--force --clean-local-data"
reset:
	@echo "[reset] WARNING: stops Docker services and removes volumes (and may clean gitignored local data)."
	@echo "[reset] Forward flags via RESET_FLAGS=...; default is interactive."
	uv run python scripts/qa/reset_all.py $(RESET_FLAGS)

# -----------------------------------------------------------------------------
# Compose profile lifecycle
# -----------------------------------------------------------------------------
# Each pair delegates to the Python wrapper `scripts/ctl.py` so the Makefile
# does not depend on shell-specific docker compose syntax.

# Start the Kafka ingestion profile (broker, Schema Registry, Connect, UI).
up-ingestion:
	uv run python $(CTL) compose up ingestion

# Stop the Kafka ingestion profile and remove its volumes.
down-ingestion:
	uv run python $(CTL) compose down ingestion

# Start the lakehouse profile (MinIO, Hive Metastore, Trino, shared Postgres).
up-lakehouse:
	uv run python $(CTL) compose up lakehouse

# Stop the lakehouse profile and remove its volumes.
down-lakehouse:
	uv run python $(CTL) compose down lakehouse

# Start the batch profile (Spark master, worker, history server).
up-batch:
	uv run python $(CTL) compose up batch

# Stop the batch profile and remove its volumes.
down-batch:
	uv run python $(CTL) compose down batch

# Start the streaming profile (Flink JobManager, TaskManager, job submitter).
up-streaming:
	uv run python $(CTL) compose up streaming

# Stop the streaming profile and remove its volumes.
down-streaming:
	uv run python $(CTL) compose down streaming

# Start the serving profile (Pinot Zookeeper, controller, broker, server).
up-serving:
	uv run python $(CTL) compose up serving

# Stop the serving profile and remove its volumes.
down-serving:
	uv run python $(CTL) compose down serving

# Start the orchestration profile (Airflow webserver, scheduler, init, GX Data Docs).
up-orchestration:
	uv run python $(CTL) compose up orchestration

# Stop the orchestration profile and remove its volumes.
down-orchestration:
	uv run python $(CTL) compose down orchestration

# Start the governance profile (DataHub GMS, frontend, actions, Elasticsearch).
up-governance:
	uv run python $(CTL) compose up governance

# Stop the governance profile and remove its volumes.
down-governance:
	uv run python $(CTL) compose down governance

# Best-effort full-stack startup. Staged profile startup remains the supported
# evidence workflow; prefer the individual up-<profile> targets.
up-all:
	uv run python $(CTL) compose up all

# Stop the best-effort full-stack startup and remove its volumes.
down-all:
	uv run python $(CTL) compose down all
