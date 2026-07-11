from __future__ import annotations

import csv
import hashlib
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
import zlib
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RAW_INPUT_HINT = "uv run python scripts/generate/run_generator.py --scale smoke --mode full --clean"
DBT_PROJECT_DIR = "infra/analytics/dbt"
DBT_BUILD_COMMAND = ["dbt", "build", "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR]
DBT_DOCS_COMMAND = ["dbt", "docs", "generate", "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR]
EVIDENCE_ROOT = Path("evidence/02_schema_design")
DBT_TARGET = Path(DBT_PROJECT_DIR) / "target"
DUCKDB_PATH = Path("data/gold/vina_bim_shop.duckdb")
SCHEMA_DESIGN_SOURCE = Path("architecture/diagrams/schema_design.puml")
ALL_ZONE_MODEL_DIRECTORIES = ("bronze", "silver", "gold")

REQUIRED_RAW_INPUTS = [
    Path("data/raw/customers/part-000.parquet"),
    Path("data/raw/sellers/part-000.parquet"),
    Path("data/raw/products/part-000.parquet"),
    Path("data/raw/product_category_map/part-000.parquet"),
    Path("data/raw/inventory_snapshots/part-000.parquet"),
    Path("data/raw/promotions/part-000.parquet"),
    Path("data/raw/orders/part-000.parquet"),
    Path("data/raw/order_items/part-000.parquet"),
    Path("data/raw/payments/part-000.parquet"),
    Path("data/raw/shipments/part-000.parquet"),
    Path("data/raw/kafka_topics/commerce_events/events.jsonl"),
    Path("data/raw/kafka_topics/catalog_events/events.jsonl"),
    Path("data/raw/kafka_topics/fulfillment_events/events.jsonl"),
    Path("data/raw/kafka_topics/ops_events/events.jsonl"),
]


def expected_artifact_paths() -> list[str]:
    return [
        "dbt_build_report.md",
        "dbt_test_results.csv",
        "dbt_model_results.csv",
        "dbt_catalog_summary.csv",
        "schema_inventory.csv",
        "table_row_counts.csv",
        "run_manifest.json",
        "screenshots/schema_design.png",
        "screenshots/gold_schema_inventory.png",
        "screenshots/dbt_test_summary.png",
    ]


def main() -> int:
    repo_root = Path(__file__).resolve().parents[2]
    validate_raw_inputs(repo_root)
    schema_design_metadata = validate_schema_design_model_coverage(repo_root)

    evidence_root = repo_root / EVIDENCE_ROOT
    screenshots_root = evidence_root / "screenshots"
    evidence_root.mkdir(parents=True, exist_ok=True)
    screenshots_root.mkdir(parents=True, exist_ok=True)

    build_result = run_command(DBT_BUILD_COMMAND, repo_root)
    build_run_results = read_json(repo_root / DBT_TARGET / "run_results.json")
    docs_result = run_command(DBT_DOCS_COMMAND, repo_root)

    manifest = read_json(repo_root / DBT_TARGET / "manifest.json")
    catalog = read_json(repo_root / DBT_TARGET / "catalog.json")

    model_rows, test_rows = split_dbt_run_results(build_run_results, manifest)
    catalog_rows = catalog_summary_rows(manifest, catalog)
    inventory_rows = schema_inventory_rows(repo_root / DUCKDB_PATH)
    row_count_rows = table_row_count_rows(repo_root / DUCKDB_PATH)

    write_csv(evidence_root / "dbt_model_results.csv", model_rows)
    write_csv(evidence_root / "dbt_test_results.csv", test_rows)
    write_csv(evidence_root / "dbt_catalog_summary.csv", catalog_rows)
    write_csv(evidence_root / "schema_inventory.csv", inventory_rows)
    write_csv(evidence_root / "table_row_counts.csv", row_count_rows)

    render_metadata = render_schema_design(repo_root, screenshots_root / "schema_design.png")
    write_table_png(
        screenshots_root / "gold_schema_inventory.png",
        "Gold Schema Inventory",
        [row for row in catalog_rows if row["schema"] == "gold"],
        ["schema", "model_name", "relation_type", "column_count", "columns"],
    )
    write_test_summary_png(screenshots_root / "dbt_test_summary.png", test_rows)

    report = build_report_markdown(model_rows, test_rows, catalog_rows, row_count_rows)
    (evidence_root / "dbt_build_report.md").write_text(report, encoding="utf-8")

    run_manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "commands": [
            command_summary(build_result),
            command_summary(docs_result),
        ],
        "dbt_model_count": len(model_rows),
        "dbt_test_count": len(test_rows),
        "catalog_model_count": len(catalog_rows),
        "schema_inventory_row_count": len(inventory_rows),
        "table_row_count": len(row_count_rows),
        **schema_design_metadata,
        **render_metadata,
        "artifacts": expected_artifact_paths(),
    }
    (evidence_root / "run_manifest.json").write_text(json.dumps(run_manifest, indent=2), encoding="utf-8")

    print(f"Wrote Section 02 evidence to {evidence_root}")
    return 0


def validate_raw_inputs(repo_root: Path) -> None:
    missing = [path for path in REQUIRED_RAW_INPUTS if not (repo_root / path).is_file()]
    if missing:
        missing_lines = "\n".join(f"- {path.as_posix()}" for path in missing)
        raise SystemExit(
            "Section 02 evidence generation requires fresh Section 01 raw inputs.\n"
            f"Run: {RAW_INPUT_HINT}\n"
            f"Missing:\n{missing_lines}"
        )


def all_zone_model_names(repo_root: Path) -> dict[str, set[str]]:
    models_root = repo_root / "infra" / "analytics" / "dbt" / "models"
    return {
        zone: {path.stem for path in (models_root / zone).glob("*.sql") if path.is_file()}
        for zone in ALL_ZONE_MODEL_DIRECTORIES
    }


def validate_schema_design_model_coverage(repo_root: Path) -> dict[str, Any]:
    source_path = repo_root / SCHEMA_DESIGN_SOURCE
    source = source_path.read_text(encoding="utf-8")
    model_names = all_zone_model_names(repo_root)
    missing_by_zone = {
        zone: sorted(name for name in names if name not in source)
        for zone, names in model_names.items()
    }
    missing_by_zone = {
        zone: names for zone, names in missing_by_zone.items() if names
    }
    if missing_by_zone:
        missing_lines = "\n".join(
            f"- {zone}: {', '.join(names)}"
            for zone, names in missing_by_zone.items()
        )
        raise SystemExit(
            "schema_design.puml is missing dbt model labels:\n"
            f"{missing_lines}"
        )
    return {
        "schema_design_source": SCHEMA_DESIGN_SOURCE.as_posix(),
        "schema_design_sha256": hashlib.sha256(source_path.read_bytes()).hexdigest(),
        "schema_design_model_counts": {
            zone: len(model_names[zone]) for zone in ALL_ZONE_MODEL_DIRECTORIES
        },
    }


def run_command(command: list[str], cwd: Path) -> dict[str, Any]:
    started = time.perf_counter()
    completed = subprocess.run(command, cwd=cwd, text=True, capture_output=True, check=False)
    elapsed = round(time.perf_counter() - started, 3)
    if completed.returncode != 0:
        raise SystemExit(
            f"Command failed: {' '.join(command)}\n"
            f"Exit code: {completed.returncode}\n"
            f"STDOUT:\n{completed.stdout}\n"
            f"STDERR:\n{completed.stderr}"
        )
    return {
        "command": " ".join(command),
        "return_code": completed.returncode,
        "elapsed_seconds": elapsed,
        "stdout_tail": tail_lines(completed.stdout),
        "stderr_tail": tail_lines(completed.stderr),
    }


def tail_lines(value: str, line_count: int = 12) -> str:
    return "\n".join(value.splitlines()[-line_count:])


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def split_dbt_run_results(
    run_results: dict[str, Any],
    manifest: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    nodes = manifest.get("nodes", {})
    model_rows: list[dict[str, Any]] = []
    test_rows: list[dict[str, Any]] = []

    for result in run_results.get("results", []):
        unique_id = result.get("unique_id", "")
        node = nodes.get(unique_id, {})
        resource_type = node.get("resource_type", unique_id.split(".", 1)[0])
        execution_time = round(float(result.get("execution_time", 0.0)), 3)

        if resource_type == "model":
            model_rows.append(
                {
                    "model_name": node.get("name", unique_id.rsplit(".", 1)[-1]),
                    "schema": node.get("schema", ""),
                    "materialized": node.get("config", {}).get("materialized", ""),
                    "status": result.get("status", ""),
                    "execution_time_seconds": execution_time,
                }
            )
        elif resource_type == "test":
            dependency_names = [
                nodes.get(dependency, {}).get("name", dependency.rsplit(".", 1)[-1])
                for dependency in node.get("depends_on", {}).get("nodes", [])
                if dependency.startswith("model.")
            ]
            test_rows.append(
                {
                    "test_name": node.get("name", unique_id.rsplit(".", 1)[-1]),
                    "status": result.get("status", ""),
                    "failures": int(result.get("failures") or 0),
                    "execution_time_seconds": execution_time,
                    "depends_on": ", ".join(dependency_names),
                }
            )

    return (
        sorted(model_rows, key=lambda row: (row["schema"], row["model_name"])),
        sorted(test_rows, key=lambda row: (row["status"], row["test_name"])),
    )


def catalog_summary_rows(manifest: dict[str, Any], catalog: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    catalog_nodes = catalog.get("nodes", {})
    for unique_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") != "model":
            continue
        catalog_node = catalog_nodes.get(unique_id, {})
        columns = sorted(catalog_node.get("columns", {}))
        rows.append(
            {
                "schema": node.get("schema", ""),
                "model_name": node.get("name", unique_id.rsplit(".", 1)[-1]),
                "relation_type": catalog_node.get("metadata", {}).get("type", ""),
                "column_count": len(columns),
                "columns": ", ".join(columns),
                "description": node.get("description", ""),
            }
        )
    return sorted(rows, key=lambda row: (row["schema"], row["model_name"]))


def schema_inventory_rows(db_path: Path) -> list[dict[str, Any]]:
    import duckdb

    query = """
        select
            c.table_schema as schema,
            c.table_name as table_name,
            t.table_type as relation_type,
            c.column_name as column_name,
            c.data_type as data_type,
            c.ordinal_position as ordinal_position
        from information_schema.columns c
        join information_schema.tables t
          on c.table_schema = t.table_schema
         and c.table_name = t.table_name
        where c.table_schema in ('bronze', 'silver', 'gold')
        order by c.table_schema, c.table_name, c.ordinal_position
    """
    with duckdb.connect(str(db_path), read_only=True) as connection:
        rows = connection.execute(query).fetchall()
    return [
        {
            "schema": row[0],
            "table_name": row[1],
            "relation_type": row[2],
            "column_name": row[3],
            "data_type": row[4],
            "ordinal_position": row[5],
        }
        for row in rows
    ]


def table_row_count_rows(db_path: Path) -> list[dict[str, Any]]:
    import duckdb

    with duckdb.connect(str(db_path), read_only=True) as connection:
        relations = connection.execute(
            """
            select table_schema, table_name, table_type
            from information_schema.tables
            where table_schema in ('bronze', 'silver', 'gold')
            order by table_schema, table_name
            """
        ).fetchall()
        rows = []
        for schema, table_name, relation_type in relations:
            row_count = connection.execute(
                f"select count(*) from {quote_identifier(schema)}.{quote_identifier(table_name)}"
            ).fetchone()[0]
            rows.append(
                {
                    "schema": schema,
                    "table_name": table_name,
                    "relation_type": relation_type,
                    "row_count": int(row_count),
                }
            )
    return rows


def quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def render_schema_design(repo_root: Path, output_path: Path) -> dict[str, str]:
    source = (repo_root / SCHEMA_DESIGN_SOURCE).read_text(encoding="utf-8")
    try:
        render_plantuml_png(source, output_path)
        return {"schema_design_render_mode": "plantuml_server"}
    except (OSError, urllib.error.URLError, TimeoutError, ValueError) as exc:
        write_table_png(
            output_path,
            "schema_design.puml render fallback",
            [{"status": "PlantUML server unavailable", "detail": str(exc)[:120]}],
            ["status", "detail"],
        )
        return {
            "schema_design_render_mode": "fallback_png",
            "schema_design_render_error": str(exc)[:240],
        }


def render_plantuml_png(source: str, output_path: Path) -> None:
    encoded = plantuml_encode(source)
    url = f"https://www.plantuml.com/plantuml/png/{encoded}"
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = response.read()
    if not payload.startswith(b"\x89PNG\r\n\x1a\n"):
        raise ValueError("PlantUML server did not return a PNG payload")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(payload)


def plantuml_encode(source: str) -> str:
    compressed = zlib.compress(source.encode("utf-8"), 9)[2:-4]
    return "".join(encode_plantuml_bytes(compressed))


def encode_plantuml_bytes(data: bytes) -> list[str]:
    encoded: list[str] = []
    for index in range(0, len(data), 3):
        block = data[index : index + 3]
        b1 = block[0]
        b2 = block[1] if len(block) > 1 else 0
        b3 = block[2] if len(block) > 2 else 0
        encoded.append(encode_6bit(b1 >> 2))
        encoded.append(encode_6bit(((b1 & 0x3) << 4) | (b2 >> 4)))
        if len(block) > 1:
            encoded.append(encode_6bit(((b2 & 0xF) << 2) | (b3 >> 6)))
        if len(block) > 2:
            encoded.append(encode_6bit(b3 & 0x3F))
    return encoded


def encode_6bit(value: int) -> str:
    if value < 10:
        return chr(48 + value)
    value -= 10
    if value < 26:
        return chr(65 + value)
    value -= 26
    if value < 26:
        return chr(97 + value)
    value -= 26
    if value == 0:
        return "-"
    if value == 1:
        return "_"
    raise ValueError(f"Cannot encode 6-bit value: {value}")


def write_test_summary_png(path: Path, test_rows: list[dict[str, Any]]) -> None:
    counts = Counter(row["status"] for row in test_rows)
    rows = [
        {"status": status, "count": count}
        for status, count in sorted(counts.items())
    ]
    write_table_png(path, "dbt Test Summary", rows, ["status", "count"])


def write_table_png(
    path: Path,
    title: str,
    rows: list[dict[str, Any]],
    columns: list[str],
    *,
    max_rows: int = 28,
) -> None:
    from PIL import Image, ImageDraw, ImageFont

    visible_rows = rows[:max_rows]
    font = ImageFont.load_default()
    padding = 10
    row_height = 28
    title_height = 42
    footer_height = 24 if len(rows) > len(visible_rows) else 0
    widths = [
        min(360, max(90, max([text_width(font, column)] + [text_width(font, str(row.get(column, ""))[:80]) for row in visible_rows]) + 2 * padding))
        for column in columns
    ]
    width = sum(widths) + 2
    height = title_height + row_height * (len(visible_rows) + 1) + footer_height + 2
    image = Image.new("RGB", (width, height), "white")
    draw = ImageDraw.Draw(image)

    draw.rectangle([0, 0, width, title_height], fill=(36, 52, 71))
    draw.text((padding, 13), title, fill="white", font=font)

    y = title_height
    draw.rectangle([0, y, width, y + row_height], fill=(230, 235, 241), outline=(150, 160, 170))
    x = 0
    for column, col_width in zip(columns, widths):
        draw.text((x + padding, y + 8), column, fill=(20, 30, 40), font=font)
        draw.line([x, y, x, height], fill=(185, 193, 202))
        x += col_width
    draw.line([x - 1, y, x - 1, height], fill=(185, 193, 202))

    for row_index, row in enumerate(visible_rows, start=1):
        y = title_height + row_height * row_index
        fill = (248, 250, 252) if row_index % 2 else (255, 255, 255)
        draw.rectangle([0, y, width, y + row_height], fill=fill, outline=(220, 225, 230))
        x = 0
        for column, col_width in zip(columns, widths):
            value = truncate_text(str(row.get(column, "")), max(8, col_width // 7))
            draw.text((x + padding, y + 8), value, fill=(20, 30, 40), font=font)
            x += col_width

    if footer_height:
        footer = f"Showing {len(visible_rows)} of {len(rows)} rows"
        draw.text((padding, height - footer_height + 6), footer, fill=(80, 90, 100), font=font)

    path.parent.mkdir(parents=True, exist_ok=True)
    image.save(path)


def text_width(font: Any, value: str) -> int:
    try:
        return int(font.getlength(value))
    except AttributeError:
        return len(value) * 7


def truncate_text(value: str, max_chars: int) -> str:
    if len(value) <= max_chars:
        return value
    return value[: max(0, max_chars - 3)] + "..."


def build_report_markdown(
    model_rows: list[dict[str, Any]],
    test_rows: list[dict[str, Any]],
    catalog_rows: list[dict[str, Any]],
    row_count_rows: list[dict[str, Any]],
) -> str:
    model_status = Counter(row["status"] for row in model_rows)
    test_status = Counter(row["status"] for row in test_rows)
    schema_counts = Counter(row["schema"] for row in catalog_rows)
    gold_rows = [row for row in row_count_rows if row["schema"] == "gold"]

    return "\n".join(
        [
            "# Section 02 dbt Evidence Report",
            "",
            "## Command Summary",
            "",
            "- `dbt build --project-dir infra/analytics/dbt --profiles-dir infra/analytics/dbt` completed before artifact extraction.",
            "- `dbt docs generate --project-dir infra/analytics/dbt --profiles-dir infra/analytics/dbt` produced `manifest.json` and `catalog.json` for summarized evidence.",
            "",
            "## dbt Results",
            "",
            f"- Models: {len(model_rows)} ({format_counter(model_status)})",
            f"- Tests: {len(test_rows)} ({format_counter(test_status)})",
            f"- Cataloged models by schema: {format_counter(schema_counts)}",
            "",
            "## Gold Row Counts",
            "",
            *[f"- `{row['table_name']}`: {row['row_count']:,} rows" for row in gold_rows],
            "",
            "## Evidence Files",
            "",
            *[f"- `evidence/02_schema_design/{path}`" for path in expected_artifact_paths() if path != "dbt_build_report.md"],
            "",
        ]
    )


def format_counter(counter: Counter[str]) -> str:
    if not counter:
        return "none"
    return ", ".join(f"{key}={value}" for key, value in sorted(counter.items()))


def command_summary(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "command": result["command"],
        "return_code": result["return_code"],
        "elapsed_seconds": result["elapsed_seconds"],
    }


if __name__ == "__main__":
    raise SystemExit(main())
