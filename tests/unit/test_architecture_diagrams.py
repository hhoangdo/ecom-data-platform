import json
from pathlib import Path


def test_lambda_architecture_plantuml_names_major_components() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    diagram = repo_root / "architecture" / "diagrams" / "lambda_architecture.puml"

    content = diagram.read_text(encoding="utf-8")

    for expected in [
        "Kafka",
        "Spark",
        "Flink",
        "MinIO",
        "Hive Metastore",
        "Trino",
        "Apache Pinot",
        "DuckDB executive mart",
        "Executive Teams",
        "BI/Livestreaming Teams",
        "Schema Registry",
        "Kafka Connect",
        "Airflow",
        "Great Expectations",
        "DataHub",
    ]:
        assert expected in content
    assert "Parquet snapshots: checkpointed table state" in content
    assert "JSONL event log: replayable business events" in content
    assert "hourly batch reads landed Bronze data" in content
    assert "direct real-time consumption" in content
    assert "Hive Metastore\\ncatalog only" in content
    assert "Trino SQL Serving" in content
    assert "Realtime serving sink" in content
    assert "Apache Pinot" in content
    assert "DuckDB executive mart" in content
    assert "local KPI mart" in content
    assert "canonical hourly SQL" in content
    assert "Flink --> DerivedKafka" in content
    assert "DerivedKafka --> Realtime" in content
    assert "Realtime --> BI : live operations" in content
    assert "MinIO --> DuckDB" in content
    assert "DuckDB --> Executive" in content
    assert "checkpoints and audit JSONL" in content
    assert "canonical hourly SQL dashboards" in content
    assert "reconciled historical SQL" in content
    assert "table registration for curated lakehouse tables" in content
    assert "hourly batch inputs" not in content
    assert "Spark --> Executive" not in content
    assert "Flink --> BI" not in content
    assert "Flink --> Realtime" not in content
    assert "landed replayable event log (JSON)" not in content
    assert "Hive Metastore + Trino" not in content
    assert "curated streaming tables" not in content


def test_excalidraw_architecture_file_is_json_and_names_major_components() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    diagram = repo_root / "architecture" / "diagrams" / "architecture.excalidraw"

    data = json.loads(diagram.read_text(encoding="utf-8"))
    labels = json.dumps(data)

    assert data["type"] == "excalidraw"
    for expected in [
        "Kafka",
        "Spark",
        "Flink",
        "MinIO",
        "Hive Metastore",
        "Trino",
        "Apache Pinot",
        "DuckDB executive mart",
        "Executive Teams",
        "BI/Livestreaming Teams",
        "Schema Registry",
        "Kafka Connect",
        "Airflow",
        "Great Expectations",
        "DataHub",
    ]:
        assert expected in labels
    for expected in [
        "Periodic table-state exports",
        "Parquet snapshots: checkpointed table state",
        "JSONL event log: replayable business events",
        "hourly batch reads landed Bronze data",
        "direct real-time consumption",
        "Bronze batch: Parquet snapshots",
        "Bronze event: JSONL replay logs",
        "Silver/Gold: curated tables",
        "Hive Metastore\\ncatalog only",
        "Trino SQL Serving",
        "Realtime serving sink",
        "Apache Pinot",
        "DuckDB executive mart",
        "local KPI mart",
        "canonical hourly SQL",
        "Derived Kafka",
        "realtime_commerce_metrics_1m",
        "realtime_ops_alerts",
        "realtime_metric_corrections",
        "checkpoints and audit JSONL",
        "hourly export from",
        "metrics / alerts",
        "reconciled",
        "historical SQL",
        "table registration\\nfor curated lakehouse tables",
        "dbt-DuckDB",
        "local parity",
        "metadata, lineage, tags",
        "Evidence package",
        "Legend",
        "Solid black = primary business data / primary serving",
        "Dashed purple = metadata / lineage",
        "Dashed amber = orchestration / control",
        "Dashed green = validation / quality",
        "Dashed cyan = evidence / audit",
        "Dashed blue = optional local export",
        "Dashed gray = secondary historical support",
    ]:
        assert expected in labels
    assert "landed replayable event log (JSON)" not in labels
    assert "landed batch snapshots (Parquet)" not in labels
    assert "spark_to_exec" not in labels
    assert "Hive Metastore + Trino" not in labels
    assert "curated streaming sink outputs" not in labels
    assert "curated streaming tables" not in labels
    assert "Vina Bim Shop Section 01 Lambda Architecture" not in labels


def test_excalidraw_architecture_arrow_colors_encode_line_taxonomy() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    diagram = repo_root / "architecture" / "diagrams" / "architecture.excalidraw"

    data = json.loads(diagram.read_text(encoding="utf-8"))
    arrows = {
        element["id"]: element
        for element in data["elements"]
        if element["type"] == "arrow"
    }

    for arrow_id in [
        "generator_to_offline",
        "producer_to_kafka",
        "kafka_to_flink",
        "derived_kafka_to_pinot",
        "trino_to_exec",
    ]:
        assert arrows[arrow_id]["strokeStyle"] == "solid"
        assert arrows[arrow_id]["strokeColor"] == "#1e1e1e"

    expected_dashed_colors = {
        "trino_to_datahub": "#6d28d9",
        "airflow_to_spark": "#9a5a00",
        "airflow_to_gx": "#9a5a00",
        "airflow_to_datahub": "#9a5a00",
        "minio_to_dbt": "#15803d",
        "gx_to_datahub": "#15803d",
        "flink_to_minio_audit": "#0e7490",
        "pinot_to_evidence": "#0e7490",
        "trino_to_evidence": "#0e7490",
        "datahub_to_evidence": "#0e7490",
        "minio_to_duckdb": "#2563eb",
        "duckdb_to_exec": "#2563eb",
        "trino_to_bi": "#757575",
    }

    for arrow_id, color in expected_dashed_colors.items():
        assert arrows[arrow_id]["strokeStyle"] == "dashed"
        assert arrows[arrow_id]["strokeColor"] == color


def test_excalidraw_architecture_elements_include_required_fields() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    diagram = repo_root / "architecture" / "diagrams" / "architecture.excalidraw"

    data = json.loads(diagram.read_text(encoding="utf-8"))
    elements = data["elements"]

    assert elements, "architecture.excalidraw should contain at least one element"

    base_required = {
        "id",
        "type",
        "x",
        "y",
        "width",
        "height",
        "angle",
        "strokeColor",
        "backgroundColor",
        "fillStyle",
        "strokeWidth",
        "strokeStyle",
        "roughness",
        "opacity",
        "groupIds",
        "frameId",
        "roundness",
        "seed",
        "version",
        "versionNonce",
        "isDeleted",
        "boundElements",
        "updated",
        "link",
        "locked",
        "index",
    }

    for element in elements:
        assert base_required.issubset(element.keys()), f"missing base keys for {element.get('id')}"

        if element["type"] == "text":
            assert {
                "text",
                "fontSize",
                "fontFamily",
                "textAlign",
                "verticalAlign",
                "containerId",
                "originalText",
                "autoResize",
                "lineHeight",
                "baseline",
            }.issubset(element.keys())

        if element["type"] == "arrow":
            assert {
                "points",
                "lastCommittedPoint",
                "startBinding",
                "endBinding",
                "startArrowhead",
                "endArrowhead",
                "elbowed",
            }.issubset(element.keys())
