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
    assert "Flink --> Realtime" in content
    assert "Realtime --> BI : live operations" in content
    assert "MinIO --> DuckDB" in content
    assert "DuckDB --> Executive" in content
    assert "curated streaming tables" in content
    assert "canonical hourly SQL dashboards" in content
    assert "reconciled historical SQL" in content
    assert "table registration for curated lakehouse tables" in content
    assert "hourly batch inputs" not in content
    assert "Spark --> Executive" not in content
    assert "Flink --> BI" not in content
    assert "landed replayable event log (JSON)" not in content
    assert "Hive Metastore + Trino" not in content


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
        "curated streaming tables",
        "hourly export from",
        "metrics / alerts",
        "reconciled",
        "historical SQL",
        "table registration\\nfor curated lakehouse tables",
    ]:
        assert expected in labels
    assert "landed replayable event log (JSON)" not in labels
    assert "landed batch snapshots (Parquet)" not in labels
    assert "spark_to_exec" not in labels
    assert "Hive Metastore + Trino" not in labels
    assert "curated streaming sink outputs" not in labels


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
