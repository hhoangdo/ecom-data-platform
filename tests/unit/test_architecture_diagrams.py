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
        "Executive Teams",
        "BI/Livestreaming Teams",
    ]:
        assert expected in content


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
        "Executive Teams",
        "BI/Livestreaming Teams",
    ]:
        assert expected in labels
