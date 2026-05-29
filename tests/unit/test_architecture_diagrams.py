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
