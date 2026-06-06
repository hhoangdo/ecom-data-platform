import json
from pathlib import Path
from unittest.mock import Mock

import jsonschema

from vina_bim_shop.generators.runner import run_generation


NORMAL_TOPICS = ["commerce_events", "catalog_events", "fulfillment_events", "ops_events"]


def _load_schema(repo_root: Path, subject: str) -> dict:
    return json.loads((repo_root / "infra" / "kafka" / "schemas" / f"{subject}.schema.json").read_text(encoding="utf-8"))


def test_json_schemas_validate_generated_source_topic_events(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    raw_root = tmp_path / "raw"
    evidence_root = tmp_path / "evidence"

    run_generation(
        config_path=repo_root / "configs" / "generator" / "base.yaml",
        scale="smoke",
        mode="full",
        raw_root=raw_root,
        evidence_root=evidence_root,
        clean=True,
        seed=123,
    )

    for topic in NORMAL_TOPICS:
        schema = _load_schema(repo_root, f"{topic}-value")
        events_path = raw_root / "kafka_topics" / topic / "events.jsonl"
        for line in events_path.read_text(encoding="utf-8").splitlines()[:25]:
            event = json.loads(line)
            jsonschema.validate(event, schema)
            assert event["event_topic"] == topic


def test_dead_letter_schema_validates_current_dlq_shape(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    raw_root = tmp_path / "raw"
    evidence_root = tmp_path / "evidence"

    run_generation(
        config_path=repo_root / "configs" / "generator" / "base.yaml",
        scale="smoke",
        mode="full",
        raw_root=raw_root,
        evidence_root=evidence_root,
        clean=True,
        seed=123,
    )

    schema = _load_schema(repo_root, "dead_letter_events-value")
    events_path = raw_root / "kafka_topics" / "dead_letter_events" / "events.jsonl"
    events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]

    assert events
    for event in events:
        jsonschema.validate(event, schema)
        assert event["event_topic"] == "dead_letter_events"
        assert event["source_topic"]
        assert event["raw_payload"]


def test_schema_registry_registers_expected_json_schema_subjects(tmp_path: Path) -> None:
    from vina_bim_shop.kafka.schema_registry import register_schema_subjects

    repo_root = Path(__file__).resolve().parents[2]
    post = Mock()
    post.return_value.json.return_value = {"id": 1}
    post.return_value.raise_for_status.return_value = None

    responses = register_schema_subjects(
        registry_url="http://registry:8081",
        schemas_dir=repo_root / "infra" / "kafka" / "schemas",
        evidence_root=tmp_path,
        post=post,
    )

    subjects = [call.kwargs["json"]["subject"] for call in post.call_args_list]
    assert subjects == [
        "commerce_events-value",
        "catalog_events-value",
        "fulfillment_events-value",
        "ops_events-value",
        "dead_letter_events-value",
    ]
    assert all(call.kwargs["json"]["schemaType"] == "JSON" for call in post.call_args_list)
    assert responses == {subject: {"id": 1} for subject in subjects}
    assert (tmp_path / "schema_registry_subjects.json").is_file()
