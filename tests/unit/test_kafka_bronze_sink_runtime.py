import importlib
import importlib.util
import json
import re
import sys
from pathlib import Path

from compose_model import load_compose_model


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_bronze_sink_module():
    spec = importlib.util.find_spec("vina_bim_shop.kafka.bronze_sink")
    assert spec is not None, "Expected vina_bim_shop.kafka.bronze_sink module for Kafka Connect Bronze sink registration."
    return importlib.import_module("vina_bim_shop.kafka.bronze_sink")


def _load_script_module(script_relative_path: str, module_name: str):
    script_path = _repo_root() / script_relative_path
    assert script_path.is_file(), f"Expected script at {script_relative_path}."
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_register_bronze_sink_renders_template_and_posts_json_payload(tmp_path: Path) -> None:
    bronze_sink = _load_bronze_sink_module()
    assert hasattr(bronze_sink, "register_bronze_sink"), "Expected Kafka Connect helper `register_bronze_sink`."

    posted = []

    def fake_post(url: str, json: dict[str, object]) -> dict[str, object]:
        posted.append({"url": url, "json": json})
        return {"status_code": 201, "body": {"name": json["name"]}}

    response = bronze_sink.register_bronze_sink(
        connect_url="http://localhost:8083",
        template_path=tmp_path / "../infra/kafka/connect/source-events-s3-sink.template.json",
        connector_name="bronze-events-s3-sink",
        bronze_bucket="bronze",
        minio_endpoint="http://minio:9000",
        minio_region="us-east-1",
        minio_access_key="vina_minio",
        minio_secret_key="vina_minio_password",
        post=fake_post,
    )

    assert posted == [
        {
            "url": "http://localhost:8083/connectors",
            "json": {
                "name": "bronze-events-s3-sink",
                "config": {
                    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
                    "tasks.max": "1",
                    "topics": "commerce_events,catalog_events,fulfillment_events,ops_events,dead_letter_events",
                    "s3.bucket.name": "bronze",
                    "s3.region": "us-east-1",
                    "store.url": "http://minio:9000",
                    "aws.access.key.id": "vina_minio",
                    "aws.secret.access.key": "vina_minio_password",
                    "storage.class": "io.confluent.connect.s3.storage.S3Storage",
                    "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
                    "topics.dir": "events",
                    "path.format": "'ingest_date='YYYY-MM-dd",
                    "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
                    "partition.duration.ms": "86400000",
                    "locale": "en-US",
                    "timezone": "UTC",
                    "timestamp.extractor": "Record",
                    "flush.size": "1000",
                    "schema.compatibility": "NONE",
                    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                },
            },
        }
    ]
    assert response == {"status_code": 201, "body": {"name": "bronze-events-s3-sink"}}


def test_register_bronze_sink_updates_existing_connector_on_conflict(tmp_path: Path) -> None:
    bronze_sink = _load_bronze_sink_module()

    calls = []

    class ConflictResponse:
        status_code = 409

        def raise_for_status(self):
            raise AssertionError("Conflict response should be handled before raise_for_status().")

    class PutResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"name": "bronze-events-s3-sink", "config": {"topics.dir": "events"}}

    def fake_post(url: str, json: dict[str, object]):
        calls.append(("post", url, json))
        return ConflictResponse()

    def fake_put(url: str, json: dict[str, object]):
        calls.append(("put", url, json))
        return PutResponse()

    response = bronze_sink.register_bronze_sink(
        connect_url="http://localhost:8083",
        template_path=tmp_path / "../infra/kafka/connect/source-events-s3-sink.template.json",
        connector_name="bronze-events-s3-sink",
        bronze_bucket="bronze",
        minio_endpoint="http://minio:9000",
        minio_region="us-east-1",
        minio_access_key="vina_minio",
        minio_secret_key="vina_minio_password",
        post=fake_post,
        put=fake_put,
    )

    assert calls[0][0] == "post"
    assert calls[0][1] == "http://localhost:8083/connectors"
    assert calls[1][0] == "put"
    assert calls[1][1] == "http://localhost:8083/connectors/bronze-events-s3-sink/config"
    assert calls[1][2]["topics.dir"] == "events"
    assert response == {"name": "bronze-events-s3-sink", "config": {"topics.dir": "events"}}


def test_register_bronze_sink_script_writes_response_artifact(monkeypatch, capsys, tmp_path: Path) -> None:
    module = _load_script_module("scripts/kafka/register_bronze_sink.py", "register_bronze_sink_script")

    evidence_root = tmp_path / "evidence"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "register_bronze_sink.py",
            "--connect-url",
            "http://localhost:8083",
            "--template-path",
            "infra/kafka/connect/source-events-s3-sink.template.json",
            "--connector-name",
            "bronze-events-s3-sink",
            "--bronze-bucket",
            "bronze",
            "--minio-endpoint",
            "http://minio:9000",
            "--minio-region",
            "us-east-1",
            "--minio-access-key",
            "vina_minio",
            "--minio-secret-key",
            "vina_minio_password",
            "--evidence-root",
            str(evidence_root),
        ],
    )
    args = module.parse_args()
    assert args.connect_url == "http://localhost:8083"
    assert args.connector_name == "bronze-events-s3-sink"
    assert args.evidence_root == str(evidence_root)

    calls = []

    def fake_register_bronze_sink(**kwargs):
        calls.append(kwargs)
        return {
            "status_code": 201,
            "body": {"name": kwargs["connector_name"], "config": {"topics.dir": "events", "path.format": "'ingest_date='YYYY-MM-dd"}},
        }

    monkeypatch.setattr(module, "register_bronze_sink", fake_register_bronze_sink)
    module.main()

    assert calls == [
        {
            "connect_url": "http://localhost:8083",
            "template_path": Path("infra/kafka/connect/source-events-s3-sink.template.json"),
            "connector_name": "bronze-events-s3-sink",
            "bronze_bucket": "bronze",
            "minio_endpoint": "http://minio:9000",
            "minio_region": "us-east-1",
            "minio_access_key": "vina_minio",
            "minio_secret_key": "vina_minio_password",
        }
    ]

    artifact = evidence_root / "kafka_connect_bronze_sink_response.json"
    assert artifact.is_file()
    assert json.loads(artifact.read_text(encoding="utf-8")) == {
        "status_code": 201,
        "body": {"name": "bronze-events-s3-sink", "config": {"topics.dir": "events", "path.format": "'ingest_date='YYYY-MM-dd"}},
    }
    assert capsys.readouterr().out.strip() == "Registered Bronze sink bronze-events-s3-sink at http://localhost:8083."


def test_kafka_connect_service_uses_deterministic_custom_image_build() -> None:
    repo_root = _repo_root()
    compose = load_compose_model(repo_root)

    kafka_connect = compose["services"]["kafka-connect"]
    assert kafka_connect["build"] == {"context": "./infra/kafka/connect", "dockerfile": "Dockerfile"}
    assert kafka_connect["image"] == "vina-bim-shop/kafka-connect:7.8.3-s3"


def test_kafka_connect_plugin_installation_pins_explicit_s3_sink_version() -> None:
    dockerfile = (_repo_root() / "infra" / "kafka" / "connect" / "Dockerfile")
    assert dockerfile.is_file(), "Expected Kafka Connect Dockerfile under infra/kafka/connect/."

    contents = dockerfile.read_text(encoding="utf-8")
    assert re.search(r"confluentinc/kafka-connect-s3:\d+\.\d+\.\d+", contents), (
        "Expected Dockerfile to pin an explicit Confluent S3 sink plugin version."
    )
    assert re.search(
        r"^FROM confluentinc/cp-kafka-connect:7\.8\.3 AS plugin-builder$",
        contents,
        flags=re.MULTILINE,
    ), "Expected a named plugin-builder stage based on the pinned Kafka Connect image."
    assert (
        "confluent-hub install --no-prompt --component-dir /opt/connect-plugins "
        "confluentinc/kafka-connect-s3:10.6.4"
    ) in contents, "Expected the builder stage to install the pinned S3 sink outside the runtime image."
    assert "mkdir -p /opt/connect-plugins" in contents, (
        "Expected the builder stage to create the Confluent Hub component directory before installation."
    )
    assert contents.index("mkdir -p /opt/connect-plugins") < contents.index("confluent-hub install"), (
        "Expected the component directory to exist before Confluent Hub uses it."
    )
    assert (
        "COPY --from=plugin-builder /opt/connect-plugins /usr/share/confluent-hub-components"
    ) in contents, "Expected the runtime stage to copy only the installed plugin directory."
