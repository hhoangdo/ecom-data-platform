import json
from pathlib import Path


def test_capture_evidence_writes_lakehouse_manifest_and_artifacts(tmp_path: Path) -> None:
    from vina_bim_shop.lakehouse.evidence import capture_evidence

    def fake_get_json(url: str):
        if url.endswith("/v1/info"):
            return {"nodeVersion": {"version": "476"}}
        return {"status": "ok", "url": url}

    def fake_run_command(command):
        joined = " ".join(command)
        if "mc ls" in joined:
            return "[2026-06-01] bronze/\n[2026-06-01] silver/\n[2026-06-01] gold/\n[2026-06-01] checkpoints/\n[2026-06-01] evidence/\n"
        if "pg_isready" in joined:
            return "lakehouse-postgres:5432 - accepting connections\n"
        if "nc -z" in joined:
            return "hive-metastore:9083 reachable\n"
        if "SHOW CATALOGS" in joined:
            return "Catalog\niceberg\nsystem\n"
        if "SHOW SCHEMAS FROM iceberg" in joined:
            return "Schema\ninformation_schema\n"
        if "smoke.sql" in joined:
            return "check_id | row_count\nadr02 | 1\n"
        return ""

    manifest = capture_evidence(
        evidence_root=tmp_path,
        get_json=fake_get_json,
        get_http=lambda _url: {"status_code": 200, "body": ""},
        run_command=fake_run_command,
    )

    expected_files = [
        "minio_health.json",
        "minio_buckets.json",
        "postgres_health.txt",
        "hive_metastore_health.txt",
        "trino_info.json",
        "trino_catalogs.txt",
        "trino_schemas.txt",
        "trino_smoke_query.txt",
        "version_matrix.json",
        "run_manifest.json",
    ]
    for relative_path in expected_files:
        assert (tmp_path / relative_path).is_file()

    buckets = json.loads((tmp_path / "minio_buckets.json").read_text(encoding="utf-8"))
    assert buckets["required_buckets"] == ["bronze", "silver", "gold", "checkpoints", "evidence"]
    assert buckets["missing_buckets"] == []
    assert manifest["service_urls"]["trino"] == "http://localhost:8080"
    assert manifest["status"] == "success"
    assert manifest["failures"] == []
    assert "minio_console" not in manifest["service_urls"]
    assert not (tmp_path / "screenshots").exists()
    assert all(not artifact.startswith("screenshots/") for artifact in manifest["artifacts"])


def test_capture_evidence_writes_failed_manifest_when_required_buckets_are_missing(tmp_path: Path) -> None:
    import pytest

    from vina_bim_shop.lakehouse.evidence import capture_evidence

    def fake_get_json(url: str):
        if url.endswith("/v1/info"):
            return {"nodeVersion": {"version": "476"}}
        return {"status": "ok"}

    def fake_run_command(command):
        joined = " ".join(command)
        if "minio-init" in joined and "ls ALIAS" in joined:
            return "[2026-06-01] bronze/\n[2026-06-01] silver/\n"
        if "SHOW CATALOGS" in joined:
            return "Catalog\niceberg\nsystem\n"
        if "SHOW SCHEMAS FROM iceberg" in joined:
            return "Schema\ninformation_schema\n"
        if "smoke.sql" in joined:
            return "iceberg,information_schema\n"
        return "ok\n"

    with pytest.raises(RuntimeError, match="Missing required MinIO buckets"):
        capture_evidence(
            evidence_root=tmp_path,
            get_json=fake_get_json,
            get_http=lambda _url: {"status_code": 200, "body": ""},
            run_command=fake_run_command,
        )

    manifest = json.loads((tmp_path / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "failed"
    assert manifest["failures"] == [
        {"step": "minio_buckets", "error": "Missing required MinIO buckets: gold, checkpoints, evidence"}
    ]


def test_capture_evidence_lists_buckets_with_one_shot_minio_client(tmp_path: Path) -> None:
    from vina_bim_shop.lakehouse.evidence import capture_evidence

    commands = []

    def fake_get_json(url: str):
        if url.endswith("/v1/info"):
            return {"nodeVersion": {"version": "476"}}
        return {"status": "ok"}

    def fake_run_command(command):
        commands.append(command)
        joined = " ".join(command)
        if "minio-init" in joined and "ls ALIAS" in joined:
            return "[2026-06-01] bronze/\n[2026-06-01] silver/\n[2026-06-01] gold/\n[2026-06-01] checkpoints/\n[2026-06-01] evidence/\n"
        if "SHOW CATALOGS" in joined:
            return "Catalog\niceberg\nsystem\n"
        if "SHOW SCHEMAS FROM iceberg" in joined:
            return "Schema\ninformation_schema\n"
        if "smoke.sql" in joined:
            return "iceberg,information_schema\n"
        return "ok\n"

    capture_evidence(
        evidence_root=tmp_path,
        get_json=fake_get_json,
        get_http=lambda _url: {"status_code": 200, "body": ""},
        run_command=fake_run_command,
    )

    assert [
        "docker",
        "compose",
        "run",
        "--rm",
        "--no-deps",
        "--entrypoint",
        "/bin/sh",
        "minio-init",
        "-c",
        "mc alias set ALIAS http://minio:9000 \"${MINIO_ROOT_USER}\" \"${MINIO_ROOT_PASSWORD}\" >/dev/null && mc ls ALIAS",
    ] in commands


def test_bucket_listing_parser_ignores_minio_client_setup_output() -> None:
    from vina_bim_shop.lakehouse.evidence import parse_bucket_listing

    raw_listing = """Added `ALIAS` successfully.
Bucket created successfully `ALIAS/bronze`.
[2026-05-31 21:56:14 UTC]     0B bronze/
[2026-05-31 21:56:14 UTC]     0B silver/
"""

    assert parse_bucket_listing(raw_listing) == ["bronze", "silver"]


def test_capture_evidence_uses_compose_ps_for_hive_health(tmp_path: Path) -> None:
    from vina_bim_shop.lakehouse.evidence import capture_evidence

    commands = []

    def fake_get_json(url: str):
        if url.endswith("/v1/info"):
            return {"nodeVersion": {"version": "476"}}
        return {"status": "ok"}

    def fake_run_command(command):
        commands.append(command)
        joined = " ".join(command)
        if "minio-init" in joined and "ls ALIAS" in joined:
            return "[2026-06-01] bronze/\n[2026-06-01] silver/\n[2026-06-01] gold/\n[2026-06-01] checkpoints/\n[2026-06-01] evidence/\n"
        if "SHOW CATALOGS" in joined:
            return "Catalog\niceberg\nsystem\n"
        if "SHOW SCHEMAS FROM iceberg" in joined:
            return "Schema\ninformation_schema\n"
        if "smoke.sql" in joined:
            return "iceberg,information_schema\n"
        return "ok\n"

    capture_evidence(
        evidence_root=tmp_path,
        get_json=fake_get_json,
        get_http=lambda _url: {"status_code": 200, "body": ""},
        run_command=fake_run_command,
    )

    assert ["docker", "compose", "ps", "hive-metastore"] in commands
    assert all("nc -z" not in " ".join(command) for command in commands)


def test_http_capture_handles_empty_successful_health_response() -> None:
    from vina_bim_shop.lakehouse.evidence import get_http_artifact

    class FakeResponse:
        status_code = 200
        text = ""

        def raise_for_status(self):
            return None

        def json(self):
            raise ValueError("no json")

    artifact = get_http_artifact("http://localhost:9000/minio/health/ready", get=lambda _url, timeout: FakeResponse())

    assert artifact == {
        "url": "http://localhost:9000/minio/health/ready",
        "status_code": 200,
        "body": "",
    }


def test_smoke_sql_runs_trino_smoke_file() -> None:
    from vina_bim_shop.lakehouse.smoke import run_smoke_sql

    commands = []

    def fake_run_command(command):
        commands.append(command)
        return "SHOW CATALOGS\niceberg\n"

    output = run_smoke_sql(run_command=fake_run_command)

    assert output == "SHOW CATALOGS\niceberg\n"
    assert commands == [
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "trino",
            "trino",
            "--file",
            "/etc/trino/sql/smoke.sql",
        ]
    ]
