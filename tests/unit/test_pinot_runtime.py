import importlib.util
import json
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_script_module(script_relative_path: str, module_name: str):
    script_path = _repo_root() / script_relative_path
    assert script_path.is_file(), f"Expected script at {script_relative_path}."
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_pinot_assets_exist_and_use_derived_topics_only() -> None:
    repo_root = _repo_root()

    expected_files = [
        repo_root / "infra" / "pinot" / "schemas" / "pinot_realtime_commerce_metrics_1m.json",
        repo_root / "infra" / "pinot" / "schemas" / "pinot_realtime_metric_corrections.json",
        repo_root / "infra" / "pinot" / "schemas" / "pinot_realtime_ops_alerts.json",
        repo_root / "infra" / "pinot" / "tables" / "pinot_realtime_commerce_metrics_1m_realtime.json",
        repo_root / "infra" / "pinot" / "tables" / "pinot_realtime_metric_corrections_realtime.json",
        repo_root / "infra" / "pinot" / "tables" / "pinot_realtime_ops_alerts_realtime.json",
        repo_root / "infra" / "pinot" / "sql" / "dashboard_pinot.sql",
        repo_root / "infra" / "pinot" / "sql" / "reconciliation_pinot.sql",
        repo_root / "infra" / "pinot" / "sql" / "reconciliation_trino.sql",
    ]
    for path in expected_files:
        assert path.is_file(), f"Missing Pinot asset: {path}"

    table_specs = {
        "pinot_realtime_commerce_metrics_1m": json.loads(
            (repo_root / "infra" / "pinot" / "tables" / "pinot_realtime_commerce_metrics_1m_realtime.json").read_text(
                encoding="utf-8"
            )
        ),
        "pinot_realtime_metric_corrections": json.loads(
            (repo_root / "infra" / "pinot" / "tables" / "pinot_realtime_metric_corrections_realtime.json").read_text(
                encoding="utf-8"
            )
        ),
        "pinot_realtime_ops_alerts": json.loads(
            (repo_root / "infra" / "pinot" / "tables" / "pinot_realtime_ops_alerts_realtime.json").read_text(
                encoding="utf-8"
            )
        ),
    }

    topics = {
        name: spec["ingestionConfig"]["streamIngestionConfig"]["streamConfigMaps"][0]["stream.kafka.topic.name"]
        for name, spec in table_specs.items()
    }
    assert topics == {
        "pinot_realtime_commerce_metrics_1m": "realtime_commerce_metrics_1m",
        "pinot_realtime_metric_corrections": "realtime_metric_corrections",
        "pinot_realtime_ops_alerts": "realtime_ops_alerts",
    }
    assert all("commerce_events" not in json.dumps(spec) for spec in table_specs.values())
    assert all("catalog_events" not in json.dumps(spec) for spec in table_specs.values())
    assert all("fulfillment_events" not in json.dumps(spec) for spec in table_specs.values())
    assert all("ops_events" not in json.dumps(spec) for spec in table_specs.values())


def test_pinot_dashboard_sql_uses_correction_contract() -> None:
    dashboard_sql = (_repo_root() / "infra" / "pinot" / "sql" / "dashboard_pinot.sql").read_text(encoding="utf-8")

    assert "effective_commerce_metrics" in dashboard_sql
    assert "pinot_realtime_metric_corrections" in dashboard_sql
    assert "max(correction_version)" in dashboard_sql.lower()
    assert "pinot_realtime_commerce_metrics_1m" in dashboard_sql


def test_bootstrap_helpers_apply_schemas_then_tables(tmp_path: Path) -> None:
    from vina_bim_shop.pinot.bootstrap import apply_assets

    calls = []

    def fake_request(method: str, path: str, *, payload=None):
        calls.append((method, path, payload))
        if path.endswith("/tables") and method == "GET":
            return {"tables": []}
        if path.endswith("/schemas") and method == "GET":
            return {"schemas": []}
        if path.startswith("/debug/tables/"):
            return [{"ingestionStatus": {"ingestionState": "HEALTHY"}}]
        if "/tables/" in path and path.endswith("/status"):
            return {"status": "ONLINE"}
        return {"status": "ok"}

    manifest = apply_assets(request=fake_request, evidence_root=tmp_path)

    schema_posts = [call for call in calls if call[0] == "POST" and call[1] == "/schemas"]
    table_posts = [call for call in calls if call[0] == "POST" and call[1] == "/tables"]
    assert len(schema_posts) == 3
    assert len(table_posts) == 3
    assert calls.index(schema_posts[-1]) < calls.index(table_posts[0])
    assert manifest["tables"] == [
        "pinot_realtime_commerce_metrics_1m",
        "pinot_realtime_metric_corrections",
        "pinot_realtime_ops_alerts",
    ]


def test_bootstrap_tolerates_flaky_controller_status_endpoints(monkeypatch, tmp_path: Path) -> None:
    from requests import HTTPError

    from vina_bim_shop.pinot.bootstrap import apply_assets

    calls = []

    def fake_request(method: str, path: str, *, payload=None):
        calls.append((method, path))
        if path == "/schemas":
            return {"schemas": []}
        if path == "/tables":
            return {"tables": []}
        if path.startswith("/debug/tables/"):
            raise HTTPError("debug endpoint failed")
        if path.endswith("/status"):
            raise HTTPError("status endpoint failed")
        return {"status": "ok"}

    monkeypatch.setattr("vina_bim_shop.pinot.bootstrap.time.sleep", lambda _seconds: None)
    manifest = apply_assets(request=fake_request, evidence_root=tmp_path)

    assert manifest["table_status"]["pinot_realtime_commerce_metrics_1m"]["status"] == "unverified"
    assert any(path.startswith("/debug/tables/") for _method, path in calls)


def test_query_examples_write_pinot_and_trino_outputs(tmp_path: Path, monkeypatch) -> None:
    from vina_bim_shop.pinot.query_examples import run_query_examples

    def fake_execute_pinot_query(name: str, query: str, *, broker_url: str):
        return {"name": name, "query": query, "broker_url": broker_url, "resultTable": {"rows": [[1]]}}

    def fake_execute_trino_query(query: str, *, trino_url: str, user: str):
        return {"query": query, "columns": ["metric_hour"], "rows": [["2026-05-01T10:00:00Z"]], "stats": {}}

    monkeypatch.setattr("vina_bim_shop.pinot.query_examples.execute_pinot_query", fake_execute_pinot_query)
    monkeypatch.setattr("vina_bim_shop.pinot.query_examples.execute_trino_query", fake_execute_trino_query)

    manifest = run_query_examples(evidence_root=tmp_path)

    assert (tmp_path / "query_outputs" / "pinot_dashboard_results.json").is_file()
    assert (tmp_path / "query_outputs" / "trino_reconciliation_results.json").is_file()
    assert (tmp_path / "query_outputs" / "reconciliation_report.md").is_file()
    assert "query_outputs/reconciliation_report.md" in manifest["artifacts"]


def test_query_examples_use_committed_correction_aware_sql_contracts(tmp_path: Path, monkeypatch) -> None:
    from vina_bim_shop.pinot.query_examples import run_query_examples

    observed_queries: list[tuple[str, str]] = []

    def fake_execute_pinot_query(name: str, query: str, *, broker_url: str):
        observed_queries.append((name, query))
        return {"name": name, "query": query, "broker_url": broker_url, "resultTable": {"rows": [[1, 2, 3]]}}

    def fake_execute_trino_query(query: str, *, trino_url: str, user: str):
        return {"query": query, "columns": ["metric_hour"], "rows": [["2026-05-01T10:00:00Z"]], "stats": {}}

    monkeypatch.setattr("vina_bim_shop.pinot.query_examples.execute_pinot_query", fake_execute_pinot_query)
    monkeypatch.setattr("vina_bim_shop.pinot.query_examples.execute_trino_query", fake_execute_trino_query)

    run_query_examples(evidence_root=tmp_path)

    query_texts = [query for _name, query in observed_queries]
    assert any("effective_commerce_metrics" in query for query in query_texts)
    assert any("latest_corrections" in query for query in query_texts)
    assert any("pinot_realtime_metric_corrections" in query for query in query_texts)


def test_refresh_evidence_runs_bootstrap_queries_and_capture_in_order(tmp_path: Path, monkeypatch) -> None:
    from vina_bim_shop.pinot.refresh_evidence import refresh_evidence

    calls: list[tuple[str, str]] = []

    def fake_require_http_json(url: str, service_name: str) -> None:
        calls.append(("require", service_name))

    def fake_apply_assets(**kwargs):
        calls.append(("apply", str(kwargs["evidence_root"])))
        return {"tables": ["pinot_realtime_commerce_metrics_1m"]}

    def fake_run_query_examples(**kwargs):
        calls.append(("query", str(kwargs["evidence_root"])))
        return {"artifacts": ["query_outputs/reconciliation_report.md"]}

    def fake_capture_evidence(**kwargs):
        calls.append(("capture", str(kwargs["evidence_root"])))
        return {"artifacts": ["run_manifest.json"]}

    monkeypatch.setattr("vina_bim_shop.pinot.refresh_evidence._require_http_json", fake_require_http_json)
    monkeypatch.setattr("vina_bim_shop.pinot.refresh_evidence.apply_assets", fake_apply_assets)
    monkeypatch.setattr("vina_bim_shop.pinot.refresh_evidence.run_query_examples", fake_run_query_examples)
    monkeypatch.setattr("vina_bim_shop.pinot.refresh_evidence.capture_evidence", fake_capture_evidence)

    manifest = refresh_evidence(evidence_root=tmp_path)

    assert calls == [
        ("require", "Pinot controller"),
        ("require", "Pinot broker"),
        ("require", "Trino"),
        ("apply", str(tmp_path)),
        ("query", str(tmp_path)),
        ("capture", str(tmp_path)),
    ]
    assert "query_outputs/reconciliation_report.md" in manifest["artifacts"]
    assert "run_manifest.json" in manifest["artifacts"]


def test_refresh_evidence_accepts_plain_text_pinot_health_payloads(monkeypatch) -> None:
    from vina_bim_shop.pinot.refresh_evidence import _require_http_json

    class _Response:
        def __init__(self, *, text: str, content_type: str):
            self.status_code = 200
            self.text = text
            self.content = text.encode("utf-8")
            self.headers = {"content-type": content_type}

        def raise_for_status(self) -> None:
            return None

        def json(self):
            raise ValueError("not json")

    monkeypatch.setattr(
        "vina_bim_shop.pinot.refresh_evidence.requests.get",
        lambda *_args, **_kwargs: _Response(text="OK", content_type="text/plain"),
    )

    payload = _require_http_json("http://localhost:9003/health", "Pinot controller")

    assert payload == {"text": "OK"}


def test_load_trino_query_strips_terminal_semicolon() -> None:
    from vina_bim_shop.pinot.query_examples import _load_trino_query

    query = _load_trino_query("2026-05-01T10:00:00+00:00", "2026-05-01T11:00:00+00:00")

    assert not query.rstrip().endswith(";")


def test_capture_evidence_writes_manifest_and_service_artifacts(tmp_path: Path) -> None:
    from vina_bim_shop.pinot.evidence import capture_evidence

    def fake_get_json(url: str):
        if url.endswith("/health"):
            return {"status": "GOOD"}
        if url.endswith("/tables"):
            return {"tables": ["pinot_realtime_commerce_metrics_1m_REALTIME", "pinot_realtime_ops_alerts_REALTIME"]}
        if "/status" in url:
            return {"status": "ONLINE"}
        if "consumingSegmentsInfo" in url:
            return {"tableName": "pinot_realtime_commerce_metrics_1m_REALTIME"}
        return {"url": url}

    def fake_post_json(url: str, payload: dict):
        return {"resultTable": {"dataSchema": {"columnNames": ["count"]}, "rows": [[1]]}, "sql": payload["sql"]}

    manifest = capture_evidence(
        evidence_root=tmp_path,
        get_json=fake_get_json,
        post_json=fake_post_json,
    )

    for relative_path in [
        "controller_health.json",
        "broker_health.json",
        "table_inventory.json",
        "table_status.json",
        "consuming_segments.json",
        "row_counts.json",
        "version_matrix.json",
        "run_manifest.json",
    ]:
        assert (tmp_path / relative_path).is_file()
    assert manifest["service_urls"]["pinot_controller"] == "http://localhost:9003"
    assert not (tmp_path / "screenshots").exists()
    assert all(not artifact.startswith("screenshots/") for artifact in manifest["artifacts"])


def test_refresh_evidence_script_parses_args_and_prints_summary(monkeypatch, capsys, tmp_path: Path) -> None:
    module = _load_script_module("scripts/pinot/refresh_evidence.py", "pinot_refresh_evidence_script")

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "refresh_evidence.py",
            "--controller-url",
            "http://localhost:9003",
            "--broker-url",
            "http://localhost:8000",
            "--trino-url",
            "http://localhost:8080",
            "--evidence-root",
            str(tmp_path),
        ],
    )
    args = module.parse_args()
    assert args.controller_url == "http://localhost:9003"
    assert args.broker_url == "http://localhost:8000"
    assert args.trino_url == "http://localhost:8080"
    assert args.evidence_root == str(tmp_path)

    monkeypatch.setattr(
        module,
        "refresh_evidence",
        lambda **kwargs: {"artifacts": ["query_outputs/reconciliation_report.md", "run_manifest.json"]},
    )
    module.main()

    assert capsys.readouterr().out.strip() == "Refreshed 2 official Pinot evidence artifacts."


def test_bootstrap_script_parses_args_and_prints_summary(monkeypatch, capsys, tmp_path: Path) -> None:
    module = _load_script_module("scripts/pinot/bootstrap.py", "pinot_bootstrap_script")

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "bootstrap.py",
            "--controller-url",
            "http://localhost:9003",
            "--evidence-root",
            str(tmp_path),
        ],
    )
    args = module.parse_args()
    assert args.controller_url == "http://localhost:9003"
    assert args.evidence_root == str(tmp_path)

    monkeypatch.setattr(
        module,
        "apply_assets",
        lambda **kwargs: {"tables": ["pinot_realtime_commerce_metrics_1m", "pinot_realtime_metric_corrections", "pinot_realtime_ops_alerts"]},
    )
    module.main()

    assert capsys.readouterr().out.strip() == "Applied 3 Pinot tables."
