from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_script_module():
    script_path = _repo_root() / "scripts" / "datahub" / "restore_search_indices.py"
    spec = importlib.util.spec_from_file_location("restore_search_indices_script", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Response:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self.payload


def test_restore_indices_reads_rows_migrated_from_restli_value_string() -> None:
    module = _load_script_module()

    assert module._restored_rows(
        {
            "value": "{args=RestoreIndicesArgs(start=0, batchSize=1000), result=RestoreIndicesResult(ignored=0, rowsMigrated=938, lastUrn=urn:li:tag:governance)}"
        }
    ) == 938


def test_restore_indices_paginates_and_requires_post_restore_search(monkeypatch, tmp_path: Path) -> None:
    module = _load_script_module()
    requests: list[dict[str, object]] = []
    responses = iter(({"rowsMigrated": 1000}, {"rowsMigrated": 2}))

    def fake_post(url: str, *, json: dict[str, object], timeout: int, headers: dict[str, str]):
        assert url == "http://localhost:8087/operations?action=restoreIndices"
        assert timeout == 60
        assert headers == {"Content-Type": "application/json"}
        requests.append(json)
        return _Response(next(responses))

    monkeypatch.setattr(module.requests, "post", fake_post)
    monkeypatch.setattr(
        module,
        "capture_search_evidence",
        lambda frontend_url: {"status": "success", "frontend_url": frontend_url},
    )

    result = module.restore_indices(
        "http://localhost:8087",
        "urn:li:%",
        1000,
        tmp_path,
        source_metadata_count=1002,
    )

    assert result["status"] == "success"
    assert result["restored_rows"] == 1002
    assert requests == [
        {"urnLike": "urn:li:%", "start": 0, "batchSize": 1000},
        {"urnLike": "urn:li:%", "start": 1000, "batchSize": 1000},
    ]
    assert json.loads((tmp_path / "restore_indices.json").read_text(encoding="utf-8"))["responses"] == [
        {"rowsMigrated": 1000},
        {"rowsMigrated": 2},
    ]


def test_restore_indices_rejects_repeated_full_page(monkeypatch, tmp_path: Path) -> None:
    module = _load_script_module()
    response = _Response({"rowsMigrated": 1000, "lastUrn": "urn:li:dataset:repeat"})
    monkeypatch.setattr(module.requests, "post", lambda *args, **kwargs: response)

    with pytest.raises(RuntimeError, match="repeated restore response"):
        module.restore_indices(
            "http://localhost:8087",
            "urn:li:%",
            1000,
            tmp_path,
            source_metadata_count=2000,
        )


def test_restore_indices_rejects_zero_restore_when_postgres_has_metadata(monkeypatch, tmp_path: Path) -> None:
    module = _load_script_module()
    monkeypatch.setattr(module.requests, "post", lambda *args, **kwargs: _Response({"rowsMigrated": 0}))

    with pytest.raises(RuntimeError, match="restored zero rows"):
        module.restore_indices(
            "http://localhost:8087",
            "urn:li:%",
            1000,
            tmp_path,
            source_metadata_count=1,
        )


def test_restore_indices_verifies_indexed_search_through_gms(monkeypatch, tmp_path: Path) -> None:
    module = _load_script_module()
    search_urls: list[str] = []

    monkeypatch.setattr(module.requests, "post", lambda *args, **kwargs: _Response({"rowsMigrated": 1}))
    monkeypatch.setattr(
        module,
        "capture_search_evidence",
        lambda gms_url: search_urls.append(gms_url) or {"status": "success"},
    )

    module.restore_indices(
        "http://localhost:8087",
        "urn:li:%",
        1000,
        tmp_path,
        source_metadata_count=1,
    )

    assert search_urls == ["http://localhost:8087"]


def test_restore_indices_waits_for_asynchronous_indexing(monkeypatch, tmp_path: Path) -> None:
    module = _load_script_module()
    search_results = iter(({"status": "failed", "failures": [{"label": "iceberg"}]}, {"status": "success"}))
    monkeypatch.setattr(module.requests, "post", lambda *args, **kwargs: _Response({"rowsMigrated": 1}))
    monkeypatch.setattr(module, "capture_search_evidence", lambda _: next(search_results))
    monkeypatch.setattr(module.time, "sleep", lambda _: None)

    result = module.restore_indices(
        "http://localhost:8087",
        "urn:li:%",
        1000,
        tmp_path,
        source_metadata_count=1,
    )

    assert result["search"]["attempt"] == 2
