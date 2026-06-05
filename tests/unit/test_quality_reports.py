from pathlib import Path

from vina_bim_shop.quality.reports import ValidationReport, render_validation_docs


def test_render_validation_docs_writes_expectation_detail_page(tmp_path: Path) -> None:
    report = ValidationReport(
        layer="gold_trino",
        suite_name="gold_trino_contract",
        success=True,
        status="success",
        severity="error",
        blocks_dag=False,
        requires_quarantine=False,
        summary="2/2 expectations passed.",
        artifacts=["quality/gold_trino_contract.json"],
        details={
            "results": [
                {
                    "success": True,
                    "expectation_config": {
                        "type": "expect_column_values_to_be_between",
                        "kwargs": {"column": "fact_order_rows", "min_value": 1},
                    },
                    "result": {"element_count": 1, "unexpected_count": 0},
                }
            ]
        },
        window={
            "start_ts": "2026-04-26T00:00:00Z",
            "end_ts": "2026-04-26T01:00:00Z",
            "mode": "hourly",
        },
    )

    render_validation_docs(reports=[report], docs_root=tmp_path)

    index = (tmp_path / "index.html").read_text(encoding="utf-8")
    detail_path = tmp_path / "reports" / "gold_trino_contract.html"
    detail = detail_path.read_text(encoding="utf-8")

    assert 'href="reports/gold_trino_contract.html"' in index
    assert detail_path.is_file()
    assert "expect_column_values_to_be_between" in detail
    assert "fact_order_rows" in detail
    assert "min_value" in detail
    assert "unexpected_count" in detail
