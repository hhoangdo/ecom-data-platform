from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from html import escape
from pathlib import Path
from typing import Any


@dataclass
class ValidationReport:
    layer: str
    suite_name: str
    success: bool
    status: str
    severity: str
    blocks_dag: bool
    requires_quarantine: bool
    summary: str
    artifacts: list[str]
    details: dict[str, Any] = field(default_factory=dict)
    window: dict[str, str] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def write_validation_report(*, report: ValidationReport, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True), encoding="utf-8")
    return output_path


def render_validation_docs(*, reports: list[ValidationReport], docs_root: Path) -> Path:
    docs_root.mkdir(parents=True, exist_ok=True)
    reports_root = docs_root / "reports"
    reports_root.mkdir(exist_ok=True)
    for report in reports:
        _write_detail_page(report=report, output_path=reports_root / f"{report.suite_name}.html")

    rows = "\n".join(
        [
            "<tr>"
            f"<td>{escape(report.layer)}</td>"
            f'<td><a href="reports/{escape(report.suite_name)}.html">{escape(report.suite_name)}</a></td>'
            f"<td>{escape(report.status)}</td>"
            f"<td>{escape(report.severity)}</td>"
            f"<td>{'yes' if report.blocks_dag else 'no'}</td>"
            f"<td>{escape(report.summary)}</td>"
            "</tr>"
            for report in reports
        ]
    )
    index = f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>GX Data Docs</title>
    <style>
      body {{ font-family: Arial, sans-serif; margin: 2rem; background: #f6f4ef; color: #1f2933; }}
      h1 {{ margin-bottom: 0.5rem; }}
      table {{ width: 100%; border-collapse: collapse; background: white; }}
      th, td {{ border: 1px solid #d7dfe7; padding: 0.65rem; text-align: left; vertical-align: top; }}
      th {{ background: #e8eef5; }}
      .note {{ color: #52606d; margin-bottom: 1.5rem; }}
    </style>
  </head>
  <body>
    <h1>GX Data Docs</h1>
    <p class="note">Static validation summary for local ADR 06 orchestration runs.</p>
    <table>
      <thead>
        <tr>
          <th>Layer</th>
          <th>Suite</th>
          <th>Status</th>
          <th>Severity</th>
          <th>Blocks DAG</th>
          <th>Summary</th>
        </tr>
      </thead>
      <tbody>
        {rows}
      </tbody>
    </table>
  </body>
</html>
"""
    output = docs_root / "index.html"
    output.write_text(index, encoding="utf-8")
    return output


def _write_detail_page(*, report: ValidationReport, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    expectation_rows = _expectation_rows(report)
    window = report.window or {}
    window_text = " / ".join(
        part
        for part in [
            window.get("mode", ""),
            window.get("start_ts", ""),
            window.get("end_ts", ""),
        ]
        if part
    )
    detail = f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>{escape(report.suite_name)} - GX Detail</title>
    <style>
      body {{ font-family: Arial, sans-serif; margin: 2rem; background: #f6f4ef; color: #1f2933; }}
      h1 {{ margin-bottom: 0.5rem; }}
      table {{ width: 100%; border-collapse: collapse; background: white; margin-top: 1rem; }}
      th, td {{ border: 1px solid #d7dfe7; padding: 0.65rem; text-align: left; vertical-align: top; }}
      th {{ background: #e8eef5; }}
      .note {{ color: #52606d; margin-bottom: 1.5rem; }}
      .pass {{ color: #0f7a32; font-weight: 700; }}
      .fail {{ color: #a61b1b; font-weight: 700; }}
    </style>
  </head>
  <body>
    <p><a href="../index.html">Back to GX summary</a></p>
    <h1>{escape(report.suite_name)}</h1>
    <p class="note">{escape(report.summary)}</p>
    <table>
      <tbody>
        <tr><th>Layer</th><td>{escape(report.layer)}</td></tr>
        <tr><th>Status</th><td>{escape(report.status)}</td></tr>
        <tr><th>Severity</th><td>{escape(report.severity)}</td></tr>
        <tr><th>Blocks DAG</th><td>{'yes' if report.blocks_dag else 'no'}</td></tr>
        <tr><th>Requires Quarantine</th><td>{'yes' if report.requires_quarantine else 'no'}</td></tr>
        <tr><th>Window</th><td>{escape(window_text or 'n/a')}</td></tr>
      </tbody>
    </table>
    <h2>Expectation Details</h2>
    <table>
      <thead>
        <tr>
          <th>Status</th>
          <th>Expectation</th>
          <th>Column</th>
          <th>Parameters</th>
          <th>Observed Result</th>
        </tr>
      </thead>
      <tbody>
        {expectation_rows}
      </tbody>
    </table>
  </body>
</html>
"""
    output_path.write_text(detail, encoding="utf-8")


def _expectation_rows(report: ValidationReport) -> str:
    results = report.details.get("results", [])
    if not results:
        return '<tr><td colspan="5">No expectation-level details recorded.</td></tr>'

    rows: list[str] = []
    for result in results:
        config = result.get("expectation_config", {})
        kwargs = config.get("kwargs", {})
        observed = result.get("result", {})
        success = bool(result.get("success", False))
        status_class = "pass" if success else "fail"
        status_label = "pass" if success else "fail"
        parameters = ", ".join(
            f"{key}={value}" for key, value in kwargs.items() if key not in {"batch_id", "column"}
        )
        observed_summary = ", ".join(f"{key}={value}" for key, value in observed.items())
        rows.append(
            "<tr>"
            f'<td class="{status_class}">{status_label}</td>'
            f"<td>{escape(str(config.get('type', 'unknown')))}</td>"
            f"<td>{escape(str(kwargs.get('column', 'n/a')))}</td>"
            f"<td>{escape(parameters or 'n/a')}</td>"
            f"<td>{escape(observed_summary or 'n/a')}</td>"
            "</tr>"
        )
    return "\n".join(rows)
