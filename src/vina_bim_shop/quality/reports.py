from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
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
    rows = "\n".join(
        [
            "<tr>"
            f"<td>{report.layer}</td>"
            f"<td>{report.suite_name}</td>"
            f"<td>{report.status}</td>"
            f"<td>{report.severity}</td>"
            f"<td>{'yes' if report.blocks_dag else 'no'}</td>"
            f"<td>{report.summary}</td>"
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
