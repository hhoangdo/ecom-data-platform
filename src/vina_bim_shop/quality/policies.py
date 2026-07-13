"""Map deployable data-quality layer outcomes to orchestration gate behavior."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ValidationSeverity(str, Enum):
    WARNING = "warning"
    ERROR = "error"


@dataclass(frozen=True)
class GateOutcome:
    layer: str
    success: bool
    severity: ValidationSeverity
    blocks_dag: bool
    requires_quarantine: bool
    status: str


def gate_outcome_for_layer(layer: str, *, success: bool, critical: bool = False) -> GateOutcome:
    """Return the quality-gate policy for one layer result.

    The returned outcome states severity and DAG blocking behavior; unsupported layers
    raise ``ValueError`` so callers cannot silently apply an undefined policy.
    """

    normalized = layer.strip().lower()
    if success:
        severity = ValidationSeverity.WARNING if normalized == "bronze_raw" else ValidationSeverity.ERROR
        return GateOutcome(
            layer=normalized,
            success=True,
            severity=severity,
            blocks_dag=False,
            requires_quarantine=False,
            status="success",
        )

    if normalized == "bronze_raw":
        return GateOutcome(
            layer=normalized,
            success=False,
            severity=ValidationSeverity.WARNING,
            blocks_dag=False,
            requires_quarantine=True,
            status="warning",
        )

    if normalized in {"silver", "gold_trino"}:
        return GateOutcome(
            layer=normalized,
            success=False,
            severity=ValidationSeverity.ERROR,
            blocks_dag=True,
            requires_quarantine=False,
            status="failed",
        )

    if normalized == "datahub":
        return GateOutcome(
            layer=normalized,
            success=False,
            severity=ValidationSeverity.WARNING if not critical else ValidationSeverity.ERROR,
            blocks_dag=critical,
            requires_quarantine=False,
            status="failed" if critical else "warning",
        )

    if normalized == "pinot_queries":
        return GateOutcome(
            layer=normalized,
            success=False,
            severity=ValidationSeverity.WARNING,
            blocks_dag=False,
            requires_quarantine=False,
            status="warning",
        )

    raise ValueError(f"Unsupported validation layer: {layer}")


def should_fail_reconciliation(*, pinot_success: bool, reconciliation_success: bool) -> bool:
    return not reconciliation_success
