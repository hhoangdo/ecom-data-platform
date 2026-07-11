import subprocess
from pathlib import Path


DEVELOP_ONLY_CANDIDATES = {
    "scripts/kafka/cleanup_kafka.py",
    "scripts/lakehouse/cleanup_lakehouse.py",
    "scripts/pinot/capture_evidence.py",
    "scripts/qa/upload_bronze.sh",
    "scripts/spark/capture_evidence.py",
}

REFERENCE_ROOTS = (
    "README.md",
    "deliverables",
    "tests",
    "infra",
    "src",
)

OFFICIAL_MACHINE_EVIDENCE_FILES = (
    "src/vina_bim_shop/kafka/evidence.py",
    "src/vina_bim_shop/flink/evidence.py",
    "src/vina_bim_shop/pinot/evidence.py",
    "src/vina_bim_shop/lakehouse/evidence.py",
    "src/vina_bim_shop/lakehouse/spark/evidence.py",
    "src/vina_bim_shop/orchestration/paths.py",
    "src/vina_bim_shop/orchestration/subprocess_helpers.py",
    "src/vina_bim_shop/orchestration/quality_helpers.py",
    "src/vina_bim_shop/orchestration/kafka_bootstrap.py",
    "src/vina_bim_shop/orchestration/pinot_bootstrap.py",
    "src/vina_bim_shop/orchestration/hourly_batch.py",
    "src/vina_bim_shop/orchestration/mini_coursework_pipeline.py",
    "src/vina_bim_shop/orchestration/reconciliation.py",
    "src/vina_bim_shop/orchestration/datahub_ingestion.py",
    "src/vina_bim_shop/orchestration/local_evidence.py",
    "scripts/kafka/capture_evidence.py",
    "scripts/flink/capture_evidence.py",
    "scripts/lakehouse/capture_evidence.py",
    "scripts/pinot/refresh_evidence.py",
)

FORBIDDEN_OFFICIAL_EVIDENCE_MARKERS = (
    "playwright",
    "npx",
    "ScreenshotCapturer",
    "screenshot_capturer",
    "screenshots/README.md",
    "_PLACEHOLDER_PNG",
    "--kafka-ui-url",
    "--flink-ui-url",
    "--minio-console-url",
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _tracked_scripts(repo_root: Path) -> list[str]:
    completed = subprocess.run(
        ["git", "ls-files", "scripts"],
        cwd=repo_root,
        text=True,
        capture_output=True,
        check=True,
    )
    script_suffixes = {".py", ".sh"}
    return sorted(line for line in completed.stdout.splitlines() if Path(line).suffix in script_suffixes)


def test_scripts_readme_documents_every_tracked_script() -> None:
    repo_root = _repo_root()
    inventory_path = repo_root / "scripts" / "README.md"

    assert inventory_path.is_file()

    inventory = inventory_path.read_text(encoding="utf-8")
    for marker in ["official-main", "develop-only candidate", "needs-review"]:
        assert marker in inventory

    for script_path in _tracked_scripts(repo_root):
        assert f"`{script_path}`" in inventory


def test_shell_scripts_are_pinned_to_lf_line_endings() -> None:
    repo_root = _repo_root()
    attributes = (repo_root / ".gitattributes").read_text(encoding="utf-8")

    assert "*.sh text eol=lf" in attributes

    completed = subprocess.run(
        ["git", "ls-files", "--eol", "*.sh"],
        cwd=repo_root,
        text=True,
        capture_output=True,
        check=True,
    )
    shell_script_lines = [line for line in completed.stdout.splitlines() if line.strip()]

    assert shell_script_lines
    assert all("w/lf" in line for line in shell_script_lines)


def test_root_readme_points_reviewers_to_script_inventory() -> None:
    repo_root = _repo_root()
    readme = (repo_root / "README.md").read_text(encoding="utf-8")

    assert "[Script inventory](scripts/README.md)" in readme


def test_develop_only_candidates_are_not_officially_referenced() -> None:
    repo_root = _repo_root()
    references: dict[str, list[str]] = {candidate: [] for candidate in DEVELOP_ONLY_CANDIDATES}

    for root in REFERENCE_ROOTS:
        path = repo_root / root
        files = [path] if path.is_file() else [candidate for candidate in path.rglob("*") if candidate.is_file()]
        for file_path in files:
            if file_path == Path(__file__):
                continue
            try:
                content = file_path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            relative_path = file_path.relative_to(repo_root).as_posix()
            for candidate in DEVELOP_ONLY_CANDIDATES:
                if candidate in content:
                    references[candidate].append(relative_path)

    assert references == {candidate: [] for candidate in DEVELOP_ONLY_CANDIDATES}


def test_official_machine_evidence_has_no_browser_or_screenshot_helpers() -> None:
    repo_root = _repo_root()

    offenders: dict[str, list[str]] = {}
    for relative_path in OFFICIAL_MACHINE_EVIDENCE_FILES:
        content = (repo_root / relative_path).read_text(encoding="utf-8")
        found = [marker for marker in FORBIDDEN_OFFICIAL_EVIDENCE_MARKERS if marker in content]
        if found:
            offenders[relative_path] = found

    assert offenders == {}
