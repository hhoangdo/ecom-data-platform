from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _section(content: str, heading: str) -> str:
    start = content.index(heading)
    next_heading = content.find("\n## ", start + len(heading))
    if next_heading == -1:
        return content[start:]
    return content[start:next_heading]


def test_generator_cli_options_table_uses_one_row_per_choice() -> None:
    repo_root = _repo_root()
    content = (repo_root / "deliverables" / "01_data_generator.md").read_text(encoding="utf-8")

    assert "`--scale smoke|medium|coursework`" not in content
    assert "`--mode offline|streaming|full`" not in content

    for option in [
        "`--scale smoke`",
        "`--scale medium`",
        "`--scale coursework`",
        "`--mode offline`",
        "`--mode streaming`",
        "`--mode full`",
    ]:
        assert option in content


def test_schema_design_documents_data_format_rationale() -> None:
    repo_root = _repo_root()
    content = (repo_root / "deliverables" / "02_schema_design.md").read_text(encoding="utf-8")

    assert "## Data Type Rationale" not in content
    assert "## Data Format Rationale" in content

    section = _section(content, "## Data Format Rationale")
    for term in [
        "JSON",
        "JSONL",
        "Parquet",
        "Iceberg",
        "Pinot",
        "Trino",
        "DuckDB",
        "Schema Registry",
        "Bronze",
        "Silver",
        "Gold",
    ]:
        assert term in section


def test_masterplan_and_domain_docs_avoid_internal_adr_or_stale_phase_language() -> None:
    repo_root = _repo_root()
    docs = [repo_root / "architecture" / "masterplan.md"]
    docs.extend((repo_root / "architecture" / "domain").glob("*.md"))

    forbidden_phrases = [
        "architecture/decisions",
        "# ADR",
        "ADRs 01-08",
        "ADR 01",
        "ADR 02",
        "ADR 03",
        "ADR 04",
        "ADR 05",
        "ADR 06",
        "ADR 07",
        "ADR 08",
        "producer is deferred",
        "Runnable Kafka/Spark/Flink",
        "deferred to Section `02`",
        "Section `02` will own",
        "planned source datasets",
        "expected in Section `02`",
        "The durable decision record lives",
        "final Gold schemas remain a Section `02` task",
    ]

    for path in docs:
        content = path.read_text(encoding="utf-8")
        for phrase in forbidden_phrases:
            assert phrase not in content, f"{path.relative_to(repo_root)} still contains {phrase!r}"


def test_domain_docs_include_current_platform_concepts() -> None:
    repo_root = _repo_root()
    content = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (repo_root / "architecture" / "domain").glob("*.md")
    )

    for concept in [
        "dead_letter_events",
        "product_category_map",
        "realtime_commerce_metrics_1m",
        "agg_hourly_reconciled_kpi",
        "DuckDB Executive Mart",
        "DataHub",
    ]:
        assert concept in content


def test_diagram_notes_declare_reviewer_render_and_arrow_conventions() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    content = (repo_root / "architecture" / "diagrams" / "README.md").read_text(encoding="utf-8")

    for phrase in [
        "Reviewer-facing primary diagram",
        "detailed-architecture.svg",
        "detailed-architecture.excalidraw",
        "Arrow conventions",
        "source, target, and flow label",
        "left-to-right or top-to-bottom",
    ]:
        assert phrase in content
