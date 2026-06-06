import importlib
import importlib.util
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_bronze_module():
    spec = importlib.util.find_spec("vina_bim_shop.lakehouse.bronze")
    assert spec is not None, "Expected vina_bim_shop.lakehouse.bronze module for Bronze landing runtime helpers."
    return importlib.import_module("vina_bim_shop.lakehouse.bronze")


def _load_script_module(script_relative_path: str, module_name: str):
    script_path = _repo_root() / script_relative_path
    assert script_path.is_file(), f"Expected script at {script_relative_path}."
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_execute_batch_snapshot_uploads_runs_all_minio_copy_commands(tmp_path: Path) -> None:
    bronze = _load_bronze_module()
    assert hasattr(bronze, "execute_batch_snapshot_uploads"), (
        "Expected Bronze batch runtime helper `execute_batch_snapshot_uploads`."
    )

    raw_root = tmp_path / "raw"
    customers_file = raw_root / "customers" / "part-000.parquet"
    orders_file = raw_root / "orders" / "part-001.parquet"
    for path in [customers_file, orders_file]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("stub", encoding="utf-8")

    commands = []

    def runner(command: list[str]) -> None:
        commands.append(command)

    summary = bronze.execute_batch_snapshot_uploads(
        raw_root=raw_root,
        snapshot_date="2026-06-01",
        minio_alias="LOCAL",
        runner=runner,
    )

    assert commands == [
        [
            "mc",
            "cp",
            str(customers_file),
            "LOCAL/bronze/batch/customers/snapshot_date=2026-06-01/part-000.parquet",
        ],
        [
            "mc",
            "cp",
            str(orders_file),
            "LOCAL/bronze/batch/orders/snapshot_date=2026-06-01/part-001.parquet",
        ],
    ]
    assert summary == {
        "uploaded_files": 2,
        "snapshot_date": "2026-06-01",
        "minio_alias": "LOCAL",
        "destinations": [command[-1] for command in commands],
    }


def test_land_bronze_batch_script_parses_args_and_prints_summary(monkeypatch, capsys, tmp_path: Path) -> None:
    module = _load_script_module("scripts/lakehouse/land_bronze_batch.py", "land_bronze_batch_script")

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "land_bronze_batch.py",
            "--raw-root",
            str(tmp_path / "raw"),
            "--snapshot-date",
            "2026-06-01",
            "--minio-alias",
            "LOCAL",
        ],
    )
    args = module.parse_args()
    assert args.raw_root == str(tmp_path / "raw")
    assert args.snapshot_date == "2026-06-01"
    assert args.minio_alias == "LOCAL"

    calls = []

    def fake_execute_batch_snapshot_uploads(**kwargs):
        calls.append(kwargs)
        return {
            "uploaded_files": 2,
            "snapshot_date": kwargs["snapshot_date"],
            "minio_alias": kwargs["minio_alias"],
        }

    monkeypatch.setattr(module, "execute_batch_snapshot_uploads", fake_execute_batch_snapshot_uploads)
    module.main()

    assert calls == [
        {
            "raw_root": Path(tmp_path / "raw"),
            "snapshot_date": "2026-06-01",
            "minio_alias": "LOCAL",
            "use_docker_mc": False,
        }
    ]
    assert capsys.readouterr().out.strip() == "Uploaded 2 Bronze batch files for snapshot 2026-06-01 via LOCAL."


def test_execute_batch_snapshot_uploads_uses_docker_minio_client_fallback_when_requested(tmp_path: Path) -> None:
    bronze = _load_bronze_module()

    raw_root = tmp_path / "raw"
    customers_file = raw_root / "customers" / "part-000.parquet"
    customers_file.parent.mkdir(parents=True, exist_ok=True)
    customers_file.write_text("stub", encoding="utf-8")

    commands = []

    def runner(command: list[str]) -> None:
        commands.append(command)

    bronze.execute_batch_snapshot_uploads(
        raw_root=raw_root,
        snapshot_date="2026-06-01",
        minio_alias="LOCAL",
        runner=runner,
        use_docker_mc=True,
    )

    assert commands == [
        [
            "docker",
            "compose",
            "run",
            "--rm",
            "--no-deps",
            "-v",
            f"{raw_root.resolve()}:/workdir:ro",
            "--entrypoint",
            "/bin/sh",
            "minio-init",
            "-c",
            'mc alias set LOCAL http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc cp "/workdir/customers/part-000.parquet" "LOCAL/bronze/batch/customers/snapshot_date=2026-06-01/part-000.parquet"',
        ]
    ]
