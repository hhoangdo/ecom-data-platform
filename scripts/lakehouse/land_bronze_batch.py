from __future__ import annotations

import argparse
from pathlib import Path

from vina_bim_shop.lakehouse.bronze import execute_batch_snapshot_uploads


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload Bronze batch snapshot files to MinIO.")
    parser.add_argument("--raw-root", required=True)
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--minio-alias", required=True)
    parser.add_argument("--use-docker-mc", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = execute_batch_snapshot_uploads(
        raw_root=Path(args.raw_root),
        snapshot_date=args.snapshot_date,
        minio_alias=args.minio_alias,
        use_docker_mc=args.use_docker_mc,
    )
    print(
        f"Uploaded {summary['uploaded_files']} Bronze batch files for snapshot {summary['snapshot_date']} via {summary['minio_alias']}."
    )


if __name__ == "__main__":
    main()
