from __future__ import annotations

import shutil
import subprocess
from collections.abc import Callable
from pathlib import Path

from vina_bim_shop.kafka.bootstrap import bootstrap_topics
from vina_bim_shop.kafka.topics import all_topic_names


CommandRunner = Callable[[list[str]], object]


def cleanup_kafka(
    *,
    runner: CommandRunner = subprocess.run,
    bootstrap_server: str = "kafka:29092",
    evidence_root: str | Path = "evidence/03_kafka_ingestion",
    clean_evidence: bool = False,
) -> None:
    for topic in all_topic_names():
        runner(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "kafka",
                "kafka-topics",
                "--bootstrap-server",
                bootstrap_server,
                "--delete",
                "--if-exists",
                "--topic",
                topic,
            ]
        )
    if clean_evidence:
        evidence_path = Path(evidence_root)
        if evidence_path.exists():
            shutil.rmtree(evidence_path)
    bootstrap_topics(runner=runner, bootstrap_server=bootstrap_server)
