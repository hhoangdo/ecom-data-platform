from __future__ import annotations

import subprocess
from collections.abc import Callable
from pathlib import Path

from vina_bim_shop.kafka.topics import load_topic_config


CommandRunner = Callable[[list[str]], object]


def bootstrap_topics(
    *,
    runner: CommandRunner = subprocess.run,
    bootstrap_server: str = "kafka:29092",
    topics_file: str | Path | None = None,
) -> None:
    config = load_topic_config(topics_file) if topics_file is not None else load_topic_config()
    topics = {**config["source_topics"], **config["derived_placeholder_topics"]}

    for topic, topic_config in topics.items():
        command = [
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            "kafka-topics",
            "--bootstrap-server",
            bootstrap_server,
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            str(topic_config["partitions"]),
            "--replication-factor",
            str(topic_config["replication_factor"]),
            "--config",
            f"cleanup.policy={topic_config['cleanup_policy']}",
        ]
        runner(command)
