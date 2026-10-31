"""Run and drain one Spark query per partition scenario using Docker Compose."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import json
import os
from pathlib import Path
import subprocess
import time
from typing import Iterator
from uuid import uuid4

from .config import PROJECT_ROOT, RuntimeConfig
from .database import ExperimentStore


class SparkPipelineError(RuntimeError):
    pass


@dataclass(frozen=True)
class SparkRun:
    container: str
    topic: str
    checkpoint: str


class SparkPipeline:
    def __init__(self, store: ExperimentStore, runtime: RuntimeConfig,
                 project_root: Path = PROJECT_ROOT) -> None:
        self.store = store
        self.runtime = runtime
        self.project_root = project_root

    def _command(self, arguments: list[str], *, env=None, check=True, timeout=90):
        return subprocess.run(
            arguments, cwd=self.project_root, env=env, check=check,
            text=True, capture_output=True, timeout=timeout,
        )

    @contextmanager
    def session(self) -> Iterator[None]:
        self._command(["docker", "compose", "up", "-d", "--wait", "spark-master", "spark-worker"],
                      timeout=self.runtime.spark_startup_timeout_seconds)
        running = self._command([
            "docker", "compose", "--profile", "streaming", "ps", "--all",
            "--status", "running", "-q", "spark-submit",
        ]).stdout.split()
        regular = []
        for container in running:
            oneoff = self._command([
                "docker", "inspect", "--format",
                '{{index .Config.Labels "com.docker.compose.oneoff"}}', container,
            ]).stdout.strip().lower()
            if oneoff == "true":
                raise SparkPipelineError("Another Spark experiment is already running.")
            regular.append(container)
        try:
            if regular:
                print("Pausing the regular Spark job for the partition experiment...", flush=True)
                self._command(["docker", "stop", "--time", "60", *regular])
            yield
        finally:
            if regular:
                print("Restarting the original Spark container...", flush=True)
                self._command(["docker", "start", *regular])

    @contextmanager
    def scenario(self, topic: str, experiment_run_id: str) -> Iterator[SparkRun]:
        token = uuid4().hex
        pipeline = SparkRun(
            container=f"streaming-partition-{token[:16]}",
            topic=topic,
            checkpoint=("file:///opt/spark/work-dir/checkpoints/partition-experiments/"
                        f"{experiment_run_id}/{token}"),
        )
        env = dict(os.environ)
        env.update(
            SPARK_INPUT_TOPIC=topic,
            SPARK_CHECKPOINT_LOCATION=pipeline.checkpoint,
            SPARK_DRIVER_HOST=pipeline.container,
        )
        try:
            self._command([
                "docker", "compose", "--profile", "streaming", "run", "-d", "-T",
                "--no-deps", "--name", pipeline.container, "spark-submit",
            ], env=env, timeout=self.runtime.spark_startup_timeout_seconds)
            yield pipeline
        finally:
            state = self._state(pipeline)
            if state is not None:
                try:
                    self._command(["docker", "stop", "--time", "60", pipeline.container])
                    state = self._state(pipeline)
                    if state is not None and state["ExitCode"] != 0:
                        raise SparkPipelineError(
                            f"Spark job {pipeline.container} exited with code {state['ExitCode']}."
                        )
                finally:
                    self._command(["docker", "rm", "-f", pipeline.container])

    def _state(self, pipeline: SparkRun) -> dict | None:
        result = self._command([
            "docker", "inspect", "--format", "{{json .State}}", pipeline.container,
        ], check=False, timeout=10)
        if result.returncode:
            if "No such" in result.stderr:
                return None
            raise SparkPipelineError(f"Cannot inspect Spark job {pipeline.container}.")
        return json.loads(result.stdout)

    def wait_for_offsets(self, pipeline: SparkRun, target: dict[int, int], *,
                         startup: bool = False) -> dict[str, object]:
        timeout = (self.runtime.spark_startup_timeout_seconds if startup
                   else self.runtime.spark_drain_timeout_seconds)
        deadline = time.monotonic() + timeout
        snapshot = None
        while True:
            state = self._state(pipeline)
            if state is None or not state["Running"]:
                raise SparkPipelineError(f"Spark job for {pipeline.topic} is not running.")
            snapshot = self.store.pipeline_progress(pipeline.checkpoint, pipeline.topic)
            if snapshot:
                status = snapshot["effective_status"]
                if status in ("stopped", "failed", "superseded"):
                    raise SparkPipelineError(f"Spark run for {pipeline.topic} is {status}.")
                offsets = {int(p): offset for p, offset in snapshot["end_offsets"].items()}
                if status == "running" and target and all(
                    offsets.get(p, -1) >= end for p, end in target.items()
                ):
                    return snapshot
            if time.monotonic() >= deadline:
                observed = snapshot["end_offsets"] if snapshot else {}
                raise SparkPipelineError(
                    f"Timed out waiting for Spark on {pipeline.topic}: "
                    f"target offsets={target}, observed offsets={observed}."
                )
            time.sleep(1)
