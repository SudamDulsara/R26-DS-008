"""
integration/upstream_runner.py
------------------------------
Runs the 3 upstream pipelines via subprocess with the exact commands
from the user's spec. No changes to upstream code.
"""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterator


@dataclass
class UpstreamConfig:
    name: str
    cwd: str | Path
    args: list[str]
    python_exe: str | None = None
    env: dict[str, str] = field(default_factory=dict)
    description: str = ""


@dataclass
class UpstreamResult:
    name: str
    success: bool
    exit_code: int | None
    started_at: str
    finished_at: str
    duration_sec: float
    stdout_tail: str = ""
    error_message: str = ""


def _venv_python(venv_dir: Path) -> str | None:
    if not venv_dir.exists():
        return None
    win = venv_dir / "Scripts" / "python.exe"
    if win.exists():
        return str(win)
    posix = venv_dir / "bin" / "python"
    if posix.exists():
        return str(posix)
    return None


def default_configs(repo_root: Path) -> list[UpstreamConfig]:
    root_venv_python = _venv_python(repo_root / ".venv")
    asr_dir = repo_root / "audio-text-project_1"
    asr_venv_python = _venv_python(asr_dir / ".venv")

    return [
        UpstreamConfig(
            name="news",
            cwd=repo_root,
            python_exe=root_venv_python,
            args=["-m", "news_pipeline", "run"],
            description="News crawler — writes news_pipeline.db",
        ),
        UpstreamConfig(
            name="ocr",
            cwd=repo_root / "ocr_pipeline",
            python_exe=root_venv_python,
            args=[
                "-m", "pipeline_b.generate",
                "--input", r"data\unseen_acts",
                "--corrector", "byt5",
                "--model", r"models\byt5-sinhala-ocr",
            ],
            env={
                "PYTHONIOENCODING": "utf-8",
                "HF_HUB_OFFLINE": "1",
            },
            description="OCR pipeline (ByT5 + Sinhala OCR model) — writes ocr_correction_pairs.db",
        ),
        UpstreamConfig(
            name="asr",
            cwd=asr_dir,
            python_exe=asr_venv_python,
            args=["main.py"],
            description="ASR pipeline — writes videos.db",
        ),
    ]


def run_one_streaming(config: UpstreamConfig) -> Iterator[tuple[str, str | UpstreamResult]]:
    started = datetime.utcnow()
    started_iso = started.isoformat() + "Z"

    python_exe = config.python_exe or sys.executable
    cmd = [python_exe, *config.args]

    env = os.environ.copy()
    env.update(config.env)
    env.setdefault("PYTHONUNBUFFERED", "1")

    yield ("line", f"$ cd {config.cwd}")
    yield ("line", f"$ {' '.join(cmd)}")

    stdout_lines: list[str] = []
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(config.cwd),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
    except FileNotFoundError as e:
        finished = datetime.utcnow()
        result = UpstreamResult(
            name=config.name,
            success=False,
            exit_code=None,
            started_at=started_iso,
            finished_at=finished.isoformat() + "Z",
            duration_sec=(finished - started).total_seconds(),
            error_message=f"Failed to launch: {e}",
        )
        yield ("line", f"[ERROR] {e}")
        yield ("result", result)
        return

    assert proc.stdout is not None
    for line in proc.stdout:
        line = line.rstrip("\r\n")
        stdout_lines.append(line)
        yield ("line", line)

    exit_code = proc.wait()
    finished = datetime.utcnow()
    tail = "\n".join(stdout_lines[-50:])

    result = UpstreamResult(
        name=config.name,
        success=(exit_code == 0),
        exit_code=exit_code,
        started_at=started_iso,
        finished_at=finished.isoformat() + "Z",
        duration_sec=(finished - started).total_seconds(),
        stdout_tail=tail,
        error_message="" if exit_code == 0 else f"Exited with code {exit_code}",
    )
    yield ("result", result)


def run_all_streaming(
    configs: list[UpstreamConfig],
) -> Iterator[tuple[str, str, str | UpstreamResult]]:
    for cfg in configs:
        for kind, payload in run_one_streaming(cfg):
            yield (cfg.name, kind, payload)
