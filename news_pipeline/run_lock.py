from __future__ import annotations

import os
from pathlib import Path
from typing import BinaryIO, Optional, Union


class PipelineRunAlreadyActive(RuntimeError):
    """Raised when another full pipeline run holds the database lock."""


class PipelineRunLock:
    def __init__(self, db_path: Union[str, Path]) -> None:
        database_path = Path(db_path)
        self.path = database_path.with_name(
            f"{database_path.name}.run.lock"
        )
        self._handle: Optional[BinaryIO] = None

    def __enter__(self) -> "PipelineRunLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        try:
            _lock_nonblocking(handle)
        except OSError as exc:
            handle.close()
            raise PipelineRunAlreadyActive(
                "Another full pipeline run is already active for "
                f"{self.path.parent}"
            ) from exc
        self._handle = handle
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        handle = self._handle
        self._handle = None
        if handle is None:
            return
        try:
            handle.seek(0)
            _unlock(handle)
        finally:
            handle.close()


def _lock_nonblocking(handle: BinaryIO) -> None:
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        return
    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock(handle: BinaryIO) -> None:
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        return
    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def pipeline_run_lock(db_path: Union[str, Path]) -> PipelineRunLock:
    return PipelineRunLock(db_path)
