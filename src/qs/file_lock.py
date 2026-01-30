from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Optional


class AlreadyLockedError(RuntimeError):
    pass


def _try_lock(handle: IO[str]) -> None:
    if os.name == "nt":  # pragma: win32 cover
        import msvcrt

        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as exc:  # pragma: win32 cover
            raise AlreadyLockedError from exc
        return

    import fcntl

    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        raise AlreadyLockedError from exc


def _unlock(handle: IO[str]) -> None:
    if os.name == "nt":  # pragma: win32 cover
        import msvcrt

        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            return
        return

    import fcntl

    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    except OSError:
        return


@dataclass
class FileLock:
    path: Path
    timeout_s: float = 0.0
    poll_interval_s: float = 0.2
    _handle: Optional[IO[str]] = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = open(self.path, "a+", encoding="utf-8")
        start = time.time()
        while True:
            try:
                _try_lock(handle)
                break
            except AlreadyLockedError:
                if self.timeout_s <= 0 or (time.time() - start) >= self.timeout_s:
                    handle.close()
                    raise
                time.sleep(self.poll_interval_s)

        handle.seek(0)
        handle.truncate(0)
        handle.write(f"pid={os.getpid()}\n")
        handle.flush()
        self._handle = handle

    def release(self) -> None:
        if not self._handle:
            return
        try:
            _unlock(self._handle)
        finally:
            try:
                self._handle.close()
            finally:
                self._handle = None

    def __enter__(self) -> "FileLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        self.release()

