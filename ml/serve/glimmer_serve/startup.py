"""Waiting for vLLM to become ready. Standard library only, so it is
tested without a GPU, vLLM or Modal.

The wait ends as soon as the server process exits, and never runs past
its deadline: each health probe and each pause is cut to the time left.
When the start fails, the server process, and only it, is stopped and
reaped. The error is fixed text: never the server's output, a payload or
a secret.
"""

from __future__ import annotations

import http.client
import subprocess
import time
import urllib.request
from collections.abc import Callable
from typing import Protocol

PROBE_TIMEOUT_S = 5.0
PROBE_EVERY_S = 2.0
STOP_GRACE_S = 10.0

EXITED = "vLLM exited before it was ready"
TIMED_OUT = "vLLM did not become ready in time"


class StartupError(RuntimeError):
    """vLLM did not become ready. Its message is EXITED or TIMED_OUT."""


class Process(Protocol):
    """The part of subprocess.Popen the wait uses."""

    def poll(self) -> int | None: ...

    def terminate(self) -> None: ...

    def kill(self) -> None: ...

    def wait(self, timeout: float | None = None) -> int: ...


def health_probe(port: int) -> Callable[[float], bool]:
    """A probe of vLLM's /health on localhost: true once it answers, false
    while it cannot, within the given timeout."""
    url = f"http://127.0.0.1:{port}/health"

    def ready(timeout: float) -> bool:
        try:
            with urllib.request.urlopen(url, timeout=timeout):  # noqa: S310 - fixed localhost URL
                return True
        except (OSError, http.client.HTTPException):
            return False

    return ready


def stop(process: Process, grace_s: float = STOP_GRACE_S) -> None:
    """Stops the server process and reaps it: SIGTERM, then SIGKILL if it
    is still running after the grace period."""
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=grace_s)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def wait_ready(
    process: Process,
    ready: Callable[[float], bool],
    deadline_s: float,
    *,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Returns once ready() holds while the process runs. Raises
    StartupError as soon as the process exits, which also reaps it, or at
    the deadline, after stopping the process."""
    end = clock() + deadline_s
    while True:
        if process.poll() is not None:
            raise StartupError(EXITED)
        left = end - clock()
        if left <= 0:
            break
        if ready(min(PROBE_TIMEOUT_S, left)):
            return
        left = end - clock()
        if left <= 0:
            break
        sleep(min(PROBE_EVERY_S, left))
    stop(process)
    raise StartupError(TIMED_OUT)
