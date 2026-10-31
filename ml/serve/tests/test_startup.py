"""Tests of the vLLM readiness wait, with a fake process, a fake clock and a
fake health probe. No GPU, vLLM, Modal or network."""

from __future__ import annotations

import subprocess
import unittest

from glimmer_serve.startup import EXITED, PROBE_EVERY_S, PROBE_TIMEOUT_S, TIMED_OUT, StartupError, stop, wait_ready

START = 1000.0


class FakeClock:
    """Monotonic time that moves only when the wait sleeps or a probe hangs."""

    def __init__(self) -> None:
        self.now = START
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class FakeProcess:
    """A vLLM server process. It exits with code once polled more than
    exits_after times, or never when exits_after is None, and may ignore
    SIGTERM."""

    def __init__(self, exits_after: int | None = None, code: int = 1, ignores_term: bool = False) -> None:
        self.exits_after = exits_after
        self.code = code
        self.ignores_term = ignores_term
        self.returncode: int | None = None
        self.polls = self.terminated = self.killed = self.waits = 0

    def poll(self) -> int | None:
        self.polls += 1
        if self.returncode is None and self.exits_after is not None and self.polls > self.exits_after:
            self.returncode = self.code
        return self.returncode

    def terminate(self) -> None:
        self.terminated += 1
        if not self.ignores_term:
            self.returncode = -15

    def kill(self) -> None:
        self.killed += 1
        self.returncode = -9

    def wait(self, timeout: float | None = None) -> int:
        self.waits += 1
        if self.returncode is None:
            raise subprocess.TimeoutExpired("vllm", timeout)
        return self.returncode


class FakeProbe:
    """A health probe that answers after ready_after probes, or never when
    ready_after is None. A hanging probe uses its whole timeout."""

    def __init__(self, clock: FakeClock, ready_after: int | None = None, hangs: bool = False) -> None:
        self.clock = clock
        self.ready_after = ready_after
        self.hangs = hangs
        self.timeouts: list[float] = []

    def __call__(self, timeout: float) -> bool:
        self.timeouts.append(timeout)
        if self.hangs:
            self.clock.now += timeout
        return self.ready_after is not None and len(self.timeouts) > self.ready_after


def wait(process: FakeProcess, probe: FakeProbe, clock: FakeClock, deadline_s: float) -> None:
    wait_ready(process, probe, deadline_s, clock=clock.clock, sleep=clock.sleep)


class TestWaitReady(unittest.TestCase):
    def test_a_server_that_already_exited_fails_at_once(self):
        for deadline in (60, 900, 1800):
            with self.subTest(deadline=deadline):
                clock, process = FakeClock(), FakeProcess(exits_after=0)
                probe = FakeProbe(clock)
                with self.assertRaises(StartupError) as ctx:
                    wait(process, probe, clock, deadline)
                self.assertEqual(str(ctx.exception), EXITED)
                self.assertEqual(clock.now, START, "none of the startup timeout is spent")
                self.assertEqual((probe.timeouts, clock.sleeps), ([], []))
                self.assertEqual((process.terminated, process.killed), (0, 0), "poll already reaped it")

    def test_a_server_that_exits_while_starting_fails_before_the_deadline(self):
        clock, process = FakeClock(), FakeProcess(exits_after=3)
        probe = FakeProbe(clock)
        with self.assertRaises(StartupError) as ctx:
            wait(process, probe, clock, 900)
        self.assertEqual(str(ctx.exception), EXITED)
        self.assertEqual(clock.now - START, 3 * PROBE_EVERY_S)
        self.assertEqual(len(probe.timeouts), 3)
        self.assertEqual((process.terminated, process.killed), (0, 0))

    def test_a_live_server_that_answers_is_ready(self):
        clock, process = FakeClock(), FakeProcess()
        probe = FakeProbe(clock, ready_after=2)
        wait(process, probe, clock, 900)
        self.assertEqual(len(probe.timeouts), 3)
        self.assertEqual(clock.sleeps, [PROBE_EVERY_S, PROBE_EVERY_S])
        self.assertIsNone(process.returncode, "a ready server keeps running")
        self.assertEqual((process.terminated, process.killed), (0, 0))

    def test_a_live_server_that_never_answers_is_stopped_at_the_deadline(self):
        clock, process = FakeClock(), FakeProcess()
        probe = FakeProbe(clock)
        with self.assertRaises(StartupError) as ctx:
            wait(process, probe, clock, 60)
        self.assertEqual(str(ctx.exception), TIMED_OUT)
        self.assertEqual(clock.now - START, 60, "never past the deadline")
        self.assertEqual(sum(clock.sleeps), 60)
        self.assertLessEqual(max(clock.sleeps), PROBE_EVERY_S)
        self.assertEqual((process.terminated, process.killed), (1, 0))
        self.assertEqual(process.returncode, -15, "stopped and reaped")
        self.assertGreaterEqual(process.waits, 1)

    def test_each_probe_and_pause_is_cut_to_the_time_left(self):
        for deadline, timeouts, sleeps in (
            (3, [3], []),
            (6, [PROBE_TIMEOUT_S], [1]),
            (7, [PROBE_TIMEOUT_S], [PROBE_EVERY_S]),
            (14, [PROBE_TIMEOUT_S, PROBE_TIMEOUT_S], [PROBE_EVERY_S, 2]),
        ):
            with self.subTest(deadline=deadline):
                clock, process = FakeClock(), FakeProcess()
                probe = FakeProbe(clock, hangs=True)
                with self.assertRaises(StartupError) as ctx:
                    wait(process, probe, clock, deadline)
                self.assertEqual(str(ctx.exception), TIMED_OUT)
                self.assertEqual(probe.timeouts, timeouts)
                self.assertEqual(clock.sleeps, sleeps)
                self.assertEqual(clock.now - START, deadline, "never past the deadline")
                self.assertEqual(process.terminated, 1)

    def test_a_server_that_ignores_sigterm_is_killed_and_reaped(self):
        clock, process = FakeClock(), FakeProcess(ignores_term=True)
        with self.assertRaises(StartupError):
            wait(process, FakeProbe(clock), clock, 4)
        self.assertEqual((process.terminated, process.killed), (1, 1))
        self.assertEqual(process.returncode, -9)
        self.assertEqual(process.waits, 2, "the timed-out wait, then the wait that reaps it")

    def test_stop_leaves_an_exited_process_alone(self):
        process = FakeProcess(exits_after=0)
        stop(process)
        self.assertEqual((process.terminated, process.killed, process.waits), (0, 0, 0))


if __name__ == "__main__":
    unittest.main()
