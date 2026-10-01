"""Self-owned resource sampling for a phase pod, read from the kernel, not from infra.

The point is to know *ourselves* which resource a pod hit -- disk-write throttling, the
memory limit, CPU throttling -- from the pod's own logs, without Cloud Monitoring, Managed
Prometheus, or exec-ing in. GKE nodes run cgroup v2, so the pod's own accounting is readable
at ``/sys/fs/cgroup/`` from inside the container; process I/O is in ``/proc/self/io``; and
free space on the scratch mount is a ``shutil.disk_usage`` away.

``monitor(label, temp_dir)`` wraps a stage: it logs one ``resource_sample`` JSON line every
``interval`` seconds and one ``resource_summary`` line on exit. Everything is best-effort --
on a dev box (macOS, or cgroup v1) the unreadable signals are simply omitted, so the same
code runs locally and in-cluster.

The signal that matters most is **``io_psi_full_avg10``** (from ``io.pressure``): sustained
high while CPU is near zero is the ``balance_dirty_pages`` write-throttle we were hitting --
the exact state that is invisible without this. See ``docs/gke-disk-io-findings.md``.

Grep a run with e.g. ``... | grep resource_summary`` for the one-line-per-stage verdict, or
``resource_sample`` for the time series.
"""

import contextlib
import json
import logging
import os
import shutil
import threading
import time
import typing

logger = logging.getLogger(__name__)

CGROUP_ROOT = "/sys/fs/cgroup"
DEFAULT_INTERVAL_SECONDS = 15.0


def _read_text(path: str) -> str | None:
    try:
        with open(path) as handle:
            return handle.read()
    except OSError:
        return None


def _read_int(path: str) -> int | None:
    text = _read_text(path)
    if text is None:
        return None
    text = text.strip()
    if not text or text == "max":
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _read_keyed(path: str) -> dict[str, int]:
    """Parse a ``key value`` file (``memory.events``, ``cpu.stat``) into ints."""
    out: dict[str, int] = {}
    for line in (_read_text(path) or "").splitlines():
        key, _, value = line.partition(" ")
        try:
            out[key] = int(value)
        except ValueError:
            continue
    return out


def _read_psi_full_avg10(path: str) -> float | None:
    """The ``full avg10`` field of a PSI file -- % of wall-clock all tasks stalled, last 10s."""
    for line in (_read_text(path) or "").splitlines():
        if not line.startswith("full "):
            continue
        for token in line.split():
            key, _, value = token.partition("=")
            if key == "avg10":
                try:
                    return float(value)
                except ValueError:
                    return None
    return None


def _proc_write_bytes() -> int | None:
    """This process's cumulative ``write_bytes`` -- the same counter the incident measured."""
    for line in (_read_text("/proc/self/io") or "").splitlines():
        if line.startswith("write_bytes:"):
            try:
                return int(line.split(":", 1)[1])
            except ValueError:
                return None
    return None


def _disk_used_pct(path: str) -> float | None:
    try:
        usage = shutil.disk_usage(path)
    except OSError:
        return None
    return 100.0 * usage.used / usage.total if usage.total else None


def _sample(temp_dir: str) -> dict[str, typing.Any]:
    """One snapshot. Keys with an unreadable source are omitted, not null-filled."""
    out: dict[str, typing.Any] = {}

    mem_current = _read_int(f"{CGROUP_ROOT}/memory.current")
    mem_max = _read_int(f"{CGROUP_ROOT}/memory.max")
    if mem_current is not None:
        out["mem_current_gib"] = round(mem_current / (1 << 30), 2)
        if mem_max:
            out["mem_pct"] = round(100.0 * mem_current / mem_max, 1)

    for field, path in (
        ("io_psi_full_avg10", f"{CGROUP_ROOT}/io.pressure"),
        ("mem_psi_full_avg10", f"{CGROUP_ROOT}/memory.pressure"),
        ("cpu_psi_full_avg10", f"{CGROUP_ROOT}/cpu.pressure"),
    ):
        value = _read_psi_full_avg10(path)
        if value is not None:
            out[field] = value

    write_bytes = _proc_write_bytes()
    if write_bytes is not None:
        out["write_bytes"] = write_bytes

    used_pct = _disk_used_pct(temp_dir)
    if used_pct is not None:
        out["localtmp_used_pct"] = round(used_pct, 1)

    return out


class _Sampler(threading.Thread):
    def __init__(self, label: str, temp_dir: str, interval: float) -> None:
        super().__init__(name="resource-monitor", daemon=True)
        self._label = label
        self._temp_dir = temp_dir
        self._interval = interval
        self._stop = threading.Event()
        self._start_time = time.monotonic()
        # Rolling extrema for the end-of-stage summary.
        self._peak_mem_pct = 0.0
        self._peak_io_psi = 0.0
        self._peak_localtmp_pct = 0.0
        self._first_write_bytes: int | None = None
        self._last_write_bytes: int | None = None

    def _observe(self, sample: dict[str, typing.Any]) -> None:
        self._peak_mem_pct = max(self._peak_mem_pct, sample.get("mem_pct", 0.0))
        self._peak_io_psi = max(self._peak_io_psi, sample.get("io_psi_full_avg10", 0.0))
        self._peak_localtmp_pct = max(
            self._peak_localtmp_pct, sample.get("localtmp_used_pct", 0.0)
        )
        write_bytes = sample.get("write_bytes")
        if write_bytes is not None:
            if self._first_write_bytes is None:
                self._first_write_bytes = write_bytes
            self._last_write_bytes = write_bytes

    def run(self) -> None:
        while not self._stop.is_set():
            sample = _sample(self._temp_dir)
            self._observe(sample)
            logger.info(
                "%s",
                json.dumps({"kind": "resource_sample", "label": self._label, **sample}),
            )
            self._stop.wait(self._interval)

    def stop_and_summarize(self) -> None:
        self._stop.set()
        self.join(timeout=self._interval + 5.0)
        summary: dict[str, typing.Any] = {
            "kind": "resource_summary",
            "label": self._label,
            "duration_s": round(time.monotonic() - self._start_time, 1),
            "peak_mem_pct": round(self._peak_mem_pct, 1),
            "peak_io_psi_full_avg10": round(self._peak_io_psi, 2),
            "peak_localtmp_used_pct": round(self._peak_localtmp_pct, 1),
        }
        if self._first_write_bytes is not None and self._last_write_bytes is not None:
            summary["total_write_gib"] = round(
                (self._last_write_bytes - self._first_write_bytes) / (1 << 30), 2
            )
        events = _read_keyed(f"{CGROUP_ROOT}/memory.events")
        # memory.events is cumulative for the pod's lifetime, not this stage -- but any
        # non-zero oom_kill/high on a batch pod that ran one stage is worth surfacing.
        for key in ("high", "max", "oom", "oom_kill"):
            if events.get(key):
                summary[f"mem_events_{key}"] = events[key]
        cpu = _read_keyed(f"{CGROUP_ROOT}/cpu.stat")
        if "throttled_usec" in cpu:
            summary["cpu_throttled_s"] = round(cpu["throttled_usec"] / 1e6, 1)
        logger.info("%s", json.dumps(summary))


@contextlib.contextmanager
def monitor(
    label: str,
    temp_dir: str | None = None,
    interval: float = DEFAULT_INTERVAL_SECONDS,
) -> typing.Iterator[None]:
    """Sample resources for the duration of the ``with`` block.

    ``temp_dir`` is the scratch mount to report free space for; defaults to ``$TMPDIR`` (the
    pod points this at ``/localtmp``) or ``/tmp``. Never raises: a monitor failure must not
    fail the phase, so the sampler thread swallows read errors and the summary is best-effort.
    """
    temp_dir = temp_dir or os.environ.get("TMPDIR") or "/tmp"
    sampler = _Sampler(label=label, temp_dir=temp_dir, interval=interval)
    sampler.start()
    try:
        yield
    finally:
        try:
            sampler.stop_and_summarize()
        except Exception:
            logger.exception("resource monitor summary failed (ignored)")
