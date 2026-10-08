"""Per-step resource use of one pod, from its own JSON log lines. No cluster access.

A pod logs a ``resource_sample`` every 15 s (memory, CPU, I/O pressure, bytes written) and also
marks its work: the phase script logs each step (``harmonize ...``, ``emit ...``,
``attribute ...``) and every cached stage logs ``Saving to ...<hash>.zarr`` and
``Finished writing to ...<hash>.zarr``. Joining the two by timestamp gives resources per step.
Needs the pod to log as JSON (``LOG_FORMAT=json``, which the Jobs built here set).
"""

import dataclasses
import datetime
import json
import re
import typing

# Loggers whose lines mark a step: the phase script, and export (which logs its own steps).
# Pipeline-specific guesses, used only when the pod logs no step events of its own. In a
# standalone kuber-job-tower these would come from the pipeline's descriptor.
MARKERS = ("__main__", "jdluc.export")
ZARR = re.compile(r"path_to_zarr=\S*?/([0-9a-f]+)\.zarr")


@dataclasses.dataclass(frozen=True)
class Step:
    label: str
    start: datetime.datetime
    end: datetime.datetime
    parent: str | None = None  # a zarr write inside a marked step


@dataclasses.dataclass(frozen=True)
class StepUse:
    step: Step
    samples: int
    peak_mem_gib: float | None
    peak_anon_gib: float | None
    cpu_cores: float | None  # mean over the step, from cumulative cpu seconds
    peak_io_psi: float | None
    written_gib: float | None
    peak_localtmp_pct: float | None

    @property
    def seconds(self) -> float:
        return (self.step.end - self.step.start).total_seconds()


def parse(lines: typing.Iterable[str]) -> list[dict[str, typing.Any]]:
    """The JSON records among ``lines``, in time order; text lines are ignored."""
    records = []
    for line in lines:
        # dask's progress bar redraws with \r and no newline, so a JSON line can follow it
        start = line.find('{"severity"')
        if start < 0:
            continue
        try:
            record = json.loads(line[start:])
        except json.JSONDecodeError:
            continue
        if "time" in record:
            record["_t"] = datetime.datetime.fromisoformat(record["time"])
            records.append(record)
    return sorted(records, key=lambda r: r["_t"])


def _windows(
    records: list[dict[str, typing.Any]],
) -> list[tuple[str, datetime.datetime, datetime.datetime]]:
    """(label, start, end) of each step: the pod's own step events if it logs any, else guessed."""
    last = records[-1]["_t"]
    events = [r for r in records if r.get("kind") == "step"]
    if events:
        # {"kind": "step", "event": "start" | "end", "name": ...}; a step with no end of its
        # own ends where the next one starts
        starts = [r for r in events if r.get("event") == "start"]
        out = []
        for i, mark in enumerate(starts):
            end = next(
                (
                    r["_t"]
                    for r in events
                    if r.get("event") == "end"
                    and r["name"] == mark["name"]
                    and r["_t"] >= mark["_t"]
                ),
                starts[i + 1]["_t"] if i + 1 < len(starts) else last,
            )
            out.append((str(mark["name"]), mark["_t"], end))
        return out
    marks = [
        r
        for r in records
        if r.get("logger") in MARKERS
        and not r["message"].startswith(("phase ", "Done", "Ingested "))
    ]
    return [
        (
            re.sub(r"\s+", " ", mark["message"]).strip(),
            mark["_t"],
            marks[i + 1]["_t"] if i + 1 < len(marks) else last,
        )
        for i, mark in enumerate(marks)
    ]


def steps(records: list[dict[str, typing.Any]]) -> list[Step]:
    """Steps with the cached-stage zarr writes inside them."""
    if not records:
        return []
    windows = _windows(records)
    if not windows:
        return [Step("whole pod (no marked steps)", records[0]["_t"], records[-1]["_t"])]
    out: list[Step] = []
    for label, start, end in windows:
        out.append(Step(label, start, end))
        started: dict[str, datetime.datetime] = {}
        for r in records:
            if not start <= r["_t"] <= end or r.get("logger") != "jdluc.storage":
                continue
            found = ZARR.search(r["message"])
            if not found:
                continue
            if r["message"].startswith("Saving to"):
                started[found[1]] = r["_t"]
            elif r["message"].startswith("Finished writing") and found[1] in started:
                out.append(
                    Step(f"write {found[1]}", started[found[1]], r["_t"], parent=label)
                )
    return out


def usage(records: list[dict[str, typing.Any]], step: Step) -> StepUse:
    inside = [
        r
        for r in records
        if r.get("kind") == "resource_sample" and step.start <= r["_t"] <= step.end
    ]

    def peak(key: str) -> float | None:
        values = [r[key] for r in inside if key in r]
        return max(values) if values else None

    def delta(key: str) -> float | None:
        values = [(r["_t"], r[key]) for r in inside if key in r]
        return values[-1][1] - values[0][1] if len(values) > 1 else None

    cpu = delta("cpu_usage_s")
    span = (
        (inside[-1]["_t"] - inside[0]["_t"]).total_seconds() if len(inside) > 1 else 0
    )
    written = delta("write_bytes")
    return StepUse(
        step=step,
        samples=len(inside),
        peak_mem_gib=peak("mem_current_gib"),
        peak_anon_gib=peak("mem_anon_gib"),
        cpu_cores=cpu / span if cpu is not None and span else None,
        peak_io_psi=peak("io_psi_full_avg10"),
        written_gib=written / (1 << 30) if written is not None else None,
        peak_localtmp_pct=peak("localtmp_used_pct"),
    )


def table(lines: typing.Iterable[str]) -> str:
    records = parse(lines)
    if not records:
        return "no JSON log lines (was the pod started with LOG_FORMAT=json, and is it still there?)"

    def f(v: float | None, spec: str = ".1f") -> str:
        return "-" if v is None else format(v, spec)

    rows = [
        f"{'step':58} {'time':>8} {'n':>3} {'mem GiB':>8} {'anon GiB':>9} {'cpu':>5} {'io psi':>6} {'wrote GiB':>9} {'tmp %':>5}"
    ]
    for step in steps(records):
        u = usage(records, step)
        label = ("  " if step.parent else "") + step.label
        minutes = f"{u.seconds / 60:.1f}m" if u.seconds >= 90 else f"{u.seconds:.0f}s"
        rows.append(
            f"{label[:58]:58} {minutes:>8} {u.samples:>3} {f(u.peak_mem_gib):>8} "
            f"{f(u.peak_anon_gib):>9} {f(u.cpu_cores):>5} {f(u.peak_io_psi):>6} "
            f"{f(u.written_gib):>9} {f(u.peak_localtmp_pct, '.0f'):>5}"
        )
    summary = [r for r in records if r.get("kind") == "resource_summary"]
    if summary:
        s = summary[-1]
        rows.append(
            f"whole pod: {s.get('duration_s', 0) / 60:.1f} min, peak memory {s.get('peak_mem_pct')}% "
            f"(anon {s.get('peak_mem_anon_pct')}%), peak io psi {s.get('peak_io_psi_full_avg10')}, "
            f"wrote {s.get('total_write_gib')} GiB"
            + (
                f", OOM events {s['mem_events_oom_kill']}"
                if s.get("mem_events_oom_kill")
                else ""
            )
        )
    return "\n".join(rows)
