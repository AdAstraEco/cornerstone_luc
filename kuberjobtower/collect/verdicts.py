"""What a pod's numbers and events mean, as plain verdicts (plan 02, section 7). Pure functions.

Every threshold is a constant below, calibrated on the single-tile runs of 29 Sept and 6 Oct 2026
and meant to be tuned after the next ones. Samples are the pod's own ``resource_sample`` records.
"""

import collections.abc
import dataclasses
import datetime
import statistics
import typing

from kuberjobtower.models import JobState, PodEvent, PodState

Sample = collections.abc.Mapping[str, typing.Any]
Severity = typing.Literal["info", "warn", "crit"]

# memory
CACHE_ONLY_MEM_PCT = 90.0  # memory.current that high with no pressure is page cache
CACHE_ONLY_PSI = 2.0
CACHE_ONLY_SAMPLES = 3
PRESSURE_ANON_PCT = 90.0  # of the limit, the heap that can be OOM-killed
PRESSURE_CRIT_ANON_PCT = 98.0
PRESSURE_PSI = 5.0
# memory.events max/high climb whenever page cache reaches the limit (an ingest pod with a 30%
# heap logged 21 of them), so they only count as pressure once the heap itself is large
PRESSURE_EVENTS_MIN_ANON_PCT = 50.0
# disk and cpu
DISK_IO_PSI = 20.0
DISK_IO_PSI_PAIR = 30.0
DISK_SHARE_OF_SAMPLES = 0.10
DISK_CPU_PSI_MAX = 10.0
TMP_WARN_PCT = 80.0
TMP_CRIT_PCT = 95.0
CPU_PSI = 20.0
CPU_PSI_SAMPLES = 3
CPU_THROTTLED_S = 30.0
IDLE_CORE_SHARE = 0.25
IDLE_MIN_SECONDS = 600.0
# lifecycle
PENDING_STALL_S = 600.0
DEADLINE_SHARE = 0.80


@dataclasses.dataclass(frozen=True)
class Verdict:
    code: str
    severity: Severity
    message: str


def _values(samples: collections.abc.Sequence[Sample], key: str) -> list[float]:
    return [float(s[key]) for s in samples if key in s]


def _consecutive(flags: collections.abc.Sequence[bool], n: int) -> bool:
    run = 0
    for flag in flags:
        run = run + 1 if flag else 0
        if run >= n:
            return True
    return False


def sample_verdicts(
    samples: collections.abc.Sequence[Sample],
    *,
    cpu_limit_cores: float | None = None,
) -> list[Verdict]:
    """Verdicts from a pod's time-ordered resource samples."""
    out: list[Verdict] = []
    if len(samples) < 2:
        return out
    mem_pct = _values(samples, "mem_pct")
    anon_pct = _values(samples, "mem_anon_pct")
    ooms = max(_values(samples, "mem_events_oom_kill") or [0])

    pressure = [
        s.get("mem_pct", 0) >= CACHE_ONLY_MEM_PCT and s.get("mem_psi_full_avg10", 0) >= PRESSURE_PSI
        for s in samples
    ]
    max_anon = max(anon_pct or [0])
    rising = any(
        (_values(samples, key) or [0])[-1] > (_values(samples, key) or [0])[0]
        for key in ("mem_events_max", "mem_events_high")
    )
    if (
        max_anon >= PRESSURE_ANON_PCT
        or _consecutive(pressure, 2)
        or (rising and max_anon >= PRESSURE_EVENTS_MIN_ANON_PCT)
        or ooms
    ):
        crit = max_anon >= PRESSURE_CRIT_ANON_PCT or ooms
        out.append(
            Verdict(
                "memory_pressure",
                "crit" if crit else "warn",
                f"heap peaked at {max_anon:.0f}% of the memory limit"
                + (f"; {int(ooms)} OOM kill(s) recorded" if ooms else ""),
            )
        )
    else:
        cached = [
            s.get("mem_pct", 0) >= CACHE_ONLY_MEM_PCT
            and s.get("mem_psi_full_avg10", 0) < CACHE_ONLY_PSI
            for s in samples
        ]
        if sum(cached) >= CACHE_ONLY_SAMPLES and not ooms:
            out.append(
                Verdict(
                    "cache_only_memory",
                    "info",
                    f"memory read {max(mem_pct):.0f}% but the heap peaked at "
                    f"{max_anon:.0f}%: page cache, not pressure",
                )
            )

    io = [s.get("io_psi_full_avg10", 0) for s in samples]
    cpu_psi = [s.get("cpu_psi_full_avg10", 0) for s in samples]
    busy = [v >= DISK_IO_PSI for v in io]
    # a brief spike is not a verdict: 10% of the samples, or two in a row at the higher level
    often = sum(busy) / len(io) >= DISK_SHARE_OF_SAMPLES
    twice = _consecutive([v >= DISK_IO_PSI_PAIR for v in io], 2)
    waiting_on_cpu = statistics.median(c for c, b in zip(cpu_psi, busy, strict=True) if b) if any(busy) else 0.0
    disk_bound = (often or twice) and waiting_on_cpu < DISK_CPU_PSI_MAX
    if disk_bound:
        out.append(
            Verdict(
                "disk_bound",
                "warn",
                f"I/O pressure was at least {DISK_IO_PSI:.0f} in {sum(busy) / len(io):.0%} of samples "
                f"(peak {max(io):.0f}) while the CPU was not starved: scratch disk throughput limits it",
            )
        )

    tmp = max(_values(samples, "localtmp_used_pct") or [0])
    if tmp >= TMP_WARN_PCT:
        out.append(
            Verdict(
                "tmp_disk_filling",
                "crit" if tmp >= TMP_CRIT_PCT else "warn",
                f"scratch volume reached {tmp:.0f}% full",
            )
        )

    throttled = _values(samples, "cpu_throttled_s")
    grew = bool(throttled) and throttled[-1] - throttled[0] >= CPU_THROTTLED_S
    if _consecutive([v >= CPU_PSI for v in cpu_psi], CPU_PSI_SAMPLES) or grew:
        out.append(Verdict("cpu_starved", "warn", "the pod waited for CPU; its CPU limit is too low"))

    usage = _values(samples, "cpu_usage_s")
    stamps = [s.get("time") for s in samples]
    if cpu_limit_cores and len(usage) > 1 and all(isinstance(t, str) for t in stamps):
        span = (
            datetime.datetime.fromisoformat(typing.cast(str, stamps[-1]))
            - datetime.datetime.fromisoformat(typing.cast(str, stamps[0]))
        ).total_seconds()
        cores = (usage[-1] - usage[0]) / span if span else 0.0
        if span > IDLE_MIN_SECONDS and cores < IDLE_CORE_SHARE * cpu_limit_cores and not disk_bound:
            out.append(
                Verdict(
                    "idle_cores",
                    "info",
                    f"used {cores:.1f} of {cpu_limit_cores:g} cores on average over {span / 60:.0f} min",
                )
            )
    return out


def pod_verdicts(
    pod: PodState,
    events: collections.abc.Sequence[PodEvent],
    *,
    now: datetime.datetime | None = None,
    deadline_s: float | None = None,
) -> list[Verdict]:
    """Verdicts from the pod's state and Kubernetes events."""
    mine = [e for e in events if e.pod == pod.name]
    out: list[Verdict] = []
    if pod.reason == "OOMKilled" or any(e.reason == "OOMKilling" for e in mine):
        out.append(Verdict("oom_killed", "crit", "the container hit its memory limit and was killed"))
    if pod.reason == "Evicted":
        detail = next((e.message for e in mine if e.reason == "Evicted"), "")
        out.append(
            Verdict(
                "evicted",
                "crit",
                "the kubelet evicted the pod" + (f": {detail.strip()}" if detail else ""),
            )
        )
    now = now or datetime.datetime.now(datetime.UTC)
    if (
        pod.phase == "Pending"
        and pod.created
        and (now - pod.created).total_seconds() > PENDING_STALL_S
        and any(e.reason == "FailedScheduling" for e in mine)
        and not any(e.reason == "TriggeredScaleUp" for e in mine)
    ):
        out.append(
            Verdict(
                "pending_stall",
                "warn",
                "pending for over 10 minutes, unschedulable, and no node-pool scale-up was triggered",
            )
        )
    if deadline_s and pod.phase == "Running" and pod.started:
        elapsed = (now - pod.started).total_seconds()
        if elapsed > DEADLINE_SHARE * deadline_s:
            out.append(
                Verdict(
                    "deadline_risk",
                    "warn",
                    f"running {elapsed / 60:.0f} min of a {deadline_s / 60:.0f} min deadline",
                )
            )
    return out


def job_verdicts(job: JobState) -> list[Verdict]:
    if job.failed_indexes:
        return [
            Verdict(
                "index_failed",
                "crit",
                f"index(es) {job.failed_indexes} failed for good: out of retries, or failed at "
                "once by the failure policy (an OOM kill is not retried)",
            )
        ]
    return []
