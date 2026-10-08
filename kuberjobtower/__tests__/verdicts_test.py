import datetime
import json
import pathlib

import pytest

from kuberjobtower.collect import events as collect_events
from kuberjobtower.collect import verdicts
from kuberjobtower.models import JobState, PodEvent, PodState

DATA = pathlib.Path(__file__).parent / "data"
UTC = datetime.UTC


def samples(name: str) -> list[dict[str, object]]:
    return [json.loads(line) for line in (DATA / name).read_text().splitlines()]


def codes(found: list[verdicts.Verdict]) -> list[str]:
    return [v.code for v in found]


# ---- the real runs the thresholds were calibrated on -----------------------------------------


def test_sri_lanka_compute_is_memory_pressure_because_the_heap_hit_96_percent() -> None:
    found = verdicts.sample_verdicts(samples("lka1_compute_samples.ndjson"))
    assert [(v.code, v.severity) for v in found] == [("memory_pressure", "warn")]
    assert "96%" in found[0].message


def test_sri_lanka_export_reads_full_but_it_is_only_page_cache() -> None:
    found = verdicts.sample_verdicts(samples("lka1_export_samples.ndjson"))
    assert [(v.code, v.severity) for v in found] == [("cache_only_memory", "info")]


def test_sri_lanka_ingest_uses_one_core_and_is_not_disk_bound() -> None:
    found = verdicts.sample_verdicts(
        samples("lka1_ingest-tiles_samples.ndjson"), cpu_limit_cores=4
    )
    assert codes(found) == ["idle_cores"]


def test_the_honduras_ingest_that_started_the_disk_theory_is_not_disk_bound_either() -> None:
    """Peak I/O pressure was 59.6, but only 3 of 275 samples reached 20 and the two at 30 or
    more were not adjacent; at about 26 MiB/s written the 288 MiB/s volume was never the limit."""
    rows = samples("hnd2_ingest-tiles_samples.ndjson")
    assert max(r["io_psi_full_avg10"] for r in rows) == 59.6  # type: ignore[type-var]
    assert sum(r["io_psi_full_avg10"] >= 20 for r in rows) == 3  # type: ignore[operator]
    assert "disk_bound" not in codes(verdicts.sample_verdicts(rows))


# ---- the rules, on made-up samples ------------------------------------------------------------


def make(n: int = 20, **kw: object) -> list[dict[str, object]]:
    base = {
        "mem_pct": 40.0, "mem_anon_pct": 20.0, "mem_psi_full_avg10": 0.0,
        "cpu_psi_full_avg10": 0.0, "io_psi_full_avg10": 0.0, "localtmp_used_pct": 5.0,
    }  # fmt: skip
    return [{**base, **kw} for _ in range(n)]


def test_disk_bound_needs_10_percent_of_samples_or_two_adjacent_high_ones() -> None:
    often = make() ; [s.update(io_psi_full_avg10=25.0) for s in often[:3]]  # 15%
    assert "disk_bound" in codes(verdicts.sample_verdicts(often))
    twice = make(); twice[4]["io_psi_full_avg10"] = twice[5]["io_psi_full_avg10"] = 35.0
    assert "disk_bound" in codes(verdicts.sample_verdicts(twice))
    spike = make(); spike[4]["io_psi_full_avg10"] = 59.0
    assert "disk_bound" not in codes(verdicts.sample_verdicts(spike))
    starved = make(cpu_psi_full_avg10=40.0); [s.update(io_psi_full_avg10=30.0) for s in starved]
    assert "disk_bound" not in codes(verdicts.sample_verdicts(starved))  # the CPU is the limit


def test_memory_events_rising_only_count_once_the_heap_is_large() -> None:
    cache = make(); cache[-1]["mem_events_max"] = 21  # a 30% heap bumping the limit with cache
    cache[0]["mem_events_max"] = 0
    assert verdicts.sample_verdicts(cache) == []
    heavy = make(mem_anon_pct=60.0); heavy[0]["mem_events_max"] = 0; heavy[-1]["mem_events_max"] = 5
    assert codes(verdicts.sample_verdicts(heavy)) == ["memory_pressure"]


def test_an_oom_kill_or_a_98_percent_heap_is_critical() -> None:
    oom = make(); oom[-1]["mem_events_oom_kill"] = 1
    assert [(v.code, v.severity) for v in verdicts.sample_verdicts(oom)] == [("memory_pressure", "crit")]
    assert verdicts.sample_verdicts(make(mem_anon_pct=98.5))[0].severity == "crit"


def test_scratch_disk_and_cpu_verdicts() -> None:
    assert verdicts.sample_verdicts(make(localtmp_used_pct=85.0))[0] == verdicts.Verdict(
        "tmp_disk_filling", "warn", "scratch volume reached 85% full"
    )
    assert verdicts.sample_verdicts(make(localtmp_used_pct=97.0))[0].severity == "crit"
    assert codes(verdicts.sample_verdicts(make(cpu_psi_full_avg10=25.0))) == ["cpu_starved"]
    throttled = make(); throttled[0]["cpu_throttled_s"] = 0.0; throttled[-1]["cpu_throttled_s"] = 120.0
    assert codes(verdicts.sample_verdicts(throttled)) == ["cpu_starved"]


# ---- pod and job verdicts, with the real 29 Sept eviction events ------------------------------


def real_events() -> list[PodEvent]:
    return [
        PodEvent(datetime.datetime.fromisoformat(d["time"]), d["reason"], d["type"], d["pod"], d["message"])
        for d in json.loads((DATA / "events.json").read_text())
    ]


def pod(name: str, phase: str = "Failed", reason: str | None = None, **kw: object) -> PodState:
    kw.setdefault("started", None)
    return PodState(name, 0, phase, "node", reason, kw.pop("exit_code", None), **kw)  # type: ignore[arg-type]


def test_the_29_sept_compare_pod_was_evicted_and_the_event_says_why() -> None:
    found = verdicts.pod_verdicts(pod("cornerstone-compare-cmp1-0-89jwj", reason="Evicted"), real_events())
    assert [(v.code, v.severity) for v in found] == [("evicted", "crit")]
    assert "using 59534476Ki, request is 48Gi" in found[0].message


def test_oom_killed_comes_from_the_container_state_or_the_node_event() -> None:
    assert codes(verdicts.pod_verdicts(pod("p", reason="OOMKilled", exit_code=137), [])) == ["oom_killed"]
    event = PodEvent(datetime.datetime.now(UTC), "OOMKilling", "Warning", "p", "Memory cgroup out of memory")
    assert codes(verdicts.pod_verdicts(pod("p"), [event])) == ["oom_killed"]


def test_a_pod_pending_for_ten_minutes_with_no_scale_up_is_stalled() -> None:
    now = datetime.datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    created = now - datetime.timedelta(minutes=15)
    failed = PodEvent(created, "FailedScheduling", "Warning", "p", "0/4 nodes are available")
    scale = PodEvent(created, "TriggeredScaleUp", "Normal", "p", "Pod triggered scale-up")
    waiting = pod("p", phase="Pending", created=created)
    assert codes(verdicts.pod_verdicts(waiting, [failed], now=now)) == ["pending_stall"]
    assert verdicts.pod_verdicts(waiting, [failed, scale], now=now) == []  # the autoscaler is working
    recent = pod("p", phase="Pending", created=now - datetime.timedelta(minutes=2))
    assert verdicts.pod_verdicts(recent, [failed], now=now) == []


def test_deadline_risk_and_index_failed() -> None:
    now = datetime.datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    running = pod("p", phase="Running", started=now - datetime.timedelta(minutes=50))
    assert codes(verdicts.pod_verdicts(running, [], now=now, deadline_s=3600)) == ["deadline_risk"]
    assert verdicts.pod_verdicts(running, [], now=now, deadline_s=7200) == []
    job = JobState("j", "compute", "r", 3, 0, 2, 1, "1", None, frozenset({"Failed"}))
    assert codes(verdicts.job_verdicts(job)) == ["index_failed"]


def test_events_roundtrip_and_archive_merge(tmp_path: pathlib.Path) -> None:
    events = real_events()
    assert collect_events.from_json(collect_events.to_json(events)) == events
    assert collect_events.archive(str(tmp_path), "r1", events[:10]) == 10
    assert collect_events.archive(str(tmp_path), "r1", events[5:20]) == 20  # merged, no duplicates
    assert len(collect_events.read_archive(str(tmp_path), "r1")) == 20
    assert collect_events.read_archive(str(tmp_path), "other") == []


@pytest.mark.parametrize("bad", ['x" OR "1"="1', "a b"])
def test_cloud_events_refuse_odd_filter_values(bad: str) -> None:
    with pytest.raises(ValueError):
        collect_events.cloud_events("p", "ns", bad, session=object())
