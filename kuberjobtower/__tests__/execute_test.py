import dataclasses

import pytest

from kuberjobtower import manifest, run
from kuberjobtower.models import Aoi, JobState, PodEvent, PodState, RunSpec
from kuberjobtower.phases import Phase


class FakeCluster:
    """Jobs by name; each ``get_job`` advances a scripted list of (active, succeeded, conditions)."""

    def __init__(
        self,
        script: dict[str, list[tuple[int, int, set[str]]]] | None = None,
    ) -> None:
        self.jobs: dict[str, JobState] = {}
        self.script = script or {}
        self.created: list[str] = []
        self.event_list: list[PodEvent] = []
        self.created_args: dict[str, list[str]] = {}
        self.created_completions: dict[str, int] = {}

    def create(self, job, *, dry_run: bool = False) -> None:  # type: ignore[no-untyped-def]
        meta = job.metadata
        self.created.append(meta.name)
        self.created_args[meta.name] = list(job.spec.template.spec.containers[0].args)
        self.created_completions[meta.name] = job.spec.completions
        self.jobs[meta.name] = JobState(
            meta.name, meta.labels["phase"], meta.labels["run-id"], job.spec.completions,
            0, 0, 0, "", meta.annotations[manifest.SPEC_HASH], frozenset(),
        )  # fmt: skip

    def get_job(self, name: str) -> JobState | None:
        job = self.jobs.get(name)
        steps = self.script.get(name)
        if job and steps:
            active, succeeded, conditions = steps.pop(0) if len(steps) > 1 else steps[0]
            job = dataclasses.replace(
                job,
                active=active,
                succeeded=succeeded,
                conditions=frozenset(conditions),
            )
            self.jobs[name] = job
        return job

    def pods(self, run_id: str, phase: str | None = None) -> list[PodState]:
        return [PodState("p-0", 0, "Failed", "node", "OOMKilled", 137, None)]

    def events(self, run_id: str) -> list[PodEvent]:
        return self.event_list


def make_plan(make_settings, phases=(Phase.INGEST_WORLD, Phase.EXPORT)):  # type: ignore[no-untyped-def]
    spec = RunSpec(
        run_id="r1", aoi=Aoi(tiles=("20N_090W",), iso_3166s=("HND",)),
        methodology="STATISTICAL", phases=phases,
    )  # fmt: skip
    return run.plan(make_settings(), spec)


def go(plan, cluster):  # type: ignore[no-untyped-def]
    lines: list[str] = []
    ok = run.execute(plan, cluster, poll_s=0, out=lines.append, sleep=lambda _: None)
    return ok, lines


def test_phases_run_in_order_and_each_is_waited_for(make_settings) -> None:  # type: ignore[no-untyped-def]
    plan = make_plan(make_settings)
    names = [p.job.name for p in plan.phases]
    cluster = FakeCluster({n: [(1, 0, set()), (0, 1, {"Complete"})] for n in names})
    ok, lines = go(plan, cluster)
    assert ok
    assert (
        cluster.created == names
    )  # barrier: the second is created only after the first completed
    assert lines[-1] == "export: done"


def test_a_finished_job_with_the_same_spec_is_adopted_not_recreated(
    make_settings,
) -> None:  # type: ignore[no-untyped-def]
    plan = make_plan(make_settings)
    cluster = FakeCluster({p.job.name: [(0, 1, {"Complete"})] for p in plan.phases})
    go(plan, cluster)
    created = list(cluster.created)
    ok, lines = go(plan, cluster)
    assert ok
    assert cluster.created == created
    assert any("adopting" in line for line in lines)


def test_a_reused_run_id_with_a_different_spec_stops(make_settings) -> None:  # type: ignore[no-untyped-def]
    first = make_plan(make_settings)
    cluster = FakeCluster({p.job.name: [(0, 1, {"Complete"})] for p in first.phases})
    go(first, cluster)
    other = dataclasses.replace(
        first, phases=tuple(
            dataclasses.replace(p, job=dataclasses.replace(p.job, deadline_s=1)) for p in first.phases
        ),
    )  # fmt: skip
    with pytest.raises(run.RunError, match="different spec"):
        go(other, cluster)


def test_a_failed_phase_stops_the_run_and_names_the_pod(make_settings) -> None:  # type: ignore[no-untyped-def]
    plan = make_plan(make_settings)
    cluster = FakeCluster({plan.phases[0].job.name: [(0, 0, {"Failed"})]})
    ok, lines = go(plan, cluster)
    assert not ok
    assert len(cluster.created) == 1  # the next phase was never created
    assert any("exit 137 OOMKilled" in line for line in lines)
    assert any("CRIT oom_killed" in line for line in lines)


def country_plan(make_settings, phases):  # type: ignore[no-untyped-def]
    spec = RunSpec(
        run_id="r1", aoi=Aoi(tiles=None, iso_3166s=("HND",)),
        methodology="STATISTICAL", phases=phases,
    )  # fmt: skip
    return run.plan(make_settings(), spec)


def test_a_country_only_run_cannot_go_on_without_a_way_to_resolve_tiles(
    make_settings,
) -> None:  # type: ignore[no-untyped-def]
    with pytest.raises(run.RunError, match="give --tile"):
        go(country_plan(make_settings, (Phase.INGEST_TILES,)), FakeCluster())


def test_countries_resolve_to_tiles_after_ingest_world_and_every_phase_gets_the_same_list(
    make_settings,
) -> None:  # type: ignore[no-untyped-def]
    settings = make_settings()
    plan = country_plan(
        make_settings, (Phase.INGEST_WORLD, Phase.INGEST_TILES, Phase.COMPUTE)
    )
    looked_up: list[tuple[str, ...]] = []

    def tiles_for(isos: tuple[str, ...]) -> tuple[str, ...]:
        looked_up.append(isos)
        return ("20N_090W", "10N_090W")

    names = [
        f"cornerstone-hnd-{p}-r1" for p in ("ingest-world", "ingest-tiles", "compute")
    ]
    cluster = FakeCluster({n: [(0, 1, {"Complete"})] for n in names})
    lines: list[str] = []
    ok = run.execute(
        plan, cluster, poll_s=0, out=lines.append, sleep=lambda _: None,
        resolve=run.lazy_tiles(settings, plan.spec, tiles_for, out=lines.append),
    )  # fmt: skip
    assert ok
    assert looked_up == [("HND",)]  # once, not per phase
    assert (
        cluster.created == names
    )  # the Job names do not change when the tiles are resolved
    assert cluster.created_args["cornerstone-hnd-compute-r1"][-3:-1] == [
        "--tile-ids",
        "10N_090W,20N_090W",
    ]
    assert cluster.created_completions == {
        "cornerstone-hnd-ingest-world-r1": 1,
        "cornerstone-hnd-ingest-tiles-r1": 2,
        "cornerstone-hnd-compute-r1": 2,
    }
    assert any("-> 2 tile(s)" in line for line in lines)


def test_resolved_tiles_are_checked_like_explicit_ones(make_settings) -> None:  # type: ignore[no-untyped-def]
    settings = make_settings(MAX_TILES="1")
    plan = country_plan(make_settings, (Phase.INGEST_TILES,))
    resolve = run.lazy_tiles(
        settings, plan.spec, lambda _: ("20N_090W", "10N_090W"), out=lambda _: None
    )
    with pytest.raises(run.RunError, match="KJT_MAX_TILES"):
        resolve(plan.phases[0])
    settings = make_settings(CONFIRM_TILES="2")
    resolve = run.lazy_tiles(
        settings, plan.spec, lambda _: ("20N_090W", "10N_090W"), out=lambda _: None
    )
    with pytest.raises(run.RunError, match="--i-know"):
        resolve(plan.phases[0])
    assert run.lazy_tiles(
        settings,
        plan.spec,
        lambda _: ("20N_090W", "10N_090W"),
        i_know=True,
        out=lambda _: None,
    )(plan.phases[0]).job


def test_countries_that_touch_no_tile_stop_the_run(make_settings) -> None:  # type: ignore[no-untyped-def]
    plan = country_plan(make_settings, (Phase.INGEST_TILES,))
    with pytest.raises(run.RunError, match="touch no tile"):
        run.lazy_tiles(make_settings(), plan.spec, lambda _: (), out=lambda _: None)(
            plan.phases[0]
        )


def test_waiting_gives_up_at_the_deadline() -> None:
    cluster = FakeCluster()
    cluster.jobs["j"] = JobState(
        "j", "compute", "r1", 1, 1, 0, 0, "", None, frozenset()
    )
    ticks = iter(range(0, 10_000, 100))
    with pytest.raises(run.RunError, match="still running"):
        run.wait_job(cluster, "j", timeout_s=250, poll_s=0, out=lambda _: None,
                     sleep=lambda _: None, clock=lambda: float(next(ticks)))  # fmt: skip


def test_after_phase_runs_for_every_phase_and_a_failing_hook_does_not_fail_the_run(
    make_settings,
) -> None:  # type: ignore[no-untyped-def]
    plan = make_plan(make_settings)
    cluster = FakeCluster({p.job.name: [(0, 1, {"Complete"})] for p in plan.phases})
    seen: list[str] = []

    def hook(pp, final) -> None:  # type: ignore[no-untyped-def]
        seen.append(str(pp.phase))
        raise OSError("bucket unreachable")

    lines: list[str] = []
    ok = run.execute(
        plan,
        cluster,
        poll_s=0,
        out=lines.append,
        sleep=lambda _: None,
        after_phase=hook,
    )
    assert ok and seen == [str(p.phase) for p in plan.phases]
    assert any("hook failed" in line for line in lines)


def test_after_phase_also_runs_when_the_phase_failed(make_settings) -> None:  # type: ignore[no-untyped-def]
    plan = make_plan(make_settings)
    cluster = FakeCluster({plan.phases[0].job.name: [(0, 0, {"Failed"})]})
    seen: list[str] = []
    ok = run.execute(plan, cluster, poll_s=0, out=lambda _: None, sleep=lambda _: None,
                     after_phase=lambda pp, final: seen.append(final.state))  # fmt: skip
    assert not ok and seen == ["failed"]


def test_a_failure_shows_the_warning_events_of_the_failed_pod(make_settings) -> None:  # type: ignore[no-untyped-def]
    import datetime

    plan = make_plan(make_settings)
    cluster = FakeCluster({plan.phases[0].job.name: [(0, 0, {"Failed"})]})
    when = datetime.datetime(2026, 10, 6, tzinfo=datetime.UTC)
    cluster.event_list = [
        PodEvent(
            when, "Evicted", "Warning", "p-0", "The node was low on resource: memory."
        ),
        PodEvent(when, "Scheduled", "Normal", "p-0", "assigned"),
        PodEvent(when, "Evicted", "Warning", "other-pod", "not mine"),
    ]
    ok, lines = go(plan, cluster)
    assert not ok
    assert any("event Evicted: The node was low on resource" in line for line in lines)
    assert not any("not mine" in line or "assigned" in line for line in lines)
