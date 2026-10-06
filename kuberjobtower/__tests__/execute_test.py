import dataclasses

import pytest

from kuberjobtower import manifest, run
from kuberjobtower.models import Aoi, JobState, PodState, RunSpec
from kuberjobtower.phases import Phase


class FakeCluster:
    """Jobs by name; each ``get_job`` advances a scripted list of (active, succeeded, conditions)."""

    def __init__(
        self, script: dict[str, list[tuple[int, int, set[str]]]] | None = None
    ) -> None:
        self.jobs: dict[str, JobState] = {}
        self.script = script or {}
        self.created: list[str] = []

    def create(self, job, *, dry_run: bool = False) -> None:  # type: ignore[no-untyped-def]
        meta = job.metadata
        self.created.append(meta.name)
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


def test_a_country_only_run_cannot_be_submitted_yet(make_settings) -> None:  # type: ignore[no-untyped-def]
    spec = RunSpec(
        run_id="r1", aoi=Aoi(tiles=None, iso_3166s=("HND",)),
        methodology="STATISTICAL", phases=(Phase.INGEST_TILES,),
    )  # fmt: skip
    with pytest.raises(run.RunError, match="give --tile"):
        go(run.plan(make_settings(), spec), FakeCluster())


def test_waiting_gives_up_at_the_deadline() -> None:
    cluster = FakeCluster()
    cluster.jobs["j"] = JobState(
        "j", "compute", "r1", 1, 1, 0, 0, "", None, frozenset()
    )
    ticks = iter(range(0, 10_000, 100))
    with pytest.raises(run.RunError, match="still running"):
        run.wait_job(cluster, "j", timeout_s=250, poll_s=0, out=lambda _: None,
                     sleep=lambda _: None, clock=lambda: float(next(ticks)))  # fmt: skip
