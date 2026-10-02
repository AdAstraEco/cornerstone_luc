import pytest

from controlplane import run
from controlplane.models import Aoi, RunSpec
from controlplane.phases import Phase


def spec(**kw) -> RunSpec:  # type: ignore[no-untyped-def]
    base = {
        "run_id": "1002-1410",
        "aoi": Aoi(tiles=("20N_090W",), iso_3166s=("HND",)),
        "methodology": "STATISTICAL",
        "phases": tuple(Phase),
    }
    return RunSpec(**{**base, **kw})


def test_single_tile_run_needs_no_boundary_and_names_fit(make_settings) -> None:  # type: ignore[no-untyped-def]
    plan = run.plan(make_settings(), spec())
    names = [p.job.name for p in plan.phases if p.job]
    assert names[2] == "cornerstone-t-20n-090w-hnd-compute-1002-1410"
    assert all(len(n) <= 63 for n in names)
    assert plan.errors == ()


def test_ingest_world_has_no_countries_and_compute_skips_ingest(make_settings) -> None:  # type: ignore[no-untyped-def]
    jobs = {p.phase: p.job for p in run.plan(make_settings(), spec()).phases if p.job}
    assert jobs[Phase.INGEST_WORLD].args == (  # type: ignore[union-attr]
        "--phase",
        "ingest-world",
        "--methodology-name",
        "STATISTICAL",
        "--concurrency",
        "4",
    )
    assert "--skip-ingest" in jobs[Phase.COMPUTE].args  # type: ignore[union-attr]
    assert "--tile-ids" not in jobs[Phase.REDUCE].args  # type: ignore[union-attr]


def test_memory_request_equals_limit_and_roles_pick_pools(make_settings) -> None:  # type: ignore[no-untyped-def]
    jobs = {p.phase: p.job for p in run.plan(make_settings(), spec()).phases if p.job}
    assert all(
        j.resources.memory_request == j.resources.memory_limit for j in jobs.values()
    )  # type: ignore[union-attr]
    assert jobs[Phase.COMPUTE].node_pool == "ns-power-node-pool"  # type: ignore[union-attr]
    assert jobs[Phase.EXPORT].node_pool == "ns-worker-node-pool"  # type: ignore[union-attr]


def test_country_only_run_defers_the_per_tile_phases(make_settings) -> None:  # type: ignore[no-untyped-def]
    plan = run.plan(make_settings(), spec(aoi=Aoi(tiles=None, iso_3166s=("HND",))))
    by_phase = {p.phase: p for p in plan.phases}
    assert by_phase[Phase.INGEST_WORLD].job is not None
    assert by_phase[Phase.COMPUTE].job is None
    assert by_phase[Phase.REDUCE].job is not None


def test_overrides(make_settings) -> None:  # type: ignore[no-untyped-def]
    overrides = run.parse_overrides(
        [
            "compute.pool=ns-worker-node-pool",
            "compute.memory=48Gi",
            "export.deadline=90m",
        ]
    )
    plan = run.plan(make_settings(), spec(overrides=overrides))
    jobs = {p.phase: p.job for p in plan.phases if p.job}
    compute = jobs[Phase.COMPUTE]
    assert compute.node_pool == "ns-worker-node-pool"  # type: ignore[union-attr]
    assert compute.resources.memory_request == compute.resources.memory_limit == "48Gi"  # type: ignore[union-attr]
    assert jobs[Phase.EXPORT].deadline_s == 5400  # type: ignore[union-attr]
    assert plan.checks == ()


@pytest.mark.parametrize(
    "text",
    [
        "compute.memory=lots",
        "compute=1",
        "nope.pool=p",
        "compute.color=red",
        "compute.cpu=",
    ],
)
def test_bad_overrides_are_rejected(text) -> None:  # type: ignore[no-untyped-def]
    with pytest.raises(ValueError):
        run.parse_overrides([text])


def test_guardrails_block(make_settings) -> None:  # type: ignore[no-untyped-def]
    settings = make_settings(MAX_PARALLELISM="4", MAX_TILES="1")
    tiles = Aoi(tiles=("20N_090W", "20N_080W"), iso_3166s=())
    plan = run.plan(settings, spec(aoi=tiles, parallelism=8))
    messages = " ".join(c.message for c in plan.errors)
    assert "MAX_PARALLELISM" in messages
    assert "MAX_TILES" in messages
    assert "--country" in messages  # compute, reduce, mosaic need countries
    foreign = run.plan(
        settings,
        spec(overrides=run.parse_overrides(["compute.pool=standard-node-pool"])),
    )
    assert any(c.name == "pool" for c in foreign.errors)


def test_job_names_never_exceed_63_characters(make_settings) -> None:  # type: ignore[no-untyped-def]
    long_aoi = Aoi(tiles=None, iso_3166s=tuple(f"A{i:02d}" for i in range(30)))
    for run_id in ("a", "0" * 16):
        plan = run.plan(make_settings(), spec(aoi=long_aoi, run_id=run_id))
        assert all(len(p.job.name) <= 63 for p in plan.phases if p.job)


def test_bad_run_id(make_settings) -> None:  # type: ignore[no-untyped-def]
    with pytest.raises(ValueError, match="run id"):
        run.plan(make_settings(), spec(run_id="Bad_Id"))


def test_tiles_are_sorted_whatever_the_caller_passes() -> None:
    aoi = Aoi(tiles=("20N_090W", "10N_080W", "20N_090W"), iso_3166s=("SLV", "HND"))
    assert aoi.tiles == ("10N_080W", "20N_090W")
    assert aoi.iso_3166s == ("HND", "SLV")
