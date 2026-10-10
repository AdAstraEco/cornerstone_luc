"""infra/kjt.py (the kuber-job-tower spec) against infra/run_phase.py. Skipped when the ``kjt``
dependency group is not installed."""

import pytest
import run_phase

pytest.importorskip("kuberjobtower")


def spec():
    import kjt

    return kjt.PIPELINE


def test_phases_match_the_pod_program():
    assert [p.name for p in spec().phases] == [str(p) for p in run_phase.Phase]
    per_item = {p.name for p in spec().phases if p.per_item}
    assert per_item == {str(p) for p in run_phase.Phase if p.is_per_tile}


def test_the_spec_is_valid():
    from kuberjobtower.testing import check_pipeline

    check_pipeline(spec(), items=("20N_090W", "20N_080W"), countries=("HND",))


def argv(phase, options=None, items=("20N_090W", "20N_080W"), countries=("HND",)):
    from kuberjobtower.spec import PhaseContext

    s = spec()
    return s.build_args(
        PhaseContext(
            s.phase(phase), "r1", items, countries, s.with_defaults(options or {})
        )
    )


def test_ingest_world_needs_no_area():
    assert argv("ingest-world") == (
        "--phase",
        "ingest-world",
        "--methodology-name",
        "STATISTICAL",
        "--concurrency",
        "4",
    )


def test_compute_gets_its_tiles_countries_and_skip_flag():
    assert argv("compute", {"skip_ingest": "true"}) == (
        "--phase",
        "compute",
        "--methodology-name",
        "STATISTICAL",
        "--skip-ingest",
        "--tile-ids",
        "20N_090W,20N_080W",
        "HND",
    )


def test_reduce_takes_its_tiles_from_the_countries():
    assert "--tile-ids" not in argv("reduce")


def test_compute_needs_keys_unless_it_skips_ingest():
    s = spec()
    compute = s.phase("compute")
    assert s.secret_for(compute, s.with_defaults({})) == "keys"
    assert s.secret_for(compute, s.with_defaults({"skip_ingest": "true"})) == "nokeys"


def test_tiles_are_checked_against_the_grid():
    s = spec()
    assert s.validate_items(["20N_090W", "20N_090W"]) == ("20N_090W",)
    with pytest.raises(ValueError, match="not tiles"):
        s.validate_items(["nope"])
