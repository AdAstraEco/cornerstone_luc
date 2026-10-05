import json
import logging
import sys
import types

import pytest
import run_phase
import structured_logging

from jdluc import tiling


@pytest.fixture(autouse=True)
def no_logging_setup(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        run_phase,
        "structured_logging",
        types.SimpleNamespace(configure=lambda **_: None),
    )
    monkeypatch.delenv("JOB_COMPLETION_INDEX", raising=False)


@pytest.fixture
def boundary_layer_is_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """The cold-start state: any attempt to resolve countries to tiles fails."""

    def boom(**_: object) -> list[str]:
        raise AssertionError("read the boundary layer")

    monkeypatch.setattr(run_phase, "get_tile_ids", boom)


def run_main(monkeypatch: pytest.MonkeyPatch, *argv: str) -> list[dict[str, object]]:
    """Run ``main`` with ``run_ingest`` recorded instead of executed."""
    calls: list[dict[str, object]] = []

    def fake_ingest(**kwargs: object) -> int:
        calls.append(kwargs)
        return 0

    monkeypatch.setattr(run_phase, "run_ingest", fake_ingest)
    monkeypatch.setattr(sys, "argv", ["run_phase.py", *argv])
    assert run_phase.main() == 0
    return calls


def test_tile_ids_arg_sorts_and_dedupes() -> None:
    assert run_phase.tile_ids_arg("20N_090W,10N_080W,20N_090W") == [
        "10N_080W",
        "20N_090W",
    ]


def test_tile_ids_arg_rejects_a_non_ten_degree_tile() -> None:
    with pytest.raises(Exception, match="not a ten-degree tile id"):
        run_phase.tile_ids_arg("21N_090W")


@pytest.mark.usefixtures("boundary_layer_is_missing")
def test_ingest_world_never_resolves_countries(monkeypatch: pytest.MonkeyPatch) -> None:
    (call,) = run_main(monkeypatch, "--phase", "ingest-world", "HND")
    assert call["tile_ids"] == [tiling.WHOLE_WORLD_TILE_ID]


@pytest.mark.usefixtures("boundary_layer_is_missing")
def test_ingest_world_needs_no_countries_at_all(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (call,) = run_main(monkeypatch, "--phase", "ingest-world")
    assert call["tile_ids"] == [tiling.WHOLE_WORLD_TILE_ID]


@pytest.mark.usefixtures("boundary_layer_is_missing")
def test_pod_index_picks_from_the_explicit_sorted_tile_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("JOB_COMPLETION_INDEX", "1")
    (call,) = run_main(
        monkeypatch, "--phase", "ingest-tiles", "--tile-ids", "20N_090W,10N_080W"
    )
    assert call["tile_ids"] == ["20N_090W"]


@pytest.mark.parametrize(
    "argv",
    [
        ["--phase", "compute", "--tile-ids", "20N_090W"],  # compute needs the countries
        ["--phase", "reduce", "--tile-ids", "20N_090W", "HND"],  # reduce derives tiles
        ["--phase", "mosaic", "--tile-ids", "20N_090W", "HND"],  # so does mosaic
        ["--phase", "ingest-tiles", "--tile-id", "20N_090W", "--tile-ids", "20N_090W"],
        ["--phase", "ingest-tiles"],  # per-tile but no index
    ],
)
def test_bad_combinations_are_usage_errors(
    monkeypatch: pytest.MonkeyPatch, argv: list[str]
) -> None:
    monkeypatch.setenv("JOB_COMPLETION_INDEX", "0")
    monkeypatch.setattr(sys, "argv", ["run_phase.py", *argv])
    with pytest.raises(SystemExit) as exc:
        run_phase.main()
    assert exc.value.code == 2


def test_json_log_line_carries_the_run_context(
    capsys: pytest.CaptureFixture[str],
) -> None:
    structured_logging.configure(
        log_format="json",
        context={"run_id": "r1", "phase": "compute", "tile": "20N_090W", "pod": None},
    )
    try:
        logging.getLogger("x").info("hello", extra={"fields": {"kind": "k"}})
    finally:
        structured_logging.configure(log_format="text", context={})
    line = json.loads(capsys.readouterr().out.splitlines()[0])
    assert line | {"time": "", "logger": ""} == {
        "severity": "INFO",
        "time": "",
        "message": "k",  # a structured record's kind names it
        "logger": "",
        "run_id": "r1",
        "phase": "compute",
        "tile": "20N_090W",
        "kind": "k",
    }


def test_mosaic_takes_its_tiles_from_the_countries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A country-named mosaic is stitched from that country's tiles, never a caller's list."""
    seen: dict[str, object] = {}

    def fake_mosaic(tile_ids: list[str], name: str) -> str:
        seen.update(tile_ids=tile_ids, name=name)
        return "gs://b/emissions/HND.vrt"

    monkeypatch.setattr(run_phase, "get_tile_ids", lambda **_: ["20N_080W", "20N_090W"])
    monkeypatch.setattr(run_phase.export, "mosaic_workflow", fake_mosaic)
    monkeypatch.setattr(sys, "argv", ["run_phase.py", "--phase", "mosaic", "HND"])
    assert run_phase.main() == 0
    assert seen == {"tile_ids": ["20N_080W", "20N_090W"], "name": "HND"}
