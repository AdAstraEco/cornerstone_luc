import json
import pathlib
import sys
import types

import pytest

from jdluc import attribute, datasets, export, ingest, tiling, trace
from jdluc.datasets import worldbank_jurisdictions
from orchestration import __main__ as cli
from orchestration import plan

TILE_IDS = {"10N_010W", "20N_010W", "30N_010W"}
# A stand-in for every GNW tile: more than the countries' tiles, as the real list is
BACKFILL_TILE_IDS = ("00N_010W", *sorted(TILE_IDS), "40N_010W")


@pytest.fixture
def ingested(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str]]:
    """Stub the boundaries and every dataset; return the (dataset, tile) pairs ingested."""
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        worldbank_jurisdictions,
        "get_ten_degree_tile_ids_for_iso_3166s",
        lambda iso_3166s: TILE_IDS,
    )
    monkeypatch.setattr(
        worldbank_jurisdictions, "get_all_iso_3166s", lambda: {"AAA", "BBB"}
    )
    monkeypatch.setattr(tiling, "GLOBAL_NATURE_WATCH_TILE_IDS", BACKFILL_TILE_IDS)
    for name, cls in list(datasets.NAME_TO_CLS.items()):

        def ingest_a_tile(
            overwrite: bool, root: str, tile_id: str, name: str = name.name
        ) -> str:
            calls.append((name, tile_id))
            return f"{tile_id:s}.tif"

        monkeypatch.setitem(
            datasets.NAME_TO_CLS,
            name,
            types.SimpleNamespace(
                ingest_a_tile=ingest_a_tile, partitioning=cls.partitioning
            ),
        )
    return calls


def write_plan(
    tmp_path: pathlib.Path,
    allow_failures: tuple[str, ...],
    backfill: bool = False,
    derive_deliverables: tuple[str, ...] = (),
    derive_export: export.Format | None = None,
    emit_export: export.Format | None = None,
) -> dict:
    plan.write_plan(
        allow_failures=allow_failures,
        backfill=backfill,
        derive_deliverables=derive_deliverables,
        derive_export=derive_export,
        emit_export=emit_export,
        iso_3166s=() if backfill else ("AAA",),
        methodology=attribute.Methodology.STATISTICAL,
        output_dir=str(tmp_path),
    )
    return {p.stem: json.loads(p.read_text()) for p in tmp_path.glob("*.json")}


def test_plan_ingests_the_boundaries_it_reads(
    ingested: list[tuple[str, str]], tmp_path: pathlib.Path
) -> None:
    task_to_items = write_plan(tmp_path=tmp_path, allow_failures=())
    assert sorted(ingested) == [
        ("WORLD_BANK_ADMIN_0", tiling.WHOLE_WORLD_TILE_ID),
        ("WORLD_BANK_ADMIN_1", tiling.WHOLE_WORLD_TILE_ID),
    ]
    assert "WORLD_BANK_ADMIN_0" not in task_to_items["ingest-world"]


def test_allowed_failures_get_their_own_list(
    ingested: list[tuple[str, str]], tmp_path: pathlib.Path
) -> None:
    task_to_items = write_plan(tmp_path=tmp_path, allow_failures=("DESCALS_OIL_PALM",))
    allowed = task_to_items["ingest-tiles-allowed-failures"]
    assert {u["dataset"] for u in allowed} == {"DESCALS_OIL_PALM"}
    assert {u["tile_id"] for u in allowed} == TILE_IDS
    assert "DESCALS_OIL_PALM" not in {
        u["dataset"] for u in task_to_items["ingest-tiles"]
    }


def test_an_unknown_allowed_failure_fails(
    ingested: list[tuple[str, str]], tmp_path: pathlib.Path
) -> None:
    with pytest.raises(AssertionError, match="DESCALS_OIL_PLAM"):
        write_plan(tmp_path=tmp_path, allow_failures=("DESCALS_OIL_PLAM",))


def test_each_planned_unit_ingests_exactly_its_tile(
    ingested: list[tuple[str, str]],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
) -> None:
    task_to_items = write_plan(tmp_path=tmp_path, allow_failures=())
    assert {u["tile_id"] for u in task_to_items["ingest-tiles"]} == TILE_IDS

    for unit in task_to_items["ingest-tiles"]:
        ingested.clear()
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "ingest.py",
                unit["dataset"],
                "AAA",
                f"--modulus={unit['modulus']:d}",
                f"--residues={unit['residue']:d}",
            ],
        )
        assert ingest.main() == 0
        assert ingested == [(unit["dataset"], unit["tile_id"])]


def test_no_export_plans_no_export(
    ingested: list[tuple[str, str]], tmp_path: pathlib.Path
) -> None:
    task_to_items = write_plan(
        tmp_path=tmp_path,
        allow_failures=(),
        derive_deliverables=("crop-class-emissions",),
    )
    assert task_to_items["export-tiles-cog"] == []
    assert task_to_items["export-tiles-zarr"] == []
    assert task_to_items["mosaics"] == []


def test_each_steps_format_picks_its_list(
    ingested: list[tuple[str, str]], tmp_path: pathlib.Path
) -> None:
    task_to_items = write_plan(
        tmp_path=tmp_path,
        allow_failures=(),
        derive_deliverables=("crop-class-emissions", "perennial-debug"),
        derive_export=export.Format.ZARR,
        emit_export=export.Format.COG,
    )
    assert {
        (u["step"], u["deliverable"], u["format"], u["tile_id"])
        for u in task_to_items["export-tiles-cog"]
    } == {("emit", "cie-emissions", "cog", tile_id) for tile_id in TILE_IDS}
    # NB: the step's format overrides perennial-debug's own (zarr) and
    # crop-class-emissions' (cog) alike
    assert {
        (u["step"], u["deliverable"], u["format"], u["tile_id"])
        for u in task_to_items["export-tiles-zarr"]
    } == {
        ("derive", deliverable, "zarr", tile_id)
        for deliverable in ("crop-class-emissions", "perennial-debug")
        for tile_id in TILE_IDS
    }
    assert task_to_items["mosaics"] == ["cie-emissions"]


def test_every_cog_deliverable_is_mosaicked(
    ingested: list[tuple[str, str]], tmp_path: pathlib.Path
) -> None:
    task_to_items = write_plan(
        tmp_path=tmp_path,
        allow_failures=(),
        derive_deliverables=("crop-class-emissions",),
        derive_export=export.Format.COG,
        emit_export=export.Format.COG,
    )
    assert task_to_items["mosaics"] == ["cie-emissions", "crop-class-emissions"]
    assert task_to_items["export-tiles-zarr"] == []


def test_an_unknown_derive_deliverable_fails(
    ingested: list[tuple[str, str]], tmp_path: pathlib.Path
) -> None:
    with pytest.raises(AssertionError, match="crop-class-emisions"):
        write_plan(
            tmp_path=tmp_path,
            allow_failures=(),
            derive_deliverables=("crop-class-emisions",),
            derive_export=export.Format.COG,
        )
    assert ingested == []


def test_a_derive_export_with_no_deliverable_fails(
    ingested: list[tuple[str, str]], tmp_path: pathlib.Path
) -> None:
    with pytest.raises(AssertionError, match="names no deliverable"):
        write_plan(
            tmp_path=tmp_path, allow_failures=(), derive_export=export.Format.ZARR
        )


def test_a_backfill_plans_every_gnw_tile_as_ingest_selects_it(
    ingested: list[tuple[str, str]],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
) -> None:
    task_to_items = write_plan(tmp_path=tmp_path, allow_failures=(), backfill=True)
    assert [u["tile_id"] for u in task_to_items["tiles"]] == list(BACKFILL_TILE_IDS)

    # The workflow passes its `iso` parameter, here --backfill, to ingest's CLI
    for unit in task_to_items["ingest-tiles"]:
        ingested.clear()
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "ingest.py",
                unit["dataset"],
                "--backfill",
                f"--modulus={unit['modulus']:d}",
                f"--residues={unit['residue']:d}",
            ],
        )
        assert ingest.main() == 0
        assert ingested == [(unit["dataset"], unit["tile_id"])]


@pytest.mark.parametrize("iso_3166s", [(), ("AAA",)])
def test_a_run_takes_either_countries_or_a_backfill(
    ingested: list[tuple[str, str]], iso_3166s: tuple[str, ...]
) -> None:
    with pytest.raises(AssertionError, match="--backfill"):
        plan.get_tile_shards(backfill=bool(iso_3166s), iso_3166s=iso_3166s)


def test_a_backfill_names_its_run_wide_outputs_world(
    ingested: list[tuple[str, str]], monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[dict] = []

    def record(**kwargs: object) -> str:
        calls.append(kwargs)
        return "uri"

    monkeypatch.setattr(export, "mosaic_workflow", record)
    monkeypatch.setattr(trace, "export_workflow", record)
    # As the workflow passes it: --backfill in the countries' place
    for argv in (
        ["mosaic", "--backfill", "--deliverable=cie-emissions"],
        ["trace", "--backfill", "--methodology=STATISTICAL"],
    ):
        monkeypatch.setattr(sys, "argv", ["orchestration", *argv])
        assert cli.main() == 0

    mosaic_kwargs, trace_kwargs = calls
    assert mosaic_kwargs["name"] == "world"
    assert mosaic_kwargs["tile_ids"] == list(BACKFILL_TILE_IDS)
    assert trace_kwargs["name"] == "world"
    assert trace_kwargs["iso_3166s"] == ("AAA", "BBB")


def test_a_country_run_names_its_outputs_by_its_countries() -> None:
    assert plan.get_name(backfill=False, iso_3166s=("HND", "GTM")) == "GTM-HND"
