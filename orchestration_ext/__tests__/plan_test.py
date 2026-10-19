import json
import pathlib
import sys

import pytest

from jdluc import attribute, export, trace
from orchestration import plan
from orchestration.__tests__.plan_test import TILE_IDS, WORLD_TILE_IDS
from orchestration_ext import __main__ as cli
from orchestration_ext import plan as ext_plan


def write_plan(
    tmp_path: pathlib.Path,
    allow_failures: tuple[str, ...] = (),
    derive_deliverables: tuple[str, ...] = (),
    derive_export: export.Format | None = None,
    emit_export: export.Format | None = None,
    scope: tuple[str, ...] = ("AAA",),
) -> dict:
    ext_plan.write_plan(
        allow_failures=allow_failures,
        derive_deliverables=derive_deliverables,
        derive_export=derive_export,
        emit_export=emit_export,
        methodology=attribute.Methodology.STATISTICAL,
        output_dir=str(tmp_path),
        scope=scope,
    )
    return {p.stem: json.loads(p.read_text()) for p in tmp_path.glob("*.json")}


def test_the_plan_is_upstreams_plus_the_exports(
    ingested: list[tuple[str, str]], tmp_path: pathlib.Path
) -> None:
    task_to_items = write_plan(
        tmp_path=tmp_path / "ext", allow_failures=("DESCALS_OIL_PALM",)
    )
    plan.write_plan(
        allow_failures=("DESCALS_OIL_PALM",),
        methodology=attribute.Methodology.STATISTICAL,
        output_dir=str(tmp_path / "core"),
        scope=("AAA",),
    )
    core = {
        p.stem: json.loads(p.read_text()) for p in (tmp_path / "core").glob("*.json")
    }
    assert set(task_to_items) - set(core) == {
        "export-tiles-cog",
        "export-tiles-zarr",
        "mosaics",
    }
    for task, items in core.items():
        if task != "tile-units":
            assert task_to_items[task] == items
    assert [
        {k: v for k, v in u.items() if not k.startswith("exports_")}
        for u in task_to_items["tile-units"]
    ] == core["tile-units"]


def test_no_export_plans_no_export(
    ingested: list[tuple[str, str]], tmp_path: pathlib.Path
) -> None:
    task_to_items = write_plan(
        tmp_path=tmp_path, derive_deliverables=("crop-class-emissions",)
    )
    assert task_to_items["export-tiles-cog"] == []
    assert task_to_items["export-tiles-zarr"] == []
    assert task_to_items["mosaics"] == []


def test_each_steps_format_picks_its_list(
    ingested: list[tuple[str, str]], tmp_path: pathlib.Path
) -> None:
    task_to_items = write_plan(
        tmp_path=tmp_path,
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
            derive_deliverables=("crop-class-emisions",),
            derive_export=export.Format.COG,
        )
    assert ingested == []


def test_a_derive_export_with_no_deliverable_fails(
    ingested: list[tuple[str, str]], tmp_path: pathlib.Path
) -> None:
    with pytest.raises(AssertionError, match="names no deliverable"):
        write_plan(tmp_path=tmp_path, derive_export=export.Format.ZARR)


@pytest.mark.parametrize("scope", [("AAA",), (plan.WORLD,)])
def test_tile_units_hold_the_export_lists_by_tile(
    ingested: list[tuple[str, str]], scope: tuple[str, ...], tmp_path: pathlib.Path
) -> None:
    task_to_items = write_plan(
        tmp_path=tmp_path,
        allow_failures=("DESCALS_OIL_PALM",),
        derive_deliverables=("crop-class-emissions",),
        derive_export=export.Format.ZARR,
        emit_export=export.Format.COG,
        scope=scope,
    )
    tile_units = task_to_items["tile-units"]
    for format in export.Format:
        assert [
            {**unit, "tile_id": u["tile_id"]}
            for u in tile_units
            for unit in u[f"exports_{format!s}"]
        ] == sorted(
            task_to_items[f"export-tiles-{format!s}"], key=lambda u: u["tile_id"]
        )


def test_world_names_its_run_wide_outputs_world(
    ingested: list[tuple[str, str]], monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[dict] = []

    def record(**kwargs: object) -> str:
        calls.append(kwargs)
        return "uri"

    monkeypatch.setattr(export, "mosaic_workflow", record)
    monkeypatch.setattr(trace, "export_workflow", record)
    for argv in (
        ["mosaic", "world", "--deliverable=cie-emissions"],
        ["trace", "world", "--methodology=STATISTICAL"],
    ):
        monkeypatch.setattr(sys, "argv", ["orchestration_ext", *argv])
        assert cli.main() == 0

    mosaic_kwargs, trace_kwargs = calls
    assert mosaic_kwargs["name"] == "world"
    assert mosaic_kwargs["tile_ids"] == list(WORLD_TILE_IDS)
    assert trace_kwargs["name"] == "world"


def test_the_cli_plan_takes_the_export_flags(
    ingested: list[tuple[str, str]], tmp_path: pathlib.Path
) -> None:
    parser, _ = cli.build_parser()
    assert (
        cli.cli.run(
            parser,
            [
                "plan",
                "AAA",
                "--methodology=STATISTICAL",
                "--emit-export=cog",
                "--derive-export=none",
                f"--output-dir={tmp_path}",
            ],
        )
        == 0
    )
    assert json.loads((tmp_path / "mosaics.json").read_text()) == ["cie-emissions"]
