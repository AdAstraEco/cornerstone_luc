import json
import pathlib

import pytest

from jdluc import attribute, derive, emit, export
from orchestration import plan
from orchestration.__tests__.plan_test import TILE_IDS
from orchestration_ext import __main__ as cli
from orchestration_ext import plan as ext_plan


def write_plan(
    tmp_path: pathlib.Path,
    allow_failures: tuple[str, ...] = (),
    derive_export: export.Format | None = None,
    derive_products: tuple[str, ...] = (),
    emit_export: export.Format | None = None,
    scope: tuple[str, ...] = ("AAA",),
) -> dict:
    ext_plan.write_plan(
        allow_failures=allow_failures,
        derive_export=derive_export,
        derive_products=derive_products,
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
    assert set(task_to_items) - set(core) == {"export-tiles-cog", "export-tiles-zarr"}
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
        tmp_path=tmp_path, derive_products=("crop-class-emissions",)
    )
    assert task_to_items["export-tiles-cog"] == []
    assert task_to_items["export-tiles-zarr"] == []


def test_each_steps_format_picks_its_list(
    ingested: list[tuple[str, str]], tmp_path: pathlib.Path
) -> None:
    task_to_items = write_plan(
        tmp_path=tmp_path,
        derive_export=export.Format.ZARR,
        derive_products=("crop-class-emissions", "perennial-debug"),
        emit_export=export.Format.COG,
    )
    assert {
        (u["step"], u["product"], u["format"], u["tile_id"])
        for u in task_to_items["export-tiles-cog"]
    } == {("emit", "cie-emissions", "cog", tile_id) for tile_id in TILE_IDS}
    # NB: the step's format overrides crop-class-emissions' own (cog)
    assert {
        (u["step"], u["product"], u["format"], u["tile_id"])
        for u in task_to_items["export-tiles-zarr"]
    } == {
        ("derive", product, "zarr", tile_id)
        for product in ("crop-class-emissions", "perennial-debug")
        for tile_id in TILE_IDS
    }


def test_an_unknown_derive_product_fails_before_any_ingest(
    ingested: list[tuple[str, str]], tmp_path: pathlib.Path
) -> None:
    with pytest.raises(AssertionError, match="crop-class-emisions"):
        write_plan(
            tmp_path=tmp_path,
            derive_export=export.Format.COG,
            derive_products=("crop-class-emisions",),
        )
    assert ingested == []


def test_a_derive_export_with_no_product_fails(
    ingested: list[tuple[str, str]], tmp_path: pathlib.Path
) -> None:
    with pytest.raises(AssertionError, match="names no product"):
        write_plan(tmp_path=tmp_path, derive_export=export.Format.ZARR)


@pytest.mark.parametrize("scope", [("AAA",), (plan.WORLD,)])
def test_tile_units_hold_the_export_lists_by_tile(
    ingested: list[tuple[str, str]], scope: tuple[str, ...], tmp_path: pathlib.Path
) -> None:
    task_to_items = write_plan(
        tmp_path=tmp_path,
        allow_failures=("DESCALS_OIL_PALM",),
        derive_export=export.Format.ZARR,
        derive_products=("crop-class-emissions",),
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
                "--derive-products=crop-class-emissions",
                f"--output-dir={tmp_path}",
            ],
        )
        == 0
    )
    units = json.loads((tmp_path / "export-tiles-cog.json").read_text())
    assert {u["product"] for u in units} == {"cie-emissions"}
    assert json.loads((tmp_path / "export-tiles-zarr.json").read_text()) == []


def test_export_tile_writes_emits_cie_bands(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict] = []

    def write_tile(**kwargs: object) -> str:
        calls.append(kwargs)
        return "uri"

    monkeypatch.setattr(emit, "workflow", lambda tile_id: f"dset-{tile_id:s}")
    monkeypatch.setattr(export, "write_tile", write_tile)
    parser, _ = cli.build_parser()
    argv = ["export-tile", "--step=emit", "--product=cie-emissions", "--format=zarr"]
    assert cli.cli.run(parser, [*argv, "--tile-id=10N_010W"]) == 0
    (kwargs,) = calls
    assert kwargs["dset"] == "dset-10N_010W"
    assert kwargs["format"] == export.Format.ZARR
    assert kwargs["name"] == "cie-emissions"
    assert kwargs["variables"] == emit.CIE_BANDS


def test_export_tile_exports_one_derive_product(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict] = []

    def export_workflow(**kwargs: object) -> list[str]:
        calls.append(kwargs)
        return ["uri"]

    monkeypatch.setattr(derive, "export_workflow", export_workflow)
    cli.export_tile(
        format=export.Format.COG,
        product="perennial-debug",
        step=ext_plan.Step.DERIVE,
        tile_id="10N_010W",
    )
    assert calls == [
        {
            "format": export.Format.COG,
            "product_names": ("perennial-debug",),
            "tile_id": "10N_010W",
        }
    ]


def test_export_tile_rejects_another_steps_product() -> None:
    with pytest.raises(AssertionError, match="not a emit product"):
        cli.export_tile(
            format=export.Format.COG,
            product="crop-class-emissions",
            step=ext_plan.Step.EMIT,
            tile_id="10N_010W",
        )
