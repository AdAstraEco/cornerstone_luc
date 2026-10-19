"""The plan, plus the lists of the fork's per-tile exports, by format."""

import enum

from jdluc import attribute, derive, export
from orchestration import plan


class Step(enum.StrEnum):
    """A step whose per-tile output can be exported as products."""

    EMIT = "emit"
    DERIVE = "derive"


# emit's one product: its Crop-Independent Emissions bands, `emit.CIE_BANDS`
CIE_EMISSIONS = "cie-emissions"

STEP_TO_PRODUCT_NAMES = {
    Step.EMIT: (CIE_EMISSIONS,),
    Step.DERIVE: tuple(derive.NAME_TO_PRODUCT),
}


def check_product(product: str, step: Step) -> None:
    assert product in STEP_TO_PRODUCT_NAMES[step], (
        f"{product=:s} is not a {step!s} product: {STEP_TO_PRODUCT_NAMES[step]}"
    )


def get_exports(
    derive_export: export.Format | None,
    derive_products: tuple[str, ...],
    emit_export: export.Format | None,
) -> list[tuple[Step, str, export.Format]]:
    """Each (step, product, format) the run exports; a step whose format is None exports
    nothing.

    emit exports its one product, derive those named. The step's format overrides each
    product's own.
    """
    for product in derive_products:
        check_product(product=product, step=Step.DERIVE)
    assert derive_export is None or derive_products, (
        f"derive exports as {derive_export!s}, but names no product"
    )
    step_to_products = {
        Step.EMIT: STEP_TO_PRODUCT_NAMES[Step.EMIT],
        Step.DERIVE: derive_products,
    }
    step_to_format = {Step.EMIT: emit_export, Step.DERIVE: derive_export}
    return [
        (step, product, format)
        for step, format in step_to_format.items()
        if format is not None
        for product in step_to_products[step]
    ]


def get_task_to_items(
    allow_failures: tuple[str, ...],
    exports: list[tuple[Step, str, export.Format]],
    methodology: attribute.Methodology,
    scope: tuple[str, ...],
) -> dict[str, list]:
    """`orchestration.plan.get_task_to_items`, plus ``export-tiles-{format}``, and each
    tile's exports in its ``tile-units`` item as ``exports_{format}``."""
    task_to_items = plan.get_task_to_items(
        allow_failures=allow_failures, methodology=methodology, scope=scope
    )
    tile_ids = [u["tile_id"] for u in task_to_items["tiles"]]
    format_to_export_units = {
        format: [
            {
                "format": str(format),
                "product": product,
                "step": str(step),
                "tile_id": tile_id,
            }
            for step, product, step_format in exports
            if step_format == format
            for tile_id in tile_ids
        ]
        for format in export.Format
    }
    for format, units in format_to_export_units.items():
        task_to_items[f"export-tiles-{format!s}"] = units
    for tile_unit in task_to_items["tile-units"]:
        for format, units in format_to_export_units.items():
            tile_unit[f"exports_{format!s}"] = [
                {k: v for k, v in u.items() if k != "tile_id"}
                for u in units
                if u["tile_id"] == tile_unit["tile_id"]
            ]
    return task_to_items


def write_plan(
    allow_failures: tuple[str, ...],
    derive_export: export.Format | None,
    derive_products: tuple[str, ...],
    emit_export: export.Format | None,
    methodology: attribute.Methodology,
    output_dir: str,
    scope: tuple[str, ...],
) -> None:
    """Write each list the workflow loops over to ``output_dir``, as ``<task>.json``.

    As `orchestration.plan.write_plan`, plus the exports. ``emit_export`` and
    ``derive_export`` are each step's export format, or None for no export. The per-tile
    export units are split by format, so the workflow can size each format's pods apart: only
    a COG stages its bands on local disk. A step with no export gets empty lists, which Argo
    skips.
    """
    # Before the boundaries' ingest, so that a typo fails fast
    plan.check_allow_failures(allow_failures=allow_failures, methodology=methodology)
    exports = get_exports(
        derive_export=derive_export,
        derive_products=derive_products,
        emit_export=emit_export,
    )
    plan.ingest_boundaries()
    plan.write_task_to_items(
        output_dir=output_dir,
        task_to_items=get_task_to_items(
            allow_failures=allow_failures,
            exports=exports,
            methodology=methodology,
            scope=scope,
        ),
    )
