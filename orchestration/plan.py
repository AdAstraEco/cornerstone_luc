"""What a run consists of, as the JSON lists the workflow loops over.

The dataset lists are a hand-kept claim about what jdluc reads: no upstream constant declares
it all. Upstream ingests lazily, from inside harmonize, so it never needs them. We ingest up
front, so that compute runs with ``skip_ingest`` and a missing dataset fails at ingest rather
than surfacing as an all-nodata band (``ignore_missing_tiles``). In particular ``faostat.load``
and ``usda_nass_quickstats.load`` read the ingest root directly and never ingest, so those
tables appear here by name. When upstream starts reading a new dataset, add it here.
"""

import collections.abc
import enum
import json
import logging
import os

from jdluc import (
    attribute,
    config,
    derive,
    emit,
    export,
    harmonize,
    jurisdictional_direct,
    statistical,
    tiling,
)
from jdluc.datasets import NAME_TO_CLS, DatasetName, worldbank_jurisdictions

logger = logging.getLogger(__name__)

# Read by every tile lookup and every per-jurisdiction rollup, but named by no workflow
BOUNDARY_DATASET_NAMES = (
    DatasetName.WORLD_BANK_ADMIN_0,
    DatasetName.WORLD_BANK_ADMIN_1,
)

# Read with `load()` straight from the ingest root, never ingested on demand
METHODOLOGY_TO_LOADED_DATASET_NAMES = {
    attribute.Methodology.JURISDICTIONAL_DIRECT: (DatasetName.USDA_NASS_QUICKSTATS,),
    attribute.Methodology.STATISTICAL: (
        DatasetName.FAOSTAT_LIVESTOCK_PATTERNS,
        DatasetName.FAOSTAT_PRODUCTION_LIVESTOCK,
    ),
}

METHODOLOGY_TO_HARMONIZED_DATASET_NAMES = {
    attribute.Methodology.JURISDICTIONAL_DIRECT: jurisdictional_direct.DATASET_NAMES,
    attribute.Methodology.STATISTICAL: statistical.DATASET_NAMES,
}


# The scope of a run over every country and GNW tile, as jdluc's `--backfill`; also what its
# run-wide outputs are named, in place of every country's code joined
WORLD = "world"


class Step(enum.StrEnum):
    """A step whose per-tile output can be exported as deliverables."""

    EMIT = "emit"
    DERIVE = "derive"


def get_dataset_names(
    methodology: attribute.Methodology, partitioning: tiling.Partitioning
) -> list[DatasetName]:
    """Every dataset of one partitioning that a run of ``methodology`` reads."""
    dataset_names = {
        *BOUNDARY_DATASET_NAMES,
        *(name for stack in harmonize.Stack for name in stack.value),
        *METHODOLOGY_TO_HARMONIZED_DATASET_NAMES[methodology],
        *METHODOLOGY_TO_LOADED_DATASET_NAMES[methodology],
    }
    return sorted(
        (
            name
            for name in dataset_names
            if NAME_TO_CLS[name].partitioning == partitioning
        ),
        key=lambda name: name.name,
    )


def scope_str(s: str) -> str:
    """One element of a run's scope: an ISO 3166 alpha-3 code, or ``world``."""
    return s if s == WORLD else worldbank_jurisdictions.iso_3166_str(s=s)


def is_world(scope: tuple[str, ...]) -> bool:
    assert scope, "the scope names no country"
    assert scope == (WORLD,) or WORLD not in scope, (
        f"{scope=} mixes {WORLD} with countries"
    )
    return scope == (WORLD,)


def get_iso_3166s(scope: tuple[str, ...]) -> tuple[str, ...]:
    """The scope's countries: those named, or for ``world`` every country, as jdluc's
    ``--backfill`` reads them."""
    return tuple(
        sorted(
            worldbank_jurisdictions.get_all_iso_3166s() if is_world(scope) else scope
        )
    )


def get_name(scope: tuple[str, ...]) -> str:
    """What the run's run-wide outputs (its mosaics and emissions-factor table) are named."""
    return WORLD if is_world(scope) else "-".join(sorted(scope))


def get_tile_ids(scope: tuple[str, ...]) -> list[str]:
    """The tiles the scope's countries touch, or for ``world`` every GNW tile, as jdluc's
    ``--backfill`` reads them: tiles no country touches included."""
    return sorted(
        tiling.GLOBAL_NATURE_WATCH_TILE_IDS
        if is_world(scope)
        else worldbank_jurisdictions.get_ten_degree_tile_ids_for_iso_3166s(
            iso_3166s=get_iso_3166s(scope=scope)
        )
    )


def get_exports(
    derive_deliverables: tuple[str, ...],
    derive_export: export.Format | None,
    emit_export: export.Format | None,
) -> list[tuple[Step, str, export.Format]]:
    """Each (step, deliverable, format) the run exports; a step whose format is None exports
    nothing.

    emit exports every deliverable it has, derive those named. The step's format overrides
    each deliverable's own.
    """
    unknown = sorted(set(derive_deliverables) - set(derive.NAME_TO_DELIVERABLE))
    assert not unknown, (
        f"{unknown} are not derive deliverables: {sorted(derive.NAME_TO_DELIVERABLE)}"
    )
    assert derive_export is None or derive_deliverables, (
        f"derive exports as {derive_export!s}, but names no deliverable"
    )
    step_to_names = {
        Step.EMIT: tuple(emit.NAME_TO_DELIVERABLE),
        Step.DERIVE: derive_deliverables,
    }
    step_to_format = {Step.EMIT: emit_export, Step.DERIVE: derive_export}
    return [
        (step, name, format)
        for step, format in step_to_format.items()
        if format is not None
        for name in step_to_names[step]
    ]


def ingest_boundaries() -> None:
    """The tile list is read off the boundaries, so the plan ingests them first.

    A no-op when they are already in the ingest root, as after any earlier run.
    """
    root = config.Config.from_dot_env().ingest_root
    for name in BOUNDARY_DATASET_NAMES:
        logger.info(f"Ingesting {name.name:s} if absent")
        NAME_TO_CLS[name].ingest_a_tile(
            overwrite=False, root=root, tile_id=tiling.WHOLE_WORLD_TILE_ID
        )


def write_plan(
    allow_failures: tuple[str, ...],
    derive_deliverables: tuple[str, ...],
    derive_export: export.Format | None,
    emit_export: export.Format | None,
    methodology: attribute.Methodology,
    output_dir: str,
    scope: tuple[str, ...],
) -> None:
    """Write each list the workflow loops over to ``output_dir``, as ``<task>.json``.

    The run covers ``scope``: one or more countries, or ``world`` for every country and GNW
    tile.

    ``allow_failures`` names ten-degree datasets whose failed tiles must not stop the run.
    Their units go to their own list, so that the workflow can run them as a task that
    compute does not wait to succeed.

    ``emit_export`` and ``derive_export`` are each step's export format, or None for no
    export. The per-tile export units are split by format, so the workflow can size each
    format's pods apart: only a COG stages its bands on local disk. Each COG deliverable is
    also mosaicked. A step with no export gets empty lists, which Argo skips.

    The flat workflow loops over each stage's list across every tile. ``tile-units`` holds
    the same units grouped by tile, for the per-tile workflow, which runs each tile's
    ingest, compute and exports as a DAG of its own.
    """
    tiled = get_dataset_names(
        methodology=methodology, partitioning=tiling.Partitioning.TEN_DEGREE_TILE
    )
    unknown = sorted(set(allow_failures) - {name.name for name in tiled})
    assert not unknown, f"{unknown} are not ten-degree datasets this run ingests"
    exports = get_exports(
        derive_deliverables=derive_deliverables,
        derive_export=derive_export,
        emit_export=emit_export,
    )

    ingest_boundaries()
    tile_ids = get_tile_ids(scope=scope)
    # One flat list of (dataset, tile), so that the workflow needs no nested loop
    ingest_units = [
        {"dataset": name.name, "tile_id": tile_id}
        for name in tiled
        for tile_id in tile_ids
    ]
    format_to_export_units = {
        format: [
            {
                "deliverable": name,
                "format": str(format),
                "step": str(step),
                "tile_id": tile_id,
            }
            for step, name, step_format in exports
            if step_format == format
            for tile_id in tile_ids
        ]
        for format in export.Format
    }
    task_to_items: dict[str, collections.abc.Sequence[object]] = {
        "ingest-world": [
            name.name
            for name in get_dataset_names(
                methodology=methodology, partitioning=tiling.Partitioning.WHOLE_WORLD
            )
            if name not in BOUNDARY_DATASET_NAMES
        ],
        "ingest-tiles": [u for u in ingest_units if u["dataset"] not in allow_failures],
        "ingest-tiles-allowed-failures": [
            u for u in ingest_units if u["dataset"] in allow_failures
        ],
        "tiles": [{"tile_id": tile_id} for tile_id in tile_ids],
        **{
            f"export-tiles-{format!s}": units
            for format, units in format_to_export_units.items()
        },
        "mosaics": [name for _, name, format in exports if format == export.Format.COG],
        "tile-units": [
            {
                "tile_id": tile_id,
                "ingests": [
                    u["dataset"]
                    for u in ingest_units
                    if u["tile_id"] == tile_id and u["dataset"] not in allow_failures
                ],
                "ingests_allowed_failures": [
                    u["dataset"]
                    for u in ingest_units
                    if u["tile_id"] == tile_id and u["dataset"] in allow_failures
                ],
                **{
                    f"exports_{format!s}": [
                        {k: v for k, v in u.items() if k != "tile_id"}
                        for u in units
                        if u["tile_id"] == tile_id
                    ]
                    for format, units in format_to_export_units.items()
                },
            }
            for tile_id in tile_ids
        ],
    }
    os.makedirs(output_dir, exist_ok=True)
    for task, items in task_to_items.items():
        path = os.path.join(output_dir, f"{task:s}.json")
        logger.info(f"Writing {len(items):d} items to {path=:s}")
        with open(path, "w") as fp:
            json.dump(items, fp)
