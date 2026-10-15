"""What a run consists of, as the JSON lists the workflow loops over.

The dataset lists are a hand-kept claim about what jdluc reads: no upstream constant declares
it all. Upstream ingests lazily, from inside harmonize, so it never needs them. We ingest up
front, so that compute runs with ``skip_ingest`` and a missing dataset fails at ingest rather
than surfacing as an all-nodata band (``ignore_missing_tiles``). In particular ``faostat.load``
and ``usda_nass_quickstats.load`` read the ingest root directly and never ingest, so those
tables appear here by name. When upstream starts reading a new dataset, add it here.
"""

import dataclasses
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


class Step(enum.StrEnum):
    """A step whose per-tile output can be exported as deliverables."""

    EMIT = "emit"
    DERIVE = "derive"


@dataclasses.dataclass(frozen=True)
class TileShard:
    """A tile, with the ``--modulus``/``--residues`` pair that selects it alone in jdluc's CLIs."""

    modulus: int
    residue: int
    tile_id: str


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


def get_tile_shards(iso_3166s: tuple[str, ...]) -> list[TileShard]:
    # Sorted, as `utils.iter_sharded` sorts, so that residue i selects exactly the i-th tile
    tile_ids = sorted(
        worldbank_jurisdictions.get_ten_degree_tile_ids_for_iso_3166s(
            iso_3166s=iso_3166s
        )
    )
    return [
        TileShard(modulus=len(tile_ids), residue=residue, tile_id=tile_id)
        for residue, tile_id in enumerate(tile_ids)
    ]


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
    iso_3166s: tuple[str, ...],
    methodology: attribute.Methodology,
    output_dir: str,
) -> None:
    """Write each list the workflow loops over to ``output_dir``, as ``<task>.json``.

    ``allow_failures`` names ten-degree datasets whose failed tiles must not stop the run.
    Their units go to their own list, so that the workflow can run them as a task that
    compute does not wait to succeed.

    ``emit_export`` and ``derive_export`` are each step's export format, or None for no
    export. The per-tile export units are split by format, so the workflow can size each
    format's pods apart: only a COG stages its bands on local disk. Each COG deliverable is
    also mosaicked. A step with no export gets empty lists, which Argo skips.
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
    shards = get_tile_shards(iso_3166s=iso_3166s)
    # One flat list of (dataset, tile), so that the workflow needs no nested loop
    ingest_units = [
        {"dataset": name.name, **dataclasses.asdict(shard)}
        for name in tiled
        for shard in shards
    ]
    task_to_items: dict[str, list[object]] = {
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
        "tiles": [dataclasses.asdict(shard) for shard in shards],
        **{
            f"export-tiles-{format!s}": [
                {
                    "deliverable": name,
                    "format": str(format),
                    "step": str(step),
                    "tile_id": shard.tile_id,
                }
                for step, name, step_format in exports
                if step_format == format
                for shard in shards
            ]
            for format in export.Format
        },
        "mosaics": [name for _, name, format in exports if format == export.Format.COG],
    }
    os.makedirs(output_dir, exist_ok=True)
    for task, items in task_to_items.items():
        path = os.path.join(output_dir, f"{task:s}.json")
        logger.info(f"Writing {len(items):d} items to {path=:s}")
        with open(path, "w") as fp:
            json.dump(items, fp)
