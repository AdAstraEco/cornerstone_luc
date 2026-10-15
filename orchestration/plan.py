"""What a run consists of, as the JSON lists the workflow loops over.

The dataset lists are a hand-kept claim about what jdluc reads: no upstream constant declares
it all. Upstream ingests lazily, from inside harmonize, so it never needs them. We ingest up
front, so that compute runs with ``skip_ingest`` and a missing dataset fails at ingest rather
than surfacing as an all-nodata band (``ignore_missing_tiles``). In particular ``faostat.load``
and ``usda_nass_quickstats.load`` read the ingest root directly and never ingest, so those
tables appear here by name. When upstream starts reading a new dataset, add it here.
"""

import dataclasses
import json
import logging
import os

from jdluc import (
    attribute,
    config,
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
    iso_3166s: tuple[str, ...],
    methodology: attribute.Methodology,
    output_dir: str,
) -> None:
    """Write each list the workflow loops over to ``output_dir``, as ``<task>.json``.

    ``allow_failures`` names ten-degree datasets whose failed tiles must not stop the run.
    Their units go to their own list, so that the workflow can run them as a task that
    compute does not wait to succeed.
    """
    tiled = get_dataset_names(
        methodology=methodology, partitioning=tiling.Partitioning.TEN_DEGREE_TILE
    )
    unknown = sorted(set(allow_failures) - {name.name for name in tiled})
    assert not unknown, f"{unknown} are not ten-degree datasets this run ingests"

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
    }
    os.makedirs(output_dir, exist_ok=True)
    for task, items in task_to_items.items():
        path = os.path.join(output_dir, f"{task:s}.json")
        logger.info(f"Writing {len(items):d} items to {path=:s}")
        with open(path, "w") as fp:
            json.dump(items, fp)
