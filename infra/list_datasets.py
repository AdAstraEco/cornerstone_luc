"""Print, as a JSON list, the datasets a run must ingest before compute -- the Argo ingest fan-out.

Upstream ingests lazily, from inside harmonize, so it never needs this list. We ingest up front,
one pod per dataset, so that compute can run with ``--skip-ingest`` and a missing dataset fails at
ingest rather than surfacing as an all-nodata band (``ignore_missing_tiles``) or a missing file.

The list is a hand-kept claim about what the code reads: no upstream constant declares it all.
In particular ``faostat.load`` and ``usda_nass_quickstats.load`` read the ingest root directly and
never ingest, so those tables appear here by name. When upstream starts reading a new dataset,
add it here.

``--partitioning`` splits the list the way the workflow needs it:

``whole-world``
    One object per dataset, ingested with ``jdluc/ingest.py NAME --backfill``, which never
    resolves countries to tiles. Includes the World Bank boundaries, which every tile lookup
    reads, so this half runs first.
``ten-degree-tile``
    Ingested with ``jdluc/ingest.py NAME ISO``, over the tiles the country's boundary touches.

Run inside the container (the workflow does this for you)::

    python infra/list_datasets.py --partitioning whole-world
"""

import argparse
import json

from jdluc import attribute, harmonize, jurisdictional_direct, statistical, tiling
from jdluc.datasets import NAME_TO_CLS, DatasetName

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


def get_dataset_names(
    methodology: attribute.Methodology, partitioning: tiling.Partitioning | None
) -> list[DatasetName]:
    """Every dataset a run of ``methodology`` reads, optionally only those of one partitioning."""
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
            if partitioning is None or NAME_TO_CLS[name].partitioning == partitioning
        ),
        key=lambda name: name.name,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--methodology-name",
        choices=sorted(e.name for e in attribute.Methodology),
        default=attribute.Methodology.STATISTICAL.name,
    )
    parser.add_argument(
        "--partitioning",
        choices=[
            str(tiling.Partitioning.TEN_DEGREE_TILE),
            str(tiling.Partitioning.WHOLE_WORLD),
        ],
        help="only datasets stored this way; all of them by default",
    )
    args = parser.parse_args()
    dataset_names = get_dataset_names(
        methodology=attribute.Methodology[str(args.methodology_name)],
        partitioning=(
            tiling.Partitioning[str(args.partitioning).upper().replace("-", "_")]
            if args.partitioning
            else None
        ),
    )
    print(json.dumps([name.name for name in dataset_names]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
