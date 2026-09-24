"""Run ONE pod's worth of one phase of an AOI run (docs/orbae/scale-out.md).

The phases, in the order the driver (``infra/run_aoi.py``) submits them:

``ingest-world``
    One pod: every whole-world dataset the AOI needs (boundaries, IPCC zones, MAPSPAM,
    NASS tables). Kept out of the per-tile fan-out precisely because these are one
    object each -- N tile pods would otherwise race to write the same path. Best-effort.
``ingest-tiles``
    One pod per tile: the ten-degree datasets for that tile. Holds the source-API
    credentials. Best-effort (an ocean/edge tile legitimately has no data).
``compute``
    One pod per tile: ``harmonize`` -> ``emit`` -> the per-(tile, country) attribute leg,
    chained in-process so a tile costs one pod and one scheduling round rather than three.
    Runs with ``skip_ingest``, so it needs no credentials, only a warm ``INGEST_ROOT``.
``reduce``
    One pod: ``trace.workflow`` for the whole AOI, which drives ``attribute.workflow``'s
    cross-tile merge. Every per-tile partial is warm by then, so this is a groupby-sum.
``export``
    One pod per tile: ``export.workflow`` serialises ``emit``'s cached scratch output to an
    emissions COG (docs/rooster-emissions-cog-spike.md). Additive and terminal -- nothing
    downstream reads it -- so it runs after ``reduce`` and reads ``emit``'s warm cache with no
    recompute.
``mosaic``
    One pod: ``export.mosaic_workflow`` stitches the per-tile emission COGs into one AOI-wide
    read-time VRT. A tiny XML over the tiles (lazy, no materialisation); runs after ``export``.

Chaining inside a pod is safe because every stage is wrapped in ``@storage.cache_to_*``:
a pod that dies mid-chain resumes from its last completed stage on retry, and rerunning a
whole phase no-ops over warm tiles. That is the resume mechanism -- there is no scheduler.

The per-tile phases take their tile from ``JOB_COMPLETION_INDEX`` (set by the k8s Indexed
Job) by indexing into the sorted tile list the AOI's countries resolve to. That derivation
is the same one ``attribute.workflow`` uses, so it is deterministic in-pod and no tile list
has to be plumbed through a ConfigMap.

Run inside the container (the Job does this for you)::

    python infra/run_phase.py --phase compute HND              # tile from JOB_COMPLETION_INDEX
    python infra/run_phase.py --phase compute --tile-id 20N_090W HND
"""

import argparse
import collections.abc
import enum
import logging
import os

import resource_monitor

from jdluc import (
    attribute,
    config,
    datasets,
    emit,
    export,
    harmonize,
    ingest,
    jurisdictional_direct,
    statistical,
    tiling,
    trace,
)
from jdluc.datasets import DatasetName, worldbank_jurisdictions

logger = logging.getLogger(__name__)

# k8s Indexed Job: each pod's 0-based index within `completions`.
COMPLETION_INDEX_ENV_VAR = "JOB_COMPLETION_INDEX"

# No workflow names these, but every stage resolves jurisdictions through them.
BOUNDARY_DATASET_NAMES = (
    DatasetName.WORLD_BANK_ADMIN_0,
    DatasetName.WORLD_BANK_ADMIN_1,
)

DEFAULT_INGEST_CONCURRENCY = 4


class Phase(enum.StrEnum):
    INGEST_WORLD = "ingest-world"
    INGEST_TILES = "ingest-tiles"
    COMPUTE = "compute"
    REDUCE = "reduce"
    EXPORT = "export"
    MOSAIC = "mosaic"

    @property
    def is_per_tile(self) -> bool:
        """Does the driver fan this phase out over the AOI's tiles (vs one pod for the AOI)?"""
        return self in (Phase.INGEST_TILES, Phase.COMPUTE, Phase.EXPORT)


def get_dataset_names(methodology: attribute.Methodology) -> tuple[DatasetName, ...]:
    """Every dataset a run of ``methodology`` reads."""
    return (
        *BOUNDARY_DATASET_NAMES,
        *(name for stack in harmonize.Stack for name in stack.value),
        *(
            (*jurisdictional_direct.DATASET_NAMES, DatasetName.USDA_NASS_QUICKSTATS)
            if methodology == attribute.Methodology.JURISDICTIONAL_DIRECT
            else statistical.DATASET_NAMES
        ),
    )


def get_dataset_names_for_phase(
    methodology: attribute.Methodology, phase: Phase
) -> tuple[DatasetName, ...]:
    """Split the dataset list by partitioning: whole-world datasets vs per-tile ones.

    This is what keeps ``ingest-world`` and ``ingest-tiles`` from overlapping. A whole-world
    dataset is a single object however many tiles the AOI has (``ingest.workflow`` rewrites
    its tile set to ``WHOLE_WORLD_TILE_ID``), so ingesting it from every tile pod would be
    N concurrent writers to one path.
    """
    assert phase in (Phase.INGEST_WORLD, Phase.INGEST_TILES), phase
    want_whole_world = phase == Phase.INGEST_WORLD
    return tuple(
        dataset_name
        for dataset_name in get_dataset_names(methodology=methodology)
        if (
            datasets.NAME_TO_CLS[dataset_name].partitioning
            == tiling.Partitioning.WHOLE_WORLD
        )
        == want_whole_world
    )


def get_tile_ids(iso_3166s: collections.abc.Iterable[str]) -> list[str]:
    """The AOI's tiles, sorted -- the index space the Indexed Jobs are sized against."""
    return sorted(
        worldbank_jurisdictions.get_ten_degree_tile_ids_for_iso_3166s(
            iso_3166s=iso_3166s
        )
    )


def get_tile_id_for_index(
    iso_3166s: collections.abc.Iterable[str], tile_index: int
) -> str:
    tile_ids = get_tile_ids(iso_3166s=iso_3166s)
    assert 0 <= tile_index < len(tile_ids), (
        f"{tile_index=:d} is outside the {len(tile_ids):d} tile(s) of {sorted(iso_3166s)}; "
        "the Job's completions and the pod's country arguments must agree"
    )
    return tile_ids[tile_index]


def get_iso_3166s_for_tile_id(
    iso_3166s: collections.abc.Iterable[str], tile_id: str
) -> list[str]:
    """Which of the AOI's countries this tile actually overlaps (a tile can hold several)."""
    return sorted(
        iso_3166
        for iso_3166 in iso_3166s
        if tile_id
        in worldbank_jurisdictions.get_ten_degree_tile_ids_for_admin_id(
            admin_id=iso_3166,
            admin_level=worldbank_jurisdictions.AdminLevel.NATIONAL,
        )
    )


def run_ingest(
    concurrency: int,
    dataset_names: collections.abc.Sequence[DatasetName],
    tile_ids: collections.abc.Sequence[str],
) -> int:
    """Ingest each dataset over ``tile_ids``, best-effort. Returns a process exit code.

    Best-effort means a per-(dataset, tile) failure is logged and the rest still run: a
    missing tile is often legitimate (an ocean or map-edge tile among the AOI's bounding
    tiles), and abandoning the pod would waste the tiles that did fetch. A phase where
    *nothing* succeeded is a different animal -- bad credentials, no network, wrong root --
    so that exits non-zero and stops the driver at the barrier.
    """
    root = config.Config.from_dot_env().ingest_root
    successes: list[tuple[DatasetName, str]] = []
    failures: list[tuple[DatasetName, str, str]] = []
    for dataset_name in dataset_names:
        logger.info(f"Ingesting {dataset_name!s} over {len(tile_ids):d} tile(s)")
        try:
            tile_id_to_result = ingest.workflow(
                concurrency=concurrency,
                dataset=datasets.NAME_TO_CLS[dataset_name],
                overwrite=False,
                root=root,
                tile_ids=tile_ids,
            )
        except Exception as exc:
            # ingest.workflow itself only raises on a bad dataset/tile combination; a
            # per-tile fetch failure comes back inside the result dict.
            logger.exception(f"Ingest of {dataset_name!s} failed outright")
            failures.append((dataset_name, "*", repr(exc)))
            continue
        for tile_id, result in sorted(tile_id_to_result.items()):
            if isinstance(result, Exception):
                failures.append((dataset_name, tile_id, repr(result)))
            else:
                successes.append((dataset_name, tile_id))

    logger.info(f"Ingested {len(successes):d} ok, {len(failures):d} failed")
    for dataset_name, tile_id, message in failures:
        logger.warning(f"  failed: {dataset_name!s} {tile_id:s}: {message:s}")
    if not successes:
        logger.error("Nothing was ingested at all; treating the phase as failed")
        return 1
    return 0


def run_compute(
    crop_names: tuple[str, ...],
    iso_3166s: collections.abc.Sequence[str],
    methodology: attribute.Methodology,
    skip_ingest: bool,
    tile_id: str,
) -> None:
    """harmonize -> emit -> the attribute leg, for one tile, chained in one process.

    Every call is cached, so the chain really means "make these warm for this tile".
    ``harmonize`` is called explicitly, rather than left to the attribute leg's internal
    call, so that ``skip_ingest`` is honoured on a cold cache: the inner calls pass
    ``skip_ingest=False`` and the cache key ignores the flag, so warming it here is what
    keeps source credentials off the compute pods.
    """
    for stack in harmonize.Stack:
        logger.info(f"harmonize {stack.name:s} {tile_id=:s}")
        harmonize.workflow(
            dataset_names=stack.value,
            ignore_missing_tiles=True,
            skip_ingest=skip_ingest,
            tile_id=tile_id,
            tile_resolution=tiling.TileResolution.GLAD,
        )
    logger.info(f"emit {tile_id=:s}")
    emit.workflow(tile_id=tile_id)

    workflow_for_tile = (
        jurisdictional_direct.workflow
        if methodology == attribute.Methodology.JURISDICTIONAL_DIRECT
        else statistical.workflow
    )
    # A tile can straddle several of the AOI's countries; the reduce sums exactly these
    # per-(tile, country) partials, so compute has to produce all of them.
    for iso_3166 in get_iso_3166s_for_tile_id(iso_3166s=iso_3166s, tile_id=tile_id):
        logger.info(f"attribute {methodology.name:s} {tile_id=:s} {iso_3166=:s}")
        workflow_for_tile(
            crop_names=crop_names,
            iso_3166=iso_3166,
            tile_id=tile_id,
        )


def run_reduce(
    crop_names: tuple[str, ...],
    iso_3166s: collections.abc.Iterable[str],
    methodology: attribute.Methodology,
) -> None:
    """The AOI's cross-tile merge; ``trace.workflow`` drives ``attribute.workflow``.

    Cheap *only* if the compute phase ran with the same methodology and crop names: those
    are cache-key arguments, so a mismatch silently
    recomputes every tile here, in one pod. The driver passes identical flags to both.
    """
    df = trace.workflow(
        # NB: not a cache-key argument; the tiles are warm, so this only fans out the reads
        concurrency=1,
        crop_names=crop_names,
        iso_3166s=tuple(sorted(iso_3166s)),
        methodology=methodology,
    )
    logger.info(f"traced {len(df):d} (jurisdiction, crop) rows")


def run_export(tile_id: str) -> None:
    """Serialise this tile's ``emit`` scratch output to a CIE COG.

    Additive and terminal (docs/rooster-emissions-cog-spike.md): it reads ``emit``'s cached
    zarr -- warm after ``compute`` -- and writes one COG, with no recompute and no edits to any
    other stage. Methodology-agnostic, since ``emit`` sits below the attribute legs, so it takes
    only the tile.
    """
    uri = export.workflow(tile_id=tile_id)
    logger.info(f"exported emissions COG to {uri:s}")


def run_mosaic(iso_3166s: collections.abc.Sequence[str]) -> None:
    """Stitch the AOI's per-tile emission COGs into one read-time VRT (one pod, after ``export``).

    Mirrors ``reduce``'s shape: a single AOI-wide pod over the same sorted tile list. Absent tiles
    (ocean/edge, or an export the AOI never produced) are skipped. Methodology-agnostic.
    """
    uri = export.mosaic_workflow(
        tile_ids=get_tile_ids(iso_3166s=iso_3166s),
        name="-".join(sorted(iso_3166s)),
    )
    logger.info(f"mosaicked AOI emissions VRT to {uri:s}")


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "iso_3166s",
        nargs=argparse.ONE_OR_MORE,
        type=worldbank_jurisdictions.iso_3166_str,
        help="the AOI: cover exactly the tiles these countries' boundaries touch",
    )
    parser.add_argument("--phase", choices=[str(p) for p in Phase], required=True)
    parser.add_argument(
        "--tile-id",
        help="run this tile instead of the one JOB_COMPLETION_INDEX picks (per-tile phases)",
    )
    parser.add_argument(
        "--tile-index",
        type=int,
        default=os.environ.get(COMPLETION_INDEX_ENV_VAR),
        help=f"index into the AOI's sorted tiles; defaults to ${COMPLETION_INDEX_ENV_VAR}",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_INGEST_CONCURRENCY,
        help="in-pod ingest threads (ingest phases only)",
    )
    parser.add_argument(
        "--methodology-name",
        choices=sorted(e.name for e in attribute.Methodology),
        default=attribute.Methodology.STATISTICAL.name,
    )
    parser.add_argument(
        "--skip-ingest",
        action="store_true",
        help="compute phase: read a pre-warmed INGEST_ROOT and never touch a source",
    )
    args = parser.parse_args()

    phase = Phase(args.phase)
    methodology = attribute.Methodology[str(args.methodology_name)]
    iso_3166s: list[str] = sorted(args.iso_3166s)

    tile_id: str | None = None
    if phase.is_per_tile:
        if args.tile_id:
            tile_id = str(args.tile_id)
        elif args.tile_index is None:
            parser.error(
                f"phase {phase!s} is per-tile: pass --tile-id, or --tile-index / "
                f"${COMPLETION_INDEX_ENV_VAR} (the Indexed Job sets it)"
            )
        else:
            tile_id = get_tile_id_for_index(
                iso_3166s=iso_3166s, tile_index=int(args.tile_index)
            )
    logger.info(f"phase {phase!s} for {iso_3166s} {tile_id=} {methodology.name=:s}")

    # Sample this pod's own resource use (disk-write throttling, memory, CPU) to its logs,
    # so we can see which resource capped without infra tooling -- see resource_monitor.py
    # and docs/gke-disk-io-findings.md.
    monitor_label = f"{phase!s}:{tile_id or '-'.join(iso_3166s)}"
    with resource_monitor.monitor(label=monitor_label):
        match phase:
            case Phase.INGEST_WORLD | Phase.INGEST_TILES:
                return run_ingest(
                    concurrency=int(args.concurrency),
                    dataset_names=get_dataset_names_for_phase(
                        methodology=methodology, phase=phase
                    ),
                    # A whole-world dataset ignores the tile set, so ingest-world's pod passes
                    # the AOI's whole list and ingest.workflow collapses it.
                    tile_ids=[tile_id]
                    if tile_id
                    else get_tile_ids(iso_3166s=iso_3166s),
                )
            case Phase.COMPUTE:
                assert tile_id
                run_compute(
                    crop_names=attribute.get_crop_names(methodology=methodology),
                    iso_3166s=iso_3166s,
                    methodology=methodology,
                    skip_ingest=args.skip_ingest,
                    tile_id=tile_id,
                )
            case Phase.REDUCE:
                run_reduce(
                    crop_names=attribute.get_crop_names(methodology=methodology),
                    iso_3166s=iso_3166s,
                    methodology=methodology,
                )
            case Phase.EXPORT:
                assert tile_id
                run_export(tile_id=tile_id)
            case Phase.MOSAIC:
                run_mosaic(iso_3166s=iso_3166s)
    logger.info(f"Done: phase {phase!s}{f' for {tile_id:s}' if tile_id else ''}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
