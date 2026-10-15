import argparse
import collections.abc
import logging

import orchestration
from jdluc import (
    attribute,
    emit,
    export,
    harmonize,
    jurisdictional_direct,
    statistical,
    tiling,
    trace,
)
from jdluc.datasets import worldbank_jurisdictions
from orchestration import plan

logger = logging.getLogger(__name__)


def compute(
    iso_3166s: tuple[str, ...], methodology: attribute.Methodology, tile_id: str
) -> None:
    """harmonize -> emit -> attribute for one tile, in one process.

    harmonize goes first with ``skip_ingest=True``: the later stages call it again with
    ``skip_ingest=False``, and the cache key ignores the flag, so warming it here is what
    keeps source credentials off compute pods.
    """
    for stack in harmonize.Stack:
        logger.info(f"harmonize {stack.name:s} {tile_id=:s}")
        harmonize.workflow(
            dataset_names=stack.value,
            ignore_missing_tiles=True,
            skip_ingest=True,
            tile_id=tile_id,
            tile_resolution=tiling.TileResolution.GLAD,
        )
    logger.info(f"emit {tile_id=:s}")
    emit.workflow(tile_id=tile_id)

    workflow_for_tile = {
        attribute.Methodology.JURISDICTIONAL_DIRECT: jurisdictional_direct.workflow,
        attribute.Methodology.STATISTICAL: statistical.workflow,
    }[methodology]
    # A tile can straddle several of the run's countries; reduce sums exactly these
    # per-(tile, country) partials, so compute has to produce all of them
    for iso_3166 in get_iso_3166s_touching_tile(iso_3166s=iso_3166s, tile_id=tile_id):
        logger.info(f"attribute {methodology.name:s} {tile_id=:s} {iso_3166=:s}")
        workflow_for_tile(
            commodity_names=attribute.get_commodity_names(methodology=methodology),
            iso_3166=iso_3166,
            tile_id=tile_id,
        )


def get_iso_3166s_touching_tile(
    iso_3166s: collections.abc.Iterable[str], tile_id: str
) -> list[str]:
    return sorted(
        iso_3166
        for iso_3166 in iso_3166s
        if tile_id
        in worldbank_jurisdictions.get_ten_degree_tile_ids_for_iso_3166s(
            iso_3166s=(iso_3166,)
        )
    )


def reduce(iso_3166s: tuple[str, ...], methodology: attribute.Methodology) -> None:
    """The cross-tile merge. Cheap only if compute ran with the same methodology."""
    df = trace.workflow(
        # NB: not a cache-key argument; the tiles are warm, so this only fans out the reads
        concurrency=1,
        commodity_names=attribute.get_commodity_names(methodology=methodology),
        iso_3166s=tuple(sorted(iso_3166s)),
        methodology=methodology,
    )
    logger.info(f"traced {len(df):d} (jurisdiction, crop) rows")


def export_tile(tile_id: str) -> None:
    """emit's cached output for one tile, as the CIE emissions COG."""
    (uri,) = emit.export_workflow(tile_id=tile_id)
    logger.info(f"exported emissions COG to {uri:s}")


def mosaic(iso_3166s: tuple[str, ...]) -> None:
    """One VRT over the run's emissions COGs."""
    uri = export.mosaic_workflow(
        deliverable_name=emit.CIE_EMISSIONS.name,
        name="-".join(sorted(iso_3166s)),
        tile_ids=[s.tile_id for s in plan.get_tile_shards(iso_3166s=iso_3166s)],
    )
    logger.info(f"mosaicked emissions VRT to {uri:s}")


def main() -> int:
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )
    parser = argparse.ArgumentParser(
        description=orchestration.__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(required=True)

    def add_subparser(
        name: str, func: collections.abc.Callable[..., None]
    ) -> argparse.ArgumentParser:
        subparser = subparsers.add_parser(
            name, help=(func.__doc__ or "").split("\n")[0]
        )
        subparser.set_defaults(func=func)
        return subparser

    def add_iso_3166s(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument(
            "iso_3166s",
            nargs=argparse.ONE_OR_MORE,
            type=worldbank_jurisdictions.iso_3166_str,
        )

    def add_methodology(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument(
            "--methodology",
            choices=list(attribute.Methodology),
            metavar="{" + ",".join(m.name for m in attribute.Methodology) + "}",
            required=True,
            type=attribute.Methodology.__getitem__,
        )

    def add_tile_id(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("--tile-id", required=True)

    subparser = add_subparser("plan", plan.write_plan)
    add_iso_3166s(subparser)
    add_methodology(subparser)
    subparser.add_argument(
        "--allow-failures",
        default=(),
        help="comma-separated ten-degree datasets whose failed tiles must not stop the run",
        type=lambda s: tuple(name for name in s.split(",") if name),
    )
    subparser.add_argument("--output-dir", required=True)

    subparser = add_subparser("compute", compute)
    add_iso_3166s(subparser)
    add_methodology(subparser)
    add_tile_id(subparser)

    subparser = add_subparser("reduce", reduce)
    add_iso_3166s(subparser)
    add_methodology(subparser)

    add_tile_id(add_subparser("export", export_tile))

    add_iso_3166s(add_subparser("mosaic", mosaic))

    # Each subcommand's flags are exactly its function's parameters
    kwargs = vars(parser.parse_args())
    func = kwargs.pop("func")
    func(**{k: tuple(v) if isinstance(v, list) else v for k, v in kwargs.items()})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
