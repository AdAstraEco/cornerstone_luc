import argparse
import collections.abc
import logging

import orchestration
from jdluc import (
    attribute,
    derive,
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

# A step's export format when it exports nothing
NO_EXPORT = "none"


def compute(
    backfill: bool,
    iso_3166s: tuple[str, ...],
    methodology: attribute.Methodology,
    tile_id: str,
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
    # A tile can straddle several of the run's countries; trace sums exactly these
    # per-(tile, country) partials, so compute has to produce all of them
    for iso_3166 in get_iso_3166s_touching_tile(
        iso_3166s=plan.get_iso_3166s(backfill=backfill, iso_3166s=iso_3166s),
        tile_id=tile_id,
    ):
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


def trace_countries(
    backfill: bool, iso_3166s: tuple[str, ...], methodology: attribute.Methodology
) -> None:
    """The cross-tile merge, written as the emissions-factor table. Cheap only if compute ran
    with the same methodology."""
    uri = trace.export_workflow(
        commodity_names=attribute.get_commodity_names(methodology=methodology),
        iso_3166s=plan.get_iso_3166s(backfill=backfill, iso_3166s=iso_3166s),
        methodology=methodology,
        name=plan.get_name(backfill=backfill, iso_3166s=iso_3166s),
    )
    logger.info(f"exported emissions factors to {uri:s}")


STEP_TO_EXPORT_WORKFLOW: dict[plan.Step, collections.abc.Callable[..., list[str]]] = {
    plan.Step.EMIT: emit.export_workflow,
    plan.Step.DERIVE: derive.export_workflow,
}


def export_tile(
    deliverable: str, format: export.Format, step: plan.Step, tile_id: str
) -> None:
    """One deliverable of one step for one tile, from emit's cached output."""
    (uri,) = STEP_TO_EXPORT_WORKFLOW[step](
        deliverable_names=(deliverable,), format=format, tile_id=tile_id
    )
    logger.info(f"exported {step!s} {deliverable=:s} to {uri:s}")


def mosaic(backfill: bool, deliverable: str, iso_3166s: tuple[str, ...]) -> None:
    """One VRT over the run's COGs of one deliverable."""
    uri = export.mosaic_workflow(
        deliverable_name=deliverable,
        name=plan.get_name(backfill=backfill, iso_3166s=iso_3166s),
        tile_ids=[
            s.tile_id
            for s in plan.get_tile_shards(backfill=backfill, iso_3166s=iso_3166s)
        ],
    )
    logger.info(f"mosaicked {deliverable=:s} to {uri:s}")


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

    # As in jdluc's CLIs: one or more countries, or --backfill for every country
    def add_iso_3166s(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument(
            "iso_3166s",
            nargs=argparse.ZERO_OR_MORE,
            type=worldbank_jurisdictions.iso_3166_str,
        )
        subparser.add_argument(
            "--backfill",
            action="store_true",
            help="every country, and every GNW tile",
        )

    def add_methodology(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument(
            "--methodology",
            choices=list(attribute.Methodology),
            metavar="{" + ",".join(m.name for m in attribute.Methodology) + "}",
            required=True,
            type=attribute.Methodology.__getitem__,
        )

    def add_export_format(subparser: argparse.ArgumentParser, flag: str) -> None:
        subparser.add_argument(
            flag,
            choices=[*export.Format, None],
            metavar="{" + ",".join([*export.Format, NO_EXPORT]) + "}",
            required=True,
            type=lambda s: None if s == NO_EXPORT else export.Format(s),
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
    subparser.add_argument(
        "--derive-deliverables",
        default=(),
        help="comma-separated derive deliverables to export",
        type=lambda s: tuple(name for name in s.split(",") if name),
    )
    add_export_format(subparser, "--derive-export")
    add_export_format(subparser, "--emit-export")
    subparser.add_argument("--output-dir", required=True)

    subparser = add_subparser("compute", compute)
    add_iso_3166s(subparser)
    add_methodology(subparser)
    add_tile_id(subparser)

    subparser = add_subparser("trace", trace_countries)
    add_iso_3166s(subparser)
    add_methodology(subparser)

    subparser = add_subparser("export-tile", export_tile)
    subparser.add_argument("--deliverable", required=True)
    subparser.add_argument(
        "--format", choices=list(export.Format), required=True, type=export.Format
    )
    subparser.add_argument(
        "--step", choices=list(plan.Step), required=True, type=plan.Step
    )
    add_tile_id(subparser)

    subparser = add_subparser("mosaic", mosaic)
    add_iso_3166s(subparser)
    subparser.add_argument("--deliverable", required=True)

    # Each subcommand's flags are exactly its function's parameters
    kwargs = vars(parser.parse_args())
    func = kwargs.pop("func")
    func(**{k: tuple(v) if isinstance(v, list) else v for k, v in kwargs.items()})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
