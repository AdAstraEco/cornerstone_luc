import argparse
import collections.abc
import logging

import orchestration_ext
from jdluc import derive, emit, export
from orchestration import __main__ as cli
from orchestration import plan
from orchestration_ext import plan as ext_plan

logger = logging.getLogger(__name__)

# A step's export format when it exports nothing
NO_EXPORT = "none"

STEP_TO_EXPORT_WORKFLOW: dict[
    ext_plan.Step, collections.abc.Callable[..., list[str]]
] = {
    ext_plan.Step.EMIT: emit.export_workflow,
    ext_plan.Step.DERIVE: derive.export_workflow,
}


def export_tile(
    deliverable: str, format: export.Format, step: ext_plan.Step, tile_id: str
) -> None:
    """One deliverable of one step for one tile, from emit's cached output."""
    (uri,) = STEP_TO_EXPORT_WORKFLOW[step](
        deliverable_names=(deliverable,), format=format, tile_id=tile_id
    )
    logger.info(f"exported {step!s} {deliverable=:s} to {uri:s}")


def mosaic(deliverable: str, scope: tuple[str, ...]) -> None:
    """One VRT over the run's COGs of one deliverable."""
    uri = export.mosaic_workflow(
        deliverable_name=deliverable,
        name=plan.get_name(scope=scope),
        tile_ids=plan.get_tile_ids(scope=scope),
    )
    logger.info(f"mosaicked {deliverable=:s} to {uri:s}")


def add_export_format(subparser: argparse.ArgumentParser, flag: str) -> None:
    subparser.add_argument(
        flag,
        choices=[*export.Format, None],
        metavar="{" + ",".join([*export.Format, NO_EXPORT]) + "}",
        required=True,
        type=lambda s: None if s == NO_EXPORT else export.Format(s),
    )


def build_parser() -> tuple[
    argparse.ArgumentParser, argparse._SubParsersAction[argparse.ArgumentParser]
]:
    """`orchestration`'s CLI, with a plan that lists the exports, and the export
    subcommands."""
    parser, subparsers = cli.build_parser()
    parser.description = orchestration_ext.__doc__

    # The same subcommand, so its flags stay upstream's, plus the exports'
    subparser = subparsers.choices["plan"]
    subparser.set_defaults(func=ext_plan.write_plan)
    subparser.add_argument(
        "--derive-deliverables",
        default=(),
        help="comma-separated derive deliverables to export",
        type=lambda s: tuple(name for name in s.split(",") if name),
    )
    add_export_format(subparser, "--derive-export")
    add_export_format(subparser, "--emit-export")

    subparser = cli.add_subparser(subparsers, "export-tile", export_tile)
    subparser.add_argument("--deliverable", required=True)
    subparser.add_argument(
        "--format", choices=list(export.Format), required=True, type=export.Format
    )
    subparser.add_argument(
        "--step", choices=list(ext_plan.Step), required=True, type=ext_plan.Step
    )
    cli.add_tile_id(subparser)

    subparser = cli.add_subparser(subparsers, "mosaic", mosaic)
    cli.add_scope(subparser)
    subparser.add_argument("--deliverable", required=True)

    return parser, subparsers


def main() -> int:
    parser, _ = build_parser()
    return cli.run(parser)


if __name__ == "__main__":
    raise SystemExit(main())
