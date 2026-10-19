import argparse
import logging

import orchestration_ext
from jdluc import derive, emit, export
from orchestration import __main__ as cli
from orchestration_ext import plan as ext_plan

logger = logging.getLogger(__name__)

# A step's export format when it exports nothing
NO_EXPORT = "none"


def export_tile(
    format: export.Format, product: str, step: ext_plan.Step, tile_id: str
) -> None:
    """One product of one step for one tile, from emit's cached output."""
    ext_plan.check_product(product=product, step=step)
    match step:
        case ext_plan.Step.EMIT:
            uri = export.write_tile(
                dset=emit.workflow(tile_id=tile_id),
                format=format,
                name=product,
                source_name="jdluc-emit",
                tile_id=tile_id,
                variables=emit.CIE_BANDS,
            )
        case ext_plan.Step.DERIVE:
            (uri,) = derive.export_workflow(
                format=format, product_names=(product,), tile_id=tile_id
            )
    logger.info(f"exported {step!s} {product=:s} to {uri:s}")


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
    """`orchestration`'s CLI, with a plan that lists the exports, and `export-tile`."""
    parser, subparsers = cli.build_parser()
    parser.description = orchestration_ext.__doc__

    # The same subcommand, so its flags stay upstream's, plus the exports'
    subparser = subparsers.choices["plan"]
    subparser.set_defaults(func=ext_plan.write_plan)
    subparser.add_argument(
        "--derive-products",
        default=(),
        help=f"comma-separated, of {','.join(derive.NAME_TO_PRODUCT):s}",
        type=lambda s: tuple(name for name in s.split(",") if name),
    )
    add_export_format(subparser, "--derive-export")
    add_export_format(subparser, "--emit-export")

    subparser = cli.add_subparser(subparsers, "export-tile", export_tile)
    subparser.add_argument(
        "--format", choices=list(export.Format), required=True, type=export.Format
    )
    subparser.add_argument("--product", required=True)
    subparser.add_argument(
        "--step", choices=list(ext_plan.Step), required=True, type=ext_plan.Step
    )
    cli.add_tile_id(subparser)

    return parser, subparsers


def main() -> int:
    parser, _ = build_parser()
    return cli.run(parser)


if __name__ == "__main__":
    raise SystemExit(main())
