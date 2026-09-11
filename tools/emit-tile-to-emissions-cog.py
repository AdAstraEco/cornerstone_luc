"""Slice-1 driver: turn one emit tile's scratch output into an emissions COG to eyeball.

Reads emit's cached zarr for a tile via ``jdluc.export.workflow`` and writes a multi-band
emissions COG. Defaults to a local path so the artifact is easy to inspect without pulling
from scratch, then reports per-band stats and checks the biomass+soil+peat == total
identity that holds by construction (see docs/rooster-emissions-cog-spike.md).

    uv run tools/emit-tile-to-emissions-cog.py <tile_id> [--out /tmp/<tile_id>.tif]
"""

import argparse
import logging
import os

import numpy
import rasterio

from jdluc import export

logger = logging.getLogger(__name__)


def report(path_to_cog: str) -> None:
    with rasterio.open(path_to_cog) as dataset:
        logger.info(
            f"{dataset.count:d} bands, {dataset.width:d}x{dataset.height:d}, crs={dataset.crs}"
        )
        name_to_band = {}
        for idx in range(1, dataset.count + 1):
            name = dataset.descriptions[idx - 1] or f"band-{idx:d}"
            band = dataset.read(idx, masked=True)
            name_to_band[name] = band
            logger.info(
                f"  {name:26s} min={band.min():+.4g} mean={band.mean():+.4g} max={band.max():+.4g}"
            )
        summed = (
            name_to_band[export.BIOMASS_BAND]
            + name_to_band[export.SOIL_BAND]
            + name_to_band[export.PEAT_BAND]
        )
        residual = numpy.abs(summed - name_to_band[export.TOTAL_BAND]).max()
        logger.info(
            f"  identity check |(biomass+soil+peat) - total| max={residual:.3g}"
        )


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tile_id")
    parser.add_argument(
        "--out",
        type=os.path.expanduser,
        default=None,
        help="Local COG path to write (default: {scratch_root}/emissions-cog/<tile_id>.tif).",
    )
    args = parser.parse_args()

    uri = export.workflow(tile_id=args.tile_id, output_uri=args.out)
    logger.info(f"Wrote {uri:s}")
    if args.out is not None:
        report(path_to_cog=args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
