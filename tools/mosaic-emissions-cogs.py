"""Slice-3 driver: stitch per-tile emission COGs into one read-time VRT to eyeball.

Wraps ``jdluc.export.mosaic`` over local per-tile COGs (e.g. those written by
``emit-tile-to-emissions-cog.py``), producing a tiny union VRT that GDAL/QGIS opens as one
AOI-wide raster -- handy for a side-by-side against Rooster's Ivory Coast fixtures. No pixels are
materialised (see docs/rooster-emissions-cog-spike.md).

    uv run tools/mosaic-emissions-cogs.py out.vrt tileA.tif tileB.tif ...
"""

import argparse
import logging
import os

import rasterio

from jdluc import export

logger = logging.getLogger(__name__)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output_vrt", type=os.path.expanduser)
    parser.add_argument(
        "cogs",
        nargs=argparse.ONE_OR_MORE,
        type=os.path.expanduser,
        help="per-tile emission COGs",
    )
    args = parser.parse_args()

    uri = export.mosaic(
        source_uris=[str(cog) for cog in args.cogs], output_uri=str(args.output_vrt)
    )
    with rasterio.open(uri) as dataset:
        logger.info(
            f"{uri}: {dataset.count:d} bands, {dataset.width:d}x{dataset.height:d}, "
            f"bounds={tuple(round(b, 3) for b in dataset.bounds)}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
