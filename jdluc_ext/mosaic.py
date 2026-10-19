"""A read-time mosaic of one deliverable's per-tile COGs: a VRT over them.

The tiles share the GLAD 10-degree grid, so an AOI-wide view needs no copy of the pixels: a
small VRT references each tile by its GDAL path, and is read lazily. Its XML is built here,
so the GDAL inside the rasterio wheels suffices, with no `gdalbuildvrt` and no `osgeo`.
"""

import collections.abc
import dataclasses
import logging
import math
import os
import tempfile
from xml.etree import ElementTree

import rasterio

from jdluc import export, storage

logger = logging.getLogger(__name__)

NUMPY_TO_GDAL_DTYPE = {"float32": "Float32"}

# Far above float64 rounding (~1e-16), far below any real difference in resolution: across a
# 40,000-pixel tile, pixel sizes this close drift apart by under a thousandth of a pixel
PIXEL_SIZE_REL_TOL = 1e-9


@dataclasses.dataclass(frozen=True)
class Tile:
    """One COG's place on the grid and its band schema, from its header."""

    band_names: tuple[str, ...]
    crs_wkt: str
    dtype: str
    gdal_path: str
    height: int
    no_data: float
    north: float
    pixel_x: float
    pixel_y: float
    west: float
    width: int


def read_tile(uri: str) -> Tile:
    gdal_path = storage.to_gdal_path(uri)
    with rasterio.open(gdal_path) as dataset:
        return Tile(
            band_names=tuple(name or "" for name in dataset.descriptions),
            crs_wkt=dataset.crs.to_wkt(),
            dtype=dataset.dtypes[0],
            gdal_path=gdal_path,
            height=dataset.height,
            no_data=math.nan if dataset.nodata is None else float(dataset.nodata),
            north=dataset.transform.f,
            pixel_x=abs(dataset.transform.a),
            pixel_y=abs(dataset.transform.e),
            west=dataset.transform.c,
            width=dataset.width,
        )


def shares_grid(tile: Tile, other: Tile) -> bool:
    """Same band schema and CRS, and pixel sizes equal to within float rounding."""
    # NB: not exactly equal, as a tile's transform is derived from its coordinates: the same
    # 0.00025 degree pixel comes out as 0.00025 for one tile, 0.00025000000000000006 for the next
    return (
        (tile.band_names, tile.crs_wkt, tile.dtype)
        == (other.band_names, other.crs_wkt, other.dtype)
        and (
            tile.no_data == other.no_data
            or (math.isnan(tile.no_data) and math.isnan(other.no_data))
        )
        and math.isclose(tile.pixel_x, other.pixel_x, rel_tol=PIXEL_SIZE_REL_TOL)
        and math.isclose(tile.pixel_y, other.pixel_y, rel_tol=PIXEL_SIZE_REL_TOL)
    )


def build_vrt_xml(tiles: collections.abc.Sequence[Tile]) -> str:
    """A multi-band VRT placing each of `tiles` on the grid of the first, over their union."""
    first = tiles[0]
    west = min(tile.west for tile in tiles)
    north = max(tile.north for tile in tiles)
    east = max(tile.west + tile.width * first.pixel_x for tile in tiles)
    south = min(tile.north - tile.height * first.pixel_y for tile in tiles)
    no_data = repr(first.no_data)
    root = ElementTree.Element(
        "VRTDataset",
        rasterXSize=str(round((east - west) / first.pixel_x)),
        rasterYSize=str(round((north - south) / first.pixel_y)),
    )
    ElementTree.SubElement(root, "SRS").text = first.crs_wkt
    ElementTree.SubElement(root, "GeoTransform").text = ", ".join(
        map(repr, (west, first.pixel_x, 0.0, north, 0.0, -first.pixel_y))
    )
    for index, band_name in enumerate(first.band_names, start=1):
        band = ElementTree.SubElement(
            root,
            "VRTRasterBand",
            band=str(index),
            dataType=NUMPY_TO_GDAL_DTYPE[first.dtype],
        )
        if band_name:
            ElementTree.SubElement(band, "Description").text = band_name
        ElementTree.SubElement(band, "NoDataValue").text = no_data
        for tile in tiles:
            source = ElementTree.SubElement(band, "SimpleSource")
            ElementTree.SubElement(
                source, "SourceFilename", relativeToVRT="0"
            ).text = tile.gdal_path
            ElementTree.SubElement(source, "SourceBand").text = str(index)
            ElementTree.SubElement(
                source,
                "SrcRect",
                xOff="0",
                xSize=str(tile.width),
                yOff="0",
                ySize=str(tile.height),
            )
            ElementTree.SubElement(
                source,
                "DstRect",
                xOff=str(round((tile.west - west) / first.pixel_x)),
                xSize=str(tile.width),
                yOff=str(round((north - tile.north) / first.pixel_y)),
                ySize=str(tile.height),
            )
            ElementTree.SubElement(source, "NODATA").text = no_data
    return ElementTree.tostring(root, encoding="unicode")


def mosaic(source_uris: collections.abc.Sequence[str], output_uri: str) -> str:
    """Write a VRT over the COGs at `source_uris` to `output_uri`; return it.

    An absent tile is skipped and logged: ocean and edge tiles have no COG. The tiles must
    share one grid and band schema; the mosaic spans their union.
    """
    present = [uri for uri in source_uris if storage.path_exists(uri=uri)]
    if absent := [uri for uri in source_uris if uri not in present]:
        logger.warning(f"Skipping {len(absent):d} absent tiles: {absent}")
    if not present:
        raise FileNotFoundError(f"None of {list(source_uris)} exist")
    tiles = [read_tile(uri=uri) for uri in present]
    for uri, tile in zip(present, tiles, strict=True):
        if not shares_grid(tile=tile, other=tiles[0]):
            raise ValueError(f"{uri:s} does not share the grid of {present[0]:s}")
    with tempfile.TemporaryDirectory() as tmpdir:
        path_to_vrt = os.path.join(tmpdir, "mosaic.vrt")
        with open(path_to_vrt, "w") as f:
            f.write(build_vrt_xml(tiles=tiles))
        storage.put_file(local_path=path_to_vrt, uri=output_uri)
    logger.info(f"Mosaicked {len(tiles):d} tiles to {output_uri=:s}")
    return output_uri


def get_mosaic_output_uri(
    name: str, output_root: str | None = None, version: str = ""
) -> str:
    """`{output_root}[/{version}]/mosaics/{name}.vrt`; see `export.get_export_root`."""
    return storage.join_uri(
        root=export.get_export_root(output_root=output_root, version=version),
        prefix=f"mosaics/{name:s}.vrt",
    )


def mosaic_workflow(
    aoi_name: str,
    deliverable_name: str,
    tile_ids: collections.abc.Sequence[str],
    output_root: str | None = None,
    version: str = "",
) -> str:
    """A VRT over one deliverable's COGs for `tile_ids`, named `{deliverable}-{aoi}`, beside
    them under the same export root and version; return its URI."""
    return mosaic(
        output_uri=get_mosaic_output_uri(
            name=f"{deliverable_name:s}-{aoi_name:s}",
            output_root=output_root,
            version=version,
        ),
        source_uris=[
            export.get_tile_output_uri(
                format=export.Format.COG,
                name=deliverable_name,
                output_root=output_root,
                tile_id=tile_id,
                version=version,
            )
            for tile_id in tile_ids
        ],
    )
