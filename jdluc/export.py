"""Serialize the ``emit`` phase's cached scratch output to a Crop-Independent Emissions COG.

This is the top of the ETL import layer stack: it *consumes* ``emit`` (reading its
cached per-tile zarr through the usual ``workflow()`` cache-hit idiom) and produces a
terminal Cloud-Optimised GeoTIFF deliverable that nothing downstream reads. It touches
no other stage's logic.

The COG carries only ``emit``'s ``cie-`` bands (see ``docs/crop_independent_emissions.md``),
one COG band per variable, named as ``emit`` names it (units after the first ``:``) and
ordered by name so every tile, and so the mosaic, agrees. All EPSG:4326, float32,
nan-nodata -- the encoding ``emit`` already writes them in.
"""

import argparse
import collections.abc
import dataclasses
import logging
import math
import os
import tempfile
import xml.etree.ElementTree as ElementTree

import numpy
import rasterio
import xarray

from jdluc import config, emit, geo, storage, utils

logger = logging.getLogger(__name__)

CIE_PREFIX = "cie-"

NO_DATA = float("nan")


def get_band_names(dset: xarray.Dataset) -> tuple[str, ...]:
    """The ``emit`` variables the COG carries: the ``cie-`` bands, by name."""
    return tuple(
        sorted(name for name in map(str, dset.data_vars) if name.startswith(CIE_PREFIX))
    )


def build_bands(dset: xarray.Dataset) -> xarray.DataArray:
    """Stack an ``emit`` dataset's ``cie-`` bands into the ordered multi-band array."""
    import rioxarray  # noqa: F401

    band_names = get_band_names(dset=dset)
    assert band_names, f"no {CIE_PREFIX!r} bands in the emit output"
    stacked = xarray.concat([dset[name] for name in band_names], dim="band").astype(
        numpy.float32
    )
    stacked = stacked.assign_coords(band=("band", list(band_names)))
    crs = dset.rio.crs or "EPSG:4326"
    return stacked.rio.write_crs(crs).rio.write_nodata(NO_DATA)


def _metadata() -> dict[str, str]:
    return {
        "watershed-processing-time": utils.get_utc_timestamp(),
        "watershed-processing-version": utils.get_git_version(),
        "watershed-product-name": "cie-emissions-cog",
        "watershed-remote-url": utils.get_git_remote_url(),
        "watershed-source-name": "jdluc-emit",
    }


def _output_uri(tile_id: str) -> str:
    return storage.join_uri(
        root=config.Config.from_dot_env().export_root,
        prefix=f"{tile_id:s}.tif",
    )


def _write_geotiff(stacked: xarray.DataArray, path_to_geotiff: str) -> None:
    import dask.diagnostics

    with dask.diagnostics.ProgressBar(dt=5, minimum=1):
        logger.info(f"Writing staged GeoTIFF to {path_to_geotiff=:s}")
        stacked.rio.to_raster(
            path_to_geotiff,
            blockxsize=512,
            blockysize=512,
            compress="ZSTD",
            driver="GTiff",
            dtype="float32",
            lock=True,
            num_threads="all_cpus",
            tiled=True,
            BIGTIFF="IF_SAFER",
        )


def workflow(tile_id: str, output_uri: str | None = None) -> str:
    """Read ``emit``'s cached scratch output for ``tile_id`` and write a CIE COG.

    ``output_uri`` defaults to ``{export_root}/{tile_id}.tif``; pass an explicit URI
    (e.g. a local path) to eyeball the artifact without touching the export root.
    Returns the written URI.
    """
    logger.info(f"Reading emit scratch output for {tile_id=:s} (cache hit expected)")
    dset = emit.workflow(tile_id=tile_id)

    stacked = build_bands(dset=dset)
    uri = output_uri if output_uri is not None else _output_uri(tile_id=tile_id)

    with tempfile.TemporaryDirectory() as tmpdir:
        path_to_geotiff = os.path.join(tmpdir, "geotiff.tif")
        _write_geotiff(stacked=stacked, path_to_geotiff=path_to_geotiff)
        geo.set_band_names_for_geotiff(
            band_names=list(map(str, stacked.band.values)),
            path_to_geotiff=path_to_geotiff,
        )
        geo.validate_geotiff(dtype="float32", path_to_geotiff=path_to_geotiff)
        path_to_cog = os.path.join(tmpdir, "cog.tif")
        geo.convert_geotiff_to_cog(
            metadata=_metadata(),
            no_data=NO_DATA,
            path_to_cog=path_to_cog,
            path_to_geotiff=path_to_geotiff,
        )
        storage.put_file(local_path=path_to_cog, uri=uri)

    logger.info(f"Wrote emissions COG for {tile_id=:s} to {uri=:s}")
    return uri


# --- Slice 3: AOI mosaic (read-time VRT over the per-tile COGs) ----------------------------------
# The per-tile COGs share the GLAD 10-degree grid, so an AOI-wide view is a *virtual* mosaic: a tiny
# VRT that references the tiles by GDAL path and is read lazily. We build the VRT XML by hand (pure
# rasterio + stdlib) because the image ships GDAL only as the rasterio/rioxarray wheels -- no
# `gdalbuildvrt` CLI, no `osgeo` -- and materialising a full mosaic would blow the pod's memory.

_NUMPY_TO_GDAL_DTYPE = {"float32": "Float32"}


@dataclasses.dataclass(frozen=True)
class _MosaicSource:
    """One tile placed on the mosaic grid: its GDAL path and pixel offset within the mosaic."""

    gdal_path: str
    x_off: int
    y_off: int
    width: int
    height: int


def _format_geo_value(value: float) -> str:
    return repr(value)


def _nodata_token(no_data: float | None) -> str:
    """A comparable stand-in for a tile's nodata, so ``nan == nan`` when checking grid agreement."""
    if no_data is None:
        return "none"
    return "nan" if math.isnan(no_data) else repr(no_data)


def build_vrt_xml(
    *,
    width: int,
    height: int,
    origin_x: float,
    origin_y: float,
    pixel_x: float,
    pixel_y: float,
    crs_wkt: str,
    band_names: collections.abc.Sequence[str],
    gdal_dtype: str,
    no_data: float,
    sources: collections.abc.Sequence[_MosaicSource],
) -> str:
    """Assemble a multi-band GDAL VRT that tiles ``sources`` onto one mosaic grid. Pure/testable."""
    root = ElementTree.Element(
        "VRTDataset", rasterXSize=str(width), rasterYSize=str(height)
    )
    ElementTree.SubElement(root, "SRS").text = crs_wkt
    ElementTree.SubElement(root, "GeoTransform").text = ", ".join(
        _format_geo_value(v) for v in (origin_x, pixel_x, 0.0, origin_y, 0.0, -pixel_y)
    )
    for band_index, band_name in enumerate(band_names, start=1):
        band = ElementTree.SubElement(
            root, "VRTRasterBand", dataType=gdal_dtype, band=str(band_index)
        )
        if band_name:
            ElementTree.SubElement(band, "Description").text = band_name
        ElementTree.SubElement(band, "NoDataValue").text = _format_geo_value(no_data)
        for source in sources:
            simple = ElementTree.SubElement(band, "SimpleSource")
            ElementTree.SubElement(
                simple, "SourceFilename", relativeToVRT="0"
            ).text = source.gdal_path
            ElementTree.SubElement(simple, "SourceBand").text = str(band_index)
            ElementTree.SubElement(
                simple,
                "SrcRect",
                xOff="0",
                yOff="0",
                xSize=str(source.width),
                ySize=str(source.height),
            )
            ElementTree.SubElement(
                simple,
                "DstRect",
                xOff=str(source.x_off),
                yOff=str(source.y_off),
                xSize=str(source.width),
                ySize=str(source.height),
            )
            ElementTree.SubElement(simple, "NODATA").text = _format_geo_value(no_data)
    return ElementTree.tostring(root, encoding="unicode")


def mosaic(source_uris: collections.abc.Sequence[str], output_uri: str) -> str:
    """Write a read-time VRT over the per-tile emission COGs in ``source_uris`` to ``output_uri``.

    Absent tiles are skipped and logged (ocean/edge tiles legitimately have no COG), never silently.
    The tiles must share one grid (pixel size, CRS, band schema); the mosaic spans their union.
    """
    present = [uri for uri in source_uris if storage.path_exists(uri=uri)]
    if dropped := [uri for uri in source_uris if uri not in present]:
        logger.warning(
            f"mosaic: skipping {len(dropped):d} absent tile COG(s): {dropped}"
        )
    if not present:
        raise FileNotFoundError(f"mosaic: none of {list(source_uris)} exist")

    grids = []
    baseline: tuple | None = (
        None  # the comparable grid/schema signature of the first tile
    )
    meta: tuple | None = None  # (pixel_x, pixel_y, crs_wkt, dtype, no_data, band_names)
    for uri in present:
        with rasterio.open(storage.to_gdal_path(uri)) as dataset:
            transform = dataset.transform
            pixel_x, pixel_y = abs(transform.a), abs(transform.e)
            crs_wkt, dtype, no_data = (
                dataset.crs.to_wkt(),
                dataset.dtypes[0],
                dataset.nodata,
            )
            names = tuple(name or "" for name in dataset.descriptions)
            signature = (
                pixel_x,
                pixel_y,
                crs_wkt,
                dtype,
                _nodata_token(no_data),
                names,
            )
            if baseline is None:
                baseline = signature
                meta = (pixel_x, pixel_y, crs_wkt, dtype, no_data, names)
            elif signature != baseline:
                raise ValueError(
                    f"mosaic: {uri} does not share the grid/schema of {present[0]}"
                )
            grids.append((uri, transform, dataset.width, dataset.height))

    assert meta is not None
    pixel_x, pixel_y, crs_wkt, dtype, no_data, band_names = meta
    west = min(transform.c for _, transform, _, _ in grids)
    north = max(transform.f for _, transform, _, _ in grids)
    east = max(transform.c + w * pixel_x for _, transform, w, _ in grids)
    south = min(transform.f - h * pixel_y for _, transform, _, h in grids)
    mosaic_width = round((east - west) / pixel_x)
    mosaic_height = round((north - south) / pixel_y)

    sources = [
        _MosaicSource(
            gdal_path=storage.to_gdal_path(uri),
            x_off=round((transform.c - west) / pixel_x),
            y_off=round((north - transform.f) / pixel_y),
            width=width,
            height=height,
        )
        for uri, transform, width, height in grids
    ]
    xml = build_vrt_xml(
        width=mosaic_width,
        height=mosaic_height,
        origin_x=west,
        origin_y=north,
        pixel_x=pixel_x,
        pixel_y=pixel_y,
        crs_wkt=crs_wkt,
        band_names=band_names,
        gdal_dtype=_NUMPY_TO_GDAL_DTYPE[str(dtype)],
        no_data=float(no_data) if no_data is not None else math.nan,
        sources=sources,
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        path_to_vrt = os.path.join(tmpdir, "mosaic.vrt")
        with open(path_to_vrt, "w") as handle:
            handle.write(xml)
        storage.put_file(local_path=path_to_vrt, uri=output_uri)
    logger.info(f"mosaicked {len(present):d} tile(s) into {output_uri=:s}")
    return output_uri


def _mosaic_output_uri(name: str) -> str:
    return storage.join_uri(
        root=config.Config.from_dot_env().export_root,
        prefix=f"mosaics/{name:s}.vrt",
    )


def mosaic_workflow(
    tile_ids: collections.abc.Sequence[str],
    name: str,
    output_uri: str | None = None,
) -> str:
    """VRT-mosaic the per-tile emission COGs for ``tile_ids`` (by their scratch paths) into one AOI
    view named ``name``. ``output_uri`` defaults to a ``.vrt`` under scratch. Returns the URI."""
    return mosaic(
        source_uris=[_output_uri(tile_id=tile_id) for tile_id in tile_ids],
        output_uri=output_uri
        if output_uri is not None
        else _mosaic_output_uri(name=name),
    )


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tile_id")
    parser.add_argument(
        "--output-uri",
        default=None,
        help="Write the COG here instead of the default scratch path (local paths ok).",
    )
    args = parser.parse_args()
    print(workflow(tile_id=args.tile_id, output_uri=args.output_uri))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
