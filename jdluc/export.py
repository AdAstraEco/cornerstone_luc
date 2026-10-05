"""Write a pipeline step's output as a deliverable: chosen bands, as a COG or a zarr.

A utility, like `storage`: it knows nothing about the steps. A step defines its deliverables,
each a named choice of the variables it computes, and calls `write_deliverable` with its own
lazy dataset (see `emit.export_workflow` and `derive.export_workflow`). Only the bands a
deliverable names are computed.

A variable may carry dimensions beyond the pixel grid (`derive` adds `crop_class` and
`discounting`). A band selects values on each of them, and the format decides the layout:

- COG: one 2-D band per combination of selected values, named `<variable>[:<value>...]`;
- zarr: the variable as is, its extra dimensions kept, cut to the selected values.

Each deliverable is written per tile to `{EXPORT_ROOT}/{deliverable}/{tile}.{tif|zarr}`.
Per-tile COGs of one deliverable can be stitched into an AOI-wide view with `mosaic_workflow`.
"""

import argparse
import collections.abc
import dataclasses
import enum
import itertools
import logging
import math
import os
import tempfile
from xml.etree import ElementTree

import numpy
import rasterio
import xarray

from jdluc import config, geo, storage, utils

logger = logging.getLogger(__name__)

NO_DATA = storage.COG_NO_DATA
PIXEL_DIMS = ("y", "x")


class Format(enum.StrEnum):
    COG = "cog"
    ZARR = "zarr"


FORMAT_TO_EXTENSION = {Format.COG: ".tif", Format.ZARR: ".zarr"}


@dataclasses.dataclass(frozen=True)
class Band:
    """A variable of the step's dataset, and the values to write on each of its extra dims."""

    variable: str
    select: collections.abc.Mapping[str, tuple[str, ...]] = dataclasses.field(
        default_factory=dict
    )


@dataclasses.dataclass(frozen=True)
class Deliverable:
    name: str
    bands: tuple[Band, ...]
    format: Format = Format.COG


def get_selections(deliverable: Deliverable) -> dict[str, list[str]]:
    """Every value the deliverable selects, by dimension: what the step needs to build."""
    dim_to_values: dict[str, dict[str, None]] = {}
    for band in deliverable.bands:
        for dim, values in band.select.items():
            dim_to_values.setdefault(dim, {}).update(dict.fromkeys(values))
    return {dim: list(values) for dim, values in dim_to_values.items()}


def iter_band_selections(
    band: Band,
) -> collections.abc.Iterator[tuple[str, dict[str, str]]]:
    """Each 2-D band a `Band` expands to: its name, and its value on each selected dimension."""
    for values in itertools.product(*band.select.values()):
        yield (
            ":".join((band.variable, *values)),
            dict(zip(band.select, values, strict=True)),
        )


def get_band_names(deliverable: Deliverable) -> list[str]:
    """The deliverable's 2-D bands, in order: its COG band names."""
    return [
        name for band in deliverable.bands for name, _ in iter_band_selections(band)
    ]


def check_deliverable(deliverable: Deliverable, dset: xarray.Dataset) -> None:
    """Each band is a variable of `dset` and selects on exactly the dims it carries beyond
    the pixel grid; no two bands share a name. `dset` may be a one-pixel stand-in."""
    for band in deliverable.bands:
        assert band.variable in dset, (
            f"{deliverable.name!r}: no variable {band.variable!r}"
        )
        carried = set(map(str, dset[band.variable].dims)) - set(PIXEL_DIMS)
        assert set(band.select) == carried, (
            f"{deliverable.name!r}: {band.variable!r} varies by {sorted(carried)}, "
            f"but the band selects on {sorted(band.select)}"
        )
    names = get_band_names(deliverable)
    assert len(set(names)) == len(names), f"{deliverable.name!r} repeats a band"


def to_cog_dataset(dset: xarray.Dataset, deliverable: Deliverable) -> xarray.Dataset:
    """The deliverable's bands as 2-D (y, x) variables, float32, NaN no-data, in order.

    Chunked so one chunk of every band fits in memory together, as `emit` writes its output.
    """
    name_to_darray = {
        name: dset[band.variable].sel(point, drop=True)
        for band in deliverable.bands
        for name, point in iter_band_selections(band)
    }
    chunk_size = geo.get_chunk_size(
        dtypes=[numpy.dtype("float32")] * len(name_to_darray)
    )
    return xarray.Dataset(
        {
            name: geo.unify_dtype_and_no_data(darray=darray.rename(None)).chunk(
                chunks=chunk_size
            )
            for name, darray in name_to_darray.items()
        }
    )


def to_zarr_dataset(dset: xarray.Dataset, deliverable: Deliverable) -> xarray.Dataset:
    """The deliverable's variables with their extra dims kept, cut to the selected values.

    float32, NaN no-data. Where two variables select different values on one dimension, each
    is NaN at the values it did not select.
    """
    variables = [band.variable for band in deliverable.bands]
    assert len(set(variables)) == len(variables), (
        f"{deliverable.name!r} names a variable twice, which a zarr cannot hold"
    )
    merged = xarray.merge(
        [
            geo.unify_dtype_and_no_data(
                darray=dset[band.variable].sel(
                    {dim: list(values) for dim, values in band.select.items()}
                )
            )
            for band in deliverable.bands
        ],
        join="outer",
    )
    # NB: zarr v3 specifies variable-length strings, not numpy's fixed-length ones
    return merged.assign_coords(
        {
            dim: merged[dim].astype(object)
            for dim in merged.dims
            if merged[dim].dtype.kind == "U"
        }
    )


def get_metadata(product_name: str, source_name: str) -> dict[str, str]:
    """Provenance, as `datasets.base` stamps on ingested COGs."""
    return {
        "watershed-processing-time": utils.get_utc_timestamp(),
        "watershed-processing-version": utils.get_git_version(),
        "watershed-product-name": product_name,
        "watershed-remote-url": utils.get_git_remote_url(),
        "watershed-source-name": source_name,
    }


def get_output_uri(
    deliverable_name: str,
    format: Format,
    tile_id: str,
    output_root: str | None = None,
) -> str:
    """`{output_root}/{deliverable}/{tile}.{tif|zarr}`; `output_root` defaults to EXPORT_ROOT."""
    if output_root is None:
        output_root = config.Config.from_dot_env().export_root
    return storage.join_uri(
        root=output_root,
        prefix=f"{deliverable_name:s}/{tile_id:s}{FORMAT_TO_EXTENSION[format]:s}",
    )


def write_deliverable(
    dset: xarray.Dataset,
    deliverable: Deliverable,
    source_name: str,
    tile_id: str,
    format: Format | None = None,
    output_root: str | None = None,
) -> str:
    """Write `deliverable`'s bands of the (lazy) `dset` for one tile; return the URI.

    `format` overrides the deliverable's own. Computes only what the bands need, as it writes.
    """
    check_deliverable(deliverable, dset)
    format = Format(format or deliverable.format)
    uri = get_output_uri(
        deliverable_name=deliverable.name,
        format=format,
        output_root=output_root,
        tile_id=tile_id,
    )
    metadata = get_metadata(product_name=deliverable.name, source_name=source_name)
    logger.info(f"Writing {deliverable.name=:s} for {tile_id=:s} as {format!s}")
    match format:
        case Format.COG:
            storage.write_dask_dataset_to_cog(
                dset=to_cog_dataset(dset, deliverable), metadata=metadata, uri=uri
            )
        case Format.ZARR:
            storage.write_dask_dataset_to_zarr(
                dset=to_zarr_dataset(dset, deliverable).assign_attrs(metadata),
                mode="w",
                path_to_zarr=uri,
            )
    logger.info(f"Wrote {deliverable.name=:s} for {tile_id=:s} to {uri=:s}")
    return uri


def main(
    description: str | None,
    export_workflow: collections.abc.Callable[..., list[str]],
    name_to_deliverable: collections.abc.Mapping[str, Deliverable],
) -> int:
    """A step's export CLI: `<tile_id> --deliverable NAME [...] [--format] [--output-root]`."""
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("tile_id")
    parser.add_argument(
        "--deliverable",
        action="append",
        choices=sorted(name_to_deliverable),
        dest="deliverable_names",
        required=True,
        help="repeatable",
    )
    parser.add_argument(
        "--format",
        choices=list(map(str, Format)),
        default=None,
        help="overrides each deliverable's own",
    )
    parser.add_argument(
        "--output-root", help="defaults to the export root; local paths ok"
    )
    args = parser.parse_args()
    for uri in export_workflow(
        deliverable_names=args.deliverable_names,
        format=args.format,
        output_root=args.output_root,
        tile_id=args.tile_id,
    ):
        print(uri)
    return 0


# --- AOI mosaic (read-time VRT over the per-tile COGs) -----------------------------------------
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
    deliverable_name: str,
    tile_ids: collections.abc.Sequence[str],
    name: str,
    output_uri: str | None = None,
) -> str:
    """VRT-mosaic one deliverable's per-tile COGs for ``tile_ids`` into one AOI view named
    ``{deliverable_name}-{name}``. ``output_uri`` defaults to a ``.vrt`` under the export root.
    Returns the URI."""
    return mosaic(
        source_uris=[
            get_output_uri(
                deliverable_name=deliverable_name, format=Format.COG, tile_id=tile_id
            )
            for tile_id in tile_ids
        ],
        output_uri=output_uri
        if output_uri is not None
        else _mosaic_output_uri(name=f"{deliverable_name:s}-{name:s}"),
    )
