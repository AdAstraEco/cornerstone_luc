"""Serialize the ``emit`` phase's cached scratch output to an emissions COG.

This is the top of the ETL import layer stack: it *consumes* ``emit`` (reading its
cached per-tile zarr through the usual ``workflow()`` cache-hit idiom) and produces a
terminal Cloud-Optimised GeoTIFF deliverable that nothing downstream reads. It touches
no other stage's logic.

The COG carries, per pixel, the same *kinds* of claimed quantities as Rooster's
emissions layer (biomass / soil / peat), taken straight from ``emit``'s already-computed
pools -- a serialization, not a re-implementation. See
``docs/rooster-emissions-cog-spike.md``.

Band schema (all EPSG:4326, float32, nan-nodata):

1. ``biomass-emissions``        -- tCO2e/ha, linear-discounted vegetation pool
2. ``soil-emissions``           -- tCO2e/ha, linear-discounted soil pool (incl. peat
                                   conversion pulse, which ``emit`` folds into soil)
3. ``peat-occupation-emissions``-- tCO2e/ha, peatland-occupation term (undiscounted, as
                                   ``emit`` adds it)
4. ``emissions-total``          -- tCO2e/ha, ``emit``'s discounted grand total; by
                                   linearity equals bands 1 + 2 + 3
5. ``hectares-per-pixel``       -- ha, area scaling so absolute per-pixel emissions are
                                   derivable downstream (band_i * band_5)
"""

import argparse
import logging
import os
import tempfile

import numpy
import xarray

from jdluc import config, emit, geo, storage, utils

logger = logging.getLogger(__name__)

# emit output-band prefixes (the part before the first ":"; the middle carries units).
VEGETATION_PREFIX = "vegetation-emissions"
SOIL_PREFIX = "soil-emissions"
PEAT_OCCUPATION_PREFIX = "peatland-occupation"
TOTAL_PREFIX = "emissions-per-hectare"
HECTARES_PREFIX = "hectares-per-pixel"

# COG band order + names.
BIOMASS_BAND = "biomass-emissions"
SOIL_BAND = "soil-emissions"
PEAT_BAND = "peat-occupation-emissions"
TOTAL_BAND = "emissions-total"
AREA_BAND = "hectares-per-pixel"
BAND_NAMES = (BIOMASS_BAND, SOIL_BAND, PEAT_BAND, TOTAL_BAND, AREA_BAND)

NO_DATA = float("nan")


def _vars_with_prefix(dset: xarray.Dataset, prefix: str) -> dict[str, xarray.DataArray]:
    return {
        name: dset[name]
        for name in map(str, dset.data_vars)
        if name.split(":")[0] == prefix
    }


def _one_var(dset: xarray.Dataset, prefix: str) -> xarray.DataArray:
    (name,) = _vars_with_prefix(dset=dset, prefix=prefix)
    return dset[name]


def _parse_span(name: str) -> emit.SpanType:
    before, after = name.rsplit(":", maxsplit=1)[1].split("-")
    return (int(before), int(after))


def _discounted_pool(dset: xarray.Dataset, prefix: str) -> xarray.DataArray:
    """The per-span pool bands under ``prefix``, reduced by emit's linear discount."""
    span_to_value = {
        _parse_span(name=name): darray
        for name, darray in _vars_with_prefix(dset=dset, prefix=prefix).items()
    }
    return emit.get_linear_discounted_total(span_to_value=span_to_value)


def build_bands(dset: xarray.Dataset) -> xarray.DataArray:
    """Stack an ``emit`` dataset into the ordered multi-band emissions array."""
    import rioxarray  # noqa: F401

    name_to_band = {
        BIOMASS_BAND: _discounted_pool(dset=dset, prefix=VEGETATION_PREFIX),
        SOIL_BAND: _discounted_pool(dset=dset, prefix=SOIL_PREFIX),
        PEAT_BAND: _one_var(dset=dset, prefix=PEAT_OCCUPATION_PREFIX),
        TOTAL_BAND: _one_var(dset=dset, prefix=TOTAL_PREFIX),
        AREA_BAND: _one_var(dset=dset, prefix=HECTARES_PREFIX),
    }
    stacked = xarray.concat(
        [name_to_band[name] for name in BAND_NAMES], dim="band"
    ).astype(numpy.float32)
    stacked = stacked.assign_coords(band=("band", list(BAND_NAMES)))
    crs = dset.rio.crs or "EPSG:4326"
    return stacked.rio.write_crs(crs).rio.write_nodata(NO_DATA)


def _metadata() -> dict[str, str]:
    return {
        "watershed-processing-time": utils.get_utc_timestamp(),
        "watershed-processing-version": utils.get_git_version(),
        "watershed-product-name": "emissions-cog",
        "watershed-remote-url": utils.get_git_remote_url(),
        "watershed-source-name": "jdluc-emit",
    }


def _output_uri(tile_id: str) -> str:
    return storage.join_uri(
        root=config.Config.from_dot_env().scratch_root,
        prefix=f"emissions-cog/{tile_id:s}.tif",
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
    """Read ``emit``'s cached scratch output for ``tile_id`` and write an emissions COG.

    ``output_uri`` defaults to ``{scratch_root}/emissions-cog/{tile_id}.tif``; pass an
    explicit URI (e.g. a local path) to eyeball the artifact without touching scratch.
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
            band_names=list(BAND_NAMES), path_to_geotiff=path_to_geotiff
        )
        geo.validate_geotiff(path_to_geotiff=path_to_geotiff)
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
