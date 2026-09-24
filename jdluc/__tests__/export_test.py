import numpy
import pytest
import rasterio
import rasterio.transform
import xarray

from jdluc import export

CIE_BAND_NAMES = (
    "cie-climate-zone",
    "cie-conversion-source",
    "cie-vegetation-emissions-undiscounted:tco2e-per-ha",
)


def _darray(value: float) -> xarray.DataArray:
    return xarray.DataArray(
        coords={"y": range(2), "x": range(3)},
        data=numpy.full((2, 3), value, dtype=numpy.float32),
        dims=("y", "x"),
    )


def _emit_like_dataset() -> xarray.Dataset:
    """A stand-in for emit.workflow output, using emit's units-infixed band names."""
    return xarray.Dataset(
        {
            # decoys: emit's existing bands, which the COG must not carry
            "emissions-per-hectare:tco2e-per-ha": _darray(999),
            "vegetation-emissions:tco2e-per-ha:2000-2005": _darray(999),
            # NB: declared out of order, to check the COG orders by name
            "cie-vegetation-emissions-undiscounted:tco2e-per-ha": _darray(3),
            "cie-climate-zone": _darray(1),
            "cie-conversion-source": _darray(2),
        }
    )


def test_build_bands_selects_and_orders_cie_bands() -> None:
    stacked = export.build_bands(dset=_emit_like_dataset())
    assert list(stacked.band.values) == list(CIE_BAND_NAMES)
    assert stacked.dtype == numpy.float32


def test_build_bands_copies_values() -> None:
    stacked = export.build_bands(dset=_emit_like_dataset())
    for name, value in zip(CIE_BAND_NAMES, (1, 2, 3), strict=True):
        numpy.testing.assert_allclose(stacked.sel(band=name).values, value)


def test_build_bands_rejects_no_cie_bands() -> None:
    with pytest.raises(AssertionError):
        export.build_bands(dset=xarray.Dataset({"conversion": _darray(1)}))


def _write_tile(
    path: str,
    *,
    west: float,
    north: float,
    pixel: float,
    values: list[float],
    size: int = 4,
    band_names: tuple[str, ...] = CIE_BAND_NAMES,
    nodata: float = export.NO_DATA,
) -> None:
    """A tiny multi-band GeoTIFF: band i filled with values[i-1], on the given EPSG:4326 grid."""
    data = numpy.stack(
        [numpy.full((size, size), value, dtype=numpy.float32) for value in values]
    )
    transform = rasterio.transform.from_origin(west, north, pixel, pixel)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=size,
        width=size,
        count=len(values),
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
        nodata=nodata,
    ) as dataset:
        dataset.write(data)
        for index, name in enumerate(band_names, start=1):
            dataset.set_band_description(index, name)


def test_mosaic_builds_a_readable_union_vrt(tmp_path) -> None:
    values_a = [float(i) for i in range(1, 4)]
    values_b = [100.0 + i for i in range(1, 4)]
    _write_tile(str(tmp_path / "a.tif"), west=0, north=10, pixel=1, values=values_a)
    _write_tile(
        str(tmp_path / "b.tif"), west=4, north=10, pixel=1, values=values_b
    )  # east of a
    out = str(tmp_path / "mosaic.vrt")

    export.mosaic([str(tmp_path / "a.tif"), str(tmp_path / "b.tif")], out)

    with rasterio.open(out) as dataset:
        assert dataset.count == len(CIE_BAND_NAMES)
        assert dataset.descriptions == CIE_BAND_NAMES
        assert dataset.crs.to_epsg() == 4326
        assert (dataset.width, dataset.height) == (8, 4)  # union of the two 4x4 tiles
        assert dataset.transform.c == 0 and dataset.transform.f == 10
        band1 = dataset.read(1)
        numpy.testing.assert_array_equal(
            band1[:, :4], values_a[0]
        )  # tile a on the left
        numpy.testing.assert_array_equal(
            band1[:, 4:], values_b[0]
        )  # tile b on the right


def test_mosaic_skips_absent_tiles(tmp_path) -> None:
    _write_tile(str(tmp_path / "a.tif"), west=0, north=10, pixel=1, values=[1.0] * 3)
    out = str(tmp_path / "mosaic.vrt")

    export.mosaic([str(tmp_path / "a.tif"), str(tmp_path / "missing.tif")], out)

    with rasterio.open(out) as dataset:
        assert (dataset.width, dataset.height) == (4, 4)  # only the present tile


def test_mosaic_rejects_grid_mismatch(tmp_path) -> None:
    _write_tile(str(tmp_path / "a.tif"), west=0, north=10, pixel=1, values=[1.0] * 3)
    _write_tile(
        str(tmp_path / "b.tif"), west=4, north=10, pixel=2, values=[1.0] * 3
    )  # coarser
    with pytest.raises(ValueError, match="does not share the grid"):
        export.mosaic(
            [str(tmp_path / "a.tif"), str(tmp_path / "b.tif")], str(tmp_path / "m.vrt")
        )


def test_mosaic_raises_when_nothing_exists(tmp_path) -> None:
    with pytest.raises(FileNotFoundError):
        export.mosaic([str(tmp_path / "nope.tif")], str(tmp_path / "m.vrt"))
