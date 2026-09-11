import numpy
import pytest
import rasterio
import rasterio.transform
import xarray

from jdluc import export
from jdluc.emit import SPAN_TO_LINEAR_DISCOUNT_WEIGHT

SPANS = sorted(SPAN_TO_LINEAR_DISCOUNT_WEIGHT)


def _darray(value: float) -> xarray.DataArray:
    return xarray.DataArray(
        coords={"y": range(2), "x": range(3)},
        data=numpy.full((2, 3), value, dtype=numpy.float32),
        dims=("y", "x"),
    )


def _emit_like_dataset(
    veg: dict[tuple[int, int], float],
    soil: dict[tuple[int, int], float],
    peat: float,
) -> xarray.Dataset:
    """A stand-in for emit.workflow output, using emit's units-infixed band names."""
    discounted = lambda pool: sum(
        pool[span] * SPAN_TO_LINEAR_DISCOUNT_WEIGHT[span] for span in SPANS
    )
    total = discounted(veg) + discounted(soil) + peat
    data_vars = {
        # decoys that share a prefix stem but must not be selected
        "land-class:class:2000": _darray(1),
        "emissions:tco2e-per-ha:2000-2005": _darray(999),
    }
    for (before, after), v in veg.items():
        data_vars[f"vegetation-emissions:tco2e-per-ha:{before}-{after}"] = _darray(v)
    for (before, after), v in soil.items():
        data_vars[f"soil-emissions:tco2e-per-ha:{before}-{after}"] = _darray(v)
    data_vars["peatland-occupation:tco2e-per-ha"] = _darray(peat)
    data_vars["emissions-per-hectare:tco2e-per-ha"] = _darray(total)
    data_vars["hectares-per-pixel:ha"] = _darray(1234.5)
    return xarray.Dataset(data_vars)


def test_parse_span() -> None:
    assert export._parse_span("vegetation-emissions:tco2e-per-ha:2010-2015") == (
        2010,
        2015,
    )


def test_build_bands_selects_and_orders_bands() -> None:
    dset = _emit_like_dataset(
        veg=dict.fromkeys(SPANS, 2.0), soil=dict.fromkeys(SPANS, 1.0), peat=5.0
    )
    stacked = export.build_bands(dset=dset)
    assert list(stacked.band.values) == list(export.BAND_NAMES)
    assert stacked.dtype == numpy.float32


def test_build_bands_discounts_each_pool() -> None:
    veg = {(2000, 2005): 1.0, (2005, 2010): 2.0, (2010, 2015): 3.0, (2015, 2020): 4.0}
    soil = dict.fromkeys(SPANS, 10.0)
    dset = _emit_like_dataset(veg=veg, soil=soil, peat=5.0)
    stacked = export.build_bands(dset=dset)

    expected_biomass = sum(veg[s] * SPAN_TO_LINEAR_DISCOUNT_WEIGHT[s] for s in SPANS)
    expected_soil = sum(soil[s] * SPAN_TO_LINEAR_DISCOUNT_WEIGHT[s] for s in SPANS)
    numpy.testing.assert_allclose(
        stacked.sel(band=export.BIOMASS_BAND).values, expected_biomass, rtol=1e-6
    )
    numpy.testing.assert_allclose(
        stacked.sel(band=export.SOIL_BAND).values, expected_soil, rtol=1e-6
    )
    numpy.testing.assert_allclose(stacked.sel(band=export.PEAT_BAND).values, 5.0)
    numpy.testing.assert_allclose(stacked.sel(band=export.AREA_BAND).values, 1234.5)


def test_build_bands_total_equals_pool_sum() -> None:
    # The identity the spike leans on: emit's discounted grand total is, by linearity,
    # the sum of the discounted biomass + soil pools plus the peat-occupation term.
    dset = _emit_like_dataset(
        veg={
            (2000, 2005): 1.0,
            (2005, 2010): 2.0,
            (2010, 2015): 3.0,
            (2015, 2020): 4.0,
        },
        soil={
            (2000, 2005): 4.0,
            (2005, 2010): 3.0,
            (2010, 2015): 2.0,
            (2015, 2020): 1.0,
        },
        peat=7.0,
    )
    stacked = export.build_bands(dset=dset)
    pool_sum = (
        stacked.sel(band=export.BIOMASS_BAND)
        + stacked.sel(band=export.SOIL_BAND)
        + stacked.sel(band=export.PEAT_BAND)
    )
    numpy.testing.assert_allclose(
        pool_sum.values, stacked.sel(band=export.TOTAL_BAND).values, rtol=1e-6
    )


# --- mosaic (slice 3) ---------------------------------------------------------------------------


def _write_tile(
    path: str,
    *,
    west: float,
    north: float,
    pixel: float,
    values: list[float],
    size: int = 4,
    band_names: tuple[str, ...] = export.BAND_NAMES,
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
    values_a = [float(i) for i in range(1, 6)]
    values_b = [100.0 + i for i in range(1, 6)]
    _write_tile(str(tmp_path / "a.tif"), west=0, north=10, pixel=1, values=values_a)
    _write_tile(
        str(tmp_path / "b.tif"), west=4, north=10, pixel=1, values=values_b
    )  # east of a
    out = str(tmp_path / "mosaic.vrt")

    export.mosaic([str(tmp_path / "a.tif"), str(tmp_path / "b.tif")], out)

    with rasterio.open(out) as dataset:
        assert dataset.count == len(export.BAND_NAMES)
        assert dataset.descriptions == export.BAND_NAMES
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
    _write_tile(str(tmp_path / "a.tif"), west=0, north=10, pixel=1, values=[1.0] * 5)
    out = str(tmp_path / "mosaic.vrt")

    export.mosaic([str(tmp_path / "a.tif"), str(tmp_path / "missing.tif")], out)

    with rasterio.open(out) as dataset:
        assert (dataset.width, dataset.height) == (4, 4)  # only the present tile


def test_mosaic_rejects_grid_mismatch(tmp_path) -> None:
    _write_tile(str(tmp_path / "a.tif"), west=0, north=10, pixel=1, values=[1.0] * 5)
    _write_tile(
        str(tmp_path / "b.tif"), west=4, north=10, pixel=2, values=[1.0] * 5
    )  # coarser
    with pytest.raises(ValueError, match="does not share the grid"):
        export.mosaic(
            [str(tmp_path / "a.tif"), str(tmp_path / "b.tif")], str(tmp_path / "m.vrt")
        )


def test_mosaic_raises_when_nothing_exists(tmp_path) -> None:
    with pytest.raises(FileNotFoundError):
        export.mosaic([str(tmp_path / "nope.tif")], str(tmp_path / "m.vrt"))
