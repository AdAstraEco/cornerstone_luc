import numpy
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
