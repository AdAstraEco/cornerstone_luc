import numpy
import pytest
import xarray

from jdluc import conversion_event, derive, emit
from jdluc.__tests__.emit_test import get_harmonized_dset_for_every_case
from jdluc.datasets import gnw_tcl, gpw_grassland
from jdluc.datasets.gpw_grassland import Grassland
from jdluc.emit import ConversionSource, GrasslandTransition


def get_dset_for_one_pixel(
    year_to_class: dict[int, Grassland], loss_year: int | None
) -> xarray.Dataset:
    """GPW 2000-2024 and TCL for one pixel; a year not in `year_to_class` holds the class before."""
    classes, current = [], Grassland.OTHER
    for year in gpw_grassland.YEARS:
        current = year_to_class.get(year, current)
        classes.append(current)

    def darray(value: float) -> xarray.DataArray:
        return xarray.DataArray([[float(value)]], dims=("y", "x"))

    return xarray.Dataset(
        {
            band_name: darray(grassland_class)
            for band_name, grassland_class in zip(
                gpw_grassland.DATASET.fully_qualified_band_names, classes, strict=True
            )
        }
        | {
            gnw_tcl.DATASET.fully_qualified_band_name: darray(
                0 if loss_year is None else loss_year - gnw_tcl.LOSS_YEAR_OFFSET
            )
        }
    )


def select(
    year_to_class: dict[int, Grassland], loss_year: int | None, reference_year: int
) -> tuple[int, int]:
    dset = get_dset_for_one_pixel(year_to_class, loss_year)
    zero = xarray.zeros_like(dset[gnw_tcl.DATASET.fully_qualified_band_name])
    candidates = emit.get_conversion_candidates(
        dset=dset,
        forest_carbon=zero,
        grassland_carbon=zero,
        is_peatland=zero == 1,
        soil_organic_carbon=zero,
    )
    event = conversion_event.materiality(
        conversion_event.read_candidates(emit.get_dset_for_output(candidates)),
        reference_year=reference_year,
        lookback_years=emit.LOOKBACK_YEARS,
    )
    return int(event.source.compute().item()), int(event.year.compute().item())


def test_transition_bitmasks_record_every_year() -> None:
    dset = get_dset_for_one_pixel(
        {
            2000: Grassland.NATURAL,
            2006: Grassland.OTHER,
            2010: Grassland.OPEN_SHRUBLAND,
            2016: Grassland.OTHER,
        },
        loss_year=None,
    )
    transitions = emit.get_grassland_transitions(dset)
    bitmask = int(transitions[GrasslandTransition.NATURAL_TO_OTHER].item())
    assert bitmask == (1 << (2006 - 2001)) | (1 << (2016 - 2001))


def test_materiality_ranks_natural_grassland_above_a_later_pasture_loss() -> None:
    # natural -> pasture in 2008, pasture -> other in 2015: emit's recency rule would say pasture
    year_to_class = {
        2000: Grassland.NATURAL,
        2008: Grassland.CULTIVATED,
        2015: Grassland.OTHER,
    }
    assert select(year_to_class, loss_year=None, reference_year=2020) == (
        ConversionSource.NATURAL_GRASSLAND,
        2008,
    )


def test_the_window_moves_with_the_reference_year() -> None:
    year_to_class = {2000: Grassland.NATURAL, 2003: Grassland.OTHER}
    # 2003 is in (2000, 2020] but not in (2004, 2024]; the 2023 forest loss only in the latter
    assert select(year_to_class, loss_year=None, reference_year=2020) == (
        ConversionSource.NATURAL_GRASSLAND,
        2003,
    )
    assert select(year_to_class, loss_year=2023, reference_year=2020) == (
        ConversionSource.NATURAL_GRASSLAND,
        2003,
    )
    assert select(year_to_class, loss_year=2023, reference_year=2024) == (
        ConversionSource.FOREST,
        2023,
    )
    assert select(year_to_class, loss_year=None, reference_year=2024) == (
        ConversionSource.NONE,
        0,
    )


def test_derive_runs_for_another_reference_year() -> None:
    output = emit.get_output_dset(dset=get_harmonized_dset_for_every_case())
    layer = derive.derive(output, reference_year=2020).compute()
    total = layer["ccl-total-emissions-amortized:tco2e-per-ha"]
    assert set(total.dims) == {"y", "x", derive.CROP_CLASS, derive.DISCOUNTING}
    assert numpy.isfinite(total.sel(y=total.y[:3])).all()


def get_harmonized_dset_with_random_histories(seed: int) -> xarray.Dataset:
    """The every-case grid, its GPW years and TCL replaced with random ones, no data included."""
    dset = get_harmonized_dset_for_every_case()
    rng = numpy.random.default_rng(seed)
    shape = dset[gnw_tcl.DATASET.fully_qualified_band_name].shape
    # NB: few classes and long runs, so departures (and returns) are frequent but not every year
    state = rng.choice([*map(float, Grassland), numpy.nan], size=shape)
    for band_name in gpw_grassland.DATASET.fully_qualified_band_names:
        if band_name not in dset:
            continue
        changes = rng.random(shape) < 0.15
        state = numpy.where(
            changes, rng.choice([*map(float, Grassland), numpy.nan], size=shape), state
        )
        dset[band_name] = dset[band_name].copy(data=state)
    loss = rng.choice([0, 0, 0, *range(1, 25)], size=shape).astype(float)
    dset[gnw_tcl.DATASET.fully_qualified_band_name] = dset[
        gnw_tcl.DATASET.fully_qualified_band_name
    ].copy(data=loss)
    return dset


@pytest.mark.parametrize("seed", range(3))
def test_recency_at_the_assessment_year_reproduces_emits_cie(seed: int) -> None:
    output = emit.get_output_dset(
        dset=get_harmonized_dset_with_random_histories(seed)
    ).compute()
    selected = conversion_event.get_cie(
        output, reference_year=emit.ASSESSMENT_YEAR, rule="recency"
    ).compute()

    # Every source occurs, so the comparison covers each branch of the rule
    assert set(numpy.unique(output["cie-conversion-source"].data)) == set(
        map(float, ConversionSource)
    )
    for name in selected:
        numpy.testing.assert_allclose(
            selected[name].data.astype(numpy.float64),
            output[name].data.astype(numpy.float64),
            atol=1e-4,
            err_msg=name,
            rtol=1e-6,
        )


def test_derive_at_the_reference_year_reproduces_derive_without_one() -> None:
    output = emit.get_output_dset(dset=get_harmonized_dset_with_random_histories(0))
    xarray.testing.assert_allclose(
        derive.derive(output, reference_year=emit.ASSESSMENT_YEAR).compute(),
        derive.derive(output).compute(),
    )
