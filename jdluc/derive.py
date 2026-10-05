"""Crop-class emissions layer: the emissions IF a given crop class occupies each pixel.

Layer 2 of the emissions data model (see the Emissions Layer Shaping Document, and
`docs/orbae/crop_independent_emissions.md` for its input). It reads `emit`'s Crop-Independent
Emissions (CIE) bands and applies what CIE deliberately leaves out:

- the crop-class land-use factor on mineral soil (F_LU, by climate zone),
- the crop-class emission factor for occupied peat (by peat climate regime),
- the linear discount that spreads each conversion's emissions over the lookback window.

There is no destination gate: unlike `emit.derive_from_cie`, nothing here asks what land cover
the pixel actually became. Every pixel answers "what if this crop class were here?".

The layer is lazy and never stored. What gets materialised is a deliverable: a named choice of
crop class and layer fields, written per tile as a COG straight from the CIE zarr. A debugging
export is just another deliverable, with more of the intermediate fields.

The file reads top to bottom as:

1. Factor tables  -- the numbers, with their sources
2. Calculations   -- one function per row of the layer, arithmetic only
3. Deliverables   -- the products, each a crop class and a choice of layer fields
4. Plumbing       -- reading CIE bands, table lookups, writing datasets and COGs
"""

import argparse
import collections.abc
import dataclasses
import enum
import itertools
import logging
import math
import typing

import numpy
import xarray

from jdluc import config, emit, storage
from jdluc.datasets.ipcc_climate_zones import Zone

logger = logging.getLogger(__name__)

# ============================================================================================
# 1. Factor tables
#
# Source: AdAstra's `GEZ_FLU_PeatEF_capped.gpkg`, derived from IPCC 2019 Refinement Vol 4
# Ch 5 Table 5.5 (F_LU) and the IPCC 2013 Wetlands Supplement (drained organic soils). The
# geopackage is keyed on the 21 FAO GEZ zones; here it is flattened onto the 10 IPCC climate
# zones the `cie-climate-zone` band carries, via the geopackage's own `IPCC_zone` and
# `peat_climate_zone` columns. Every zone matches one of those columns' values except
# TROPICAL_MONTANE, which they do not have (see below).
# ============================================================================================

REFERENCE_YEAR = emit.ASSESSMENT_YEAR


class CropClass(enum.StrEnum):
    ANNUAL = "annual"
    PERENNIAL = "perennial"
    PADDY_RICE = "paddy-rice"
    PASTURE = "pasture"


# F_LU: the fraction of reference soil organic carbon retained once the crop class occupies the
# land. Capped at 1.0: the layer does not model SOC gain, so paddy rice (1.35) and tropical
# perennials (1.01) lose nothing. Pasture is 1.0 everywhere (Table 6.2).
#
# NB: TROPICAL_MONTANE is ASSUMED to take the tropical moist/wet values. The geopackage has no
# montane zone: its FAO GEZ "Tropical mountain system" has `IPCC_zone` "Tropical Moist/Wet",
# and we take that as the closest match. The two classifications are not the same areas, so this
# is a judgement call to confirm. `emit` instead approximates montane as 0.76 for annual crops.
CROP_CLASS_TO_ZONE_TO_FLU: dict[CropClass, dict[Zone, float]] = {
    CropClass.ANNUAL: {
        Zone.TROPICAL_MONTANE: 0.83,
        Zone.TROPICAL_WET: 0.83,
        Zone.TROPICAL_MOIST: 0.83,
        Zone.TROPICAL_DRY: 0.92,
        Zone.WARM_TEMPERATE_MOIST: 0.69,
        Zone.WARM_TEMPERATE_DRY: 0.76,
        Zone.COOL_TEMPERATE_MOIST: 0.70,
        Zone.COOL_TEMPERATE_DRY: 0.77,
        Zone.BOREAL_MOIST: 0.70,
        Zone.BOREAL_DRY: 0.77,
    },
    CropClass.PERENNIAL: {
        Zone.TROPICAL_MONTANE: 1.00,
        Zone.TROPICAL_WET: 1.00,
        Zone.TROPICAL_MOIST: 1.00,
        Zone.TROPICAL_DRY: 1.00,
        Zone.WARM_TEMPERATE_MOIST: 0.72,
        Zone.WARM_TEMPERATE_DRY: 0.72,
        Zone.COOL_TEMPERATE_MOIST: 0.72,
        Zone.COOL_TEMPERATE_DRY: 0.72,
        Zone.BOREAL_MOIST: 0.72,
        Zone.BOREAL_DRY: 0.72,
    },
    CropClass.PADDY_RICE: {zone: 1.00 for zone in Zone},
    CropClass.PASTURE: {zone: 1.00 for zone in Zone},
}


class PeatRegime(enum.StrEnum):
    TROPICAL = "tropical"
    TEMPERATE = "temperate"
    BOREAL = "boreal"


ZONE_TO_PEAT_REGIME: dict[Zone, PeatRegime] = {
    Zone.TROPICAL_MONTANE: PeatRegime.TROPICAL,
    Zone.TROPICAL_WET: PeatRegime.TROPICAL,
    Zone.TROPICAL_MOIST: PeatRegime.TROPICAL,
    Zone.TROPICAL_DRY: PeatRegime.TROPICAL,
    Zone.WARM_TEMPERATE_MOIST: PeatRegime.TEMPERATE,
    Zone.WARM_TEMPERATE_DRY: PeatRegime.TEMPERATE,
    Zone.COOL_TEMPERATE_MOIST: PeatRegime.TEMPERATE,
    Zone.COOL_TEMPERATE_DRY: PeatRegime.TEMPERATE,
    Zone.BOREAL_MOIST: PeatRegime.BOREAL,
    Zone.BOREAL_DRY: PeatRegime.BOREAL,
}

# Annual emissions from drained peat occupied by the crop class, tCO2e/ha/yr (all gases).
# Pasture takes the well-drained value. Paddy rice has no column in the source: ASSUMED annual.
# Where the climate zone is missing, the CIE value (`emit.PEATLAND_EMISSIONS_ANNUAL_TCO2E_PER_HA`)
# stands.
CROP_CLASS_TO_PEAT_REGIME_TO_OCCUPATION_EF: dict[CropClass, dict[PeatRegime, float]] = {
    CropClass.ANNUAL: {
        PeatRegime.TROPICAL: 56.92,
        PeatRegime.TEMPERATE: 38.18,
        PeatRegime.BOREAL: 37.48,
    },
    CropClass.PERENNIAL: {
        PeatRegime.TROPICAL: 60.76,
        PeatRegime.TEMPERATE: 12.25,
        PeatRegime.BOREAL: 1.88,
    },
    CropClass.PADDY_RICE: {
        PeatRegime.TROPICAL: 56.92,
        PeatRegime.TEMPERATE: 38.18,
        PeatRegime.BOREAL: 37.48,
    },
    CropClass.PASTURE: {
        PeatRegime.TROPICAL: 42.32,
        PeatRegime.TEMPERATE: 29.87,
        PeatRegime.BOREAL: 27.82,
    },
}

# The 621 tCO2e/ha peat transformation pulse is crop-generic: every class takes it whole.
PEAT_TRANSFORMATION_FACTOR = 1.0

# Discounting: the fraction of a conversion's emissions charged in REFERENCE_YEAR, by the year
# the conversion happened. A scheme is a 1-D array over the assessment window -- dimension
# `conversion_year`, LOOKBACK_YEARS (20) long, REFERENCE_YEAR - 19 .. REFERENCE_YEAR -- that
# sums to 1. Conversions outside the window are charged nothing.
LOOKBACK_YEARS = emit.LOOKBACK_YEARS
CONVERSION_YEAR = "conversion_year"
ASSESSMENT_WINDOW = numpy.arange(
    REFERENCE_YEAR - LOOKBACK_YEARS + 1, REFERENCE_YEAR + 1
)


def get_discount_weights(weights: collections.abc.Sequence[float]) -> xarray.DataArray:
    """`weights`, one per year of the assessment window, as a (conversion_year: 20) array."""
    return xarray.DataArray(
        numpy.asarray(weights, dtype=numpy.float64),
        coords={CONVERSION_YEAR: ASSESSMENT_WINDOW},
        dims=(CONVERSION_YEAR,),
    )


# Linear, as in Maverick's post-processing (its `LDF.xlsx`): rising in equal steps from 0.0025
# (REFERENCE_YEAR - 19) to 0.0975 (REFERENCE_YEAR).
LINEAR_DISCOUNT_WEIGHTS = get_discount_weights(
    numpy.linspace(
        start=1 / LOOKBACK_YEARS**2,
        stop=(2 * LOOKBACK_YEARS - 1) / LOOKBACK_YEARS**2,
        num=LOOKBACK_YEARS,
    )
)

# Equal: 1/20 for every year of the window.
EQUAL_DISCOUNT_WEIGHTS = get_discount_weights(
    numpy.full(LOOKBACK_YEARS, 1 / LOOKBACK_YEARS)
)

# Five-year spans, as `emit` discounts to line up with MAPSPAM's 5-year snapshots: every year
# in a span (before, after] takes the span's weight, which is the linear weights' mean over it.
FIVE_YEAR_SPAN_DISCOUNT_WEIGHTS = get_discount_weights(
    [
        next(
            weight
            for (before, after), weight in emit.SPAN_TO_LINEAR_DISCOUNT_WEIGHT.items()
            if before < year <= after
        )
        for year in ASSESSMENT_WINDOW
    ]
)

DISCOUNTING_TO_WEIGHTS: dict[str, xarray.DataArray] = {
    "linear": LINEAR_DISCOUNT_WEIGHTS,
    "equal": EQUAL_DISCOUNT_WEIGHTS,
    "five-year-spans": FIVE_YEAR_SPAN_DISCOUNT_WEIGHTS,
}
DEFAULT_DISCOUNTING = "linear"

assert all(
    weights.sizes == {CONVERSION_YEAR: LOOKBACK_YEARS}
    and math.isclose(float(weights.sum()), 1)
    for weights in DISCOUNTING_TO_WEIGHTS.values()
)
assert numpy.allclose(
    LINEAR_DISCOUNT_WEIGHTS.coarsen({CONVERSION_YEAR: 5}).mean(),
    FIVE_YEAR_SPAN_DISCOUNT_WEIGHTS.coarsen({CONVERSION_YEAR: 5}).mean(),
)

assert all(
    set(zone_to_flu) == set(Zone) for zone_to_flu in CROP_CLASS_TO_ZONE_TO_FLU.values()
)
assert all(
    0 <= flu <= 1
    for zone_to_flu in CROP_CLASS_TO_ZONE_TO_FLU.values()
    for flu in zone_to_flu.values()
)
assert set(ZONE_TO_PEAT_REGIME) == set(Zone)
assert all(
    set(regime_to_ef) == set(PeatRegime)
    for regime_to_ef in CROP_CLASS_TO_PEAT_REGIME_TO_OCCUPATION_EF.values()
)
assert (
    set(CROP_CLASS_TO_ZONE_TO_FLU)
    == set(CROP_CLASS_TO_PEAT_REGIME_TO_OCCUPATION_EF)
    == set(CropClass)
)


# ============================================================================================
# 2. Calculations
#
# Every function is arithmetic on named arrays. Units: tCO2e per hectare, except where named
# per year.
#
# Dimensions. CIE bands are on the pixel grid, (y, x). Two inputs add a dimension:
#
# - the factors (F_LU and the peat occupation EF) are (y, x, crop_class);
# - the discount weights are (conversion_year: 20, discounting), one column per scheme.
#
# Arithmetic broadcasts by dimension name, so a field carries `crop_class` exactly when it
# depends on a factor, and `discounting` exactly when it is discounted -- nothing here needs to
# say which. `crop_class_layer` notes the dimensions of each step; `get_layer` logs their sizes.
# ============================================================================================


def is_conversion(conversion_source: xarray.DataArray) -> xarray.DataArray:
    """Whether CIE records a conversion event (forest, natural grassland or pasture lost).

    Which event a pixel carries, where sources conflict, is resolved upstream in `emit`.
    """
    return conversion_source != emit.ConversionSource.NONE


def discount_weight(
    conversion_year: xarray.DataArray, discount_weights: xarray.DataArray
) -> xarray.DataArray:
    """The fraction of a pixel's conversion emissions charged in the reference year.

    (y, x) conversion years and (conversion_year: 20, ...) weights -> (y, x, ...) weights,
    where `...` is whatever else the weights carry (e.g. `discounting`); 0 outside the window.
    """
    weight = sum(
        xarray.where(
            conversion_year == year,
            discount_weights.sel({CONVERSION_YEAR: year}, drop=True),
            0.0,
        )
        for year in discount_weights[CONVERSION_YEAR].values.tolist()
    )
    assert isinstance(weight, xarray.DataArray)
    return weight


def vegetation_emissions_amortized(
    vegetation_emissions_undiscounted: xarray.DataArray,
    is_conversion: xarray.DataArray,
    discount_weight: xarray.DataArray,
) -> xarray.DataArray:
    """Vegetation carbon lost at conversion, discounted."""
    return (
        xarray.where(is_conversion, vegetation_emissions_undiscounted, 0.0)
        * discount_weight
    )


def mineral_soil_emissions_amortized(
    mineral_soil_carbon_at_risk: xarray.DataArray,
    flu: xarray.DataArray,
    is_conversion: xarray.DataArray,
    discount_weight: xarray.DataArray,
) -> xarray.DataArray:
    """Mineral soil carbon lost to the crop class's land use, discounted.

    CIE's carbon at risk is the full stock (zero on peat); the crop class keeps the fraction F_LU.
    """
    soil_loss = mineral_soil_carbon_at_risk * (1 - flu)
    return xarray.where(is_conversion, soil_loss, 0.0) * discount_weight


def peat_transformation_emissions_amortized(
    peat_transformation_emissions_undiscounted: xarray.DataArray,
    is_conversion: xarray.DataArray,
    discount_weight: xarray.DataArray,
) -> xarray.DataArray:
    """The one-off pulse from draining peat at conversion, discounted."""
    pulse = peat_transformation_emissions_undiscounted * PEAT_TRANSFORMATION_FACTOR
    return xarray.where(is_conversion, pulse, 0.0) * discount_weight


def is_peat(peat_occupation_emissions: xarray.DataArray) -> xarray.DataArray:
    """Whether the pixel is on peat. CIE puts a nonzero occupation potential on all peat."""
    return peat_occupation_emissions > 0


def peat_occupation_emissions_per_year(
    is_peat: xarray.DataArray, peat_occupation_ef: xarray.DataArray
) -> xarray.DataArray:
    """Annual emissions from the crop class occupying drained peat, whether or not converted."""
    return xarray.where(is_peat, peat_occupation_ef, 0.0)


def total_emissions_amortized(
    vegetation_emissions_amortized: xarray.DataArray,
    mineral_soil_emissions_amortized: xarray.DataArray,
    peat_transformation_emissions_amortized: xarray.DataArray,
    peat_occupation_emissions_per_year: xarray.DataArray,
) -> xarray.DataArray:
    """Everything charged to the crop class on this pixel in the reference year."""
    return (
        vegetation_emissions_amortized
        + mineral_soil_emissions_amortized
        + peat_transformation_emissions_amortized
        + peat_occupation_emissions_per_year
    )


@dataclasses.dataclass(frozen=True)
class CIE:
    """The `emit` CIE bands that this layer reads (see `read_cie`)."""

    conversion_source: xarray.DataArray
    conversion_year: xarray.DataArray
    vegetation_emissions_undiscounted: xarray.DataArray
    mineral_soil_carbon_at_risk: xarray.DataArray
    peat_transformation_emissions_undiscounted: xarray.DataArray
    peat_occupation_emissions: xarray.DataArray
    climate_zone: xarray.DataArray
    hectares_per_pixel: xarray.DataArray


@dataclasses.dataclass(frozen=True)
class Factors:
    """The factors, per pixel and per crop class: (y, x, crop_class). See `lookup_factors`."""

    flu: xarray.DataArray
    peat_occupation_ef: xarray.DataArray


@dataclasses.dataclass(frozen=True)
class CropClassLayer:
    """Layer 2: one field per row, in the shaping document's order.

    The fields that depend on a factor carry the `crop_class` dimension; the rest do not.
    """

    conversion_source: xarray.DataArray
    conversion_year: xarray.DataArray
    climate_zone: xarray.DataArray
    discount_weight: xarray.DataArray
    flu: xarray.DataArray
    peat_occupation_ef: xarray.DataArray
    mineral_soil_emissions_amortized: xarray.DataArray
    peat_transformation_emissions_amortized: xarray.DataArray
    vegetation_emissions_amortized: xarray.DataArray
    peat_occupation_emissions_per_year: xarray.DataArray
    is_peat: xarray.DataArray
    total_emissions_amortized: xarray.DataArray
    hectares_per_pixel: xarray.DataArray


FIELD_TO_UNITS: dict[str, str | None] = {
    "conversion_source": None,
    "conversion_year": None,
    "climate_zone": None,
    "discount_weight": None,
    "flu": None,
    "peat_occupation_ef": "tco2e-per-ha-per-year",
    "mineral_soil_emissions_amortized": "tco2e-per-ha",
    "peat_transformation_emissions_amortized": "tco2e-per-ha",
    "vegetation_emissions_amortized": "tco2e-per-ha",
    "peat_occupation_emissions_per_year": "tco2e-per-ha-per-year",
    "is_peat": None,
    # NB: adds the amortized terms to a year's peat occupation, as `emit` does
    "total_emissions_amortized": "tco2e-per-ha",
    "hectares_per_pixel": "ha",
}
assert set(FIELD_TO_UNITS) == {
    field.name for field in dataclasses.fields(CropClassLayer)
}


def crop_class_layer(
    cie: CIE, factors: Factors, discount_weights: xarray.DataArray
) -> CropClassLayer:
    """Layer 2, computed from the CIE bands, for every crop class the factors carry.

    `discount_weights` are the discounting schemes, (conversion_year: 20, discounting).
    """
    # (y, x): one value per pixel
    converted = is_conversion(cie.conversion_source)
    on_peat = is_peat(cie.peat_occupation_emissions)

    # (y, x, discounting): discounted, but the same for every crop class
    weight = discount_weight(cie.conversion_year, discount_weights)
    vegetation = vegetation_emissions_amortized(
        cie.vegetation_emissions_undiscounted, converted, weight
    )
    peat_transformation = peat_transformation_emissions_amortized(
        cie.peat_transformation_emissions_undiscounted, converted, weight
    )

    # (y, x, discounting, crop_class): discounted, and takes a factor
    mineral_soil = mineral_soil_emissions_amortized(
        cie.mineral_soil_carbon_at_risk, factors.flu, converted, weight
    )

    # (y, x, crop_class): takes a factor, but is a yearly rate, so is not discounted
    peat_occupation = peat_occupation_emissions_per_year(
        on_peat, factors.peat_occupation_ef
    )

    return CropClassLayer(
        conversion_source=cie.conversion_source,
        conversion_year=cie.conversion_year,
        climate_zone=cie.climate_zone,
        discount_weight=weight,
        flu=factors.flu,
        peat_occupation_ef=factors.peat_occupation_ef,
        mineral_soil_emissions_amortized=mineral_soil,
        peat_transformation_emissions_amortized=peat_transformation,
        vegetation_emissions_amortized=vegetation,
        peat_occupation_emissions_per_year=peat_occupation,
        is_peat=on_peat,
        # (y, x, discounting, crop_class): the union of its terms' dimensions
        total_emissions_amortized=total_emissions_amortized(
            vegetation, mineral_soil, peat_transformation, peat_occupation
        ),
        hectares_per_pixel=cie.hectares_per_pixel,
    )


# ============================================================================================
# 3. Deliverables
#
# A deliverable is a product written per tile, as a COG. Each band is a layer field and, for
# the fields that vary by them, the crop classes and discounting schemes to write it for: one
# COG band per combination. Any field of `CropClassLayer` is on the menu. Defining a new
# product means adding an entry here.
# ============================================================================================

MENU: tuple[str, ...] = tuple(
    field.name for field in dataclasses.fields(CropClassLayer)
)


@dataclasses.dataclass(frozen=True)
class Band:
    field: str
    crop_classes: tuple[CropClass, ...] = ()
    discountings: tuple[str, ...] = ()


@dataclasses.dataclass(frozen=True)
class Deliverable:
    name: str
    bands: tuple[Band, ...]

    def __post_init__(self) -> None:
        unknown = {band.field for band in self.bands} - set(MENU)
        assert not unknown, f"{self.name!r} asks for bands not on the menu: {unknown}"
        unknown = {d for band in self.bands for d in band.discountings} - set(
            DISCOUNTING_TO_WEIGHTS
        )
        assert not unknown, f"{self.name!r} asks for unknown discountings: {unknown}"


CROP_CLASS_EMISSIONS = Deliverable(
    name="crop-class-emissions",
    bands=(
        Band(
            "total_emissions_amortized",
            crop_classes=tuple(CropClass),
            discountings=("linear",),
        ),
        Band("hectares_per_pixel"),
    ),
)

PERENNIAL_DEBUG = Deliverable(
    name="perennial-debug",
    bands=(
        Band("conversion_source"),
        Band("conversion_year"),
        Band("climate_zone"),
        Band("discount_weight", discountings=("linear",)),
        Band("flu", crop_classes=(CropClass.PERENNIAL,)),
        Band("peat_occupation_ef", crop_classes=(CropClass.PERENNIAL,)),
        Band("vegetation_emissions_amortized", discountings=("linear",)),
        Band(
            "mineral_soil_emissions_amortized",
            crop_classes=(CropClass.PERENNIAL,),
            discountings=("linear",),
        ),
        Band("peat_transformation_emissions_amortized", discountings=("linear",)),
        Band("peat_occupation_emissions_per_year", crop_classes=(CropClass.PERENNIAL,)),
        Band(
            "total_emissions_amortized",
            crop_classes=(CropClass.PERENNIAL,),
            discountings=("linear",),
        ),
        Band("hectares_per_pixel"),
    ),
)

PERENNIAL_DISCOUNTING_COMPARISON = Deliverable(
    name="perennial-discounting-comparison",
    bands=(
        Band("vegetation_emissions_amortized", discountings=("equal", "linear")),
        Band(
            "mineral_soil_emissions_amortized",
            crop_classes=(CropClass.PERENNIAL,),
            discountings=("equal", "linear"),
        ),
        Band(
            "peat_transformation_emissions_amortized", discountings=("equal", "linear")
        ),
        # NB: a yearly rate, not discounted, so one band whatever the discounting
        Band("peat_occupation_emissions_per_year", crop_classes=(CropClass.PERENNIAL,)),
        Band(
            "total_emissions_amortized",
            crop_classes=(CropClass.PERENNIAL,),
            discountings=("equal", "linear"),
        ),
        Band("hectares_per_pixel"),
    ),
)

NAME_TO_DELIVERABLE: dict[str, Deliverable] = {
    deliverable.name: deliverable
    for deliverable in (
        CROP_CLASS_EMISSIONS,
        PERENNIAL_DEBUG,
        PERENNIAL_DISCOUNTING_COMPARISON,
    )
}


# ============================================================================================
# 4. Plumbing
# ============================================================================================

BAND_PREFIX = "ccl-"
CROP_CLASS = "crop_class"
DISCOUNTING = "discounting"
# The dimensions a band can select on, in the order their values appear in band names
SELECTABLE_DIMS = (CROP_CLASS, DISCOUNTING)


def get_selection(band: Band) -> dict[str, tuple[str, ...]]:
    """`band`'s chosen values, by dimension, for the dimensions it selects on."""
    dim_to_values = {
        CROP_CLASS: tuple(map(str, band.crop_classes)),
        DISCOUNTING: band.discountings,
    }
    return {dim: values for dim, values in dim_to_values.items() if values}


def read_cie(dset: xarray.Dataset) -> CIE:
    """The CIE bands of an `emit` output dataset, by field name."""

    def cie(name: str) -> xarray.DataArray:
        return dset[f"cie-{name:s}"]

    return CIE(
        # NB: no conversion is NONE (0); NaN no-data reads as no conversion too
        conversion_source=cie("conversion-source").fillna(0),
        conversion_year=cie("conversion-year").fillna(0),
        vegetation_emissions_undiscounted=cie(
            "vegetation-emissions-undiscounted:tco2e-per-ha"
        ),
        mineral_soil_carbon_at_risk=cie("mineral-soil-carbon-at-risk:tco2e-per-ha"),
        peat_transformation_emissions_undiscounted=cie(
            "peat-transformation-emissions-undiscounted:tco2e-per-ha"
        ),
        peat_occupation_emissions=cie(
            "peat-occupation-emissions:tco2e-per-ha-per-year"
        ),
        climate_zone=cie("climate-zone"),
        hectares_per_pixel=cie("hectares-per-pixel:ha"),
    )


def lookup_by_zone(
    climate_zone: xarray.DataArray, zone_to_value: dict[Zone, float], default: float
) -> xarray.DataArray:
    """`zone_to_value` per pixel, as a lazy blockwise lookup; `default` where the zone is missing."""
    lookup = numpy.full(256, default, dtype=numpy.float32)
    for zone, value in zone_to_value.items():
        lookup[zone.value] = value
    return xarray.apply_ufunc(
        lookup.__getitem__,
        climate_zone.fillna(0).astype(numpy.uint8),
        dask="parallelized",
        output_dtypes=[numpy.float32],
    )


def lookup_factors(
    climate_zone: xarray.DataArray, crop_classes: collections.abc.Sequence[CropClass]
) -> Factors:
    """Per-pixel factors from the tables, for `crop_classes` only, along `crop_class`.

    Each class is its own lookup, so its own dask chunk along `crop_class`: selecting one
    class computes only that class. A missing zone takes F_LU = 1 (no mineral soil loss, as in
    `emit`) and the CIE peat occupation value.
    """

    def by_class(lookup_one: collections.abc.Callable[[CropClass], xarray.DataArray]):
        return (
            xarray.concat(
                [lookup_one(crop_class) for crop_class in crop_classes], dim=CROP_CLASS
            )
            .assign_coords(
                {CROP_CLASS: [str(crop_class) for crop_class in crop_classes]}
            )
            .transpose(..., CROP_CLASS)
        )  # (y, x, crop_class), as the fields they reach

    def flu(crop_class: CropClass) -> xarray.DataArray:
        return lookup_by_zone(
            climate_zone=climate_zone,
            zone_to_value=CROP_CLASS_TO_ZONE_TO_FLU[crop_class],
            default=1.0,
        )

    def peat_occupation_ef(crop_class: CropClass) -> xarray.DataArray:
        regime_to_ef = CROP_CLASS_TO_PEAT_REGIME_TO_OCCUPATION_EF[crop_class]
        return lookup_by_zone(
            climate_zone=climate_zone,
            zone_to_value={
                zone: regime_to_ef[regime]
                for zone, regime in ZONE_TO_PEAT_REGIME.items()
            },
            default=emit.PEATLAND_EMISSIONS_ANNUAL_TCO2E_PER_HA,
        )

    return Factors(flu=by_class(flu), peat_occupation_ef=by_class(peat_occupation_ef))


def get_discount_weights_by_scheme(
    discountings: collections.abc.Sequence[str],
) -> xarray.DataArray:
    """The named schemes side by side: (conversion_year: 20, discounting)."""
    return (
        xarray.concat(
            [DISCOUNTING_TO_WEIGHTS[discounting] for discounting in discountings],
            dim=DISCOUNTING,
        )
        .assign_coords({DISCOUNTING: list(discountings)})
        .transpose(CONVERSION_YEAR, ...)
    )


def get_field_to_dims() -> dict[str, frozenset[str]]:
    """Which selectable dimensions each layer field carries, from running the layer on a pixel."""
    pixel = xarray.DataArray(numpy.zeros((1, 1)), dims=("y", "x"))
    per_class = pixel.expand_dims({CROP_CLASS: 1})
    layer = crop_class_layer(
        cie=CIE(**{field.name: pixel for field in dataclasses.fields(CIE)}),
        factors=Factors(flu=per_class, peat_occupation_ef=per_class),
        discount_weights=get_discount_weights_by_scheme([DEFAULT_DISCOUNTING]),
    )
    return {
        field: frozenset(set(getattr(layer, field).dims) & set(SELECTABLE_DIMS))
        for field in MENU
    }


FIELD_TO_DIMS = get_field_to_dims()


def check_deliverable(deliverable: Deliverable) -> None:
    """Each band selects on exactly the dimensions its field carries, and no band repeats."""
    for band in deliverable.bands:
        selected = set(get_selection(band))
        carried = FIELD_TO_DIMS[band.field]
        assert selected == carried, (
            f"{deliverable.name!r}: {band.field!r} varies by {sorted(carried)}, "
            f"but the band selects on {sorted(selected)}"
        )
    names = [name for band in deliverable.bands for name in get_band_names(band)]
    assert len(set(names)) == len(names), f"{deliverable.name!r} repeats a band"


def get_band_name(field: str, *values: str) -> str:
    """The band a layer field is written to: `ccl-<field>:<units>[:<value>...]`.

    `values` are the band's crop class and/or discounting, in `SELECTABLE_DIMS` order.
    """
    words = [BAND_PREFIX + field.replace("_", "-"), FIELD_TO_UNITS[field], *values]
    return ":".join(str(word) for word in words if word)


def iter_band_selections(
    band: Band,
) -> collections.abc.Iterator[tuple[str, dict[str, str]]]:
    """Each COG band a `Band` expands to: its name, and its value on each selected dimension."""
    selection = get_selection(band)
    for values in itertools.product(*selection.values()):
        yield (
            get_band_name(band.field, *values),
            dict(zip(selection, values, strict=True)),
        )


def get_band_names(band: Band) -> list[str]:
    return [name for name, _ in iter_band_selections(band)]


for _deliverable in NAME_TO_DELIVERABLE.values():
    check_deliverable(_deliverable)


def to_dataset(
    layer: CropClassLayer, bands: collections.abc.Iterable[Band]
) -> xarray.Dataset:
    """`bands` of `layer`, one 2-D band per combination of selected values, in order.

    `emit`-style encoding: float32, NaN no-data, chunked for the number of bands written.
    """
    name_to_darray = {
        name: getattr(layer, band.field).rename(None).sel(point, drop=True)
        for band in bands
        for name, point in iter_band_selections(band)
    }
    dset = emit.get_dset_for_output(name_to_darray=name_to_darray)
    assert list(dset) == list(name_to_darray)
    return dset


def get_layer(
    dset: xarray.Dataset,
    crop_classes: collections.abc.Sequence[CropClass],
    discountings: collections.abc.Sequence[str] = (DEFAULT_DISCOUNTING,),
) -> CropClassLayer:
    """Layer 2 for `crop_classes` and `discountings`, from an `emit` output dataset."""
    cie = read_cie(dset)
    layer = crop_class_layer(
        cie=cie,
        factors=lookup_factors(cie.climate_zone, crop_classes),
        discount_weights=get_discount_weights_by_scheme(discountings),
    )
    log_layer_shapes(layer=layer)
    return layer


def log_layer_shapes(layer: CropClassLayer) -> None:
    """At DEBUG, each field's dimensions, sizes and chunking: what a computation will touch."""
    if not logger.isEnabledFor(logging.DEBUG):
        return
    for field in MENU:
        darray = getattr(layer, field)
        chunks = (
            {dim: len(sizes) for dim, sizes in darray.chunksizes.items()}
            if darray.chunks is not None
            else "in memory"
        )
        logger.debug(f"{field:s}: sizes {dict(darray.sizes)}, chunk count {chunks}")


def derive(
    dset: xarray.Dataset,
    crop_classes: collections.abc.Sequence[CropClass] = tuple(CropClass),
    discountings: collections.abc.Sequence[str] = (DEFAULT_DISCOUNTING,),
) -> xarray.Dataset:
    """All of layer 2 for `crop_classes` and `discountings`, as a lazy dataset."""
    bands = [
        Band(
            field,
            crop_classes=tuple(crop_classes) if CROP_CLASS in dims else (),
            discountings=tuple(discountings) if DISCOUNTING in dims else (),
        )
        for field, dims in FIELD_TO_DIMS.items()
    ]
    return to_dataset(layer=get_layer(dset, crop_classes, discountings), bands=bands)


def derive_deliverable(
    dset: xarray.Dataset, deliverable: Deliverable
) -> xarray.Dataset:
    """A deliverable's bands as a lazy dataset, from an `emit` output dataset.

    Only the crop classes and discountings the deliverable names are built, so only they are
    computed.
    """
    check_deliverable(deliverable)

    def unique(values: collections.abc.Iterable[typing.Any]) -> list[typing.Any]:
        return list(dict.fromkeys(values))

    bands = deliverable.bands
    layer = get_layer(
        dset,
        crop_classes=unique(c for band in bands for c in band.crop_classes),
        discountings=unique(d for band in bands for d in band.discountings)
        or [DEFAULT_DISCOUNTING],
    )
    return to_dataset(layer=layer, bands=bands)


def get_output_uri(deliverable: Deliverable, output_root: str, tile_id: str) -> str:
    return storage.join_uri(
        root=output_root, prefix=f"{deliverable.name:s}/{tile_id:s}.tif"
    )


def export_workflow(
    tile_id: str,
    deliverable_names: collections.abc.Sequence[str],
    output_root: str | None = None,
) -> list[str]:
    """Write each named deliverable for one tile as a COG; return the URIs written.

    `output_root` defaults to the export root; pass a local directory to inspect the output.
    """
    deliverables = [NAME_TO_DELIVERABLE[name] for name in deliverable_names]
    if output_root is None:
        output_root = config.Config.from_dot_env().export_root
    logger.info(f"Reading emit scratch output for {tile_id=:s} (cache hit expected)")
    dset = emit.workflow(tile_id=tile_id)

    uris = []
    for deliverable in deliverables:
        uri = get_output_uri(deliverable, output_root=output_root, tile_id=tile_id)
        storage.write_dataset_to_cog(
            dset=derive_deliverable(dset, deliverable),
            metadata=storage.get_cog_metadata(
                product_name=deliverable.name, source_name="jdluc-derive"
            ),
            uri=uri,
        )
        logger.info(f"Wrote {deliverable.name=:s} for {tile_id=:s} to {uri=:s}")
        uris.append(uri)
    return uris


def main() -> int:
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tile_id")
    parser.add_argument(
        "--deliverable",
        action="append",
        choices=sorted(NAME_TO_DELIVERABLE),
        dest="deliverable_names",
        required=True,
        help="repeatable",
    )
    parser.add_argument("--output-root", help="defaults to the export root")
    args = parser.parse_args()
    export_workflow(
        deliverable_names=args.deliverable_names,
        output_root=args.output_root,
        tile_id=args.tile_id,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
