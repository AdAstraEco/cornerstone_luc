"""Conversion event selection (spike): from `emit`'s year-free `cand-` bands to one event per pixel.

`emit` picks each pixel's conversion event itself, inside the window ending at its fixed
ASSESSMENT_YEAR, and its `cie-` bands carry only that event. This module makes the same choice
later and as a parameter: given the `cand-` bands (every candidate event in every year, see
`emit.get_conversion_candidates`), a reference year and a rule, it returns the `cie-` bands that
`derive.read_cie` reads. So one `emit` output serves any reference year and any rule, and `derive`
is unchanged downstream of the selection.

Two rules, each a plain function in NAME_TO_RULE; DEFAULT_RULE is the one in force:

- `recency`: the current Cornerstone pipeline (`emit.get_conversion_record`). Forest loss in
  the window wins; otherwise the later of the last departures from natural grassland and from
  pasture. For emit's own ASSESSMENT_YEAR it reproduces the `cie-` event bands.
- `materiality`: rooster poc_v6's default (`first_in_priority_wins`). Forest outranks natural
  grassland, which outranks pasture, whatever their years; the winner is dated by its latest
  event in the window.
"""

import collections.abc
import dataclasses

import numpy
import xarray

from jdluc import emit
from jdluc.emit import ConversionSource, GrasslandTransition

# The years the `cand-` bands can date an event in: TCL runs further, but GPW stops in 2024
FIRST_CANDIDATE_YEAR = emit.CANDIDATE_BASE_YEAR
LAST_CANDIDATE_YEAR = 2024


@dataclasses.dataclass(frozen=True)
class Candidates:
    """The `emit` `cand-` bands, by field name (see `read_candidates`)."""

    tcl_loss_year: xarray.DataArray
    transitions: dict[GrasslandTransition, xarray.DataArray]
    forest_biomass: xarray.DataArray
    grassland_biomass: xarray.DataArray
    mineral_soil_carbon: xarray.DataArray
    is_peat: xarray.DataArray


@dataclasses.dataclass(frozen=True)
class Event:
    """One conversion event per pixel: its source (`emit.ConversionSource`), and its year or 0."""

    source: xarray.DataArray
    year: xarray.DataArray


def read_candidates(dset: xarray.Dataset) -> Candidates:
    """The `cand-` bands of an `emit` output dataset."""
    return Candidates(
        tcl_loss_year=dset["cand-tcl-loss-year"].fillna(0),
        transitions={
            transition: dset[f"cand-gpw-{transition:s}"].fillna(0)
            for transition in GrasslandTransition
        },
        forest_biomass=dset["cand-forest-biomass:tco2e-per-ha"],
        grassland_biomass=dset["cand-grassland-biomass:tco2e-per-ha"],
        mineral_soil_carbon=dset["cand-mineral-soil-carbon:tco2e-per-ha"],
        # NB: no data reads as no peat, as `emit`'s `== 1` does
        is_peat=dset["cand-is-peat"] == 1,
    )


def get_window_mask(first_year: int, last_year: int) -> numpy.uint32:
    """The bits of a transition bitmask for first_year..last_year, both inclusive."""
    low = first_year - emit.CANDIDATE_BASE_YEAR
    high = last_year - emit.CANDIDATE_BASE_YEAR
    assert 0 <= low <= high < 32
    return numpy.uint32(((1 << (high + 1)) - 1) & ~((1 << low) - 1))


def latest_year_in_window(
    bitmasks: collections.abc.Sequence[xarray.DataArray],
    first_year: int,
    last_year: int,
) -> xarray.DataArray:
    """The latest year any of `bitmasks` has an event in first_year..last_year, or 0 where none."""
    window = get_window_mask(first_year=first_year, last_year=last_year)

    def latest(*arrays: numpy.ndarray) -> numpy.ndarray:
        bits = (
            numpy.bitwise_or.reduce([array.astype(numpy.uint32) for array in arrays])
            & window
        )
        # NB: frexp's exponent is the position of the highest set bit, plus one
        _, exponent = numpy.frexp(bits.astype(numpy.float64))
        return numpy.where(bits > 0, emit.CANDIDATE_BASE_YEAR + exponent - 1, 0).astype(
            numpy.float32
        )

    return xarray.apply_ufunc(
        latest, *bitmasks, dask="parallelized", output_dtypes=[numpy.float32]
    )


GRASSLAND_SOURCES = (ConversionSource.NATURAL_GRASSLAND, ConversionSource.PASTURE)


def get_source_to_year(
    candidates: Candidates,
    first_year: int,
    last_year: int,
    grassland_source_to_transitions: dict[
        ConversionSource, tuple[GrasslandTransition, ...]
    ],
) -> dict[ConversionSource, xarray.DataArray]:
    """Per source, the latest year in first_year..last_year it lost land, or 0 where none.

    Which GPW transitions count as losing a grassland source is the rule's to say.
    """
    tcl = candidates.tcl_loss_year
    return {
        ConversionSource.FOREST: tcl.where(
            (tcl >= first_year) & (tcl <= last_year), other=0
        ),
        **{
            source: latest_year_in_window(
                bitmasks=[candidates.transitions[t] for t in transitions],
                first_year=first_year,
                last_year=last_year,
            )
            for source, transitions in grassland_source_to_transitions.items()
        },
    }


# --- Rule: materiality (rooster poc_v6's `first_in_priority_wins`) ---------------------------

# Rooster's grouping, which leaves out pasture abandonment and changes into no data
MATERIALITY_TRANSITIONS: dict[ConversionSource, tuple[GrasslandTransition, ...]] = {
    ConversionSource.NATURAL_GRASSLAND: (
        GrasslandTransition.NATURAL_TO_CULTIVATED,
        GrasslandTransition.NATURAL_TO_OTHER,
    ),
    ConversionSource.PASTURE: (GrasslandTransition.CULTIVATED_TO_OTHER,),
}
# Highest priority first
MATERIALITY = (
    ConversionSource.FOREST,
    ConversionSource.NATURAL_GRASSLAND,
    ConversionSource.PASTURE,
)


def materiality(
    candidates: Candidates, reference_year: int, lookback_years: int
) -> Event:
    """The first source in MATERIALITY with an event in the window wins, at its latest year there.

    The window is (reference_year - lookback_years, reference_year], as in `emit`.
    """
    source_to_year = get_source_to_year(
        candidates=candidates,
        first_year=reference_year - lookback_years + 1,
        last_year=reference_year,
        grassland_source_to_transitions=MATERIALITY_TRANSITIONS,
    )
    source = xarray.zeros_like(candidates.tcl_loss_year, dtype=numpy.uint8)
    year = xarray.zeros_like(candidates.tcl_loss_year, dtype=numpy.float32)
    # NB: lowest priority first, so each higher one overwrites where it has an event
    for candidate in reversed(MATERIALITY):
        has_event = source_to_year[candidate] > 0
        source = xarray.where(has_event, numpy.uint8(candidate), source)
        year = xarray.where(has_event, source_to_year[candidate], year)
    return Event(source=source, year=year)


# --- Rule: recency (the current Cornerstone pipeline, `emit.get_conversion_record`) ----------

# Leaving the class, to anything at all: `emit.get_last_departure_year` tests "the class last
# year and not this year", so abandonment and a no-data year both count
DEPARTURE_TRANSITIONS: dict[ConversionSource, tuple[GrasslandTransition, ...]] = {
    ConversionSource.NATURAL_GRASSLAND: (
        GrasslandTransition.NATURAL_TO_CULTIVATED,
        GrasslandTransition.NATURAL_TO_OTHER,
        GrasslandTransition.NATURAL_TO_NO_DATA,
    ),
    ConversionSource.PASTURE: (
        GrasslandTransition.CULTIVATED_TO_OTHER,
        GrasslandTransition.CULTIVATED_TO_NATURAL,
        GrasslandTransition.CULTIVATED_TO_NO_DATA,
    ),
}


def recency(candidates: Candidates, reference_year: int, lookback_years: int) -> Event:
    """Forest loss in the window wins; otherwise the later of the last departures from natural
    grassland and from pasture, at that year. `emit.get_conversion_record`'s choice, made for
    any reference year: for its own ASSESSMENT_YEAR, it reproduces the `cie-` event bands.
    """
    source_to_year = get_source_to_year(
        candidates=candidates,
        first_year=reference_year - lookback_years + 1,
        last_year=reference_year,
        grassland_source_to_transitions=DEPARTURE_TRANSITIONS,
    )
    forest_year = source_to_year[ConversionSource.FOREST]
    natural_year = source_to_year[ConversionSource.NATURAL_GRASSLAND]
    pasture_year = source_to_year[ConversionSource.PASTURE]

    from_forest = forest_year > 0
    # NB: one class a year, so the two departures never share a year: a tie is two zeros
    from_natural = ~from_forest & (natural_year > pasture_year)
    from_pasture = ~from_forest & (pasture_year > natural_year)
    source = (
        from_forest * numpy.uint8(ConversionSource.FOREST)
        + from_natural * numpy.uint8(ConversionSource.NATURAL_GRASSLAND)
        + from_pasture * numpy.uint8(ConversionSource.PASTURE)
    ).astype(numpy.uint8)
    year = xarray.where(
        from_forest, forest_year, numpy.maximum(natural_year, pasture_year)
    )
    return Event(source=source, year=year)


# --- The rule in force -----------------------------------------------------------------------

Rule = collections.abc.Callable[[Candidates, int, int], Event]
NAME_TO_RULE: dict[str, Rule] = {"materiality": materiality, "recency": recency}
# NB: to try another rule, change this line
DEFAULT_RULE = "recency"


def get_cie(
    dset: xarray.Dataset,
    reference_year: int,
    rule: str = DEFAULT_RULE,
    lookback_years: int = emit.LOOKBACK_YEARS,
) -> xarray.Dataset:
    """The `cie-` bands `derive.read_cie` reads, for `reference_year` under `rule`.

    The pools follow `emit.get_crop_independent_emissions`, gated on the selected event rather
    than on `emit`'s own: vegetation by the source's stock, mineral soil off peat, the peat
    pulse on peat. The bands that need no event are `emit`'s, copied.
    """
    assert FIRST_CANDIDATE_YEAR <= reference_year - lookback_years + 1, reference_year
    assert reference_year <= LAST_CANDIDATE_YEAR, reference_year
    candidates = read_candidates(dset)
    event = NAME_TO_RULE[rule](candidates, reference_year, lookback_years)

    has_source = event.source != ConversionSource.NONE
    from_grassland = event.source.isin(list(GRASSLAND_SOURCES))
    return xarray.Dataset(
        {
            "cie-conversion-source": event.source,
            "cie-conversion-year": event.year,
            "cie-vegetation-emissions-undiscounted:tco2e-per-ha": (
                candidates.forest_biomass.where(
                    event.source == ConversionSource.FOREST, other=0
                )
                + candidates.grassland_biomass.where(from_grassland, other=0)
            ),
            "cie-mineral-soil-carbon-at-risk:tco2e-per-ha": (
                candidates.mineral_soil_carbon.where(
                    has_source & ~candidates.is_peat, other=0
                )
            ),
            "cie-peat-transformation-emissions-undiscounted:tco2e-per-ha": (
                emit.PEATLAND_EMISSIONS_PULSE_TCO2E_PER_HA
                * (has_source & candidates.is_peat).astype(numpy.float32)
            ),
        }
        | {
            name: dset[name]
            for name in (
                "cie-peat-occupation-emissions:tco2e-per-ha-per-year",
                "cie-climate-zone",
                "cie-hectares-per-pixel:ha",
            )
        }
    )
