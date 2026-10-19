"""A deliverable: a named choice of a step's variables, written per tile by `write_tile`.

A step's dataset can carry dims beyond the pixel grid (a crop class, a discounting scheme).
A deliverable keeps only the values it names on each, in the order it names them, so only
those are computed; `export.write_tile` lays out what is left as COG bands or a zarr.

    CIE_EMISSIONS = Deliverable(name="cie-emissions", variables=("cie-climate-zone", ...))
    write_deliverable(
        dset=emit.workflow(tile_id=tile_id),
        deliverable=CIE_EMISSIONS,
        format=export.Format.COG,
        source_name="jdluc-emit",
        tile_id=tile_id,
    )
"""

import collections.abc
import dataclasses

import xarray

from jdluc import export


@dataclasses.dataclass(frozen=True)
class Deliverable:
    name: str
    variables: tuple[str, ...]
    # The values to keep, in order, on each dim they are named for; other dims are kept whole
    select: collections.abc.Mapping[str, tuple[str, ...]] = dataclasses.field(
        default_factory=dict
    )


def select(dset: xarray.Dataset, deliverable: Deliverable) -> xarray.Dataset:
    """`dset` cut to the deliverable's selected values; lazy."""
    if unknown := sorted(set(deliverable.select) - set(map(str, dset.dims))):
        raise ValueError(f"{deliverable.name!r} selects on {unknown}, not dims of dset")
    return dset.sel({dim: list(values) for dim, values in deliverable.select.items()})


def write_deliverable(
    dset: xarray.Dataset,
    deliverable: Deliverable,
    format: export.Format,
    source_name: str,
    tile_id: str,
    output_root: str | None = None,
    version: str = "",
) -> str:
    """Write `deliverable` of the (lazy) `dset` for one tile; return the URI."""
    return export.write_tile(
        dset=select(dset=dset, deliverable=deliverable),
        format=format,
        name=deliverable.name,
        output_root=output_root,
        source_name=source_name,
        tile_id=tile_id,
        variables=deliverable.variables,
        version=version,
    )
