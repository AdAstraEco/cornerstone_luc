"""The only module that imports the pipeline: tile and country validation.

Explicit tile ids are checked against the pure tile-id rules, so no boundary file is read.
Imports are inside the functions so ``status``, ``logs`` and
``top`` never pay for geopandas.
"""

import collections.abc


def validate_tiles(tiles: collections.abc.Iterable[str]) -> tuple[str, ...]:
    """Sorted, de-duplicated, each a ten-degree tile of the GFW grid the pipeline covers."""
    from jdluc import tiling

    result = tuple(sorted(set(tiles)))
    unknown = [t for t in result if t not in tiling.GLOBAL_NATURE_WATCH_TILE_IDS]
    if unknown:
        raise ValueError(f"not tiles of the pipeline's grid: {unknown}")
    return result


def validate_countries(iso_3166s: collections.abc.Iterable[str]) -> tuple[str, ...]:
    from jdluc.datasets import worldbank_jurisdictions

    return tuple(sorted({worldbank_jurisdictions.iso_3166_str(c) for c in iso_3166s}))


def methodology_names() -> tuple[str, ...]:
    from jdluc import attribute

    return tuple(sorted(m.name for m in attribute.Methodology))
