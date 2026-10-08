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


def check_boundary_config() -> None:
    """Fail now, before any Job exists, if the pipeline's ``.env`` cannot be read for INGEST_ROOT."""
    from jdluc import config

    try:
        config.Config.from_dot_env()
    except (KeyError, OSError) as exc:
        raise ValueError(
            f"a country-only run resolves its tiles on this machine, which needs the pipeline's .env "
            f"(INGEST_ROOT and the other jdluc settings; see .env.example): {exc!r}. Or give --tile"
        ) from exc


def tiles_for_countries(iso_3166s: collections.abc.Iterable[str]) -> tuple[str, ...]:
    """The tiles the countries' boundaries touch, read from the pipeline's INGEST_ROOT (its ``.env``)."""
    from jdluc.datasets import worldbank_jurisdictions

    try:
        found = worldbank_jurisdictions.get_ten_degree_tile_ids_for_iso_3166s(
            iso_3166s=iso_3166s
        )
    except (KeyError, FileNotFoundError) as exc:
        raise ValueError(
            f"could not read the country boundaries ({exc!r}): resolving countries to tiles reads "
            "INGEST_ROOT from the pipeline's .env, and needs ingest-world to have run; "
            "or give --tile"
        ) from exc
    return tuple(sorted(found))


def methodology_names() -> tuple[str, ...]:
    from jdluc import attribute

    return tuple(sorted(m.name for m in attribute.Methodology))
