"""How kuber-job-tower runs this pipeline: the phases, the pod command and its arguments.

The tower finds ``PIPELINE`` through the entry point ``kuberjobtower.pipelines`` (``pyproject.toml``).
This file imports only ``kuberjobtower.spec`` at module level, so importing it is cheap; ``jdluc``
(geopandas and friends) is imported inside the hooks that need it.

Pod sizes, node pools, Secret names, retries and the image are the tower's settings, not this
file's (see ``infra/kuber-job-tower.md``). The phase names and flags here must match
``infra/run_phase.py``; ``infra/kjt_test.py`` checks the phase names.
"""

import collections.abc

from kuberjobtower.spec import (
    SCRATCH_DIR,
    ItemsNotAvailable,
    OptionDef,
    PhaseContext,
    PhaseDef,
    PipelineSpec,
    SecretMount,
)

# Phases in the order a run goes through them. A per-item phase runs one pod per ten-degree tile.
PHASES = (
    PhaseDef(
        "ingest-world",
        secret_role="keys",
        summary="whole-world datasets (boundaries, climate zones, ...), one pod",
    ),
    PhaseDef(
        "ingest-tiles",
        per_item=True,
        secret_role="keys",
        summary="the ten-degree datasets of each tile",
    ),
    PhaseDef(
        "compute",
        per_item=True,
        needs_countries=True,
        secret_role="nokeys",
        summary="harmonize, emit and attribute, one tile per pod",
    ),
    PhaseDef(
        "reduce",
        needs_countries=True,
        secret_role="nokeys",
        summary="merge the per-tile results for the whole area, one pod",
    ),
    PhaseDef(
        "export",
        per_item=True,
        secret_role="nokeys",
        summary="write each tile's emissions COG",
    ),
    PhaseDef(
        "mosaic",
        needs_countries=True,
        secret_role="nokeys",
        summary="stitch the tile COGs into one VRT, one pod",
    ),
)


def build_args(ctx: PhaseContext) -> tuple[str, ...]:
    """The ``infra/run_phase.py`` argv. ``--methodology-name`` goes to every phase identically: it
    picks the datasets ingest fetches and is a cache-key argument of the attribute legs, so compute
    and reduce disagreeing would silently recompute the whole area inside the reduce pod."""
    opts = ctx.options
    args = ["--phase", ctx.phase.name, "--methodology-name", opts["methodology"]]
    if ctx.phase.name in ("ingest-world", "ingest-tiles"):
        args += ["--concurrency", opts["ingest_concurrency"]]
    if ctx.phase.name == "compute" and opts["skip_ingest"] == "true":
        args.append("--skip-ingest")
    if ctx.items and ctx.phase.per_item:
        args += ["--tile-ids", ",".join(ctx.items)]
    if (
        ctx.phase.name != "ingest-world"
    ):  # it writes the boundaries, so it needs no area
        args += list(ctx.countries)
    return tuple(args)


def secret_for(
    phase: PhaseDef, options: collections.abc.Mapping[str, str]
) -> str | None:
    """Compute only reads a pre-warmed INGEST_ROOT when ``skip_ingest`` is on; otherwise it needs keys."""
    if phase.name == "compute" and options["skip_ingest"] != "true":
        return "keys"
    return phase.secret_role


def validate_items(tiles: collections.abc.Iterable[str]) -> tuple[str, ...]:
    """Sorted, de-duplicated ten-degree tiles of the pipeline's grid (no boundary file is read)."""
    from jdluc import tiling

    result = tuple(sorted(set(tiles)))
    unknown = [t for t in result if t not in tiling.GLOBAL_NATURE_WATCH_TILE_IDS]
    if unknown:
        raise ValueError(f"not tiles of the pipeline's grid: {unknown}")
    return result


def validate_countries(codes: collections.abc.Iterable[str]) -> tuple[str, ...]:
    from jdluc.datasets import worldbank_jurisdictions

    return tuple(sorted({worldbank_jurisdictions.iso_3166_str(c) for c in codes}))


def items_for_countries(countries: tuple[str, ...]) -> tuple[str, ...]:
    """The tiles the countries' boundaries touch. Reads the boundaries from INGEST_ROOT (``.env``),
    which ``ingest-world`` writes, so a country-only run resolves its tiles after that phase."""
    from jdluc.datasets import worldbank_jurisdictions

    try:
        found = worldbank_jurisdictions.get_ten_degree_tile_ids_for_iso_3166s(
            iso_3166s=countries
        )
    except (KeyError, FileNotFoundError) as exc:
        raise ItemsNotAvailable(
            f"could not read the country boundaries ({exc!r}): resolving countries to tiles needs "
            "INGEST_ROOT in the pipeline's .env and ingest-world to have run; or pass tiles with --item"
        ) from exc
    return tuple(sorted(found))


def methodologies() -> tuple[str, ...]:
    from jdluc import attribute

    return tuple(sorted(m.name for m in attribute.Methodology))


PIPELINE = PipelineSpec(
    name="cornerstone",
    title="Cornerstone LUC",
    phases=PHASES,
    command=("python", "infra/run_phase.py"),
    build_args=build_args,
    # LOG_FORMAT=json makes run_phase.py print one JSON object per line (image contract v1);
    # CPL_TMPDIR sends GDAL's temp files to the per-pod scratch volume.
    env={"LOG_FORMAT": "json", "CPL_TMPDIR": SCRATCH_DIR},
    # Config.from_dot_env reads a file, so the Secret is mounted at /app/.env.
    secret_mounts={
        "keys": SecretMount("/app/.env", sub_path=".env"),
        "nokeys": SecretMount("/app/.env", sub_path=".env"),
    },
    secret_for=secret_for,
    options=(
        OptionDef(
            "methodology",
            "attribution methodology",
            "STATISTICAL",
            choices=methodologies,
        ),
        OptionDef(
            "skip_ingest",
            "compute reads a pre-warmed INGEST_ROOT and never touches a source",
            "false",
            choices=("true", "false"),
        ),
        OptionDef("ingest_concurrency", "in-pod ingest threads", "4"),
    ),
    validate_items=validate_items,
    validate_countries=validate_countries,
    items_for_countries=items_for_countries,
    # the pipeline's own names, so runs made before the tower stay recognised
    labels={"app": "cornerstone"},
    name_prefix="cornerstone",
    annotation_prefix="cornerstone.adastra.eco/",
    step_loggers=("__main__", "jdluc.export"),
    item_noun="tile",
)
