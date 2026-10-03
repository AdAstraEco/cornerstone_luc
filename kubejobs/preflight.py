"""Offline pre-flight checks: everything that can be decided from the settings and the plan.

Errors block a submit; warnings need confirmation. The checks that need the cluster (does the
request fit a node's allocatable memory, what else runs on the pool, do the Secrets exist) arrive
with ``cluster.py``.
"""

import collections.abc

from kubejobs.models import Check, JobSpec, RunSpec
from kubejobs.phases import Phase
from kubejobs.settings import Settings

NEEDS_COUNTRIES = (Phase.COMPUTE, Phase.REDUCE, Phase.MOSAIC)


def check(
    settings: Settings,
    spec: RunSpec,
    jobs: collections.abc.Sequence[JobSpec],
    *,
    allow_foreign_pool: bool = False,
) -> tuple[Check, ...]:
    out: list[Check] = []

    if spec.parallelism > settings.max_parallelism:
        out.append(
            Check(
                "parallelism",
                "error",
                f"parallelism {spec.parallelism} exceeds KUBEJOBS_MAX_PARALLELISM={settings.max_parallelism}",
            )
        )
    tiles = spec.aoi.tiles or ()
    if len(tiles) > settings.max_tiles:
        out.append(
            Check(
                "tiles",
                "error",
                f"{len(tiles)} tiles exceed KUBEJOBS_MAX_TILES={settings.max_tiles}",
            )
        )
    elif len(tiles) >= settings.confirm_tiles:
        out.append(
            Check(
                "tiles",
                "warning",
                f"{len(tiles)} tiles is at or above KUBEJOBS_CONFIRM_TILES="
                f"{settings.confirm_tiles}: needs --i-know",
            )
        )
    if not spec.aoi.iso_3166s:
        needing = [p for p in spec.phases if p in NEEDS_COUNTRIES]
        if needing:
            out.append(
                Check(
                    "countries",
                    "error",
                    f"{', '.join(map(str, needing))} need a country (--country): the attribute leg is per tile and country",
                )
            )
    if not spec.aoi.tiles and not spec.aoi.iso_3166s:
        out.append(Check("aoi", "error", "give --tile or --country"))

    for job in jobs:
        if job.completions > settings.max_completions_per_job:
            out.append(
                Check(
                    "completions",
                    "error",
                    f"{job.phase}: {job.completions} completions exceed "
                    f"KUBEJOBS_MAX_COMPLETIONS_PER_JOB={settings.max_completions_per_job} (splitting is not built yet)",
                )
            )
        if not settings.pool_allowed(job.node_pool):
            out.append(
                Check(
                    "pool",
                    "warning" if allow_foreign_pool else "error",
                    f"{job.phase}: pool {job.node_pool!r} does not match KUBEJOBS_ALLOWED_POOL_REGEX"
                    + (
                        ""
                        if allow_foreign_pool
                        else " (--allow-foreign-pool overrides)"
                    ),
                )
            )
    return tuple(out)
