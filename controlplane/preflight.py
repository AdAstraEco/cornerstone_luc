"""Offline pre-flight checks: everything that can be decided from the settings and the plan.

Errors block a submit; warnings need confirmation. The checks that need the cluster (does the
request fit a node's allocatable memory, what else runs on the pool, do the Secrets exist) arrive
with ``cluster.py``.
"""

import collections.abc

from controlplane.models import Check, JobSpec, RunSpec
from controlplane.phases import Phase
from controlplane.settings import Settings

NEEDS_COUNTRIES = (Phase.COMPUTE, Phase.REDUCE, Phase.MOSAIC)
# An e2-standard-8 node has 27.6 GiB allocatable (measured); the light pool is that machine today.
LIGHT_NODE_ALLOCATABLE_GIB = 27


def memory_gib(quantity: str) -> float:
    number, unit = quantity[:-2], quantity[-2:]
    return (
        float(number) * {"Ki": 1 / 1024**2, "Mi": 1 / 1024, "Gi": 1, "Ti": 1024}[unit]
    )


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
                f"parallelism {spec.parallelism} exceeds CONTROLPLANE_MAX_PARALLELISM={settings.max_parallelism}",
            )
        )
    tiles = spec.aoi.tiles or ()
    if len(tiles) > settings.max_tiles:
        out.append(
            Check(
                "tiles",
                "error",
                f"{len(tiles)} tiles exceed CONTROLPLANE_MAX_TILES={settings.max_tiles}",
            )
        )
    elif len(tiles) >= settings.confirm_tiles:
        out.append(
            Check(
                "tiles",
                "warning",
                f"{len(tiles)} tiles is at or above CONTROLPLANE_CONFIRM_TILES="
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
                    f"CONTROLPLANE_MAX_COMPLETIONS_PER_JOB={settings.max_completions_per_job} (splitting is not built yet)",
                )
            )
        if not settings.pool_allowed(job.node_pool):
            out.append(
                Check(
                    "pool",
                    "warning" if allow_foreign_pool else "error",
                    f"{job.phase}: pool {job.node_pool!r} does not match CONTROLPLANE_ALLOWED_POOL_REGEX"
                    + (
                        ""
                        if allow_foreign_pool
                        else " (--allow-foreign-pool overrides)"
                    ),
                )
            )
        if (
            job.node_pool == settings.pool_light
            and memory_gib(job.resources.memory_limit) > LIGHT_NODE_ALLOCATABLE_GIB
        ):
            out.append(
                Check(
                    "fit",
                    "warning",
                    f"{job.phase} asks {job.resources.memory_limit} but the light pool's nodes "
                    f"(e2-standard-8) have about {LIGHT_NODE_ALLOCATABLE_GIB} GiB allocatable: "
                    "the pod would stay Pending",
                )
            )
    return tuple(out)
