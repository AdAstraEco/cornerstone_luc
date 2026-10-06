"""Plain frozen dataclasses: what a run is asked to do, and the fully resolved Job it becomes."""

import collections.abc
import dataclasses
import typing

from kuberjobtower.phases import Phase


@dataclasses.dataclass(frozen=True)
class Aoi:
    """Tiles are always sorted: pod ``i`` of a per-tile Job runs ``tiles[i]``."""

    tiles: tuple[str, ...] | None  # None: resolve from the countries after ingest-world
    iso_3166s: tuple[str, ...]

    def __post_init__(self) -> None:
        # The sorted order is the index space of every per-tile Job, so it cannot be a caller's job.
        if self.tiles is not None:
            object.__setattr__(self, "tiles", tuple(sorted(set(self.tiles))))
        object.__setattr__(self, "iso_3166s", tuple(sorted(set(self.iso_3166s))))


@dataclasses.dataclass(frozen=True)
class RunSpec:
    run_id: str
    aoi: Aoi
    methodology: str
    phases: tuple[Phase, ...]
    parallelism: int = 8
    ingest_concurrency: int = 4
    skip_ingest: bool = (
        True  # compute only; False also gives compute the credentials Secret
    )
    # Phase -> field -> value, e.g. {COMPUTE: {"memory": "48Gi", "pool": "..."}}
    overrides: collections.abc.Mapping[Phase, collections.abc.Mapping[str, str]] = (
        dataclasses.field(default_factory=dict)
    )
    image: str | None = None
    ttl_s: int = 1800
    fail_fast: bool = False


@dataclasses.dataclass(frozen=True)
class ResourceSpec:
    cpu_request: str
    cpu_limit: str
    memory_request: str
    memory_limit: str
    localtmp: str


@dataclasses.dataclass(frozen=True)
class JobSpec:
    """No settings lookups left: ``manifest.build_job`` is a pure function of this."""

    name: str
    namespace: str
    phase: Phase
    run_id: str
    completions: int
    parallelism: int
    tiles: tuple[str, ...]
    command: tuple[str, ...]  # exec form, never a shell
    args: tuple[str, ...]
    resources: ResourceSpec
    node_pool: str
    secret: str
    service_account: str
    image: str
    ttl_s: int
    deadline_s: int
    max_failed_indexes: int
    retries_per_index: int
    fail_index_on_oom: bool
    labels: collections.abc.Mapping[str, str]


Severity = typing.Literal["error", "warning"]


@dataclasses.dataclass(frozen=True)
class Check:
    name: str
    severity: Severity
    message: str


@dataclasses.dataclass(frozen=True)
class PhasePlan:
    phase: Phase
    job: JobSpec | None  # None: not plannable yet, see note
    note: str | None = None


@dataclasses.dataclass(frozen=True)
class RunPlan:
    spec: RunSpec
    phases: tuple[PhasePlan, ...]
    checks: tuple[Check, ...]

    @property
    def errors(self) -> tuple[Check, ...]:
        return tuple(c for c in self.checks if c.severity == "error")
