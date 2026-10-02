"""Turn a ``RunSpec`` into the Jobs it needs: names, arguments, sizes, pools. No cluster access."""

import collections.abc
import re

from controlplane import preflight
from controlplane.models import (
    JobSpec,
    PhasePlan,
    ResourceSpec,
    RunPlan,
    RunSpec,
)
from controlplane.phases import DEFAULT_SPECS, Phase, PoolRole, SecretRole
from controlplane.settings import Settings

COMMAND = ("python", "infra/run_phase.py")
MANAGED_BY = "cornerstone-controlplane"
MAX_NAME = 63
RUN_ID = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,14}[a-z0-9])?")
CPU = re.compile(r"\d+(?:\.\d+)?m?")
MEMORY = re.compile(r"\d+(?:\.\d+)?(?:Ki|Mi|Gi|Ti)")
FIELDS = (
    "cpu",
    "cpu-limit",
    "memory",
    "memory-request",
    "localtmp",
    "pool",
    "secret",
    "deadline",
    "retries",
)


def parse_duration(text: str) -> int:
    match = re.fullmatch(r"(\d+)([smh]?)", text)
    if not match:
        raise ValueError(f"{text!r} is not a duration like 90m, 4h or 3600")
    return int(match[1]) * {"": 1, "s": 1, "m": 60, "h": 3600}[match[2]]


def parse_overrides(
    pools: collections.abc.Iterable[str], sets: collections.abc.Iterable[str]
) -> dict[Phase, dict[str, str]]:
    """``--pool compute=P`` and ``--set compute.memory=48Gi`` -> ``{COMPUTE: {...}}``."""
    out: dict[Phase, dict[str, str]] = {}
    pairs = [("pool", p) for p in pools] + [("set", s) for s in sets]
    for kind, text in pairs:
        key, sep, value = text.partition("=")
        phase_name, dot, field = key.partition(".")
        if kind == "pool":
            phase_name, field = key, "pool"
        elif not dot:
            raise ValueError(f"--set wants PHASE.FIELD=VALUE, got {text!r}")
        if not sep or not value:
            raise ValueError(f"{text!r} has no value")
        try:
            phase = Phase(phase_name)
        except ValueError:
            raise ValueError(f"unknown phase {phase_name!r} in {text!r}") from None
        if field not in FIELDS:
            raise ValueError(
                f"unknown field {field!r} in {text!r}; one of {', '.join(FIELDS)}"
            )
        out.setdefault(phase, {})[field] = value
    return out


def _validated(field: str, value: str) -> str:
    patterns = {
        "cpu": CPU,
        "cpu-limit": CPU,
        "memory": MEMORY,
        "memory-request": MEMORY,
        "localtmp": MEMORY,
    }
    if field in patterns and not patterns[field].fullmatch(value):
        raise ValueError(f"{field}={value!r} is not a valid quantity")
    return value


def job_name(slug: str, phase: Phase, run_id: str) -> str:
    """``cornerstone-{slug}-{phase}-{run_id}``, the slug trimmed so the whole fits 63 chars."""
    fixed = len(f"cornerstone--{phase}-{run_id}")
    trimmed = slug[: max(MAX_NAME - fixed, 1)].rstrip("-")
    return f"cornerstone-{trimmed}-{phase}-{run_id}"


def aoi_slug(spec: RunSpec) -> str:
    def dns(s: str) -> str:
        return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")

    tiles, isos = spec.aoi.tiles, spec.aoi.iso_3166s
    parts = []
    if tiles:
        parts.append(f"t-{dns(tiles[0])}" if len(tiles) == 1 else f"t{len(tiles)}")
    if isos:
        parts.append(
            dns("-".join(isos))
            if len(isos) <= 3
            else dns(f"{isos[0]}-plus{len(isos) - 1}")
        )
    return "-".join(parts)


def phase_args(spec: RunSpec, phase: Phase) -> tuple[str, ...]:
    """The ``infra/run_phase.py`` argv. ``--methodology-name`` goes to every phase, identically:
    it picks the datasets ingest fetches and is a cache-key argument of the attribute legs, so
    compute and reduce disagreeing would silently recompute the AOI in the reduce pod."""
    args = ["--phase", str(phase), "--methodology-name", spec.methodology]
    if phase in (Phase.INGEST_WORLD, Phase.INGEST_TILES):
        args += ["--concurrency", str(spec.ingest_concurrency)]
    if phase == Phase.COMPUTE and spec.skip_ingest:
        args.append("--skip-ingest")
    if spec.aoi.tiles and (phase.is_per_tile or phase == Phase.MOSAIC):
        args += ["--tile-ids", ",".join(spec.aoi.tiles)]
    if (
        phase != Phase.INGEST_WORLD
    ):  # ingest-world writes the boundaries; it needs no AOI
        args += list(spec.aoi.iso_3166s)
    return tuple(args)


def job_for(settings: Settings, spec: RunSpec, phase: Phase) -> JobSpec:
    base = DEFAULT_SPECS[phase]
    over = {k: _validated(k, v) for k, v in spec.overrides.get(phase, {}).items()}
    pools = {PoolRole.HEAVY: settings.pool_heavy, PoolRole.LIGHT: settings.pool_light}
    secret_role = base.secret
    if phase == Phase.COMPUTE and not spec.skip_ingest:
        secret_role = SecretRole.KEYS
    secrets = {
        SecretRole.KEYS: settings.secret,
        SecretRole.NOKEYS: settings.secret_nokeys,
    }
    secret = over.get("secret", secrets[secret_role])
    if secret not in secrets.values():
        raise ValueError(
            f"{phase}: secret {secret!r} is not one of the configured Secrets"
        )
    memory = over.get("memory", base.memory)
    tiles = spec.aoi.tiles or ()
    completions = len(tiles) if phase.is_per_tile else 1
    parallelism = min(spec.parallelism, completions) if phase.is_per_tile else 1
    if phase.is_per_tile and not tiles:
        raise ValueError(f"{phase}: tiles are not resolved yet")
    labels = {
        "app": "cornerstone",
        "app.kubernetes.io/managed-by": MANAGED_BY,
        "phase": str(phase),
        "run-id": spec.run_id,
        "aoi": aoi_slug(spec),
    }
    if completions == 1 and tiles and phase.is_per_tile:
        labels["tile-id"] = tiles[0]
    return JobSpec(
        name=job_name(aoi_slug(spec), phase, spec.run_id),
        namespace=settings.namespace,
        phase=phase,
        run_id=spec.run_id,
        completions=completions,
        parallelism=parallelism,
        tiles=tiles if phase.is_per_tile or phase == Phase.MOSAIC else (),
        command=COMMAND,
        args=phase_args(spec, phase),
        resources=ResourceSpec(
            cpu_request=over.get("cpu", base.cpu_request),
            cpu_limit=over.get("cpu-limit", base.cpu_limit),
            memory_request=over.get("memory-request", memory),
            memory_limit=memory,
            localtmp=over.get("localtmp", base.localtmp),
        ),
        node_pool=over.get("pool", pools[base.pool]),
        secret=secret,
        service_account=settings.service_account,
        image=spec.image or settings.image,
        ttl_s=spec.ttl_s,
        deadline_s=parse_duration(over["deadline"])
        if "deadline" in over
        else base.pod_deadline_s,
        max_failed_indexes=0 if spec.fail_fast else completions,
        retries_per_index=int(over.get("retries", base.retries_per_index)),
        fail_index_on_oom=base.fail_index_on_oom,
        labels=labels,
    )


def plan(
    settings: Settings, spec: RunSpec, *, allow_foreign_pool: bool = False
) -> RunPlan:
    """The dry run: one Job per phase in pipeline order, plus the checks. Never writes."""
    if not RUN_ID.fullmatch(spec.run_id):
        raise ValueError(
            f"run id {spec.run_id!r} must be 1-16 chars of a-z, 0-9 and '-'"
        )
    phases = tuple(p for p in Phase if p in spec.phases)
    plans: list[PhasePlan] = []
    for phase in phases:
        if phase.is_per_tile and not spec.aoi.tiles:
            plans.append(
                PhasePlan(
                    phase, None, "tiles resolve from the countries after ingest-world"
                )
            )
        else:
            plans.append(PhasePlan(phase, job_for(settings, spec, phase)))
    jobs = [p.job for p in plans if p.job]
    return RunPlan(
        spec=spec,
        phases=tuple(plans),
        checks=preflight.check(
            settings, spec, jobs, allow_foreign_pool=allow_foreign_pool
        ),
    )
