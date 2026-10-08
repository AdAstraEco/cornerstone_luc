"""Turn a ``RunSpec`` into the Jobs it needs (``plan``, no cluster access), and run them in order
against any ``ClusterAPI`` (``execute``)."""

import collections.abc
import dataclasses
import math
import re
import time
import typing

from kuberjobtower import manifest, preflight
from kuberjobtower.collect import verdicts
from kuberjobtower.models import (
    Aoi,
    JobSpec,
    JobState,
    PhasePlan,
    PodEvent,
    PodState,
    ResourceSpec,
    RunPlan,
    RunSpec,
)
from kuberjobtower.phases import DEFAULT_SPECS, Phase, PoolRole, SecretRole
from kuberjobtower.settings import Settings

COMMAND = ("python", "infra/run_phase.py")
MANAGED_BY = "kuber-job-tower"
MAX_NAME = 63
# How long the run waits past a phase's own worst case before giving up on it.
BARRIER_SLACK_S = 900
RUN_ID = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,14}[a-z0-9])?")
CPU = re.compile(r"\d+(?:\.\d+)?m?")
MEMORY = re.compile(r"\d+(?:\.\d+)?(?:Ki|Mi|Gi|Ti)")
# Overridable field -> its value pattern (None: any text, checked where it is used).
FIELDS: dict[str, re.Pattern[str] | None] = {
    "cpu": CPU,
    "cpu-limit": CPU,
    "memory": MEMORY,
    "memory-request": MEMORY,
    "localtmp": MEMORY,
    "pool": None,
    "secret": None,
    "deadline": None,
    "retries": None,
}


def parse_duration(text: str) -> int:
    match = re.fullmatch(r"(\d+)([smh]?)", text)
    if not match:
        raise ValueError(f"{text!r} is not a duration like 90m, 4h or 3600")
    return int(match[1]) * {"": 1, "s": 1, "m": 60, "h": 3600}[match[2]]


def parse_overrides(sets: collections.abc.Iterable[str]) -> dict[Phase, dict[str, str]]:
    """``compute.memory=48Gi`` -> ``{COMPUTE: {"memory": "48Gi"}}``, validated."""
    out: dict[Phase, dict[str, str]] = {}
    for text in sets:
        key, _, value = text.partition("=")
        phase_name, _, field = key.partition(".")
        if not value or not field:
            raise ValueError(f"expected PHASE.FIELD=VALUE, got {text!r}")
        try:
            phase = Phase(phase_name)
        except ValueError:
            raise ValueError(f"unknown phase {phase_name!r} in {text!r}") from None
        if field not in FIELDS:
            raise ValueError(
                f"unknown field {field!r} in {text!r}; one of {', '.join(FIELDS)}"
            )
        pattern = FIELDS[field]
        if pattern and not pattern.fullmatch(value):
            raise ValueError(f"{field}={value!r} is not a valid quantity")
        out.setdefault(phase, {})[field] = value
    return out


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


def phase_args(
    spec: RunSpec, phase: Phase, tiles: tuple[str, ...] = ()
) -> tuple[str, ...]:
    """The ``infra/run_phase.py`` argv. ``--methodology-name`` goes to every phase, identically:
    it picks the datasets ingest fetches and is a cache-key argument of the attribute legs, so
    compute and reduce disagreeing would silently recompute the AOI in the reduce pod."""
    args = ["--phase", str(phase), "--methodology-name", spec.methodology]
    if phase in (Phase.INGEST_WORLD, Phase.INGEST_TILES):
        args += ["--concurrency", str(spec.ingest_concurrency)]
    if phase == Phase.COMPUTE and spec.skip_ingest:
        args.append("--skip-ingest")
    if tiles and phase.is_per_tile:
        args += ["--tile-ids", ",".join(tiles)]
    if (
        phase != Phase.INGEST_WORLD
    ):  # ingest-world writes the boundaries; it needs no AOI
        args += list(spec.aoi.iso_3166s)
    return tuple(args)


def job_for(
    settings: Settings,
    spec: RunSpec,
    phase: Phase,
    tiles: tuple[str, ...] | None = None,
) -> JobSpec:
    """``tiles`` are those resolved from the countries during a run; the Job names and labels
    keep coming from ``spec``, so every phase of one run is named alike."""
    base = DEFAULT_SPECS[phase]
    over = spec.overrides.get(phase, {})
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
    tiles = (spec.aoi.tiles if tiles is None else tiles) or ()
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
        tiles=tiles if phase.is_per_tile else (),
        command=COMMAND,
        args=phase_args(spec, phase, tiles),
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
        run_uid=spec.run_uid,
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


class RunError(RuntimeError):
    pass


def lazy_tiles(
    settings: Settings,
    spec: RunSpec,
    tiles_for: collections.abc.Callable[[tuple[str, ...]], tuple[str, ...]],
    *,
    allow_foreign_pool: bool = False,
    i_know: bool = False,
    out: collections.abc.Callable[[str], None] = print,
) -> collections.abc.Callable[[PhasePlan], PhasePlan]:
    """Plan a per-tile phase that ``plan`` could not, once the countries' tiles can be read.

    The tiles are looked up on the first call and kept, so every phase of the run gets the same
    list. The checks ``plan`` runs on an explicit tile list run here on the resolved one.
    """
    resolved: tuple[str, ...] | None = None

    def resolve(pp: PhasePlan) -> PhasePlan:
        nonlocal resolved
        if resolved is None:
            resolved = tuple(
                sorted(set(tiles_for(spec.aoi.iso_3166s)))
            )  # pod i runs tiles[i]
            if not resolved:
                raise RunError(
                    f"countries {', '.join(spec.aoi.iso_3166s)} touch no tile of the grid"
                )
            out(
                f"countries {', '.join(spec.aoi.iso_3166s)} -> {len(resolved)} tile(s): {', '.join(resolved)}"
            )
        job = job_for(settings, spec, pp.phase, resolved)
        checks = preflight.check(
            settings,
            dataclasses.replace(spec, aoi=Aoi(resolved, spec.aoi.iso_3166s)),
            [job],
            allow_foreign_pool=allow_foreign_pool,
        )
        errors = [c.message for c in checks if c.severity == "error"]
        if errors:
            raise RunError("; ".join(errors))
        if len(resolved) >= settings.confirm_tiles and not i_know:
            raise RunError(
                f"{len(resolved)} tiles is at or above KJT_CONFIRM_TILES={settings.confirm_tiles}: "
                "ingest-world has run, resubmit with --i-know to go on"
            )
        return PhasePlan(pp.phase, job)

    return resolve


def wait_job(
    cluster: ClusterAPI,
    name: str,
    *,
    timeout_s: float,
    poll_s: float = 15,
    out: collections.abc.Callable[[str], None] = print,
    sleep: collections.abc.Callable[[float], None] = time.sleep,
    clock: collections.abc.Callable[[], float] = time.monotonic,
) -> JobState:
    """Poll until the Job is complete or failed, printing a line whenever its counts change."""
    deadline = clock() + timeout_s
    last: tuple[object, ...] | None = None
    while True:
        job = cluster.get_job(name)
        if job is None:
            raise RunError(
                f"{name} disappeared while waiting (deleted by someone else?)"
            )
        now = (job.state, job.active, job.succeeded, job.failed)
        if now != last:
            out(
                f"  {job.name}: {job.state}  active {job.active}  "
                f"succeeded {job.succeeded}/{job.completions}  failed {job.failed}"
            )
            last = now
        # Some clusters lag on the final condition; counts settle it too.
        if job.state in ("complete", "failed") or job.succeeded >= job.completions > 0:
            return job
        if clock() > deadline:
            raise RunError(
                f"{name} still {job.state} after {timeout_s:.0f}s; the Job is untouched"
            )
        sleep(poll_s)


class ClusterAPI(typing.Protocol):
    def create(self, job: typing.Any, *, dry_run: bool = False) -> None: ...
    def get_job(self, name: str) -> JobState | None: ...
    def pods(self, run_id: str, phase: str | None = None) -> list[PodState]: ...
    def events(self, run_id: str) -> list[PodEvent]: ...


def _safe_events(cluster: ClusterAPI, run_id: str) -> list[PodEvent]:
    try:
        return cluster.events(run_id)
    except Exception:  # events are context for the failure, never a reason to hide it
        return []


def execute(
    plan: RunPlan,
    cluster: ClusterAPI,
    *,
    poll_s: float = 15,
    out: collections.abc.Callable[[str], None] = print,
    sleep: collections.abc.Callable[[float], None] = time.sleep,
    clock: collections.abc.Callable[[], float] = time.monotonic,
    after_phase: collections.abc.Callable[[PhasePlan, JobState], None] | None = None,
    resolve: collections.abc.Callable[[PhasePlan], PhasePlan] | None = None,
) -> bool:
    """Create each phase's Job in order and wait for it: the barrier. Resumable by run id.

    A Job that already exists with the same spec hash is adopted (a finished phase is skipped,
    a running one is waited for); with a different hash the run id was reused for a different
    run, which stops here rather than guess. ``after_phase`` runs once a phase has ended,
    pass or fail, while its pods still exist (to archive their logs); it never fails the run.
    ``resolve`` plans a phase whose tiles were not known at submit time (see ``lazy_tiles``).
    Returns False when a phase failed.
    """
    for pp in plan.phases:
        if pp.job is None and resolve is not None:
            pp = resolve(pp)
        if pp.job is None:
            raise RunError(f"{pp.phase}: {pp.note}; give --tile for a real run")
        job = manifest.build_job(pp.job)
        assert job.metadata and job.metadata.annotations
        want = job.metadata.annotations[manifest.SPEC_HASH]
        existing = cluster.get_job(pp.job.name)
        if existing is None:
            cluster.create(job)
            out(f"{pp.phase}: created {pp.job.name} ({pp.job.completions} pod(s))")
        elif existing.spec_hash != want:
            raise RunError(
                f"{pp.job.name} exists with a different spec; use a new --run-id, or "
                f"`cleanup {plan.spec.run_id}` first"
            )
        else:
            out(f"{pp.phase}: adopting {pp.job.name} ({existing.state})")
        waves = math.ceil(pp.job.completions / pp.job.parallelism)
        final = wait_job(
            cluster,
            pp.job.name,
            timeout_s=pp.job.deadline_s * waves + BARRIER_SLACK_S,
            poll_s=poll_s,
            out=out,
            sleep=sleep,
            clock=clock,
        )
        if after_phase is not None:
            try:
                after_phase(pp, final)
            except Exception as exc:
                out(f"  (after-phase hook failed, the run goes on: {exc})")
        if final.state == "failed" or final.failed_indexes:
            out(f"{pp.phase}: FAILED (failed indexes: {final.failed_indexes or 'all'})")
            events = _safe_events(cluster, plan.spec.run_id)
            for pod in cluster.pods(plan.spec.run_id, str(pp.phase)):
                out(
                    f"  pod {pod.name} index {pod.index} {pod.phase} exit {pod.exit_code} {pod.reason or ''}"
                )
                for v in verdicts.pod_verdicts(pod, events):
                    out(f"    {v.severity.upper()} {v.code}: {v.message}")
                warnings = [
                    e for e in events if e.pod == pod.name and e.type == "Warning"
                ]
                # scheduling hiccups are routine; show them only when nothing else was reported
                for event in (
                    [e for e in warnings if e.reason != "FailedScheduling"] or warnings
                )[-3:]:
                    out(f"    event {event.reason}: {event.message[:200]}")
            for v in verdicts.job_verdicts(final):
                out(f"  {v.severity.upper()} {v.code}: {v.message}")
            return False
        out(f"{pp.phase}: done")
    return True
