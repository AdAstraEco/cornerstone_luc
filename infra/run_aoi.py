"""Drive a whole AOI run on GKE: one k8s Indexed Job per phase (docs/scale-out.md).

The driver is deliberately dumb -- submit a phase, wait for it, submit the next:

    ingest-world  ->  ingest-tiles  ->  [barrier]  ->  compute  ->  [barrier]  ->  reduce
    (1 pod)           (1 pod / tile)                   (1 pod / tile)              (1 pod)

Everything else that a workflow engine would give us, the pipeline already has: every
stage is wrapped in ``@storage.cache_to_*``, so retry == rerun and a warm tile is a no-op.
That is why there is no Dask cluster and no Argo controller here (see docs/scale-out.md for
the options weighed, and the *revisit triggers* for when this stops being the right shape).

Fan-out is ``completions``/``parallelism`` on an Indexed Job: each pod maps its
``JOB_COMPLETION_INDEX`` onto the AOI's sorted tile list, recomputed in-pod from the same
country arguments, so no tile list is plumbed through a ConfigMap. The scaling knob stays
"parallelism x node-pool autoscaler". Per-pod work lives in ``infra/run_phase.py``.

Infra identifiers (namespace, image, node pools, secrets) are read from ``infra/cluster.env``
(see ``infra/cluster.env.example``), so nothing cluster-specific is hard-coded here.

    # one-time: copy the template and fill in your cluster's values
    cp infra/cluster.env.example infra/cluster.env    # then edit

    # render every phase's manifest and list the tiles, but submit nothing
    uv run python infra/run_aoi.py HND --dry-run

    # the real thing: all four phases, with barriers between them
    source infra/cluster.env
    uv run python infra/run_aoi.py HND

    # or one phase at a time (the cache makes any phase safe to repeat)
    uv run python infra/run_aoi.py HND --phases compute --parallelism 16

Watch:     kubectl -n "$K8S_NAMESPACE" get jobs,pods -l app=cornerstone -w
Logs:      kubectl -n "$K8S_NAMESPACE" logs -l app=cornerstone --tail=20 -f
Teardown:  kubectl -n "$K8S_NAMESPACE" delete jobs -l app=cornerstone
"""

import argparse
import dataclasses
import datetime
import json
import logging
import math
import os
import pathlib
import re
import string
import subprocess
import time

import run_phase

from jdluc import attribute
from jdluc.datasets import worldbank_jurisdictions

logger = logging.getLogger(__name__)

INFRA_ENV_PATH = pathlib.Path(__file__).parent / "cluster.env"
TEMPLATE_PATH = pathlib.Path(__file__).parent / "k8s" / "phase-job.yaml"

# Job-template placeholder -> the cluster.env / environment variable that fills it. These
# are needed for every phase; the per-phase node pool and secret are resolved separately.
TEMPLATE_TO_INFRA_VAR = {
    "NAMESPACE": "K8S_NAMESPACE",
    "SERVICE_ACCOUNT": "K8S_SERVICE_ACCOUNT",
    "IMAGE": "IMAGE",
}
# Optional per-phase overrides, each falling back to the required base variable.
INFRA_VAR_TO_FALLBACK = {
    "NODE_POOL_INGEST": "NODE_POOL",
    "NODE_POOL_REDUCE": "NODE_POOL",
    "K8S_SECRET_COMPUTE": "K8S_SECRET",
}

DEFAULT_PARALLELISM = 8
# How long the driver waits past a phase's own worst case before giving up on it.
BARRIER_SLACK_SECONDS = 900
POLL_SECONDS = 30


@dataclasses.dataclass(frozen=True)
class PhaseSpec:
    """The pod shape of one phase: where it runs, how big, how long, with which secret.

    ``node_pool_var`` / ``secret_var`` name *optional* cluster.env variables; each falls
    back to the base one (see INFRA_VAR_TO_FALLBACK), so a minimal cluster.env still works
    and the cheap-pool / no-credentials split is opt-in.
    """

    cpu_limit: str
    cpu_request: str
    # Size of the per-pod pd-ssd scratch volume (/localtmp): must hold this phase's peak
    # coexisting local temps. Ingest converts ~8 GB-class COGs (x --concurrency); compute and
    # reduce stream results to GCS and only need room for Dask spill / GDAL cache.
    localtmp_size: str
    memory_limit: str
    memory_request: str
    node_pool_var: str
    pod_deadline_seconds: int
    secret_var: str


PHASE_TO_SPEC = {
    # Network-bound, and every object is one whole-world download (MAPSPAM is the big one).
    run_phase.Phase.INGEST_WORLD: PhaseSpec(
        cpu_limit="4",
        cpu_request="2",
        localtmp_size="100Gi",
        memory_limit="24Gi",
        memory_request="4Gi",
        node_pool_var="NODE_POOL_INGEST",
        pod_deadline_seconds=4 * 3600,
        secret_var="K8S_SECRET",
    ),
    # Also network-bound, but one tile's worth: cheap pool, modest memory. Carries the
    # source-API credentials -- the only phase that needs them. localtmp holds ~8 GB-class
    # COG temps x --concurrency concurrent conversions, with headroom.
    run_phase.Phase.INGEST_TILES: PhaseSpec(
        cpu_limit="4",
        cpu_request="2",
        localtmp_size="100Gi",
        memory_limit="24Gi",
        memory_request="4Gi",
        node_pool_var="NODE_POOL_INGEST",
        pod_deadline_seconds=2 * 3600,
        secret_var="K8S_SECRET",
    ),
    # ~one tile per e2-highmem-8 node (peak ~39 GiB observed), leaving headroom for system
    # pods, and a deadline that tolerates a data-dense farmland tile. Results stream to GCS,
    # so localtmp only needs room for Dask spill / the GDAL block cache.
    run_phase.Phase.COMPUTE: PhaseSpec(
        cpu_limit="8",
        cpu_request="6",
        localtmp_size="20Gi",
        memory_limit="60Gi",
        memory_request="48Gi",
        node_pool_var="NODE_POOL",
        pod_deadline_seconds=6 * 3600,
        secret_var="K8S_SECRET_COMPUTE",
    ),
    # A groupby-sum over kilobyte-scale parquets, plus reading the admin-1 boundary layer.
    run_phase.Phase.REDUCE: PhaseSpec(
        cpu_limit="4",
        cpu_request="2",
        localtmp_size="10Gi",
        memory_limit="32Gi",
        memory_request="8Gi",
        node_pool_var="NODE_POOL_REDUCE",
        pod_deadline_seconds=2 * 3600,
        secret_var="K8S_SECRET_COMPUTE",
    ),
}


def load_infra_env() -> dict[str, str]:
    """Read ``infra/cluster.env`` (``KEY=value`` / ``export KEY=value``), overlaid by os.environ.

    The file is gitignored; the process environment wins, so ``source infra/cluster.env`` (or
    CI-injected vars) overrides it. Returns only the keys the template needs, with each
    optional per-phase override defaulted to its base variable.
    """
    wanted = (
        set(TEMPLATE_TO_INFRA_VAR.values())
        | set(INFRA_VAR_TO_FALLBACK)
        | set(INFRA_VAR_TO_FALLBACK.values())
    )
    values: dict[str, str] = {}
    if INFRA_ENV_PATH.exists():
        for line in INFRA_ENV_PATH.read_text().splitlines():
            line = line.strip().removeprefix("export ").strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, raw = line.partition("=")
            raw = re.sub(
                r"\s+#.*$", "", raw
            )  # strip inline comments (the .example uses them)
            values[key.strip()] = raw.strip().strip("'\"")
    values.update({k: v for k, v in os.environ.items() if k in wanted})
    values = {k: v for k, v in values.items() if k in wanted and v}
    for var, fallback in INFRA_VAR_TO_FALLBACK.items():
        if var not in values and fallback in values:
            values[var] = values[fallback]
    return values


def get_required_infra_vars() -> list[str]:
    return sorted(
        set(TEMPLATE_TO_INFRA_VAR.values())
        | {spec.node_pool_var for spec in PHASE_TO_SPEC.values()}
        | {spec.secret_var for spec in PHASE_TO_SPEC.values()}
    )


def dns_safe(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def aoi_slug(iso_3166s: list[str]) -> str:
    """A short label for the AOI, kept inside the 63-char DNS-1123 budget for Job names."""
    if len(iso_3166s) <= 3:
        return dns_safe("-".join(iso_3166s))
    return dns_safe(f"{iso_3166s[0]}-plus{len(iso_3166s) - 1:d}")


def job_name(iso_3166s: list[str], phase: run_phase.Phase, run_id: str) -> str:
    """e.g. (['HND'], COMPUTE, '09101423') -> 'cornerstone-hnd-compute-09101423'."""
    return f"cornerstone-{aoi_slug(iso_3166s):s}-{phase!s}-{run_id:s}"


def get_phase_args(
    concurrency: int,
    iso_3166s: list[str],
    methodology_name: str,
    phase: run_phase.Phase,
    skip_glad_crop_filter: bool,
) -> list[str]:
    """The ``infra/run_phase.py`` argv for one phase.

    ``--methodology-name`` and ``--skip-glad-crop-filter`` go to *every* phase that has a
    cache key involving them, identically: they select which datasets ingest fetches and
    they are cache-key arguments for the attribute legs, so compute and reduce disagreeing
    would silently recompute the whole AOI in the reduce pod.
    """
    args = ["--phase", str(phase), "--methodology-name", methodology_name]
    if phase in (run_phase.Phase.INGEST_WORLD, run_phase.Phase.INGEST_TILES):
        args += ["--concurrency", str(concurrency)]
    if phase == run_phase.Phase.COMPUTE:
        # The ingest phases have already warmed INGEST_ROOT, so compute never touches a
        # source -- which is what lets its pods run without the API-key Secret.
        args += ["--skip-ingest"]
    if skip_glad_crop_filter and phase in (
        run_phase.Phase.COMPUTE,
        run_phase.Phase.REDUCE,
    ):
        args += ["--skip-glad-crop-filter"]
    return [*args, *iso_3166s]


def render_manifest(
    completions: int,
    infra: dict[str, str],
    parallelism: int,
    phase: run_phase.Phase,
    phase_args: list[str],
    *,
    dry_run: bool,
    iso_3166s: list[str],
    run_id: str,
) -> str:
    spec = PHASE_TO_SPEC[phase]
    mapping = {
        "ARGS": json.dumps(phase_args),
        "COMPLETIONS": str(completions),
        "CPU_LIMIT": spec.cpu_limit,
        "CPU_REQUEST": spec.cpu_request,
        "JOB_NAME": job_name(iso_3166s=iso_3166s, phase=phase, run_id=run_id),
        "LOCALTMP_SIZE": spec.localtmp_size,
        "MEMORY_LIMIT": spec.memory_limit,
        "MEMORY_REQUEST": spec.memory_request,
        "PARALLELISM": str(min(parallelism, completions)),
        "PHASE": str(phase),
        "POD_DEADLINE_SECONDS": str(spec.pod_deadline_seconds),
        "RUN_ID": run_id,
    }
    template_to_var = {
        **TEMPLATE_TO_INFRA_VAR,
        "NODE_POOL": spec.node_pool_var,
        "SECRET_NAME": spec.secret_var,
    }
    for placeholder, infra_var in template_to_var.items():
        value = infra.get(infra_var)
        if value is None:
            # On a dry-run, leave unfilled infra as a visible ${...} placeholder.
            if not dry_run:
                raise KeyError(infra_var)
            value = "${" + infra_var + "}"
        mapping[placeholder] = value
    return string.Template(TEMPLATE_PATH.read_text()).substitute(mapping)


def submit(manifest: str, namespace: str) -> None:
    """Apply via kubectl so we inherit the caller's context/credentials, not a k8s client."""
    subprocess.run(
        ["kubectl", "-n", namespace, "apply", "-f", "-"],
        input=manifest,
        text=True,
        check=True,
    )


def count_indexes(failed_indexes: str) -> int:
    """Count a Job's ``.status.failedIndexes``, which k8s compresses to e.g. ``"0,3-5"``."""
    count = 0
    for part in filter(None, failed_indexes.split(",")):
        first, _, last = part.partition("-")
        count += int(last) - int(first) + 1 if last else 1
    return count


@dataclasses.dataclass(frozen=True)
class JobStatus:
    """The parts of a Job's ``.status`` the barrier reads.

    ``failed`` counts failed *pods* and ``failed_indexes`` failed *indexes* -- they differ
    once ``backoffLimitPerIndex`` retries an index.
    """

    active: int
    failed: int
    failed_indexes: int
    succeeded: int
    true_conditions: frozenset[str]

    @classmethod
    def get(cls, name: str, namespace: str) -> JobStatus:
        completed = subprocess.run(
            ["kubectl", "-n", namespace, "get", "job", name, "-o", "json"],
            capture_output=True,
            check=True,
            text=True,
        )
        status = json.loads(completed.stdout).get("status", {})
        return cls(
            active=int(status.get("active", 0)),
            failed=int(status.get("failed", 0)),
            failed_indexes=count_indexes(str(status.get("failedIndexes", ""))),
            succeeded=int(status.get("succeeded", 0)),
            true_conditions=frozenset(
                str(condition["type"])
                for condition in status.get("conditions", [])
                if condition.get("status") == "True"
            ),
        )


def wait_for_job(completions: int, name: str, namespace: str, timeout: int) -> bool:
    """Block until the Job completes or fails; return whether it completed.

    This is the barrier. Polling rather than ``kubectl wait --for=condition=complete``
    because that hangs until its own timeout when the Job *fails*, and because the poll
    gives us a progress line per interval.
    """
    deadline = time.monotonic() + timeout
    while True:
        status = JobStatus.get(name=name, namespace=namespace)
        logger.info(
            f"{name:s}: {status.succeeded:d}/{completions:d} succeeded, "
            f"{status.failed:d} failed, {status.active:d} active"
        )
        if "Complete" in status.true_conditions:
            return True
        if "Failed" in status.true_conditions:
            logger.error(
                f"{name:s} FAILED; see `kubectl -n {namespace:s} logs job/{name:s}`"
            )
            return False
        if (
            status.failed_indexes
            and status.succeeded + status.failed_indexes >= completions
        ):
            # Every index is terminal, so the Job is finished whatever its conditions say
            # yet. Belt and braces: without this the barrier could sit polling a Job that
            # will never change again, until the driver's own timeout.
            logger.error(f"{name:s}: {status.failed_indexes:d} index(es) failed")
            return False
        if time.monotonic() > deadline:
            logger.error(
                f"{name:s} still running after {timeout:d}s; giving up waiting. The Job is "
                "untouched -- inspect it, then rerun this driver (warm tiles are no-ops)."
            )
            return False
        time.sleep(POLL_SECONDS)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "iso_3166s",
        nargs=argparse.ONE_OR_MORE,
        type=worldbank_jurisdictions.iso_3166_str,
        help="the AOI: cover exactly the tiles these countries' boundaries touch",
    )
    parser.add_argument(
        "--phases",
        nargs=argparse.ONE_OR_MORE,
        choices=[str(p) for p in run_phase.Phase],
        default=[str(p) for p in run_phase.Phase],
        help="run only these phases, in the order given (default: all four, in order)",
    )
    parser.add_argument(
        "--parallelism",
        default=DEFAULT_PARALLELISM,
        type=int,
        help="max pods in flight per per-tile phase (with the autoscaler, THE scaling knob)",
    )
    parser.add_argument(
        "--concurrency",
        default=run_phase.DEFAULT_INGEST_CONCURRENCY,
        type=int,
        help="in-pod ingest threads (ingest phases only)",
    )
    parser.add_argument(
        "--methodology-name",
        choices=sorted(e.name for e in attribute.Methodology),
        default=attribute.Methodology.STATISTICAL.name,
    )
    parser.add_argument("--skip-glad-crop-filter", action="store_true")
    parser.add_argument(
        "--run-id",
        help="suffix that makes this run's Job names unique (default: UTC MMDDHHMM). "
        "Reusing one resumes: an already-Complete phase is applied unchanged and skipped",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="render every phase's manifest and list the tiles, but submit nothing",
    )
    args = parser.parse_args()

    infra = load_infra_env()
    if not args.dry_run:
        missing = [var for var in get_required_infra_vars() if not infra.get(var)]
        if missing:
            parser.error(
                f"missing infra values {missing}; copy infra/cluster.env.example to "
                "infra/cluster.env and fill it in (or `source` it), or pass --dry-run"
            )

    iso_3166s = sorted(args.iso_3166s)
    run_id = dns_safe(
        str(args.run_id)
        if args.run_id
        else datetime.datetime.now(datetime.UTC).strftime("%m%d%H%M")
    )
    tile_ids = run_phase.get_tile_ids(iso_3166s=iso_3166s)
    logger.info(f"AOI {iso_3166s} -> {len(tile_ids):d} tile(s): {tile_ids}")

    namespace = infra.get("K8S_NAMESPACE")
    for phase in map(run_phase.Phase, args.phases):
        completions = len(tile_ids) if phase.is_per_tile else 1
        parallelism = min(int(args.parallelism), completions)
        name = job_name(iso_3166s=iso_3166s, phase=phase, run_id=run_id)
        manifest = render_manifest(
            completions=completions,
            dry_run=args.dry_run,
            infra=infra,
            iso_3166s=iso_3166s,
            parallelism=parallelism,
            phase=phase,
            run_id=run_id,
            phase_args=get_phase_args(
                concurrency=int(args.concurrency),
                iso_3166s=iso_3166s,
                methodology_name=str(args.methodology_name),
                phase=phase,
                skip_glad_crop_filter=args.skip_glad_crop_filter,
            ),
        )
        if args.dry_run:
            print(f"# --- {name} ---\n{manifest}")
            continue

        assert namespace, "K8S_NAMESPACE is required to submit (validated above)"
        submit(manifest=manifest, namespace=namespace)
        logger.info(
            f"submitted {name:s}: {completions:d} index(es), {parallelism:d} at a time"
        )
        # The barrier. Worst case is every batch of `parallelism` pods using its full
        # deadline; past that the driver stops waiting rather than block forever.
        timeout = (
            PHASE_TO_SPEC[phase].pod_deadline_seconds
            * math.ceil(completions / parallelism)
            + BARRIER_SLACK_SECONDS
        )
        if not wait_for_job(
            completions=completions, name=name, namespace=namespace, timeout=timeout
        ):
            logger.error(f"stopping: phase {phase!s} did not complete")
            return 1
        logger.info(f"phase {phase!s} complete")

    if not args.dry_run:
        logger.info(f"AOI {iso_3166s} done ({run_id=:s})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
