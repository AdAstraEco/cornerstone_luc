"""The only module that talks to the cluster. Everything else sees ``JobState`` / ``PodState``."""

import collections.abc

from lightkube import ApiError, Client, KubeConfig
from lightkube.core.exceptions import ConfigError
from lightkube.resources.batch_v1 import Job
from lightkube.resources.core_v1 import Pod

from kubejobs import manifest
from kubejobs.models import JobState, PodState
from kubejobs.settings import Settings

RUN_LABEL = "run-id"
MANAGED = "app.kubernetes.io/managed-by=cornerstone-kubejobs"
INDEX_LABEL = "batch.kubernetes.io/job-completion-index"


class ClusterError(RuntimeError):
    pass


class Cluster:
    """A namespaced client on the one kubectl context the settings allow.

    The context is chosen explicitly from ``KUBEJOBS_KUBE_CONTEXT`` rather than whatever is
    active, so a different active context can never redirect a write.
    """

    def __init__(self, settings: Settings) -> None:
        try:
            config = KubeConfig.from_env().get(context_name=settings.kube_context)
        except ConfigError as exc:
            raise ClusterError(f"kubeconfig: {exc}") from exc
        if config is None:
            raise ClusterError(f"no kubeconfig context named {settings.kube_context!r}")
        self.namespace = settings.namespace
        self._client = Client(config=config, namespace=settings.namespace)

    def create(self, job: Job, *, dry_run: bool = False) -> None:
        try:
            self._client.create(job, dry_run=dry_run)
        except ApiError as exc:
            raise ClusterError(f"create {job.metadata.name if job.metadata else '?'}: {exc}") from exc

    def get_job(self, name: str) -> JobState | None:
        try:
            return _job_state(self._client.get(Job, name))
        except ApiError as exc:
            if exc.status.code == 404:
                return None
            raise ClusterError(f"get {name}: {exc}") from exc

    def jobs(self, run_id: str | None = None) -> list[JobState]:
        found = self._client.list(Job, labels=_labels(run_id))
        return sorted((_job_state(j) for j in found), key=lambda j: j.name)

    def pods(self, run_id: str, phase: str | None = None) -> list[PodState]:
        labels = _labels(run_id) | ({"phase": phase} if phase else {})
        return sorted(
            (_pod_state(p) for p in self._client.list(Pod, labels=labels)),
            key=lambda p: (p.name if p.index is None else f"{p.index:06d}"),
        )

    def logs(self, pod: str, *, tail: int | None = None) -> collections.abc.Iterator[str]:
        try:
            yield from self._client.log(pod, tail_lines=tail, newlines=False)
        except ApiError as exc:
            raise ClusterError(f"logs {pod}: {exc}") from exc

    def delete_run(self, run_id: str) -> list[str]:
        """Delete this run's Jobs (and, by cascade, their pods). Only objects we created."""
        names = [j.name for j in self.jobs(run_id)]
        for name in names:
            self._client.delete(Job, name, cascade="Background")  # type: ignore[arg-type]
        return names


def _labels(run_id: str | None) -> dict[str, str]:
    labels = {"app": "cornerstone", "app.kubernetes.io/managed-by": "cornerstone-kubejobs"}
    return labels | ({RUN_LABEL: run_id} if run_id else {})


def _job_state(job: Job) -> JobState:
    meta, spec, status = job.metadata, job.spec, job.status
    assert meta and meta.name and spec
    return JobState(
        name=meta.name,
        phase=(meta.labels or {}).get("phase", "?"),
        run_id=(meta.labels or {}).get(RUN_LABEL, "?"),
        completions=spec.completions or 0,
        active=(status.active if status else 0) or 0,
        succeeded=(status.succeeded if status else 0) or 0,
        failed=(status.failed if status else 0) or 0,
        failed_indexes=(status.failedIndexes if status else "") or "",
        spec_hash=(meta.annotations or {}).get(manifest.SPEC_HASH),
        conditions=frozenset(
            c.type for c in (status.conditions if status else None) or [] if c.status == "True"
        ),
    )


def _pod_state(pod: Pod) -> PodState:
    meta, spec, status = pod.metadata, pod.spec, pod.status
    assert meta and meta.name
    terminated = None
    for cs in (status.containerStatuses if status else None) or []:
        if cs.state and cs.state.terminated:
            terminated = cs.state.terminated
    index = (meta.labels or {}).get(INDEX_LABEL)
    return PodState(
        name=meta.name,
        index=int(index) if index is not None else None,
        phase=(status.phase if status else None) or "?",
        node=spec.nodeName if spec else None,
        reason=(terminated.reason if terminated else None) or (status.reason if status else None),
        exit_code=terminated.exitCode if terminated else None,
        started=status.startTime if status else None,  # type: ignore[arg-type]
    )
