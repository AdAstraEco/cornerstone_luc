"""The kubelet's own view of a pod, from Cloud Monitoring (kept about six weeks).

``memory/used_bytes`` with ``memory_type=non-evictable`` is the working set the kubelet compares
with the pod's memory request when it decides to evict; it is the number that matters for
eviction, and it can differ from what the container's own cgroup reports.
"""

import dataclasses
import datetime
import typing

from kuberjobtower.collect import logs

SCOPE = "https://www.googleapis.com/auth/monitoring.read"
URL = "https://monitoring.googleapis.com/v3/projects/{project}/timeSeries"
GIB = 1 << 30


@dataclasses.dataclass(frozen=True)
class KubeletView:
    peak_working_set_gib: float | None  # non-evictable memory
    memory_request_gib: float | None
    memory_limit_gib: float | None
    peak_cpu_cores: float | None

    def line(self) -> str:
        parts = []
        if self.peak_working_set_gib is not None:
            ws = f"peak working set {self.peak_working_set_gib:.1f} GiB"
            if self.memory_request_gib:
                ws += f" of a {self.memory_request_gib:.0f} GiB request ({self.peak_working_set_gib / self.memory_request_gib:.0%})"
            parts.append(ws)
        if self.peak_cpu_cores is not None:
            parts.append(f"peak CPU {self.peak_cpu_cores:.1f} cores")
        return "kubelet view (Cloud Monitoring): " + ", ".join(parts)


def _peaks(
    session: typing.Any,
    project: str,
    metric: str,
    selector: str,
    start: datetime.datetime,
    end: datetime.datetime,
    aligner: str,
) -> dict[str, float]:
    """Peak of each series of ``metric``, keyed by its ``memory_type`` label (or '')."""
    params = {
        "filter": f'metric.type="kubernetes.io/container/{metric}" {selector}',
        "interval.startTime": start.astimezone(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "interval.endTime": end.astimezone(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "aggregation.alignmentPeriod": "60s",
        "aggregation.perSeriesAligner": aligner,
        "view": "FULL",
    }
    response = session.get(URL.format(project=project), params=params, timeout=60)
    if response.status_code != 200:
        raise logs.LogsUnavailable(
            f"Cloud Monitoring answered {response.status_code}: {response.text[:200]}"
        )
    out: dict[str, float] = {}
    for series in response.json().get("timeSeries", []):
        values = [
            float(p["value"].get("doubleValue", p["value"].get("int64Value", 0)))
            for p in series["points"]
        ]
        if values:
            out[series["metric"].get("labels", {}).get("memory_type", "")] = max(values)
    return out


def kubelet_view(
    project: str,
    namespace: str,
    pod: str,
    start: datetime.datetime,
    end: datetime.datetime,
    *,
    container: str = "phase",
    session: typing.Any = None,
) -> KubeletView:
    for value in (namespace, pod, container):
        if not logs.SAFE.fullmatch(value):
            raise ValueError(f"{value!r} cannot go into a monitoring filter")
    if session is None:
        import google.auth
        from google.auth.transport.requests import AuthorizedSession

        credentials, _ = google.auth.default(scopes=[SCOPE])
        session = AuthorizedSession(credentials)
    selector = (
        f'resource.labels.namespace_name="{namespace}" resource.labels.pod_name="{pod}" '
        f'resource.labels.container_name="{container}"'
    )

    def peak(metric: str, aligner: str = "ALIGN_MAX") -> dict[str, float]:
        return _peaks(session, project, metric, selector, start, end, aligner)

    used = peak("memory/used_bytes")
    request = peak("memory/request_bytes").get("")
    limit = peak("memory/limit_bytes").get("")
    cpu = peak("cpu/core_usage_time", "ALIGN_RATE").get("")
    return KubeletView(
        peak_working_set_gib=used["non-evictable"] / GIB if "non-evictable" in used else None,
        memory_request_gib=request / GIB if request else None,
        memory_limit_gib=limit / GIB if limit else None,
        peak_cpu_cores=cpu,
    )
