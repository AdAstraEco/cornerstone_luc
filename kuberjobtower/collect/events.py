"""Kubernetes events about a run's pods, kept after Kubernetes forgets them (it keeps about an hour).

They are what tells an OOM kill from an eviction ("The node was low on resource: memory ... using
59534476Ki, request is 48Gi"), and how long a pod waited for a node. Sources, in order: the
cluster, our archive, Cloud Logging (GKE's event exporter keeps them 30 days).
"""

import collections.abc
import datetime
import json
import re
import typing

from kuberjobtower.collect import logs
from kuberjobtower.models import PodEvent


class EventSource(typing.Protocol):
    def events(self, run_id: str) -> list[PodEvent]: ...


def _key(e: PodEvent) -> tuple[str, str, str, str]:
    return (e.time.isoformat(), e.reason, e.pod, e.message)


def to_json(events: collections.abc.Iterable[PodEvent]) -> str:
    return json.dumps(
        [
            {"time": e.time.isoformat(), "reason": e.reason, "type": e.type, "pod": e.pod, "message": e.message}
            for e in events
        ],
        indent=0,
    )


def from_json(text: str) -> list[PodEvent]:
    return [
        PodEvent(
            time=datetime.datetime.fromisoformat(d["time"]),
            reason=d["reason"],
            type=d["type"],
            pod=d["pod"],
            message=d["message"],
        )
        for d in json.loads(text)
    ]


def archive_uri(root: str, run_id: str) -> str:
    return f"{root.rstrip('/')}/runs/{run_id}/events.json"


def read_archive(root: str, run_id: str) -> list[PodEvent]:
    import fsspec  # type: ignore[import-untyped]

    fs, path = fsspec.url_to_fs(archive_uri(root, run_id))
    if not fs.exists(path):
        return []
    with fs.open(path, "r") as handle:
        return from_json(handle.read())


def archive(root: str, run_id: str, events: collections.abc.Sequence[PodEvent]) -> int:
    """Merge ``events`` into the run's stored list (a phase end sees only the live hour)."""
    import fsspec

    merged = {_key(e): e for e in [*read_archive(root, run_id), *events]}
    ordered = sorted(merged.values(), key=lambda e: e.time)
    fs, path = fsspec.url_to_fs(archive_uri(root, run_id))
    fs.makedirs(path.rsplit("/", 1)[0], exist_ok=True)
    with fs.open(path, "w") as handle:
        handle.write(to_json(ordered))
    return len(ordered)


def cloud_events(
    project: str, namespace: str, run_id: str, *, session: typing.Any = None
) -> list[PodEvent]:
    for value in (namespace, run_id):
        if not logs.SAFE.fullmatch(value):
            raise ValueError(f"{value!r} cannot go into a log filter")
    filter_ = (
        'resource.type="k8s_pod" logName:"events" '
        f'resource.labels.namespace_name="{namespace}" '
        f'jsonPayload.involvedObject.name:"-{run_id}-"'
    )
    mine = re.compile(rf"-{re.escape(run_id)}-\d+-[a-z0-9]+$")
    found = []
    for entry in logs.cloud_entries(project, filter_, session=session):
        j = entry.get("jsonPayload", {})
        involved = j.get("involvedObject", {})
        if involved.get("kind") != "Pod" or not mine.search(involved.get("name", "")):
            continue
        stamp = j.get("lastTimestamp") or j.get("firstTimestamp") or entry["timestamp"]
        found.append(
            PodEvent(
                time=datetime.datetime.fromisoformat(stamp.replace("Z", "+00:00")),
                reason=j.get("reason", "?"),
                type=j.get("type", "Normal"),
                pod=involved["name"],
                message=j.get("message", ""),
            )
        )
    return sorted(found, key=lambda e: e.time)


def events_for(
    *,
    cluster: EventSource,
    project: str,
    namespace: str,
    archive_root: str,
    run_id: str,
    source: str = "auto",
    cloud_session: typing.Any = None,
) -> tuple[str, list[PodEvent]]:
    """The first source that has events for the run, as (source name, events)."""
    if source in ("auto", "pod") and (found := cluster.events(run_id)):
        return "cluster", found
    if source in ("auto", "archive") and (found := read_archive(archive_root, run_id)):
        return "archive", found
    if source in ("auto", "cloud") and (
        found := cloud_events(project, namespace, run_id, session=cloud_session)
    ):
        return "cloud", found
    raise logs.LogsUnavailable(f"no events found for run {run_id} (tried {source})")
