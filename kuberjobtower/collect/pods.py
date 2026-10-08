"""A phase's pod records (when each pod was created, started and ended, where, and how), archived."""

import collections.abc
import dataclasses
import datetime
import json

from kuberjobtower.collect import verdicts
from kuberjobtower.models import PodEvent, PodState


def record(pod: PodState, events: collections.abc.Sequence[PodEvent]) -> dict[str, object]:
    def seconds(a: datetime.datetime | None, b: datetime.datetime | None) -> float | None:
        return round((b - a).total_seconds(), 1) if a and b else None

    return {
        **dataclasses.asdict(pod),
        "pending_s": seconds(pod.created, pod.started),
        "run_s": seconds(pod.started, pod.finished),
        "verdicts": [v.code for v in verdicts.pod_verdicts(pod, events)],
    }


def archive(
    root: str,
    run_id: str,
    phase: str,
    pods: collections.abc.Sequence[PodState],
    events: collections.abc.Sequence[PodEvent],
) -> str:
    """Write ``runs/<run>/pods/<phase>.json``; rewritten if the phase is archived again."""
    import fsspec  # type: ignore[import-untyped]

    uri = f"{root.rstrip('/')}/runs/{run_id}/pods/{phase}.json"
    fs, path = fsspec.url_to_fs(uri)
    fs.makedirs(path.rsplit("/", 1)[0], exist_ok=True)
    with fs.open(path, "w") as handle:
        handle.write(json.dumps([record(p, events) for p in pods], indent=1, default=str))
    return uri
