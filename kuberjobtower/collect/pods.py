"""A phase's pod records (when each pod was created, started and ended, where, and how), archived."""

import collections.abc
import dataclasses
import datetime
import json

from kuberjobtower.collect import cost, verdicts
from kuberjobtower.models import NodeInfo, PodEvent, PodState


def record(
    pod: PodState,
    events: collections.abc.Sequence[PodEvent],
    node: NodeInfo | None = None,
) -> dict[str, object]:
    def seconds(a: datetime.datetime | None, b: datetime.datetime | None) -> float | None:
        return round((b - a).total_seconds(), 1) if a and b else None

    return {
        **dataclasses.asdict(pod),
        "pending_s": seconds(pod.created, pod.started),
        "run_s": seconds(pod.started, pod.finished),
        "verdicts": [v.code for v in verdicts.pod_verdicts(pod, events)],
        "machine_type": node.instance_type if node else None,
        "node_pool": node.pool if node else None,
        "cost_usd": round(c.usd, 4) if (c := cost.pod_cost(pod, node)) else None,
        "node_share": round(c.share, 2) if c else None,
    }


def read_archive(root: str, run_id: str) -> dict[str, list[dict[str, object]]]:
    """Every phase's stored pod records for the run, keyed by phase."""
    import fsspec  # type: ignore[import-untyped]

    fs, path = fsspec.url_to_fs(f"{root.rstrip('/')}/runs/{run_id}/pods")
    if not fs.exists(path):
        return {}
    out = {}
    for name in sorted(fs.ls(path, detail=False)):
        if name.endswith(".json"):
            with fs.open(name, "r") as handle:
                out[name.rsplit("/", 1)[-1].removesuffix(".json")] = json.loads(handle.read())
    return out


def archive(
    root: str,
    run_id: str,
    phase: str,
    pods: collections.abc.Sequence[PodState],
    events: collections.abc.Sequence[PodEvent],
    nodes: collections.abc.Mapping[str, NodeInfo | None] | None = None,
) -> str:
    """Write ``runs/<run>/pods/<phase>.json``; rewritten if the phase is archived again."""
    import fsspec  # type: ignore[import-untyped]

    uri = f"{root.rstrip('/')}/runs/{run_id}/pods/{phase}.json"
    fs, path = fsspec.url_to_fs(uri)
    fs.makedirs(path.rsplit("/", 1)[0], exist_ok=True)
    with fs.open(path, "w") as handle:
        records = [record(p, events, (nodes or {}).get(p.node or "")) for p in pods]
        handle.write(json.dumps(records, indent=1, default=str))
    return uri
