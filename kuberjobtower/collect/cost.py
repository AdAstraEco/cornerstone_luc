"""A rough dollar figure for a pod, so the order of magnitude is visible before and after a run.

Pod cost = the pod's share of its node for the time it ran, plus its scratch volume for the time it
existed. The share is the larger of its memory and CPU requests over the node's allocatable, since
that is what keeps another pod off the node; a pod that fills the node pays for all of it. This is
list price, on demand, and leaves out the idle minutes before an autoscaler removes the node, spot
and committed-use discounts, network and storage operations. Rates are third-party figures
(economize.cloud, 2 Oct 2026): check them against billing before relying on the cents.
"""

import collections.abc
import dataclasses

from kuberjobtower.models import NodeInfo, PodState

RATES_AS_OF = "2026-10-02"
MACHINE_USD_PER_HOUR = {"e2-standard-8": 0.268, "e2-highmem-8": 0.3616}  # us-central1
PD_SSD_USD_PER_GIB_MONTH = 0.17  # premium-rwo, us-central1
HOURS_PER_MONTH = 730


@dataclasses.dataclass(frozen=True)
class PodCost:
    node_usd: float
    disk_usd: float
    share: float
    instance_type: str

    @property
    def usd(self) -> float:
        return self.node_usd + self.disk_usd


def node_share(pod: PodState, node: NodeInfo) -> float | None:
    parts = []
    if pod.memory_request_bytes and node.allocatable_memory_bytes:
        parts.append(pod.memory_request_bytes / node.allocatable_memory_bytes)
    if pod.cpu_request_m and node.allocatable_cpu_m:
        parts.append(pod.cpu_request_m / node.allocatable_cpu_m)
    return min(1.0, max(parts)) if parts else None


def pod_cost(pod: PodState, node: NodeInfo | None) -> PodCost | None:
    """None when the pod never ran, its node is unknown, or the machine type has no rate."""
    if not (node and node.instance_type in MACHINE_USD_PER_HOUR and pod.started and pod.finished):
        return None
    share = node_share(pod, node)
    if share is None:
        return None
    ran_h = max(0.0, (pod.finished - pod.started).total_seconds()) / 3600
    existed_h = ran_h
    if pod.created:
        existed_h = max(0.0, (pod.finished - pod.created).total_seconds()) / 3600
    scratch_gib = (pod.scratch_bytes or 0) / (1 << 30)
    return PodCost(
        node_usd=ran_h * MACHINE_USD_PER_HOUR[node.instance_type] * share,
        disk_usd=scratch_gib * PD_SSD_USD_PER_GIB_MONTH / HOURS_PER_MONTH * existed_h,
        share=share,
        instance_type=node.instance_type,
    )


def total(costs: collections.abc.Iterable[PodCost | None]) -> tuple[float, int]:
    """(sum of the known costs, how many pods had no estimate)."""
    known = [c for c in costs if c is not None]
    return sum(c.usd for c in known), sum(c is None for c in costs)
