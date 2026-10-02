"""The phases of a run and the pod shape each one gets. Standard library only.

The ``Phase`` values are the ``--phase`` contract with ``infra/run_phase.py``, which cannot
import this package (the image installs ``jdluc`` only), so the two are kept equal by
``phases_test.py``. Sizes come from the 29 Sept to 2 Oct 2026 single-tile run
(docs/control-plane/01-kubernetes-library.md section 6); where a phase was not measured the old
limit is kept and only the request is raised to equal it.
"""

import dataclasses
import enum


class Phase(enum.StrEnum):
    INGEST_WORLD = "ingest-world"
    INGEST_TILES = "ingest-tiles"
    COMPUTE = "compute"
    REDUCE = "reduce"
    EXPORT = "export"
    MOSAIC = "mosaic"

    @property
    def is_per_tile(self) -> bool:
        """Does the run fan this phase out over the tiles (vs one pod for the whole AOI)?"""
        return self in (Phase.INGEST_TILES, Phase.COMPUTE, Phase.EXPORT)


class PoolRole(enum.StrEnum):
    LIGHT = "light"  # CONTROLPLANE_POOL_LIGHT
    HEAVY = "heavy"  # CONTROLPLANE_POOL_HEAVY


class SecretRole(enum.StrEnum):
    KEYS = "keys"  # CONTROLPLANE_SECRET: source-API credentials, for the ingest phases
    NOKEYS = "nokeys"  # CONTROLPLANE_SECRET_NOKEYS


@dataclasses.dataclass(frozen=True)
class PhaseSpec:
    """Memory request always equals the limit, so the kubelet cannot evict a pod that is
    still inside its limit (the compare-tool evictions, doc 01 section 6)."""

    cpu_request: str
    cpu_limit: str
    memory: str
    localtmp: str
    pool: PoolRole
    secret: SecretRole
    pod_deadline_s: int
    retries_per_index: int = 1
    # An OOM retried on the same machine type fails identically, so fail the index at once.
    fail_index_on_oom: bool = False


H = 3600

DEFAULT_SPECS: dict[Phase, PhaseSpec] = {
    # Only the 12 min wall-clock was measured; memory is not, so the old 24Gi limit stays.
    Phase.INGEST_WORLD: PhaseSpec(
        "2", "4", "24Gi", "100Gi", PoolRole.LIGHT, SecretRole.KEYS, 3 * H // 2
    ),
    # Disk-bound (io_psi_full_avg10 59.6), 19-24 GiB incl. page cache, 75 min measured.
    Phase.INGEST_TILES: PhaseSpec(
        "2",
        "4",
        "24Gi",
        "300Gi",
        PoolRole.LIGHT,
        SecretRole.KEYS,
        3 * H,
        retries_per_index=2,
    ),
    # Peak 52.7-53.5 GiB measured; 56Gi leaves about 5% and fits an e2-highmem-8 node.
    Phase.COMPUTE: PhaseSpec(
        "6",
        "8",
        "56Gi",
        "20Gi",
        PoolRole.HEAVY,
        SecretRole.NOKEYS,
        4 * H,
        fail_index_on_oom=True,
    ),
    # Unmeasured at scale (CZE: seconds). 24Gi, not the old 32Gi limit: with request == limit a
    # 32Gi pod could never schedule on the light pool's 27.6 GiB nodes.
    Phase.REDUCE: PhaseSpec(
        "2", "4", "24Gi", "10Gi", PoolRole.LIGHT, SecretRole.NOKEYS, H
    ),
    # Measured: 50 min, peak scratch about 88 GiB of 200, 87.8 GiB written, memory at the cap
    # as reclaimable page cache.
    Phase.EXPORT: PhaseSpec(
        "2", "4", "24Gi", "200Gi", PoolRole.LIGHT, SecretRole.NOKEYS, 3 * H
    ),
    Phase.MOSAIC: PhaseSpec(
        "1", "2", "8Gi", "10Gi", PoolRole.LIGHT, SecretRole.NOKEYS, H
    ),
}
assert set(DEFAULT_SPECS) == set(Phase)
