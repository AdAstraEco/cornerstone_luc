# Design note: scaling the pipeline over multiple machines

**Status:** implemented, not yet run in-cluster at scale — see *As built* below. This is the
follow-up that [`docs/orbae/cornerstone-k8s.md`](cornerstone-k8s.md)
explicitly deferred: the cornerstone proved the repo runs, is correct, and scales in our GKE
cluster; this note chooses how to operate that at production scale. Guiding constraint, per the
repo ethos ([`architecture.md`](architecture.md)): **simplicity over performance**.

## Recommendation (TL;DR)

Keep the cornerstone's direction — plain k8s Jobs on GKE, one tile per pod — and evolve it into:

- **k8s Indexed Jobs** for fan-out (one Job object per stage per run, `completions`/`parallelism`),
- **the pipeline's own `@cache_to_*` caching** as the resume/retry mechanism,
- **a small driver script** (~100 lines) for sequencing the linear stage chain.

No Dask cluster, no Argo (yet), no second compute platform. `jdluc` stays unmodified.

## The three facts that decide this

1. **The workload is a job array, not a task graph.** Every heavy stage — harmonize, emit, and
   both attribute legs — is per-tile, fits one highmem node (~39 GB peak observed), runs tens of
   minutes to a few hours, and has zero cross-tile traffic. The only cross-tile step is
   attribute's final merge (`attribute.merge_dfs`) — a pandas `groupby().sum()` over
   kilobyte-scale parquets.
2. **The cache is already the resume mechanism.** Every stage is wrapped in
   `@storage.cache_to_zarr` / `@storage.cache_to_parquet`, writing idempotently to
   `SCRATCH_ROOT`. Rerunning anything skips warm tiles. This duplicates the headline feature of
   a workflow orchestrator, so our orchestration can stay dumb.
3. **The scale is modest.** North America is 51 tiles; the global land surface is a few hundred
   10° tiles. Fan-out is O(hundreds), never O(hundreds of thousands). Nothing at that scale
   requires a scheduler.

## Options considered

### A. Plain k8s Jobs, evolved — **recommended**

Replace the one-Job-per-tile loop in `infra/run-tiles.py` with one **Indexed Job per stage per
run**: `completions: N, parallelism: M`; each pod maps its `JOB_COMPLETION_INDEX` into the
sorted tile list for the AOI, computed in-pod from the country arguments — deterministic, so no
tile-list plumbing via ConfigMaps. `backoffLimitPerIndex` gives per-tile retries.

Sequencing is a driver script: submit → `kubectl wait --for=condition=complete` → next stage.

- **Pros:** no new infrastructure or controllers on the shared nonprod cluster; failures isolate
  per index; a crashed run is "rerun the driver" (the cache no-ops warm tiles); the scaling knob
  stays "parallelism × node-pool autoscaler", as in the cornerstone.
- **Cons:** no UI; the driver is hand-rolled. Acceptable while the DAG is a linear chain with
  two barriers (see below).

### B. Argo Workflows

The right tool *eventually*: DAG with `withParam` fan-out, retries, a UI, many AOIs in flight.
But it means installing and operating a controller + CRDs on a shared cluster (the cornerstone
deliberately stayed namespace-scoped; cluster-admin may not be available), learning its
templating, and debugging two layers when a pod dies. Its headline feature — resume from partial
failure — duplicates what `@cache_to_*` already provides. Wrong trade today. See *revisit
triggers* below.

### C. Distributed Dask cluster (dask-kubernetes / Coiled)

Wrong granularity. Tasks are hour-long with ~40 GB working sets and are already internally
parallel via each stage's in-process `LocalCluster` (nested clusters are awkward). An outer
scheduler adds a SPOF, driver/worker version skew, and a `storage.py` change. The argument in
`cornerstone-k8s.md` §"Why Jobs, not a Dask cluster" holds at production scale, not just for the
feasibility test.

### D. GCP Batch

Genuinely simple job arrays on raw VMs — attractive greenfield. But GKE + Workload Identity +
node pools is the incumbent platform; a second compute platform (IAM, quotas, images,
monitoring) is net complexity. (Cloud Run Jobs is out regardless: its 32 GiB memory cap is below
the observed ~39 GB per-tile peak.)

### E. Reuse existing Celery infrastructure

Hours-long 40 GB tasks don't fit Celery's model; it means long-running highmem workers instead
of scale-to-zero Jobs; and it couples this public repo to internal infrastructure.

## The recommended shape, concretely

A full AOI run is **three submissions and two barriers**, following the ingest/compute split
already designed in [`ingest-compute-separation.md`](ingest-compute-separation.md):

1. **Ingest** — Indexed Job over tiles; standard node pool; carries the API-key Secret;
   best-effort (log and continue past missing ocean/edge tiles). Warms `INGEST_ROOT`.
2. **Compute** — Indexed Job over tiles; highmem pool; `--skip-ingest`; no secrets. Each pod
   runs **harmonize → emit → per-tile attribute** for its tile (both attribute legs are per-tile
   and cached: `jurisdictional_direct.workflow`, `statistical.workflow`). Chaining the stages
   inside one pod avoids two extra barriers and halves scheduling overhead; the cache makes it
   safe — a pod that dies mid-chain resumes from its last completed stage on retry.
3. **Reduce** — one small pod running `attribute.workflow` + `trace.workflow` per country. All
   per-tile caches are warm by then, so this is just the merge (validated single-pod on
   El Salvador).

Code changes are minor and stay under `infra/`:

- `run_tile.py`: add `--skip-ingest`, stage chaining, and the index→tile mapping;
- `k8s/tile-job.yaml`: become an Indexed Job template (`completions`, `parallelism`,
  `backoffLimitPerIndex`);
- a new driver (evolution of `run-tiles.py`): submit the three phases with `kubectl wait`
  barriers.

Keep the cornerstone guardrails: `activeDeadlineSeconds`, short `ttlSecondsAfterFinished`,
ingest best-effort / compute strict.

## As built

Implemented as above, entirely under `infra/`, with three deliberate deviations. Operational
detail lives in [`infra/k8s/README.md`](../../infra/k8s/README.md).

- `infra/run_phase.py` (was `run_tile.py`) — the in-pod entrypoint for every phase: dataset
  selection, the index→tile mapping, `--skip-ingest`, and the in-pod stage chain.
- `infra/k8s/phase-job.yaml` (was `tile-job.yaml`) — one Indexed Job template for all phases.
- `infra/run_aoi.py` (was `run-tiles.py`) — the driver: per-phase pod shapes, submit, barrier.

**Four phases, not three.** Ingest is split into `ingest-world` and `ingest-tiles`, because
the whole-world datasets (boundaries, IPCC climate zones, MAPSPAM, USDA NASS QuickStats) are
`Partitioning.WHOLE_WORLD`: fanning them out over N tiles would have N pods racing to write
one object. The split is derived at runtime from each dataset's `partitioning`, not a hand-
maintained list, so a new dataset lands in the right phase automatically. `ingest-world` is a
single pod; only `ingest-tiles` is indexed over tiles.

**The barrier polls `kubectl get job -o json`, not `kubectl wait`.** `kubectl wait
--for=condition=complete` blocks until its own timeout when the Job *fails*, and gives no
progress output. Polling yields a succeeded/failed/active line per interval and stops on
either terminal condition (plus a `failedIndexes` fallback for the case where every index is
terminal before the Job's conditions catch up).

**Compute and reduce must be given identical flags.** `methodology`, `crop_names` and
`skip_glad_crop_filter` are cache-key arguments on the attribute legs, so a mismatch doesn't
error — it silently recomputes the whole AOI inside the single reduce pod. The driver passes
one set of flags to both.

The per-phase node pool and Secret are `cluster.env` overrides (`NODE_POOL_INGEST`,
`NODE_POOL_REDUCE`, `K8S_SECRET_COMPUTE`), each falling back to the base variable, so the
cheap-pool and no-credentials splits are opt-in and a minimal `cluster.env` still works.

## Revisit triggers (when to step up to Argo)

- multiple concurrent AOIs run by multiple people;
- scheduled/recurring runs;
- the driver growing conditionals beyond the linear chain;
- wanting a UI for run visibility.

The Indexed-Job structure ports to an Argo `withParam` DAG almost mechanically, so nothing is
foreclosed by starting simple.

## Storage, not compute, is the first scale risk

Orthogonal to the orchestration choice, but it will bite first. Verified against the local
Honduras run (`20N_090W`): one tile's stores are 40000×40000 float32 grids —
**harmonize ≈ 60 GiB and emit ≈ 119 GiB uncompressed** (10 and 20 variables respectively).
That is consistent with the historical North America benchmark in `architecture.md`
(4.1 / 8.1 TiB over 51 tiles, older region-grid code path). On-disk size depends heavily on
compression: the ocean-dominated Honduras tile compresses ~135:1 (emit lands at 879 MiB), but
data-dense farmland tiles will compress far less, so a continent lands in the **low-TiB range**
on disk and attribute streams the full per-pixel zarr back through the clip/rollup. Before the
first continent-scale run, decide lifecycle/retention for `SCRATCH_ROOT` (e.g. delete or
downgrade emit zarrs once attribute rollups exist, GCS lifecycle rules on the prefix).
