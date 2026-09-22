# Spike: run the pipeline in our k8s cloud, one tile per Job

**Status:** proposal / scaffolding on branch `spike/dask-k8s`. Nothing is deployed. This document is the plan; the runnable stubs live in [`infra/`](../infra/).

## Goal

**De-risk the stakeholder decision of whether to build on top of this repository** by demonstrating that its pipeline can be *run and scaled inside our infrastructure* (the Maverick nonprod shared GKE cluster). This is a feasibility exercise, not an execution-engine bake-off: we are **not** choosing a final distribution layer, and we are **not** evaluating Dask as a technology. We are answering, with honest numbers, three questions a stakeholder needs before committing:

1. **Does it run here?** — the real pipeline executes in a cluster pod, authenticating to GCS and doing real I/O the way our infra does, with essentially **no change to `jdluc`**.
2. **Is it correct here?** — a tile's output in-cluster matches a known-good local run within tolerance.
3. **Can we scale it here?** — many tiles run concurrently on our infra, and throughput rises with the resources we give it.

The strongest evidence is *"we ran the repo essentially unmodified, on a real jurisdiction, in our cluster."* The word **unmodified** is doing real work: the less we touch the code, the cleaner the claim.

The target workload is **Honduras (`HND`)** — small enough to iterate on in minutes, real enough to exercise the whole harmonize→emit path over GCS.

## Unit of work: one whole tile per pod

The pipeline processes each 10° tile independently and, today, serially (`emit.main` etc. loop `for tile_id in ...`); a single tile's working set is designed to fit one machine. So the natural unit to distribute is **one whole tile**: a pod runs `emit.workflow(tile_id)`, which reads its sources and writes its output zarr straight to GCS. There is ~no cross-node traffic, and throughput scales with how many tiles run at once.

Crucially this needs **no code change**: the stage already spins its own in-process dask `LocalCluster` (in `storage.write_dask_dataset_to_zarr`) for the per-tile threaded compute. One pod == one laptop run of one tile.

## Mechanism: one k8s Job per tile (not a dask cluster)

We fan tiles out as plain k8s `Job`s in the `dan` namespace, using only privileges Dan already has. This is the minimal instrument for the feasibility question — it adds no long-running scheduler to operate and keeps the thing under test (the repo) isolated from any distribution-framework of our own.

- **One image** ([`infra/Dockerfile`](../infra/Dockerfile)) built from `uv.lock`, so the pod resolves exactly what a laptop run resolves.
- **A Job template** ([`infra/k8s/tile-job.yaml`](../infra/k8s/tile-job.yaml)) rendered once per tile by [`infra/run-tiles.py`](../infra/run-tiles.py), which resolves the tiles a country touches and `kubectl apply`s one Job each. `restartPolicy: Never`, `backoffLimit: 1`, `ttlSecondsAfterFinished` for self-cleanup.
- **The scaling knob is submission + autoscaling**: submit N Jobs and the highmem node pool (`e2-highmem-8`, autoscaling 0→N) runs as many concurrently as it has room for. No `kubectl scale` of a worker pool.
- **GCS access via Workload Identity**: pods run as an existing KSA (already annotated to the GSA the Celery workers use). **No new IAM.** Cluster-specific names live in `infra/cluster.env` (see `infra/cluster.env.example`), not in tracked files.
- **In-pod parallelism** stays as-is: the stage's own `LocalCluster` (sized by `NUMBER_OF_DASK_WORKERS`) uses the pod's cores for the per-tile compute. Parallelism *across* tiles = number of Job pods; parallelism *within* a tile = the inner threads.

### Why Jobs, not a Dask cluster on k8s

An earlier draft of this cornerstone stood up a remote Dask scheduler + worker Deployments and submitted tiles to it. That works, but for *this* goal it is the wrong instrument: it drags in evaluating and operating Dask itself (version skew between driver and workers, a shared scheduler a fat tile can destabilize, a `storage.py` change to reach the remote scheduler) — none of which is the question. A Dask-operational failure would masquerade as "the repo doesn't work in our infra." One Job per tile is a strict subset of that machinery minus the scheduler and minus the code change; failures isolate to a single tile's Job. Choosing a real production distribution layer (Dask, Celery fan-out, Argo, …) is a **follow-up**, informed by this cornerstone but out of its scope.

## Known integration points / risks to verify in the cornerstone

- **Cache key needs `.git` in the image.** `storage.cache_to_zarr` derives the stage's module path via `git.Repo(search_parent_directories=True)`, so the image must contain the repo's `.git`. The [`.dockerignore`](../.dockerignore) trims the build context but deliberately keeps `.git`. (Worth a follow-up: derive the path without git.)
- **`.env` is a file, not env.** `Config.from_dot_env()` reads a `.env` file, so pods mount one from a Secret (`infra/k8s/README.md`). Pre-ingest HND once so workers hit the `SCRATCH_ROOT` cache and don't need the USDA/Harvard API keys.
- **Caching can mask a scaled run.** `cache_to_zarr` short-circuits when the output already exists in `SCRATCH_ROOT`. If a scaling run re-uses already-computed tiles it will finish instantly and *look* like it scaled. Each measured run must do real work: bump the stage `version=`, clear the prefix, or use fresh tiles.
- **Per-tile memory.** A tile's working set must fit one `e2-highmem-8` (64 GiB; pod limited to 60 GiB). If it doesn't, that Job's pod OOMKills (exit 137) — under one-Job-per-tile that is an isolated, observable finding, not a silent whole-run failure.
- **GDAL config on the pod.** `write_dask_dataset_to_zarr` sets GDAL/rasterio options via `rasterio.Env` at compute time; the same values are also set as pod env in the Dockerfile as belt-and-braces. Keep the two roughly in sync.

## Milestones (each a go/no-go)

- **M0 — plumbing.** Build/push image; `run-tiles.py HND --dry-run` renders manifests; one real Job runs a trivial "open one COG from GCS" path on a pod. Proves image + Workload Identity + the `.env` Secret + connectivity.
- **M1 — one real tile, correct.** Submit `--stage harmonize` for one HND tile; diff the output zarr against a known-good single-host run within tolerance. Proves the real graph + VRT/GCS I/O + correctness. **This is the headline artifact.**
- **M2 — scale.** Submit all HND tiles; let the node pool run them concurrently; record wall-clock and cost per tile, and extrapolate to a continent (North America is 51 tiles). The evaluation deliverable.

## Success criteria

1. **Runs** — the real pipeline executes in-cluster on our image, authenticating to GCS via Workload Identity, with no change to `jdluc`.
2. **Correct** — a tile's in-cluster output matches the single-host run within a stated tolerance.
3. **Scales** — many tiles run concurrently and throughput rises with pod count; a rough cost-per-tile and a continent-scale extrapolation exist.

## Correctness check (M1)

The pass/fail line for M1 needs pinning before the run: a known-good reference (a local single-host run of the same tile) and a numeric tolerance (e.g. `xarray.testing.assert_allclose` with an agreed `rtol`, since GDAL warp is not bit-reproducible across environments). Defining this is part of M1, not an afterthought.

## Out of scope for this cornerstone

- The `attribute`/`trace` stages (take extra args; add once harmonize/emit are proven).
- Choosing the **production** distribution layer (Dask cluster, Celery fan-out, Argo Workflows, …). This cornerstone proves the repo runs and scales in our infra; how we'd operate that at production scale is a follow-up it informs.
- Any production hardening: autoscaling policy, retries/back-pressure tuning, dashboards/alerts.
