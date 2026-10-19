# Running the pipeline on Argo Workflows

**Status:** our preferred way to run the pipeline in the cloud, as of 2026-10-15. It replaces a hand-rolled driver (`infra/run_aoi.py` + `infra/k8s/phase-job.yaml`, since removed) that ran one k8s Indexed Job per phase. Proven end to end on Honduras (1 tile) and Côte d'Ivoire (2 tiles), on our nonprod GKE cluster. Those runs predate the task layout described here, which arrived with the `orchestration` package (`b795c6f`). This note is written from the user's side: what you need, how to run a country, and what to expect. It is not a guide to operating Argo itself.

> **This repo is public.** The cluster, project, bucket, image registry and node-pool names are deliberately absent from this note. They live in the deployed workflow file and its env files, which are kept outside the repo and shared within the team. [`orchestration/workflows/`](../../orchestration/workflows/) and [`orchestration_ext/workflows/`](../../orchestration_ext/workflows/) hold committed copies of the workflows with placeholders for those names.

## Design choices

These are the decisions behind the shape below, kept here because the notes that made them have been retired.

**One tile per pod, and no Dask cluster.** The pipeline processes each 10° tile independently, and one tile's working set fits one highmem node. So the unit of distribution is a whole tile: a pod runs a tile's stages with each stage's own in-process `LocalCluster`, exactly as a laptop run does. Nothing crosses between tiles except attribute's final merge, a `groupby().sum()` over kilobyte-scale parquets. A shared Dask scheduler was tried first and dropped. It adds version skew between driver and workers, a scheduler a heavy tile can destabilise, and a `storage.py` change, and a Dask failure would then look like a pipeline failure.

**The cache is the resume mechanism.** Every stage is wrapped in `@storage.cache_to_zarr` / `@storage.cache_to_parquet`, writing idempotently to `SCRATCH_ROOT`. Retrying anything skips finished work, so orchestration can stay simple.

**The options weighed:**

| Option                  | Verdict                                                                                                                                                              |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Plain k8s Jobs + driver | What we ran first: one Indexed Job per phase and a polling script. Fine for a linear chain; no UI, and the driver is ours to maintain.                               |
| **Argo Workflows**      | **Current.** Chosen once several people were running countries, a UI was wanted, and Argo was installed on the cluster anyway, so we no longer operate a controller. |
| Distributed Dask        | Wrong granularity: tasks are hour-long with ~40 GB working sets, and already parallel inside the pod.                                                                |
| GCP Batch               | Simple job arrays, but a second compute platform next to GKE. Cloud Run Jobs is out regardless: its 32 GiB cap is below a tile's peak.                               |
| Celery                  | Hours-long 40 GB tasks don't fit its model, and it would tie this public repo to internal infrastructure.                                                            |

What Argo did **not** change: `jdluc` runs unmodified inside the pods. Argo replaces only the driver: sequencing, fan-out, retries and visibility. Our own glue is the small `orchestration` package, one subcommand per workflow task, and it may not change a result: every cache-key argument goes straight through to `jdluc`.

**Upstream's orchestration, and ours on top.** `orchestration/` is upstream's: plan, ingest-tile, compute and trace, and the `cornerstone` WorkflowTemplate that runs them. We never edit it, so merges from upstream don't conflict. What only this fork has (the per-tile exports, their mosaics, and the plan's export flags, which need our emit bands, `derive.py` and the COG/zarr exports) lives in `orchestration_ext/`. It reuses upstream's CLI through `orchestration.__main__.build_parser()`, adds the export flags to its `plan` and adds `export-tile` and `mosaic`; its plan is upstream's `get_task_to_items` plus the export lists. Our workflows define only plan, the exports and mosaic, run by `python -m orchestration_ext`, and call upstream's ingest-world, ingest-tile, compute and trace by `templateRef` from the `cornerstone` WorkflowTemplate. An import-linter contract keeps `jdluc` and `orchestration` from importing `orchestration_ext`.

**Ingest is separate from compute.** They are different workloads:

|                       | Ingest                            | Compute                   |
| --------------------- | --------------------------------- | ------------------------- |
| Bound by              | network and disk                  | memory and CPU            |
| Needs source-API keys | **yes** (USDA, Harvard Dataverse) | **no** (reads warm cache) |
| Cadence               | once per country × source version | every code change         |
| Idempotent            | yes (skips existing tiles)        | yes (`@cache_to_*`)       |

So only the ingest pods carry the keys, ingest can run on cheaper nodes, a flaky third-party source fails an ingest step rather than a compute run, and one warm `INGEST_ROOT` serves many compute runs. `compute` first runs harmonize with `skip_ingest=True`, so it reads that cache and never needs the keys.

**Storage, not compute, is the first scale risk.** Per-tile intermediates are hundreds of GiB uncompressed (harmonize ~340 GiB, emit more), and they stay in `SCRATCH_ROOT`. Compute is a one-off cost; storage recurs. Before a continent-scale run, decide retention for `SCRATCH_ROOT`, for example GCS lifecycle rules that delete or downgrade the emit zarrs once the deliverables exist. Sizes and what could shrink them are in [`stage-contents.md`](stage-contents.md#storage).

## The pieces

| Piece    | What it is                                                                                                                                | Where it lives                                                                                                  |
| -------- | ----------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| Image    | `Dockerfile`, built from a commit and tagged with its short SHA                                                                           | the team's Artifact Registry                                                                                    |
| Workflow | our Argo `Workflow` YAML (the DAG below), plus upstream's `cornerstone` WorkflowTemplate it calls by `templateRef`; pod sizes, node pools | outside the repo (team-shared); `orchestration/workflows/` and `orchestration_ext/workflows/` have placeholders |
| Config   | each pod's `.env`: `INGEST_ROOT`, `SCRATCH_ROOT`, `EXPORT_ROOT`, API keys                                                                 | two k8s Secrets, built from a cloud `.env`                                                                      |
| Identity | the `argo-workflow` service account, bound to a GCP service account with bucket access                                                    | set up by the platform team                                                                                     |
| Data     | ingest cache, scratch zarrs, exported deliverables                                                                                        | one GCS bucket, set in the Secrets                                                                              |

The bucket is set **only** in the Secrets. To point a run at a different bucket, change the cloud `.env`, recreate the Secrets, and submit. Neither the image nor the workflow changes.

## The DAG

This is the flat workflow, [`flat.example.yaml`](../../orchestration_ext/workflows/flat.example.yaml): each stage is one fan-out over every tile. *The per-tile DAG* below is the alternative for when some tiles fail. Each run covers its `scope`: one country, or `world` for every country (see *Running every country*):

```
plan -+-> ingest-world[dataset] -------------------------+-> compute[tile] -+-> trace
      +-> ingest-tiles[dataset][tile] -------------------+                  +-> export-tiles-cog[step, deliverable, tile] -> mosaic[deliverable]
      +-> ingest-tiles-allowed-failures[dataset][tile] --+                  +-> export-tiles-zarr[step, deliverable, tile]
```

| Step                            | Pods                     | Does                                                                                                                            | Secret    |
| ------------------------------- | ------------------------ | ------------------------------------------------------------------------------------------------------------------------------- | --------- |
| `plan`                          | 1                        | `python -m orchestration_ext plan ISO`: ingests the boundaries if absent, then writes the JSON lists every fan-out loops over   | no keys   |
| `ingest-world`                  | 1 per dataset            | `jdluc/ingest.py NAME --backfill`: climate zones, plus MAPSPAM and FAOSTAT (statistical) or NASS (jurisdictional direct)        | with keys |
| `ingest-tiles`                  | 1 per dataset × tile     | `python -m orchestration ingest-tile --dataset=NAME --tile-id=T`: one dataset on one tile                                       | with keys |
| `ingest-tiles-allowed-failures` | 1 per dataset × tile     | The same, for the datasets in `allow-failures`                                                                                  | with keys |
| `compute`                       | 1 per tile               | `python -m orchestration compute`: harmonize (from the warm cache) → emit → attribute, for each country the tile touches        | no keys   |
| `trace`                         | 1                        | `python -m orchestration trace`: `trace.workflow` over the country, the cross-tile merge, written as the emissions-factor table | no keys   |
| `export-tiles-cog`              | 1 per deliverable × tile | `python -m orchestration_ext export-tile`: writes one emit or derive deliverable as a COG, from emit's cache                    | no keys   |
| `export-tiles-zarr`             | 1 per deliverable × tile | The same, as a zarr                                                                                                             | no keys   |
| `mosaic`                        | 1 per COG deliverable    | `python -m orchestration_ext mosaic`: writes a VRT over the country's COGs of one deliverable                                   | no keys   |

Every step runs a CLI from the image directly, with no shell scripting around it. Steps run by `python -m orchestration` are upstream's templates, called by `templateRef`.

**Ingest runs one pod per dataset and tile, and fails loudly by default.** The tiles of a country run side by side, which a large country or a global run needs. A dataset behind one global download (GPW livestock, Liao) would fetch it once per pod; setting `SOURCE_CACHE_ROOT` in the Secrets shares those downloads between pods. Already-ingested tiles are skipped, so a warm rerun is quick. A failed tile fails its pod, and that fails the run: compute's `--ignore-missing-tiles` would otherwise turn a missing tile into an all-nodata band without complaint. The exception is the datasets in the `allow-failures` parameter, by default only `DESCALS_OIL_PALM`, which is published only where oil palm was found, so most tiles have none. They run as `ingest-tiles-allowed-failures`, and compute runs whether or not that step fails; its failed tiles show as failed pods in `argo get`. The dataset list is ours, not upstream's: see `orchestration/plan.py` for why, and add to it when upstream starts reading a new dataset.

**The tile list is derived, never typed in.** `plan` ingests the World Bank boundaries itself (a no-op once present), because the tiles are read off them. It then lists the tiles the country's boundary touches, the same derivation upstream's CLIs use, and each ingest pod is given its tile by id. An earlier version took a hand-typed tile count, and nothing checked it against the country.

**`trace` was called `reduce`** until it was renamed after the upstream stage it runs. Images from then on have no `reduce` subcommand, so the workflow must name the task `trace`.

**Exports are chosen per step, and run off to the side.** `emit-export` and `derive-export` each take `cog`, `zarr` or `none`. The step's format overrides each deliverable's own. `plan` turns them into one unit per (step, deliverable, tile), split by format, so a COG pod gets the disk it stages on and a zarr pod doesn't. A step with no export gets empty lists, and Argo skips a task with nothing to loop over, so the workflow needs no `when:` conditions. Every export reads only emit's cached output, so all start once `compute` finishes and run alongside `trace`. derive is lazy and never cached: it runs only as an export, so `derive-export=none` means derive doesn't run at all. emit's zarr in `SCRATCH_ROOT` is there whatever `emit-export` says, but scratch isn't kept. `trace` always writes its table: it is one small file, and the run's main result.

**`export` and `derive` became `export-tiles-cog` and `export-tiles-zarr`**, both running `python -m orchestration export-tile` (now `python -m orchestration_ext export-tile`). Images from then on have no `export` subcommand, and run derive through orchestration rather than `jdluc/derive.py`.

## The per-tile DAG

In the flat DAG, `compute` waits on every tile's ingest, so one failed (dataset, tile) stops compute on every tile. [`per-tile.example.yaml`](../../orchestration_ext/workflows/per-tile.example.yaml) gives each tile a DAG of its own, so a broken tile stops only itself. Use it to backfill the tiles that work while the broken ones are debugged.

```
plan -+-> ingest-world[dataset] -+-> tile[tile] -+-> trace
      +--------------------------+               +-> mosaic[deliverable]

tile:  ingest[dataset] -------------------+-> compute -+-> export-cog[step, deliverable]
       ingest-allowed-failures[dataset] --+            +-> export-zarr[step, deliverable]
```

The steps are the same pods as in the flat DAG, grouped differently. `plan` writes `tile-units`, one item per tile with its ingests and exports, from the same units as the flat lists.

- **A failed tile fails only itself.** Within a tile, compute waits for its ingests, with `allow-failures` as in the flat DAG. A failed required ingest leaves the tile's compute unscheduled, and the other tiles carry on.
- **`compute` checks its inputs.** It runs with `--check-ingests`, and raises before harmonize if a required ingest is missing from the ingest root. The DAG already rules that out; the check is a second line of defence, as harmonize would cache a missing tile as nodata.
- **`trace` and `mosaic` need every tile.** A table or mosaic missing tiles would pass for a whole one, so with any tile failed neither runs, and the run ends Failed. `argo get` lists the failed tiles. Once they're fixed, `argo retry` reruns them, then trace and mosaic.
- **Tiles wait for `ingest-world`.** A tile's DAG can't depend on one task of the outer DAG, so every tile starts once all of `ingest-world` is in. That's minutes once the whole-world datasets are cached, and up to 1.5h cold. In return, each tile's compute starts as soon as its own ingests finish, not after every ingest in the run.

## One-off setup

Done once per cluster, or again whenever the cloud config changes.

**Identity.** The pods reach GCS through Workload Identity: the workflow's k8s service account is bound to a GCP service account with read/write access to the bucket. No keys go in the image or the Secrets. The platform team set this up; a new cluster or bucket needs the same binding before anything will run.

**The two Secrets.** `Config.from_dot_env()` reads a `.env` *file*, not the process environment, so each pod mounts a Secret at `/app/.env`. The cloud config is kept apart from the repo-root `.env`, which stays pointed at local paths for local runs. Start it from the repo's `.env.example`, and keep the real files outside the repo, beside the deployed workflow:

```bash
cp .env.example ../cloud.env   # outside the repo; fill in real values
```

All six fields must be **present**, or `Config.from_dot_env()` raises: `INGEST_ROOT`, `SCRATCH_ROOT`, `EXPORT_ROOT`, `NUMBER_OF_DASK_WORKERS` (the per-pod Dask worker count; keep it at or below the pod's CPU limit), `USDA_NASS_API_KEY` and `HARVARD_DATAVERSE_GUESTBOOK_JSON`.

Only the ingest steps use the API keys. To keep the keys off the other pods, make `cloud-nokeys.env`, a copy of `cloud.env` with the two key fields present but blank. Then create one Secret from each file, in the namespace the workflow runs in:

```bash
kubectl --context "$KUBE_CONTEXT" -n argo create secret generic <secret-with-keys> \
  --from-file=.env=../cloud.env
kubectl --context "$KUBE_CONTEXT" -n argo create secret generic <secret-no-keys> \
  --from-file=.env=../cloud-nokeys.env
```

The workflow names both Secrets. To change the config, delete and recreate them.

**The image.** Build it from a clean commit and push it, as in the header of the `Dockerfile`. Then set the workflow's image to the new tag, in both our workflow and the `cornerstone` WorkflowTemplate, and update the template:

```bash
kubectl --context "$KUBE_CONTEXT" -n argo apply -f cornerstone.yaml
```

The template's tasks take their image, sizes and node pool from the template, not from our workflow, so compute's power-pool `nodeSelector` goes in the template's copy.

**What a run leaves behind.** Pods and their autoscaled nodes go away by themselves. The Secrets, the images in the registry and the bucket's data stay until deleted. The data is the point of the run; before a continent-scale run, decide how long to keep `SCRATCH_ROOT` (see *Storage, not compute, is the first scale risk* under *Design choices*).

## Running a country

You need the `argo` and `kubectl` CLIs, a kube context for the cluster (through the GKE connect gateway), and the workflow YAML. Then:

```bash
argo --context "$KUBE_CONTEXT" -n argo submit cornerstone-workflow.yaml \
  -p scope=CIV -p derive-export=cog -p derive-deliverables=crop-class-emissions \
  --generate-name cornerstone-civ-
```

| To                     | Run                                   |
| ---------------------- | ------------------------------------- |
| See recent runs        | `argo -n argo list --since 24h`       |
| Follow one live        | `argo -n argo watch <name>`           |
| See the step tree      | `argo -n argo get <name>`             |
| Read a pod's log       | `argo -n argo logs <name> <pod-name>` |
| Re-run after a failure | `argo -n argo retry <name>`           |
| Stop a run             | `argo -n argo stop <name>`            |

(Each of these also needs `--context "$KUBE_CONTEXT"`.)

The Argo UI shows the same information. Ask the platform team for its address.

### Parameters

- **`scope`:** one ISO 3166 alpha-3 code, or `world` for every country. `orchestration` accepts several countries, but the workflow passes a single value.
- **`allow-failures`:** a comma-separated list of tiled datasets whose failed tiles don't fail the run, by default `DESCALS_OIL_PALM`.
- **`methodology`:** `STATISTICAL` (the default) or `JURISDICTIONAL_DIRECT`, which is US-only.
- **`emit-export`:** `cog` (the default), `zarr` or `none`: how to write emit's `cie-emissions`.
- **`derive-export`:** `cog`, `zarr` or `none` (the default): how to write the `derive-deliverables`. With `none`, derive doesn't run.
- **`derive-deliverables`:** a comma-separated list of `crop-class-emissions` (the default), `perennial-discounting-comparison` and `perennial-debug`. `plan` rejects any other name before anything else runs.

### Outputs

Paths are relative to `EXPORT_ROOT`:

| Step                | Writes                                                      |
| ------------------- | ----------------------------------------------------------- |
| `trace`             | `emissions-factors/{scope}-{methodology}.parquet`           |
| `export-tiles-cog`  | `{deliverable}/{tile}.tif`, e.g. `cie-emissions/{tile}.tif` |
| `export-tiles-zarr` | `{deliverable}/{tile}.zarr`                                 |
| `mosaic`            | `mosaics/{deliverable}-{scope}.vrt`                         |

Each deliverable has its own folder. Files at the top level of `EXPORT_ROOT`, or a `mosaics/{iso}.vrt`, come from images older than `91efe40` and are superseded.

Intermediate results (the ingest cache and scratch zarrs) stay under `INGEST_ROOT` and `SCRATCH_ROOT`.

## Running every country

`-p scope=world` runs every country the pipeline produces factors for, over every GNW tile, as `--backfill` does locally:

```bash
argo --context "$KUBE_CONTEXT" -n argo submit cornerstone-workflow.yaml \
  -p scope=world --generate-name cornerstone-world-
```

The workflow's `parallelism` and `activeDeadlineSeconds` are sized for one country. Raise them in the copy you submit this from.

## What to expect

### Timings

Measured on the first Côte d'Ivoire run. This was a cold bucket and ran **without** the pd-ssd scratch described below, so the disk-heavy steps should come in faster from now on. Ingest then ran one pod per dataset over all the country's tiles; it now runs one pod per dataset and tile, so its per-pod times will be shorter:

| Step                                        | Per pod                                             |
| ------------------------------------------- | --------------------------------------------------- |
| `ingest-world` (data already present)       | ~1m                                                 |
| `ingest-tiles` (cold)                       | 2.5h to over 3h                                     |
| `ingest-tiles` (data already present)       | minutes                                             |
| `compute`                                   | 57–61m                                              |
| `trace`                                     | 2m                                                  |
| `export-tiles-cog` (`cie-emissions`)        | 1h30–1h54, vs ~50m with pd-ssd under the old driver |
| `export-tiles-cog` (`crop-class-emissions`) | 38–47m                                              |
| `mosaic`                                    | 39s                                                 |

### Reruns are cheap

Every stage is cached (`@storage.cache_to_*`). Rerunning a country, or `argo retry` after a failure, skips everything already in the bucket. When a cold `ingest-tiles` pod hit its 3h deadline, `argo retry` finished the remaining dataset in 5 minutes and the run went on to completion.

A zarr cache entry counts only once a `.complete` marker beside it has been written, after its last chunk. A pod killed mid-write (out of memory, deadline, preemption) leaves a zarr without a marker, and the next attempt to write it raises `FileExistsError` rather than reading back missing chunks as fill values or writing over a zarr another pod may still be writing. **An Argo retry of such a pod therefore fails too**: once no pod is writing it, delete the zarr by hand to let it be recomputed. Entries written before the marker was introduced have none, so each raises once until deleted: expect to clear old zarrs from an existing bucket before its first run. Within a run each tile has one `compute` pod, but two runs whose countries share a tile shouldn't overlap.

The flip side: **a new bucket means a cold run.** The cache keys don't include the bucket, so copying the `INGEST_ROOT` and `SCRATCH_ROOT` objects across at the same relative paths, `.complete` markers included, warms it again. And the cache is content-blind: changing code without bumping a stage's `version=` reads back the old result (see [`stage-contents.md`](stage-contents.md)).

### Disk is what limits speed

Ingest, export and derive each write tens of GB of uncompressed GeoTIFF locally before converting it to a COG. One cold ingest pod wrote about 128 GB. On the node's shared boot disk, those writes throttle to a crawl: I/O pressure reached 53% while CPU pressure stayed at zero.

The workflow therefore gives each pod its own **pd-ssd volume at `/localtmp`**, with `TMPDIR` and `CPL_TMPDIR` pointed at it, as the old driver did. The volume is created with the pod and deleted with it. Background is in [`gke-disk-io-findings.md`](gke-disk-io-findings.md). Don't shrink these volumes: pd-ssd throughput scales with size.

The volume is a generic ephemeral volume on GKE's built-in pd-ssd StorageClass (`premium-rwo`), so no StorageClass object of our own is needed. It works on any machine family, e2 included. Separately, each container has a small `ephemeral-storage` request and limit (2Gi / 8Gi). That covers only the container's own overlay (logs and anything not under `/localtmp`), so a stray writer evicts its own pod and not the node.

### Pod sizing

The sizes in `orchestration/workflows/` and `orchestration_ext/workflows/`, with the reasons the old driver recorded for its own, similar sizes. "Light" is the shared 2 / 4 CPU and 24Gi memory shape.

| Task               | CPU req / limit | Memory | `/localtmp` | Deadline | Why                                                                                                                                                                     |
| ------------------ | --------------- | ------ | ----------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `plan`             | light           | 24Gi   | 20Gi        | 30m      | Reads the boundaries and writes a few small JSON lists                                                                                                                  |
| `ingest-world`     | light           | 24Gi   | 100Gi       | 1h30     | Network-bound; each pod is one whole-world download, MAPSPAM the largest                                                                                                |
| `ingest-tile`      | light           | 24Gi   | 150Gi       | 3h       | One dataset on one tile: room for a global source download (GPW livestock, ~5 GB) plus one tile staged uncompressed for its COG (Liao, ~55 GB)                          |
| `compute`          | 6 / 8           | 56Gi   | 20Gi        | 4h       | Peak ~39 GiB observed: one tile per e2-highmem-8 node, with room for system pods. Results stream to GCS, so `/localtmp` only needs room for Dask spill and GDAL's cache |
| `trace`            | light           | 24Gi   | 20Gi        | 1h       | A groupby-sum over kilobyte-scale parquets, plus reading the admin-1 boundaries                                                                                         |
| `export-tile-cog`  | light           | 24Gi   | 200Gi       | 3h       | See below                                                                                                                                                               |
| `export-tile-zarr` | light           | 24Gi   | 20Gi        | 3h       | Streams chunks straight to the bucket; nothing is staged locally. Not yet measured                                                                                      |
| `mosaic`           | 1 / 2           | 8Gi    | 20Gi        | 1h       | Reads each tile COG's header and writes a small VRT; no pixels                                                                                                          |

Memory requests equal limits. Every container also has a 2Gi / 8Gi `ephemeral-storage` request and limit (above). The old driver used 4–8Gi memory requests below 24–32Gi limits, a 48Gi / 60Gi compute pod and a 6h compute deadline; neither set has been measured at scale.

A COG export is limited by disk, not memory, since Dask writes in chunks. `cog_translate` first writes an **uncompressed** tiled intermediate, and only then compresses the final COG. That intermediate is ~33 GB for a 5-band float32 10° tile, so ~70 GB for the 10 `cie-` bands. 200Gi leaves headroom and clears GDAL's free-space check, which we keep on so that a too-small disk fails at the start rather than at 97%.

The highmem pool the compute pods use carries a `NoSchedule` taint, shared with other workloads, so pods bound for it need a matching toleration. Without one, the autoscaler won't even add a node.

## Gotchas

- **Pod logs disappear once a run is cleaned up.** Argo isn't set to archive them. Save any log you'll want before the run is garbage-collected.
- **`argo get -o json` embeds raw log text**, which strict JSON parsers reject. Use `json.loads(..., strict=False)` in Python, or the table output.
- **`argo retry` restarts the run's clock.** The reported duration covers only the retry, not the time since first submission.
- **No pod logs its own resource peaks.** For memory and disk use, read `kubectl top` or the GKE metrics.
- **`mosaic` only covers COGs.** A zarr deliverable stays one zarr per tile.
- **`compute` and `trace` must use the same methodology.** It picks the attribute leg and its crop names, which are cache-key arguments, so a mismatch doesn't fail: it silently recomputes every tile inside the single `trace` pod. The workflow passes one `methodology` parameter to both, so this bites only when running `python -m orchestration` by hand.
- **A warm cache can fake a fast run.** When measuring timings, make sure the run does real work (fresh tiles, a fresh prefix, or a bumped stage `version=`), or it will finish in seconds and look as if it scaled.

## Open questions

- **Deadlines.** A cold `ingest-tiles` needed more than 3h without pd-ssd, and the `cie-emissions` COG export came within 6 minutes of its 3h. Re-measure with pd-ssd before choosing new limits.
- **Sizes.** The workflow's sizes (*Pod sizing* above) have not been measured at scale, and the per-tile ingest pods have not been timed.
- **Output versioning.** Every deliverable is written at a fixed path, so a rerun overwrites the last. The emissions-factor table especially wants a version (data version, commit or run) in its path before results are shared.
- **Several countries per run.** Either one country or `world`, for now.
- **Ingest versioning.** `INGEST_ROOT` isn't keyed by source-dataset version, and mutable sources (Dataverse, GFW) aren't pinned, so a re-ingest isn't guaranteed to reproduce an earlier one.
- **Storage retention.** No lifecycle rules are set on `SCRATCH_ROOT` yet (see *Design choices*).
