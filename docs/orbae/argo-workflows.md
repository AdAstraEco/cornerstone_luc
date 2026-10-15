# Running the pipeline on Argo Workflows

**Status:** our preferred way to run the pipeline in the cloud, as of 2026-10-15. It replaces the hand-rolled driver (`infra/run_aoi.py` + `infra/k8s/phase-job.yaml`, since removed) described in [`scale-out.md`](scale-out.md). Proven end to end on Honduras (1 tile) and Côte d'Ivoire (2 tiles), on our nonprod GKE cluster. This note is written from the user's side: what you need, how to run a country, and what to expect. It is not a guide to operating Argo itself.

> **This repo is public.** The cluster, project, bucket, image registry and node-pool names are deliberately absent from this note. They live in the gitignored `infra/cluster.env` and `infra/cloud.env`, and in the workflow file, which is kept outside the repo for the same reason (it is shared within the team).

## Why Argo now

[`scale-out.md`](scale-out.md) chose a small driver script over Argo, and listed the triggers for revisiting that: several people running AOIs at once, wanting a UI, the driver growing beyond a linear chain. Those have now arrived. Argo is also now installed on the cluster as part of its own infrastructure, so we no longer have to run a controller ourselves, which was the main argument against it.

What did **not** change: `jdluc` and `infra/run_phase.py` run unmodified inside the pods, one pod per tile per phase. Argo replaces only the driver: sequencing, fan-out, retries and visibility. The "no new compute platform, `jdluc` unmodified" principle of the earlier notes still holds.

## The pieces

| Piece    | What it is                                                                             | Where it lives                                |
| -------- | -------------------------------------------------------------------------------------- | --------------------------------------------- |
| Image    | `infra/Dockerfile`, built from a commit and tagged with its short SHA                  | the team's Artifact Registry                  |
| Workflow | one Argo `Workflow` YAML: the DAG below, pod sizes, node pools                         | outside the repo (team-shared)                |
| Config   | each pod's `.env`: `INGEST_ROOT`, `SCRATCH_ROOT`, `EXPORT_ROOT`, API keys              | two k8s Secrets, built from `infra/cloud.env` |
| Identity | the `argo-workflow` service account, bound to a GCP service account with bucket access | set up by the platform team                   |
| Data     | ingest cache, scratch zarrs, exported deliverables                                     | one GCS bucket, set in the Secrets            |

The bucket is set **only** in the Secrets. To point a run at a different bucket, change `infra/cloud.env`, recreate the Secrets, and submit. Neither the image nor the workflow changes.

## The DAG

Each run covers one country (`iso`):

```
list-world-datasets -> ingest-world[dataset] -> list-tiles -+-> ingest-tiles[dataset][tile] ---------------------+-> compute[tile] -+-> reduce -> export[tile] -> mosaic
list-tile-datasets ----------------------------------------+-> ingest-tiles-allowed-failures[dataset][tile] ----+                  +-> derive[tile]
```

| Step                            | Pods                 | Does                                                                                   | Secret      |
| ------------------------------- | -------------------- | -------------------------------------------------------------------------------------- | ----------- |
| `list-world-datasets`           | 1                    | `infra/list_datasets.py --partitioning=whole-world`: the whole-world datasets, as JSON | no keys     |
| `list-tile-datasets`            | 1                    | `infra/list_datasets.py --partitioning=ten-degree-tile`: the tiled datasets, as JSON   | no keys     |
| `ingest-world`                  | 1 per dataset        | `jdluc/ingest.py NAME --backfill`: boundaries, climate zones, MAPSPAM, FAOSTAT, NASS   | with keys   |
| `list-tiles`                    | 1                    | `python -m jdluc.datasets.worldbank_jurisdictions ISO`: the country's tiles, as JSON   | no keys     |
| `ingest-tiles`                  | 1 per dataset × tile | `jdluc/ingest.py NAME ISO --modulus=N --residues=i`: one dataset on one tile           | with keys   |
| `ingest-tiles-allowed-failures` | 1 per dataset × tile | The same, for the datasets in `ingest-allow-failures`                                  | with keys   |
| `compute`                       | 1 per tile           | harmonize → emit → attribute, with `--skip-ingest`                                     | no keys     |
| `reduce`                        | 1                    | `trace.workflow` over the country: the cross-tile merge                                | with keys\* |
| `export`                        | 1 per tile           | Writes the CIE emissions COG                                                           | with keys\* |
| `mosaic`                        | 1                    | Writes a VRT over the country's CIE COGs                                               | with keys\* |
| `derive`                        | 1 per tile           | Writes one crop-class deliverable (`jdluc/derive.py`) from emit's cache                | no keys     |

\* These don't need the source-API keys. The old driver ran them without keys; tightening this is an open question below.

Every step runs a CLI from the image directly, with no shell scripting around it.

**Ingest runs one pod per dataset and tile, and fails loudly by default.** The tiles of a country run side by side, which a large country or a global run needs. A dataset behind one global download (GPW livestock, Liao) would fetch it once per pod; setting `SOURCE_CACHE_ROOT` in the Secrets shares those downloads between pods. Already-ingested tiles are skipped, so a warm rerun is quick. A failed tile fails its pod, and that fails the run: compute's `--ignore-missing-tiles` would otherwise turn a missing tile into an all-nodata band without complaint. The exception is the datasets in the `ingest-allow-failures` parameter, a JSON list that is by default only `DESCALS_OIL_PALM`, which is published only where oil palm was found, so most tiles have none. They run as `ingest-tiles-allowed-failures`, and compute runs whether or not that step fails; its failed tiles show as failed pods in `argo get`. The dataset list is ours, not upstream's: see `infra/list_datasets.py` for why, and add to it when upstream starts reading a new dataset.

**The tile list is derived, never typed in.** `list-tiles` prints the tiles the country's boundary touches, the same derivation upstream's CLIs use, each with the `--modulus` and `--residues` that select it alone. Each ingest pod passes those to upstream's ingest CLI, so the pods and the CLI can't disagree about which tile is which. It runs after `ingest-world` because that is the step that fetches the boundaries. An earlier version took a hand-typed tile count, and nothing checked it against the country.

**`derive` runs off to the side.** It needs only emit's cached output, so it starts once `compute` finishes and runs alongside `reduce` and `export`. Nothing downstream reads its output.

## One-off setup

Done once per cluster, or again whenever the cloud config changes.

**Identity.** The pods reach GCS through Workload Identity: the workflow's k8s service account is bound to a GCP service account with read/write access to the bucket. No keys go in the image or the Secrets. The platform team set this up; a new cluster or bucket needs the same binding before anything will run.

**The two Secrets.** `Config.from_dot_env()` reads a `.env` *file*, not the process environment, so each pod mounts a Secret at `/app/.env`. The cloud config is kept apart from the repo-root `.env`, which stays pointed at local paths for local runs:

```bash
cp infra/cloud.env.example infra/cloud.env   # gitignored; fill in real values
```

All six fields must be **present**, or `Config.from_dot_env()` raises: `INGEST_ROOT`, `SCRATCH_ROOT`, `EXPORT_ROOT`, `NUMBER_OF_DASK_WORKERS` (the per-pod Dask worker count; keep it at or below the pod's CPU limit), `USDA_NASS_API_KEY` and `HARVARD_DATAVERSE_GUESTBOOK_JSON`.

Only the ingest steps use the API keys. To keep the keys off the other pods, make `infra/cloud-nokeys.env`, a copy of `cloud.env` with the two key fields present but blank. Then create one Secret from each file, in the namespace the workflow runs in:

```bash
kubectl --context "$KUBE_CONTEXT" -n argo create secret generic <secret-with-keys> \
  --from-file=.env=infra/cloud.env
kubectl --context "$KUBE_CONTEXT" -n argo create secret generic <secret-no-keys> \
  --from-file=.env=infra/cloud-nokeys.env
```

The workflow names both Secrets. To change the config, delete and recreate them.

**The image.** Build it from a clean commit and push it, as in the header of `infra/Dockerfile`. Then set the workflow's image to the new tag.

**What a run leaves behind.** Pods and their autoscaled nodes go away by themselves. The Secrets, the images in the registry and the bucket's data stay until deleted. The data is the point of the run; before a continent-scale run, decide how long to keep `SCRATCH_ROOT` (see *Storage, not compute, is the first scale risk* in [`scale-out.md`](scale-out.md)).

## Running a country

You need the `argo` and `kubectl` CLIs, a kube context for the cluster (through the GKE connect gateway), and the workflow YAML. Then:

```bash
argo --context "$KUBE_CONTEXT" -n argo submit cornerstone-workflow.yaml \
  -p iso=CIV -p derive-deliverable=crop-class-emissions --generate-name cornerstone-civ-
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

- **`iso`:** one ISO 3166 alpha-3 code. `run_phase.py` accepts several countries, but the workflow passes a single value.
- **`ingest-allow-failures`:** a JSON list of tiled datasets whose failed tiles don't fail the run, by default `["DESCALS_OIL_PALM"]`.
- **`methodology`:** `STATISTICAL` (the default) or `JURISDICTIONAL_DIRECT`, which is US-only.
- **`derive-deliverable`:** one of `crop-class-emissions` (the default), `perennial-discounting-comparison` or `perennial-debug`. One deliverable per run. `perennial-debug` is written as zarr, not COG.

### Outputs

Paths are relative to `EXPORT_ROOT`:

| Step     | Writes                                  |
| -------- | --------------------------------------- |
| `export` | `cie-emissions/{tile}.tif`              |
| `mosaic` | `mosaics/cie-emissions-{iso}.vrt`       |
| `derive` | `{deliverable}/{tile}.tif` (or `.zarr`) |

Each deliverable has its own folder. Files at the top level of `EXPORT_ROOT`, or a `mosaics/{iso}.vrt`, come from images older than `91efe40` and are superseded.

Intermediate results (the ingest cache and scratch zarrs) stay under `INGEST_ROOT` and `SCRATCH_ROOT`.

## What to expect

### Timings

Measured on the first Côte d'Ivoire run. This was a cold bucket and ran **without** the pd-ssd scratch described below, so the disk-heavy steps should come in faster from now on:

| Step                                  | Per pod                                             |
| ------------------------------------- | --------------------------------------------------- |
| `ingest-world` (data already present) | ~1m                                                 |
| `list-tiles`                          | 14s                                                 |
| `ingest-tiles` (cold)                 | 2.5h to over 3h                                     |
| `ingest-tiles` (data already present) | minutes                                             |
| `compute`                             | 57–61m                                              |
| `reduce`                              | 2m                                                  |
| `export`                              | 1h30–1h54, vs ~50m with pd-ssd under the old driver |
| `derive` (`crop-class-emissions`)     | 38–47m                                              |
| `mosaic`                              | 39s                                                 |

### Reruns are cheap

Every stage is cached (`@storage.cache_to_*`). Rerunning a country, or `argo retry` after a failure, skips everything already in the bucket. When a cold `ingest-tiles` pod hit its 3h deadline, `argo retry` finished the remaining dataset in 5 minutes and the run went on to completion.

The flip side: **a new bucket means a cold run.** The cache keys don't include the bucket, so copying the `INGEST_ROOT` and `SCRATCH_ROOT` objects across at the same relative paths warms it again. And the cache is content-blind: changing code without bumping a stage's `version=` reads back the old result (see [`data_flow.md`](data_flow.md)).

### Disk is what limits speed

Ingest, export and derive each write tens of GB of uncompressed GeoTIFF locally before converting it to a COG. One cold ingest pod wrote about 128 GB. On the node's shared boot disk, those writes throttle to a crawl: I/O pressure reached 53% while CPU pressure stayed at zero.

The workflow therefore gives each pod its own **pd-ssd volume at `/localtmp`**, with `TMPDIR` and `CPL_TMPDIR` pointed at it, as the old driver did. The volume is created with the pod and deleted with it. Background is in [`../gke-disk-io-findings.md`](../gke-disk-io-findings.md). Don't shrink these volumes: pd-ssd throughput scales with size.

The volume is a generic ephemeral volume on GKE's built-in pd-ssd StorageClass (`premium-rwo`), so no StorageClass object of our own is needed. It works on any machine family, e2 included. Separately, the old driver set a small `ephemeral-storage` request and limit (2Gi / 8Gi) on each container. That covers only the container's own overlay (logs and anything not under `/localtmp`), so a stray writer evicts its own pod and not the node.

### Pod sizing, as the old driver measured it

The old driver's per-phase sizes, with the reasons it recorded. The workflow's sizes are close to these but not identical (see *Open questions*):

| Phase          | CPU req / limit | Memory req / limit | `/localtmp` | Deadline | Why                                                                                                                                                                                                                                 |
| -------------- | --------------- | ------------------ | ----------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ingest-world` | 2 / 4           | 4Gi / 24Gi         | 100Gi       | 4h       | Network-bound; each object is one whole-world download, MAPSPAM the largest                                                                                                                                                         |
| `ingest-tiles` | 2 / 4           | 4Gi / 24Gi         | 100Gi       | 2h       | Network-bound, one tile's worth. `/localtmp` holds ~8 GB-class COG temps × `--concurrency`                                                                                                                                          |
| `compute`      | 6 / 8           | 48Gi / 60Gi        | 20Gi        | 6h       | Peak ~39 GiB observed: about one tile per e2-highmem-8 node, with room for system pods. The deadline allows for a data-dense farmland tile. Results stream to GCS, so `/localtmp` only needs room for Dask spill and the GDAL cache |
| `reduce`       | 2 / 4           | 8Gi / 32Gi         | 10Gi        | 2h       | A groupby-sum over kilobyte-scale parquets, plus reading the admin-1 boundaries                                                                                                                                                     |
| `export`       | 2 / 4           | 8Gi / 24Gi         | 200Gi       | 3h       | See below                                                                                                                                                                                                                           |
| `mosaic`       | 1 / 2           | 2Gi / 8Gi          | 10Gi        | 1h       | Reads each tile COG's header and writes a small VRT; no pixels                                                                                                                                                                      |

`export` is limited by disk, not memory, since Dask writes in chunks. `cog_translate` first writes an **uncompressed** tiled intermediate, and only then compresses the final COG. That intermediate is ~33 GB for a 5-band float32 10° tile, so ~70 GB for the 11 `cie-` bands. 200Gi leaves headroom and clears GDAL's free-space check, which we keep on so that a too-small disk fails at the start rather than at 97%.

The highmem pool the compute pods use carries a `NoSchedule` taint, shared with other workloads, so pods bound for it need a matching toleration. Without one, the autoscaler won't even add a node.

## Gotchas

- **Pod logs disappear once a run is cleaned up.** Argo isn't set to archive them. Save any log you'll want (especially the `resource_monitor` JSON lines, which hold the memory and disk peaks) before the run is garbage-collected.
- **`argo get -o json` embeds raw log text**, which strict JSON parsers reject. Use `json.loads(..., strict=False)` in Python, or the table output.
- **`argo retry` restarts the run's clock.** The reported duration covers only the retry, not the time since first submission.
- **`derive` doesn't go through `run_phase.py`**, so it has no `resource_monitor` output. Use `kubectl top` or GKE metrics instead.
- **`mosaic` only covers CIE.** There is no mosaic for the `derive` deliverables yet.

## Open questions

- **Deadlines.** A cold `ingest-tiles` needed more than 3h without pd-ssd, and `export` came within 6 minutes of its 3h. Re-measure with pd-ssd before choosing new limits.
- **Secrets.** `reduce`, `export` and `mosaic` could run without the keys, as they did under the old driver.
- **Sizes and pools.** The workflow's memory requests and node pools differ slightly from the old driver's (*Pod sizing* above). Neither set has been measured at scale.
- **Where the workflow file lives.** It is outside the repo because it names infrastructure. A version with placeholders in place of those names could be committed if we want it under version control.
- **Several countries per run**, and several `derive` deliverables per run.
