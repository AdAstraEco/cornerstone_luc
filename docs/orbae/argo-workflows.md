# Running the pipeline on Argo Workflows

**Status:** our preferred way to run the pipeline in the cloud, as of 2026-10-15. It replaces the
hand-rolled driver (`infra/run_aoi.py` + `infra/k8s/phase-job.yaml`) described in
[`scale-out.md`](scale-out.md). Proven end to end on Honduras (1 tile) and Côte d'Ivoire
(2 tiles), on our nonprod GKE cluster. This note is written from the user's side: what you
need, how to run a country, and what to expect. It is not a guide to operating Argo itself.

> **This repo is public.** The cluster, project, bucket, image registry and node-pool names
> are deliberately absent from this note. They live in the gitignored `infra/cluster.env` and
> `infra/cloud.env`, and in the workflow file, which is kept outside the repo for the same
> reason (it is shared within the team).

## Why Argo now

[`scale-out.md`](scale-out.md) chose a small driver script over Argo, and listed the triggers
for revisiting that: several people running AOIs at once, wanting a UI, the driver growing
beyond a linear chain. Those have now arrived. Argo is also now installed on the cluster as
part of its own infrastructure, so we no longer have to run a controller ourselves, which was
the main argument against it.

What did **not** change: `jdluc` and `infra/run_phase.py` run unmodified inside the pods, one
pod per tile per phase. Argo replaces only the driver: sequencing, fan-out, retries and
visibility. The "no new compute platform, `jdluc` unmodified" principle of the earlier notes
still holds.

## The pieces

| Piece | What it is | Where it lives |
|---|---|---|
| Image | `infra/Dockerfile`, built from a commit and tagged with its short SHA | the team's Artifact Registry |
| Workflow | one Argo `Workflow` YAML: the DAG below, pod sizes, node pools | outside the repo (team-shared) |
| Config | each pod's `.env`: `INGEST_ROOT`, `SCRATCH_ROOT`, `EXPORT_ROOT`, API keys | two k8s Secrets, built from `infra/cloud.env` |
| Identity | the `argo-workflow` service account, bound to a GCP service account with bucket access | set up by the platform team |
| Data | ingest cache, scratch zarrs, exported deliverables | one GCS bucket, set in the Secrets |

The bucket is set **only** in the Secrets. To point a run at a different bucket, change
`infra/cloud.env`, recreate the Secrets, and submit. Neither the image nor the workflow
changes.

## The DAG

Each run covers one country (`iso`):

```
ingest-world -> list-tiles -> ingest-tiles[tile] -> compute[tile] -+-> reduce -> export[tile] -> mosaic
                                                                  +-> derive[tile]
```

| Step | Pods | Does | Secret |
|---|---|---|---|
| `ingest-world` | 1 | Whole-world datasets: boundaries, climate zones, MAPSPAM, NASS | with keys |
| `list-tiles` | 1 | Prints the country's sorted 10° tiles as JSON, which the per-tile steps fan out over | no keys |
| `ingest-tiles` | 1 per tile | Fetches the per-tile datasets into `INGEST_ROOT` | with keys |
| `compute` | 1 per tile | harmonize → emit → attribute, with `--skip-ingest` | no keys |
| `reduce` | 1 | `trace.workflow` over the country: the cross-tile merge | with keys* |
| `export` | 1 per tile | Writes the CIE emissions COG | with keys* |
| `mosaic` | 1 | Writes a VRT over the country's CIE COGs | with keys* |
| `derive` | 1 per tile | Writes one crop-class deliverable (`jdluc/derive.py`) from emit's cache | no keys |

\* These don't need the source-API keys. The old driver ran them without keys; tightening
this is an open question below.

**The tile list is derived, never typed in.** `list-tiles` runs `run_phase.get_tile_ids`, the
same derivation upstream uses: the tiles a country's boundary touches. It runs after
`ingest-world` because that is the step that fetches the boundaries. An earlier version took a
hand-typed tile count, and nothing checked it against the country.

**`derive` runs off to the side.** It needs only emit's cached output, so it starts once
`compute` finishes and runs alongside `reduce` and `export`. Nothing downstream reads its
output.

## Running a country

You need the `argo` and `kubectl` CLIs, a kube context for the cluster (through the GKE connect
gateway), and the workflow YAML. Then:

```bash
argo --context "$KUBE_CONTEXT" -n argo submit cornerstone-workflow.yaml \
  -p iso=CIV -p derive-deliverable=crop-class-emissions --generate-name cornerstone-civ-
```

| To | Run |
|---|---|
| See recent runs | `argo -n argo list --since 24h` |
| Follow one live | `argo -n argo watch <name>` |
| See the step tree | `argo -n argo get <name>` |
| Read a pod's log | `argo -n argo logs <name> <pod-name>` |
| Re-run after a failure | `argo -n argo retry <name>` |
| Stop a run | `argo -n argo stop <name>` |

(Each of these also needs `--context "$KUBE_CONTEXT"`.)

The Argo UI shows the same information. Ask the platform team for its address.

### Parameters

- **`iso`:** one ISO 3166 alpha-3 code. `run_phase.py` accepts several countries, but the
  workflow passes a single value.
- **`derive-deliverable`:** one of `crop-class-emissions` (the default),
  `perennial-discounting-comparison` or `perennial-debug`. One deliverable per run.
  `perennial-debug` is written as zarr, not COG.

### Outputs

Paths are relative to `EXPORT_ROOT`:

| Step | Writes |
|---|---|
| `export` | `cie-emissions/{tile}.tif` |
| `mosaic` | `mosaics/cie-emissions-{iso}.vrt` |
| `derive` | `{deliverable}/{tile}.tif` (or `.zarr`) |

Each deliverable has its own folder. Files at the top level of `EXPORT_ROOT`, or a
`mosaics/{iso}.vrt`, come from images older than `91efe40` and are superseded.

Intermediate results (the ingest cache and scratch zarrs) stay under `INGEST_ROOT` and
`SCRATCH_ROOT`.

## What to expect

### Timings

Measured on the first Côte d'Ivoire run. This was a cold bucket and ran **without** the pd-ssd
scratch described below, so the disk-heavy steps should come in faster from now on:

| Step | Per pod |
|---|---|
| `ingest-world` (data already present) | ~1m |
| `list-tiles` | 14s |
| `ingest-tiles` (cold) | 2.5h to over 3h |
| `ingest-tiles` (data already present) | minutes |
| `compute` | 57–61m |
| `reduce` | 2m |
| `export` | 1h30–1h54, vs ~50m with pd-ssd under the old driver |
| `derive` (`crop-class-emissions`) | 38–47m |
| `mosaic` | 39s |

### Reruns are cheap

Every stage is cached (`@storage.cache_to_*`). Rerunning a country, or `argo retry` after a
failure, skips everything already in the bucket. When a cold `ingest-tiles` pod hit its 3h
deadline, `argo retry` finished the remaining dataset in 5 minutes and the run went on to
completion.

The flip side: **a new bucket means a cold run.** The cache keys don't include the bucket, so
copying the `INGEST_ROOT` and `SCRATCH_ROOT` objects across at the same relative paths warms it
again. And the cache is content-blind: changing code without bumping a stage's `version=` reads
back the old result (see [`data_flow.md`](data_flow.md)).

### Disk is what limits speed

Ingest, export and derive each write tens of GB of uncompressed GeoTIFF locally before
converting it to a COG. One cold ingest pod wrote about 128 GB. On the node's shared boot disk,
those writes throttle to a crawl: I/O pressure reached 53% while CPU pressure stayed at zero.

The workflow therefore gives each pod its own **pd-ssd volume at `/localtmp`**, with `TMPDIR`
and `CPL_TMPDIR` pointed at it, as the old driver did. The volume is created with the pod and
deleted with it. Background is in [`../gke-disk-io-findings.md`](../gke-disk-io-findings.md).
Don't shrink these volumes: pd-ssd throughput scales with size.

## Gotchas

- **Pod logs disappear once a run is cleaned up.** Argo isn't set to archive them. Save any
  log you'll want (especially the `resource_monitor` JSON lines, which hold the memory and disk
  peaks) before the run is garbage-collected.
- **`argo get -o json` embeds raw log text**, which strict JSON parsers reject. Use
  `json.loads(..., strict=False)` in Python, or the table output.
- **`argo retry` restarts the run's clock.** The reported duration covers only the retry, not
  the time since first submission.
- **`derive` doesn't go through `run_phase.py`**, so it has no `resource_monitor` output.
  Use `kubectl top` or GKE metrics instead.
- **`mosaic` only covers CIE.** There is no mosaic for the `derive` deliverables yet.

## Open questions

- **Deadlines.** A cold `ingest-tiles` needed more than 3h without pd-ssd, and `export` came
  within 6 minutes of its 3h. Re-measure with pd-ssd before choosing new limits.
- **Secrets.** `reduce`, `export` and `mosaic` could run without the keys, as they did under
  the old driver.
- **Sizes and pools.** The workflow's memory requests and node pools differ slightly from the
  old driver's `PHASE_TO_SPEC` in `infra/run_aoi.py`. Neither set has been measured at scale.
- **Where the workflow file lives.** It is outside the repo because it names infrastructure. A
  version with placeholders in place of those names could be committed if we want it under
  version control.
- **Several countries per run**, and several `derive` deliverables per run.
