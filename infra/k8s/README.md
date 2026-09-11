# Deploy notes

Operational companion to [`docs/orbae/cornerstone-k8s.md`](../../docs/orbae/cornerstone-k8s.md) (the feasibility cornerstone) and [`docs/orbae/scale-out.md`](../../docs/orbae/scale-out.md) (how it scales). Everything targets one namespace of a nonprod-shared GKE cluster and uses only namespace-level privileges. The unit of work is **one Indexed Job per phase of a run, one pod per tile** — no dask scheduler, no Argo controller, no change to `jdluc`.

## Infra config

Cluster-specific identifiers (project, cluster/context, namespace, KSA, node pools, image, bucket) live in `infra/cluster.env`, **not** hard-coded in tracked files. Copy the template, fill it in, and source it — the commands below use its variables:

```bash
cp infra/cluster.env.example infra/cluster.env   # then edit; infra/cluster.env is gitignored
source infra/cluster.env
```

## Prerequisites

- Context set to the target cluster: `kubectl config use-context "$KUBE_CONTEXT"`
- The image built and pushed (see [`infra/Dockerfile`](../Dockerfile)); `$IMAGE` comes from `cluster.env`.

## The cloud `.env` Secret

The cloud config is kept **separate** from the repo-root `.env` (which stays pointed at local paths for local runs). Copy the template and fill in real values:

```bash
cp infra/cloud.env.example infra/cloud.env   # then edit; infra/cloud.env is gitignored
```

The roots live under the `cornerstone/` prefix of the bucket in `cluster.env`: `INGEST_ROOT = gs://$GCS_BUCKET/cornerstone/ingest` and `.../cornerstone/scratch`.

`Config.from_dot_env()` reads a `.env` **file**, not process env, so the pods mount `infra/cloud.env` as a Secret at `/app/.env`:

```bash
kubectl -n "$K8S_NAMESPACE" create secret generic "$K8S_SECRET" --from-file=.env=infra/cloud.env
```

All six fields must be **present** or `Config.from_dot_env()` raises: `INGEST_ROOT`, `SCRATCH_ROOT`, `EXPORT_ROOT` (where the emissions COGs / mosaic VRTs are written — a terminal deliverable, sibling to scratch), `NUMBER_OF_DASK_WORKERS` (the *inner* per-pod dask worker count — keep it ≤ the pod's cpu limit), `USDA_NASS_API_KEY`, `HARVARD_DATAVERSE_GUESTBOOK_JSON`.

Only the two ingest phases actually *use* the API keys. To keep them off the compute nodes, create a second Secret whose key fields are present but blank, and point `K8S_SECRET_COMPUTE` at it:

```bash
kubectl -n "$K8S_NAMESPACE" create secret generic cornerstone-env-nokeys --from-file=.env=infra/cloud-nokeys.env
```

## Running an AOI

[`infra/run_aoi.py`](../run_aoi.py) resolves the tiles an AOI touches and submits one Indexed Job per phase, waiting for each before starting the next:

```
ingest-world  ->  ingest-tiles  ->  [barrier]  ->  compute  ->  [barrier]  ->  reduce
(1 pod)           (1 pod / tile)                   (1 pod / tile)              (1 pod)
```

Each pod maps its `JOB_COMPLETION_INDEX` onto the AOI's sorted tile list, recomputed in-pod from the same country arguments — so no tile list is plumbed through a ConfigMap. Per-pod work is [`infra/run_phase.py`](../run_phase.py); the Job template is [`phase-job.yaml`](phase-job.yaml) (a template — do not `kubectl apply` it by hand).

```bash
source infra/cluster.env

# render every phase's manifest and list the tiles, but submit nothing
# (works even before cluster.env is filled -- unset infra stays a visible ${...})
uv run python infra/run_aoi.py HND --dry-run

# the real thing: all four phases, with barriers between them
uv run python infra/run_aoi.py HND

# or one phase at a time; the @cache_to_* caches make any phase safe to repeat
uv run python infra/run_aoi.py USA MEX --phases ingest-tiles --parallelism 16
uv run python infra/run_aoi.py USA MEX --phases compute reduce --parallelism 16
```

Scaling knob: `--parallelism` × the node pool autoscaler. A crashed or abandoned run is "rerun the driver" — warm tiles are no-ops. Reusing `--run-id` reuses the Job names, so an already-`Complete` phase is skipped.

```bash
kubectl -n "$K8S_NAMESPACE" get jobs,pods -l app=cornerstone -w         # watch progress
kubectl -n "$K8S_NAMESPACE" logs -l phase=compute --tail=20 -f          # tail one phase
kubectl -n "$K8S_NAMESPACE" get pods -l run-id=09101423                 # one run's pods
```

## Cleanup & residue

**Baseline: leave nothing running.** After every run, confirm no Jobs/pods remain and nodes have drained:

```bash
kubectl -n "$K8S_NAMESPACE" delete jobs -l app=cornerstone      # remove all cornerstone Jobs + pods
kubectl -n "$K8S_NAMESPACE" get jobs,pods -l app=cornerstone    # expect: no resources
kubectl get nodes -l cloud.google.com/gke-nodepool="$NODE_POOL"   # expect: none, once drained
```

Completed/failed Jobs also self-delete via `ttlSecondsAfterFinished` (30 min); once their pods are gone the node pool autoscales back to zero (~10–15 min), so an idle cluster costs nothing. Note the TTL deletes pods and their logs too — pull logs before it fires, or read them from Cloud Logging.

### Everything this changes, and how it's reverted

| Residue                                                                                       | Left behind?                                            | To restore prior state                                             |
| --------------------------------------------------------------------------------------------- | ------------------------------------------------------- | ------------------------------------------------------------------ |
| **Jobs + pods**                                                                               | no — TTL 30 min + explicit delete                       | `kubectl -n "$K8S_NAMESPACE" delete jobs -l app=cornerstone`       |
| **Autoscaled nodes** in the node pools                                                        | no — scale back to 0 when idle                          | automatic; verify with `kubectl get nodes` above                   |
| **Secrets** (`$K8S_SECRET`, `$K8S_SECRET_COMPUTE`)                                            | yes, until deleted                                      | `kubectl -n "$K8S_NAMESPACE" delete secret "$K8S_SECRET"`          |
| **Container image** (`$IMAGE`) in Artifact Registry                                           | yes — persists (storage cost)                           | `gcloud artifacts docker images delete "$IMAGE"`                   |
| **GCS data** under `cornerstone/ingest` + `cornerstone/scratch`                               | **yes — intended.** Outputs are the point; safe to keep | delete the prefixes only if you want a clean slate                 |
| **Local**: kube-context selection, built docker image, `infra/cloud.env`, `infra/cluster.env` | yes (laptop only)                                       | `docker image rm` the local build if desired; nothing cluster-side |

Before a continent-scale run, decide retention for `SCRATCH_ROOT` — see *Storage, not compute, is the first scale risk* in [`docs/orbae/scale-out.md`](../../docs/orbae/scale-out.md).

### Not touched at all

No new IAM (reuses the existing KSA ↔ GSA Workload Identity binding), no cluster-wide objects, no CRDs, no changes to the node pool config or its autoscaler, no changes to any other namespace. The only cluster-scope effect is transient nodes the autoscaler adds and removes on its own.
