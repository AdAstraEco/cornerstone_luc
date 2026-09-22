# Spike deploy notes

Operational companion to [`docs/cornerstone-k8s.md`](../../docs/cornerstone-k8s.md). **Nothing here has been applied.** Everything targets one namespace of a nonprod-shared GKE cluster and uses only namespace-level privileges. The unit of work is **one k8s Job per tile** (approach B) — no dask scheduler, no change to `jdluc`.

## Infra config

Cluster-specific identifiers (project, cluster/context, namespace, KSA, node pool, image, bucket) live in `infra/cluster.env`, **not** hard-coded in tracked files. Copy the template, fill it in, and source it — the commands below use its variables:

```bash
cp infra/cluster.env.example infra/cluster.env   # then edit; infra/cluster.env is gitignored
source infra/cluster.env
```

## Prerequisites

- Context set to the target cluster: `kubectl config use-context "$KUBE_CONTEXT"`
- The cornerstone image built and pushed (see [`infra/Dockerfile`](../Dockerfile)); `$IMAGE` comes from `cluster.env`.

## The cloud `.env` Secret

The cloud config is kept **separate** from the repo-root `.env` (which stays pointed at local paths for local runs). Copy the template and fill in real values:

```bash
cp infra/cloud.env.example infra/cloud.env   # then edit; infra/cloud.env is gitignored
```

For this cornerstone the roots live under the `cornerstone/` prefix of the bucket in `cluster.env`:
`INGEST_ROOT = gs://$GCS_BUCKET/cornerstone/ingest` and `.../cornerstone/scratch` (both folders already created).

`Config.from_dot_env()` reads a `.env` **file**, not process env, so the pods mount `infra/cloud.env` as a Secret at `/app/.env`:

```bash
kubectl -n "$K8S_NAMESPACE" create secret generic "$K8S_SECRET" --from-file=.env=infra/cloud.env
```

Minimum keys: `INGEST_ROOT`, `SCRATCH_ROOT`, `NUMBER_OF_DASK_WORKERS` (the *inner* per-tile thread count, ~8), plus `USDA_NASS_API_KEY` and `HARVARD_DATAVERSE_GUESTBOOK_JSON` if workers will ingest. To avoid needing the API keys on workers, pre-ingest the target once from a trusted host so the stages hit the `SCRATCH_ROOT` cache and skip ingest.

## Milestones

`infra/run-tiles.py` resolves the tiles a country touches and submits one Job per tile, reading infra values from `cluster.env` and shelling out to `kubectl` with your current context.

```bash
source infra/cluster.env

# M0 -- render manifests, list tiles, submit nothing (works even before cluster.env is filled)
uv run python infra/run-tiles.py HND --dry-run

# M1 -- one real tile end-to-end. Submit the single-tile stage, then diff its output zarr
# against a known-good local run (see docs/cornerstone-k8s.md for the correctness check).
uv run python infra/run-tiles.py HND --stage harmonize

# M2 -- submit every tile HND touches; the node pool autoscales to run them concurrently.
uv run python infra/run-tiles.py HND --stage emit
kubectl -n "$K8S_NAMESPACE" get jobs -l app=cornerstone -w         # watch progress
kubectl -n "$K8S_NAMESPACE" logs -l app=cornerstone --tail=20 -f   # tail pod logs
```

## Cleanup & residue

**Baseline: leave nothing running.** After every run, confirm no Jobs/pods remain and nodes have drained:

```bash
kubectl -n "$K8S_NAMESPACE" delete jobs -l app=cornerstone      # remove all cornerstone Jobs + pods
kubectl -n "$K8S_NAMESPACE" get jobs,pods -l app=cornerstone    # expect: no resources
kubectl get nodes -l cloud.google.com/gke-nodepool="$NODE_POOL"   # expect: none, once drained
```

Completed/failed Jobs also self-delete via `ttlSecondsAfterFinished` (1h); once their pods are gone the node pool autoscales back to zero (~10–15 min), so an idle cornerstone costs nothing.

### Everything the cornerstone changes, and how it's reverted

| Residue | Left behind? | To restore prior state |
| --- | --- | --- |
| **Jobs + pods** | no — TTL 1h + explicit delete | `kubectl -n "$K8S_NAMESPACE" delete jobs -l app=cornerstone` |
| **Autoscaled nodes** in the node pool | no — scale back to 0 when idle | automatic; verify with `kubectl get nodes` above |
| **Secret** (`$K8S_SECRET`) | yes, until deleted | `kubectl -n "$K8S_NAMESPACE" delete secret "$K8S_SECRET"` |
| **Container image** (`$IMAGE`) in Artifact Registry | yes — persists (storage cost) | `gcloud artifacts docker images delete "$IMAGE"` |
| **GCS data** under `cornerstone/ingest` + `cornerstone/scratch` | **yes — intended.** Outputs are the point; safe to keep | delete the prefixes only if you want a clean slate |
| **Local**: kube-context selection, built docker image, `infra/cloud.env`, `infra/cluster.env` | yes (laptop only) | `docker image rm` the local build if desired; nothing cluster-side |

### Not touched at all

No new IAM (reuses the existing KSA ↔ GSA Workload Identity binding), no cluster-wide objects, no CRDs, no changes to the node pool config or its autoscaler, no changes to any other namespace. The only cluster-scope effect is transient nodes the autoscaler adds and removes on its own.
