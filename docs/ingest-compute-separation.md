# Design note: separating ingest from compute

Working notes toward a systematic split of **ingest** (fetch source data → warm `INGEST_ROOT`)
from **compute** (harmonize → emit → attribute → trace → `SCRATCH_ROOT`). Prompted by the
one-off `us-ingest` job.

**Status:** the *Target systematic design* below is implemented by the phase driver in
[`scale-out.md`](scale-out.md) (§*As built*) — `infra/run_aoi.py` + `infra/run_phase.py`, with
ingest split into `ingest-world` / `ingest-tiles` and compute running `--skip-ingest` against
the warm cache on a separate Secret. Orchestration is a driver script, not Argo (see that
note's options and revisit triggers). Item 4, **versioning**, is *not* done.

## Why separate them

They are genuinely different workloads:

| | Ingest | Compute |
| --- | --- | --- |
| Bound by | network / I/O | memory + CPU |
| Peak memory (per tile, observed) | ~15 GB | ~39 GB (8 workers) |
| Right node | standard (e2-standard-8) | highmem (e2-highmem-8) |
| Needs 3rd-party API keys | **yes** (USDA, Harvard Dataverse) | **no** (reads warm cache) |
| Cadence | once per AOI × source-version | every code change / rerun |
| Idempotent | yes (`ingest_a_tile` skips existing) | yes (`@cache_to_*` skips existing) |

Consequences that make separation worthwhile:
- **Credential isolation** — only the ingest stage carries the API-key Secret; compute workers never see it (they hit a pre-warmed cache). Smaller secret blast radius.
- **Cost** — ingest runs on cheap standard nodes; only compute needs highmem.
- **Reuse** — `INGEST_ROOT` is a shared, read-mostly cache. Ingest once; many compute runs (different code versions, methodologies, reruns) reuse it.
- **Failure isolation** — a flaky upstream source (mutable Dataverse/GFW endpoints) fails an ingest job, not a compute run.
- **Independent scheduling/scaling** — ingest can run ahead of compute, at its own pace.

## What the code already supports

- `harmonize.workflow(skip_ingest=True)` reads a pre-warmed cache without touching sources.
- `jdluc.ingest` is a standalone CLI; `ingest_a_tile` is idempotent (writes only if absent).
- So the decoupling needs orchestration, not pipeline surgery.

## Current one-off approach (this spike)

- **Ingest job** (`.cache/us-ingest-job.yaml`): ingest-only, standard pool, best-effort
  (continues past a per-dataset failure and reports), no compute stages. Warms `INGEST_ROOT`.
- **Compute job**: currently still runs its own ingest (`skip_ingest=False`) as cache hits.
  To fully decouple, compute jobs should run with `--skip-ingest` against the warm cache.

## Target systematic design

1. **Ingest stage** — fan out over the `(dataset × tile)` matrix for an AOI. `ingest.workflow`
   already has a concurrency knob, but the CLI runs datasets serially; parallelize with a Job
   `parallelism`/indexed Job, or an Argo `withParam` over the matrix. Idempotent + resumable.
   Holds the API-key Secret. Runs on standard nodes. Output: populated `INGEST_ROOT`.
2. **Compute stage** — fan out per tile with `skip_ingest=True`, highmem nodes, reads
   `INGEST_ROOT`. Then the consolidation reduce (attribute/trace `groupby-sum`) — one pod per
   country for now (validated on El Salvador), a dedicated reduce step at larger scale.
3. **Orchestration** — a dependency edge: an AOI's compute waits on its ingest. Natural fits:
   **Argo Workflows** (DAG: ingest fan-out → barrier → compute fan-out → reduce), or Helm-
   templated Jobs driven by a small controller. (See the k8s-config note in
   `docs/cornerstone-k8s.md` — hand-rolled templating should become Helm/Kustomize/indexed Jobs.)
4. **Versioning** — `INGEST_ROOT` keyed by source dataset version (re-ingest only on a source
   bump); `SCRATCH_ROOT` keyed by code `version=` ints. Pin mutable sources (Dataverse DOIs,
   GFW versions) so ingest is reproducible.

## Guardrails

- Ingest **best-effort** (log + continue past a missing tile — e.g. ocean/edge tiles), compute
  **strict**.
- Both idempotent, so re-running is safe and resumes from cache.
- Keep `activeDeadlineSeconds` + short `ttlSecondsAfterFinished` on every job (fail-safe cleanup).
