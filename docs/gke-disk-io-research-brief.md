# Research brief: sustaining high local disk I/O on GKE for a geospatial ETL

**Audience:** a research agent with web access. Produce a synthesis of GKE / GCP best
practice (with citations) that answers the questions in §6. This is a *research* task —
no access to our cluster is needed or assumed. Do not propose changes to our pipeline's
algorithms; focus on the storage/compute/observability layer.

**One-line problem:** our batch pods do large, bursty local disk writes (GDAL COG
conversion of multi-GB GeoTIFFs) and get throttled to a crawl by the Linux dirty-page
writeback limiter because temp files land on the node's shared boot-disk overlay. We need
the *right* way to give a pod (or many workers inside one pod) fast, isolated, adequately
sized scratch space — plus how to monitor it and how to fail cleanly when it's exhausted.

---

## 1. What we run

- **Workload:** a geospatial land-use-change pipeline. Per unit of work ("tile", a
  10°×10° region) it downloads several global raster datasets, **combines 5 yearly
  GeoTIFFs into one multi-band ~8 GB GeoTIFF, then runs a GDAL/rasterio Cloud-Optimized
  GeoTIFF (COG) conversion** (builds overviews; writes several more 8 GB-class temp files).
  CPU-light, **very disk-write-heavy in bursts**.
- **Inside each pod:** a Dask `LocalCluster` with N workers (the "vertical scaling" knob).
  Multiple workers can each be doing GDAL temp-heavy work concurrently → the per-*pod*
  disk demand rises with worker count.
- **Orchestration:** Kubernetes **Indexed Jobs** on **GKE**, one pod per tile, driven by a
  small Python driver. Node pools **autoscale 0→N** and back. Durable outputs go to **GCS**
  (a cache; a warm result makes a rerun a no-op). Pods authenticate to GCS via **Workload
  Identity**. Nodes are currently **e2-highmem-8** (8 vCPU / 64 GiB RAM, ~46 GiB boot-disk
  overlay, ~17.5 GiB of that reported as allocatable ephemeral-storage).

## 2. The incident we observed (evidence)

Two ingest pods were bin-packed onto one node (each requests only 2 vCPU, so two fit).
Both writing GLAD COG temps to `/tmp` (container overlay on the node boot disk) at once:

- Both processes near-**0 CPU** (4m and 18m millicores) yet "running" for 45+ min.
- A worker thread stuck in kernel state **`D` (uninterruptible sleep), wchan =
  `balance_dirty_pages`** — the kernel throttling the writer because dirty pages were being
  produced faster than the backing device could flush them.
- Forward progress ~**8 MB/s** (from `/proc/<pid>/io` `write_bytes` deltas) — i.e. crawling.
- Node overlay `df`: **26 GB used / 20 GB free**, with each pod's COG step needing another
  ~16–24 GB of coexisting temps → a real risk of filling the disk and triggering
  **DiskPressure eviction**.

So two coupled failure modes: **(a) write-bandwidth saturation** of the shared node disk,
and **(b) capacity exhaustion** of ephemeral storage.

## 3. Fixes we've already considered — and why they're insufficient

- **Spread pods (pod anti-affinity / fat resource requests → one pod per node).** Removes
  *inter-pod* contention but **does nothing for the intra-pod case**: one pod with many
  Dask workers writing GDAL temps to the same node disk hits the identical wall. It also
  caps horizontal density (N tiles → N nodes) and couples scheduling to machine type. We do
  **not** want a solution whose only lever is "put fewer things on a node" — that defeats
  vertical scaling.
- **Lower parallelism to 1.** Stopgap only; doesn't scale.

We want the storage layer itself to sustain the load, so both horizontal (more pods) and
vertical (more workers/pod) scaling stay open.

## 4. Environment / constraints the recommendations must respect

- GKE (Standard node pools, cluster autoscaler, scale-to-zero). Willing to add/adjust node
  pools, machine types, disk types, and pod specs. Willing to add StorageClasses / CSI
  config if standard on GKE.
- Batch Indexed Jobs (`restartPolicy: Never`, `backoffLimitPerIndex`, per-pod
  `activeDeadlineSeconds`). Durable state is in GCS; local disk is pure scratch and can be
  thrown away on failure.
- Cost-aware but not cost-constrained for short bursts (nodes scale to zero after).
- Prefer solutions that are **config/manifest-level** over pipeline rewrites, but do
  surface pipeline-level options (e.g. streaming to GCS) as alternatives with tradeoffs.

## 5. Prior art to check in our own stack (note for the researcher)

GDAL/rasterio honor several env knobs (`TMPDIR`, `CPL_TMPDIR`, `GDAL_CACHEMAX`) and can
read/write GCS directly via `/vsigs/`. Please establish current best practice for these in
a GKE context rather than assuming.

## 6. Research questions

Group your findings under these headings. Prioritize official GCP/GKE docs, Kubernetes
docs, and GDAL docs; use blogs/talks for corroboration and real-world numbers.

### A. Storage architecture for high local I/O on GKE
1. Compare, for large bursty scratch writes: **node boot-disk overlay/emptyDir**, an
   **attached Persistent Disk** (pd-balanced / pd-ssd) via a **generic ephemeral volume**,
   and **Local SSD (NVMe)** ephemeral storage on GKE. Throughput/IOPS ceilings, how they
   scale with disk size and machine vCPU count, latency, and cost.
2. What is the **actual write-throughput ceiling** of a GKE node boot disk, and how does
   PD bandwidth/IOPS scale **per GB provisioned** and **per vCPU** (there are documented
   per-instance and per-vCPU caps)? At what point must you switch to Local SSD?
3. **Local SSD on GKE**: how to provision (ephemeral-storage-local-ssd), how it maps to
   `/tmp`/emptyDir, its data-loss semantics (fine for scratch), size increments, and its
   interaction with the cluster autoscaler and scale-to-zero.
4. How to actually **direct container `/tmp` and GDAL temps** onto the chosen fast disk
   (emptyDir mount + `TMPDIR`/`CPL_TMPDIR`, or Local SSD ephemeral). Gotchas with
   memory-backed emptyDir (`medium: Memory` = tmpfs → consumes RAM).

### B. Throttling & the dirty-page writeback limiter
5. Best practice around `balance_dirty_pages` throttling for write-heavy workloads: is
   tuning `vm.dirty_ratio` / `vm.dirty_bytes` advisable **in containers on GKE** (these are
   node-level/sysctl, often host-global) — and is it even the right lever vs. just using a
   faster disk? Any GKE-supported way to set node sysctls (node config / DaemonSet)?
6. Does **`O_DIRECT`** or GDAL's streaming/`GDAL_CACHEMAX` tuning meaningfully change the
   dirty-page pressure for COG creation?

### C. Avoiding large local temps altogether
7. **Streaming COG creation / writing outputs directly to GCS** via GDAL `/vsigs/`
   (vs. gcsfuse). Does it eliminate the big local temp, or just move the I/O? Throughput,
   reliability, and multipart-write behavior for 8 GB-class objects.
8. **gcsfuse CSI on GKE** as a scratch/target: performance profile for large sequential
   writes, and whether it's appropriate for GDAL temp I/O (likely not — confirm).

### D. Monitoring / observability
9. Which **GKE / Cloud Monitoring metrics** expose node **ephemeral-storage usage**, disk
   **throughput and IOPS**, and **PV/PVC** usage? (e.g. `kubelet_volume_stats_*`,
   node `ephemeral_storage_used_bytes`, Compute Engine disk metrics.) Which are on by
   default vs. need Managed Prometheus / kube-state-metrics.
10. How to **detect the throttled-but-not-dead state** we hit (near-zero CPU, high D-state,
    `balance_dirty_pages`) from metrics/alerts rather than by exec-ing into a pod. Useful
    signals: disk write saturation %, `node_disk_io_time_weighted`, PSI (pressure stall
    information) for I/O if available on GKE nodes.
11. Recommended **alerts/SLOs** for a batch pipeline: ephemeral-storage nearing limit, disk
    saturated, pod runtime exceeding expected.

### E. Clean failure & eviction
12. How to make a pod that **overruns its scratch space fail fast and cleanly** instead of
    hanging or destabilizing the node: setting `resources.requests/limits` for
    **`ephemeral-storage`** (so the kubelet evicts *that* pod deterministically), and how
    that interacts with emptyDir `sizeLimit` and Local SSD. What does the eviction look
    like, and does it cleanly free the node for neighbors?
13. Best practice to keep a **hung/throttled** batch pod from silently burning its deadline:
    liveness/startup probes for batch pods, `activeDeadlineSeconds`, and pod-failure
    signals that let a controller retry a different way. (We already use
    `backoffLimitPerIndex` + per-pod `activeDeadlineSeconds`.)
14. Node-level protection: how to stop one greedy pod's disk use from evicting unrelated
    pods (ephemeral-storage isolation, dedicated scratch volumes per pod).

### F. Vertical-scaling angle (the case that rules out "fewer pods")
15. For **one pod with N Dask workers** all doing GDAL temp I/O: how to size and isolate
    local scratch (per-worker temp dirs, Local SSD sizing vs N, whether to give each worker
    its own volume) so that adding workers doesn't re-create the bandwidth/capacity wall.

## 7. Desired deliverable

A concise written synthesis with:
- A recommended **default** (what disk type + mount + env config we should adopt for these
  pods) and the reasoning, with GCP-documented throughput/IOPS/cost figures.
- A short **decision guide**: when boot disk is fine, when to attach pd-ssd, when to use
  Local SSD, when to stream to GCS instead.
- A **monitoring + alerting** checklist (specific metric names + where they come from).
- A **clean-failure** checklist (specific spec fields: ephemeral-storage requests/limits,
  emptyDir sizeLimit, deadlines/probes) and the eviction behavior each produces.
- Citations (prefer official docs) and any notable real-world benchmarks.

Flag anything that contradicts the framing above (e.g. if `balance_dirty_pages` tuning is a
red herring, say so and explain why).
