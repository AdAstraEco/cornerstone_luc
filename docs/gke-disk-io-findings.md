# GKE disk-I/O for bursty geospatial ETL — findings (pass 1)

**Status:** first pass. Answers the high-value questions (§6.A, D, E and the core of B/C)
with GCP-documented figures. Items explicitly deferred to pass 2 are flagged
**[PASS 2]**. This is a research synthesis; nothing here has been tested on our cluster.

---

## TL;DR / recommended default

**Give these pods a dedicated node pool whose ephemeral storage is Local SSD (NVMe), and
point GDAL's temp dirs at it.** Concretely:

1. Create the batch node pool with `--ephemeral-storage-local-ssd count=N`. This moves the
   node's *entire* ephemeral layer — container writable layer, `/var/lib/containerd`,
   `/var/log/pods`, **and every `emptyDir`** — off the boot disk onto RAID-0 NVMe Local SSD.
2. Mount an `emptyDir` (default medium — **not** `medium: Memory`) at a scratch path and set
   `TMPDIR`, `CPL_TMPDIR`, and `GDAL`'s output temp location to it. Because the node pool is
   Local-SSD-backed, that `emptyDir` is automatically on NVMe.
3. Set `resources.requests`/`limits` for **`ephemeral-storage`** on the container and an
   `emptyDir.sizeLimit`, so an over-running pod is evicted **by itself, deterministically**,
   instead of hanging or triggering node-wide `DiskPressure`.

**Why this and not "just attach a bigger/faster PD":** the incident was two coupled
failures — (a) write-bandwidth saturation and (b) the Linux `balance_dirty_pages` throttle
collapsing throughput to ~8 MB/s once dirty pages outran a slow-flushing shared boot disk.
Local SSD attacks both: ~an order of magnitude more sustained write bandwidth *and* IOPS
than any reasonably-sized PD, node-local, and (with a dedicated pool) not shared with
unrelated pods. It is the standard GCP answer for "fast, isolated, disposable scratch."

---

## A. Storage architecture — the numbers that decide it

GCP PD performance is **`baseline + size_GiB × per-GiB-rate`, then capped by a per-VM ceiling
that scales with vCPU count** (you get the *lesser* of the disk limit and the VM limit).
([PD performance overview](https://docs.cloud.google.com/compute/docs/disks/performance))

| Option | Write throughput | IOPS | Notes |
|---|---|---|---|
| **Boot-disk overlay / `emptyDir` on boot disk** (today) | pd-balanced: **140 MiB/s baseline + 0.28 MiB/s per GiB**, VM-capped | 3,000 + 6/GiB | Shared by *all* pods + system writes on the node. This is what throttled us. |
| **pd-balanced** generic ephemeral volume | 140 + 0.28/GiB, max **1,200 MiB/s / 80,000 IOPS** per instance | 3,000 + 6/GiB | Per-*pod* isolation if one volume per pod; still network-attached, still VM-capped. |
| **pd-ssd** generic ephemeral volume | 240 + 0.48/GiB, max **1,200 MiB/s / 100,000 IOPS** | 6,000 + 30/GiB | Higher IOPS/GiB; same 1,200 MiB/s VM ceiling. |
| **Local SSD (NVMe)** ephemeral | **~90,000 write IOPS / partition**; aggregate to **6,240 MiB/s write** at 12 TB (32×375 GiB) on general-purpose | 170k read / 90k write per partition | Node-local, disposable, no VM-throughput cap of the PD kind. 375 GiB increments. |
([Local SSD disks](https://docs.cloud.google.com/compute/docs/disks/local-ssd),
[Local SSD on GKE](https://cloud.google.com/blog/products/containers-kubernetes/high-performance-aiml-storage-through-local-ssd-support-on-gke))

**Reading of our incident against these numbers.** A default GKE boot disk delivers
~140 MiB/s sustained *at best*; two pods sharing it, each generating 8-GB-class dirty-page
bursts faster than it flushes, put a writer into `D`-state on `balance_dirty_pages` and
forward progress fell to ~8 MB/s. So the boot disk's raw ceiling was *already* marginal for
one pod and hopeless for two — and the writeback throttle turned "marginal" into "crawl."
The fix is a device whose sustained write bandwidth is comfortably above aggregate demand:
Local SSD.

**Per-GiB scaling caveat (why you can't just grow a PD):** PD throughput scales with size
*only up to the per-VM cap*, and that cap scales with vCPUs. An **e2-highmem-8** (8 vCPU) is
a low tier for PD bandwidth — you will hit the VM ceiling, not the disk ceiling, well before
1,200 MiB/s. Local SSD sidesteps this because its bandwidth is delivered by the physically
attached NVMe devices, not metered against the VM's network-storage budget.

### Local SSD on GKE — provisioning facts (§A.3)
- Flag: `--ephemeral-storage-local-ssd count=N` at cluster/node-pool create (GKE ≥
  1.25.3-gke.1800; default on 3rd-gen+ machine series). GKE RAIDs the disks and mounts them
  under `/var/lib/kubelet`, `/var/log/pods`, `/var/lib/containerd`.
- **Data-loss semantics:** data is gone when the Pod terminates or the node is deleted /
  repaired / upgraded. **Fine for us** — local disk is pure scratch, durable state is GCS.
- **Autoscaler / scale-to-zero:** the scheduler is Local-SSD-aware; the cluster autoscaler
  scales the pool (including 0→N and back) normally. Works with our Indexed-Job model.
- **Sizing:** 375 GiB increments, up to ~9 TiB depending on machine type. Requires
  `n1-standard-1` or larger (not `e2-medium`).
- **Hard limitation:** you **cannot mix** Local-SSD-backed `emptyDir` and boot-disk `emptyDir`
  pods in the *same* node pool → put these batch pods in their **own** node pool.
([About Local SSD for GKE](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/local-ssd))

### Directing `/tmp` and GDAL temps (§A.4)
- Mount `emptyDir: {}` (default medium) at e.g. `/scratch`; set `TMPDIR=/scratch`,
  `CPL_TMPDIR=/scratch`, and ensure GDAL/rasterio COG creation writes its overviews/temps
  there. On a Local-SSD pool this `emptyDir` is NVMe automatically.
- **Do not** use `emptyDir.medium: Memory` — that's tmpfs and counts against pod **RAM**;
  for 8-GB-class temps it will blow the memory limit. Use disk-backed `emptyDir`.

---

## B. The dirty-page throttle — is it the right lever? (partial)

Short answer for pass 1: **`balance_dirty_pages` is a symptom, not the root cause. The
primary lever is a faster device (Local SSD), not sysctl tuning.** The throttle exists to
stop the writer outrunning the disk; once the disk can absorb the burst, the throttle stops
firing. Tuning `vm.dirty_ratio` / `vm.dirty_bytes` would only let *more* data buffer in RAM
before the same wall — on a shared node that risks larger, spikier flushes and worse
neighbours. Treat sysctl tuning as a secondary optimisation *after* moving to Local SSD.

**[PASS 2]** exact GKE-supported mechanism for node sysctls (node system-config /
`allowlistedUnsafeSysctls` / DaemonSet), whether these sysctls are host-global vs
per-cgroup on GKE's kernel, and whether `O_DIRECT`/`GDAL_CACHEMAX` measurably reduce
dirty-page pressure for COG creation.

---

## C. Avoiding local temps altogether (partial)

Direction for pass 1: writing the COG straight to GCS via GDAL **`/vsigs/`** can eliminate
the big *output* temp, but COG creation still needs local scratch for **overview building**
(GDAL builds overviews on a temp before assembling the final COG), so it *moves* rather than
*removes* the I/O. gcsfuse is **not** appropriate for GDAL temp I/O (random/seeky temp
access over a FUSE-to-object layer). Keep gcsfuse out of the scratch path.

**[PASS 2]** `/vsigs/` multipart-write throughput/reliability for 8-GB objects, whether
`CPL_VSIL_GDAL_TEMP` / streaming COG driver options remove the overview temp, and a
concrete recommendation on stream-to-GCS vs Local-SSD-then-upload.

---

## D. Monitoring / observability (core set)

Default-on / cheap:
- **Node ephemeral storage:** `kubelet_volume_stats_used_bytes` /
  `_available_bytes` (per PVC/emptyDir where local storage isolation is on), and node
  `ephemeral_storage_used_bytes`. Kubelet also drives the `DiskPressure` node condition.
- **Compute Engine disk metrics** (for PD-backed volumes):
  `compute.googleapis.com/instance/disk/{write_bytes_count, write_ops_count,
  throttled_*}` — throughput/IOPS and whether you're hitting caps.

Detecting the **throttled-but-not-dead** state we hit (§D.10): the tell is *high disk
write-time / saturation with near-zero CPU*. Watch node **disk utilisation %** /
`node_disk_io_time_weighted_seconds` (from node-exporter) and **I/O PSI**
(`/proc/pressure/io`) if exposed on the node — a rising I/O-pressure stall with flat CPU is
exactly the `balance_dirty_pages` signature, catchable from metrics instead of `exec`-ing in.

**[PASS 2]** which of these need Google Managed Prometheus / kube-state-metrics / node-exporter
DaemonSet vs are on by default in Cloud Monitoring; confirm PSI availability on GKE's node
image; concrete alert/SLO thresholds (§D.11).

---

## E. Clean failure & eviction (checklist)

- **`resources.requests.ephemeral-storage`** — scheduler bin-packs against this. Set it to
  the realistic per-pod scratch high-water mark so two pods aren't packed onto storage that
  can't hold both.
- **`resources.limits.ephemeral-storage`** — when the pod's local usage exceeds this, the
  **kubelet evicts *that pod alone*** (not neighbours), deterministically, and frees the
  node. This is the key lever turning "hang + node DiskPressure" into "one clean pod
  failure." ([ephemeral storage limits & eviction](https://jorijn.com/en/knowledge-base/kubernetes/storage/kubernetes-ephemeral-storage-limits-and-eviction/))
- **`emptyDir.sizeLimit`** — finer-grained cap on the scratch volume specifically; kubelet
  evicts the pod when the volume exceeds it, even below the pod-level limit. Belt-and-braces
  with the limit above.
- **Isolation (§E.14):** with per-pod `ephemeral-storage` limits + a dedicated Local-SSD
  pool, one greedy pod can't evict unrelated pods — it hits its own limit first. (Local SSD
  is shared *capacity* on the node, so the `ephemeral-storage` limit remains the real
  guardrail; size the pool for worst-case concurrent pods.)
- **Hung-pod guardrails (§E.13):** you already run `backoffLimitPerIndex` +
  `activeDeadlineSeconds`. Keep `activeDeadlineSeconds` set to a realistic multiple of a
  healthy tile's runtime so a throttled pod that *isn't* hitting a disk limit still gets
  killed and retried rather than silently burning to deadline.

On eviction: the evicted pod's ephemeral storage (incl. `emptyDir`) is reclaimed, freeing
the node for neighbours; with `restartPolicy: Never` + `backoffLimitPerIndex` the Indexed
Job retries that index on a fresh pod.

---

## F. Vertical scaling — N Dask workers in one pod

The intra-pod case is why "spread pods / one-per-node" is not a real fix. With Local SSD:
- One Local-SSD-backed `emptyDir` shared by all N workers gives them a common NVMe scratch
  pool — aggregate NVMe write bandwidth (thousands of MiB/s) is high enough that N
  concurrent GDAL temp streams don't re-hit the boot-disk wall.
- Give each worker its **own temp subdir** under the scratch mount (distinct `TMPDIR` per
  worker) to avoid name collisions; they can share one device.
- **Size the Local SSD for N × per-worker high-water mark** (each COG step needs
  ~16–24 GB of coexisting temps). E.g. 4 workers × ~24 GB ≈ 96 GB → one 375 GiB Local SSD
  is comfortable; scale disk count with worker count if you push N high.
- Set the pod's `ephemeral-storage` limit to the same N-aware figure so an over-parallel pod
  fails cleanly rather than filling the node.

---

## Decision guide

- **Boot disk / default `emptyDir` is fine** only for light, non-bursty temp use (small
  files, low concurrency). Not our workload.
- **Attach a pd-ssd generic ephemeral volume** if you want per-pod isolation without a
  dedicated pool *and* your bandwidth need is < the VM cap — but on 8-vCPU e2 the VM cap is
  low, so this is a weak middle option for us.
- **Local SSD ephemeral (recommended)** when you need sustained multi-hundred-MiB/s+ bursty
  writes with disposable data — our COG conversion. Dedicated node pool, scale-to-zero OK.
- **Stream to GCS (`/vsigs/`)** to shrink the *output* temp, but keep Local SSD for the
  unavoidable overview-building scratch. Not a full replacement — see §C.

---

## Open items still deferred (post pass 2)
1. Node sysctl mechanism on GKE + whether `O_DIRECT`/`GDAL_CACHEMAX` help (§B).
2. `/vsigs/` streaming COG numbers + does it remove the overview temp (§C).
3. Cost comparison (pd-ssd vs Hyperdisk vs Local SSD) for the burst model.

---

# Pass 2 — node pool, self-owned monitoring, and disk decoupled from the node

This pass answers four questions raised after reading the code: **(1)** monitoring we own,
not infra-reported; **(2)** a project node pool and its specs; **(3)** what "trade
performance for simplicity/predictability/visibility" leaves us; **(4)** the ability to
scale *vertically* and disk options not welded to node/pod config.

## Context correction from the code (supersedes the capacity framing above)

Reading the pipeline changed the problem shape — the pass-1 doc over-weighted capacity:

- **The TiB-scale grids never touch local disk.** `storage.write_dask_dataset_to_zarr`
  writes `harmonize`/`emit` zarr straight to `SCRATCH_ROOT` (a `gs://` URI) chunk-by-chunk
  via gcsfs. Local disk in compute holds only GDAL's 512 MiB block cache + any Dask spill.
- **Local disk is an *ingest*-only demand.** The only real scratch writer is
  `datasets/base.py:RasterDataset.ingest_a_tile`: download source GeoTIFFs → combine into
  one ~8 GB multiband file in a `tempfile.TemporaryDirectory()` (→ `/tmp` → boot-disk
  overlay) → `rio_cogeo.cog_translate(in_memory=False)` writes a second ~8 GB COG + overviews
  → upload to GCS → tmpdir deleted.
- **So the requirement is bandwidth + isolation for ~tens of GB *transient*, not capacity.**
  Two compounding amplifiers, both in code: ingest requests only 2 vCPU and declares **zero**
  ephemeral storage (`phase-job.yaml`), so the scheduler bin-packs several ingest pods onto
  one node's shared boot disk; and in-pod `--concurrency` (default 4, `run_phase.py`) runs up
  to 4 COG conversions into the same `/tmp` at once.

Net: the fix lives on the **ingest path**, is **manifest + a few env vars**, and does *not*
need big disks or the highmem pool.

## (1) Monitoring we own — read the kernel from inside the pod

Don't wait for Cloud Monitoring to tell us which resource capped. GKE nodes run **cgroup v2**
(COS), and the pod's own cgroup files are readable at `/sys/fs/cgroup/` from inside the
container. Wrap each stage in `run_phase.py` with a sampler thread (a few lines, `psutil`
optional) that logs a structured line every ~10–15 s and a summary at stage end. Read:

| Signal | File (in-pod) | Tells us |
|---|---|---|
| **I/O pressure (PSI)** | `/sys/fs/cgroup/io.pressure` | **The smoking gun.** `full avg10` high *while CPU is near-zero* is the exact `balance_dirty_pages` throttle signature we hit — catchable without `exec`. |
| Bytes written / rate | `/proc/self/io` (`write_bytes` deltas) | Actual write MB/s — literally the ~8 MB/s crawl we measured by hand. |
| Scratch free/used | `shutil.disk_usage(TMPDIR)` | Capacity headroom on the scratch mount; log %used. |
| Memory pressure (PSI) | `/sys/fs/cgroup/memory.pressure` | Reclaim stalls before OOM. |
| Memory now / max / events | `memory.current`, `memory.max`, `memory.events` | `high`/`oom`/`oom_kill` counters = "we hit the memory limit," definitively. |
| CPU throttling | `cpu.stat` (`throttled_usec`), `cpu.pressure` | Whether the CPU *limit* is the cap. |

Emit as JSON to stdout → lands in Cloud Logging with **no Managed Prometheus / node-exporter
dependency**. Per-stage summary to capture: peak RSS, peak scratch %used, total bytes
written, max io-PSI `full avg10`, and any `memory.events` increment. That single summary line
per stage per tile answers "what resource hit its limit, on which tile" from logs alone —
which is exactly the self-owned visibility asked for. (Node-wide `/proc/pressure/{io,memory}`
is also readable in-pod if we want the whole-node view for the bin-packing case.)

**This is the highest-leverage item and it's independent of every infra decision below** —
do it first; it also tells us empirically whether pd-ssd is enough before spending on faster
disk.

## (2) + (3) A project node pool, and what the simplicity trade leaves us

The machine-family facts that constrain the choice:

- **e2 supports *no* Local SSD** ([machine families](https://docs.cloud.google.com/compute/docs/general-purpose-machines)),
  and Hyperdisk on e2/n2 needs account-team approval
  ([Hyperdisk Balanced](https://docs.cloud.google.com/compute/docs/disks/hd-types/hyperdisk-balanced)).
  So on e2 the only self-serve fast scratch is a **PD generic ephemeral volume**.
- **e2 caps at e2-highmem-16 (16 vCPU / 128 GiB).** N2 scales to n2-highmem-128
  (128 vCPU / ~864 GiB). That ceiling difference *is* the vertical-scaling story (§4).
- Local SSD is **node-pool-wide, all-or-nothing, 375 GiB increments, tied to machine type** —
  the opposite of "not welded to node config."

Given the stated priority (**simplicity, predictability, visibility > peak performance**),
the recommendation is **not** Local SSD (pass 1's default flips):

> **Own pool: a single `n2-highmem-8` pool, autoscaling 0→N, with per-pod PD generic
> ephemeral volumes for scratch.** One pool covers both workloads (compute needs the RAM;
> ingest under-uses it, but scale-to-zero + short bursts make that cheap). The existing infra
> already supports per-phase pools (`NODE_POOL_INGEST` / `NODE_POOL` / `NODE_POOL_REDUCE`), so
> splitting later is a `cluster.env` edit, not a code change.

Why N2 over staying on e2:
- **Predictability:** N2 has dedicated vCPUs; e2 uses dynamic resource management with more
  performance variance — directly against "predictability."
- **Vertical runway:** N2 gives the *option* of a big single-pod node (§4); e2 dead-ends at
  16 vCPU.
- **Keeps fast-disk doors open:** N2 can take Local SSD or (with account-team) Hyperdisk if we
  ever want them, without another migration.

If cost dominates and the monitoring from (1) shows PD is comfortably enough, **staying on
e2-highmem-8 with PD generic ephemeral volumes is a legitimate cheaper variant** — you only
lose the vertical ceiling and some predictability.

**What the trade leaves us (the per-pod PD scratch volume):** attach a
[generic ephemeral volume](https://kubernetes.io/docs/concepts/storage/ephemeral-volumes/)
backed by pd-ssd via the PD CSI driver — one disk per pod, provisioned when the pod schedules,
deleted with it. Point `TMPDIR`/`CPL_TMPDIR` at it and set `ephemeral-storage` requests.

```yaml
# StorageClass (once): WaitForFirstConsumer so the disk lands in the pod's zone
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata: {name: cornerstone-scratch}
provisioner: pd.csi.storage.gke.io
parameters: {type: pd-ssd}
volumeBindingMode: WaitForFirstConsumer
---
# in the pod spec: a per-pod scratch disk, sized to the working set
volumes:
  - name: scratch
    ephemeral:
      volumeClaimTemplate:
        spec:
          accessModes: ["ReadWriteOnce"]
          storageClassName: cornerstone-scratch
          resources: {requests: {storage: 100Gi}}   # ingest; compute needs far less
# ... mount at /scratch, set env TMPDIR=/scratch, CPL_TMPDIR=/scratch
```

- **Performance we're accepting:** pd-ssd = 240 MiB/s baseline + 0.48 MiB/s per GiB, up to the
  per-VM cap ([PD performance](https://docs.cloud.google.com/compute/docs/disks/performance)).
  Lower than Local SSD's multi-GB/s — but the working set is tens of GB of transient temp, so
  a 100 GiB pd-ssd (~288 MiB/s) at low concurrency is comfortable, and crucially it is
  **isolated per pod** (no boot-disk neighbour contention) and **capacity-accounted**.
- **Predictable & simple:** it's a number in the pod spec, dynamically provisioned, works on
  **every machine family including e2**, and needs no node-pool-wide flag or RAID.
- **Clean failure:** pair with `resources.limits.ephemeral-storage` (for the container's own
  `/tmp`/logs) and the volume's size cap; an over-run fails that pod alone.
- **Also lower ingest `--concurrency`** (to 1–2) so one pod's scratch isn't multiplied by 4.

## (4) Vertical scaling, and disk that isn't welded to the node

The question — *"does GCP give good disk-like options not tied to node/pod config?"* — has a
clean answer: **yes, and it's the same generic-ephemeral-volume mechanism, ideally on
Hyperdisk Balanced.**

- **Generic ephemeral volume = disk decoupled from the node.** Its size/type live in the
  *pod* spec; CSI provisions and attaches it wherever the pod lands. Nothing about it is baked
  into the node pool (contrast Local SSD, which is). To go vertical you change *two numbers* —
  the node machine size and the volume's `requests.storage` — not the pool's identity.
- **Hyperdisk Balanced decouples *performance* from size and node too.** You provision
  throughput (140–2,400 MiB/s) and IOPS (3,000–160,000) **independently of capacity**; the
  first 3,000 IOPS + 140 MiB/s are free
  ([Hyperdisk Balanced](https://docs.cloud.google.com/compute/docs/disks/hd-types/hyperdisk-balanced)).
  So a single big vertical pod can get a *small* disk with *large* throughput — dial the knob,
  don't grow the disk. StorageClass just adds
  `provisioned-throughput-on-create` / `provisioned-iops-on-create`
  ([Hyperdisk on GKE](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/hyperdisk)).
  Caveat: **needs a Hyperdisk-native family (N2D / C3 / C4 / N4 …); e2 and n2 require
  account-team approval.** Still capped by the VM's per-instance limit.

**Concrete vertical-scaling shape:** one pod, `restartPolicy: Never`, on a big node
(e.g. `n2-highmem-32/-64`, or C3/N4 if going Hyperdisk), a Dask `LocalCluster` with N workers,
and **one generic ephemeral volume sized `N × ~24 GB`** with per-worker `TMPDIR`
subdirectories on it. Because the volume is per-pod, the *same manifest* scales from
one-tile-per-small-node to one-big-pod-many-workers by changing the node selector and two
size numbers — no node-pool-wide storage config, which is exactly the "not tied to node/pod
config" property wanted. (This also cleanly supersedes pass-1 §F, which assumed Local SSD.)

## Revised default (replaces pass-1 TL;DR)

1. **One project pool, `n2-highmem-8`, autoscale 0→N** (e2-highmem-8 is the cheaper fallback
   if monitoring shows PD is enough and the vertical ceiling isn't wanted).
2. **Per-pod pd-ssd generic ephemeral volume** at `/scratch`; `TMPDIR`/`CPL_TMPDIR` → it;
   `ephemeral-storage` requests/limits set. Ingest `--concurrency` 1–2.
3. **Self-owned cgroup-v2 sampler** in `run_phase.py` logging PSI + memory.events + scratch
   %used + write-rate per stage — do this first, it's free and infra-independent.
4. **Vertical option, when wanted:** bigger N2 (or C3/N4 + **Hyperdisk Balanced** to dial
   throughput independently) node running a single multi-worker pod, same per-pod volume
   sized to N. Nothing about the pool changes.

Decision guide, updated: **default = per-pod pd-ssd generic ephemeral volume** (simple,
predictable, machine-agnostic, visible). **Reach for Hyperdisk Balanced** only to tune disk
throughput independently for a big vertical pod (accept the machine-family constraint).
**Reach for Local SSD** only if monitoring proves pd/Hyperdisk genuinely can't sustain the
burst — accepting its node-welded, all-or-nothing tradeoffs. **Stream to GCS (`/vsigs/`)**
shrinks but doesn't remove the ingest temp (overviews still need local scratch).
