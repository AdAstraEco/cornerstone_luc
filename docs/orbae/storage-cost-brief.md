# Investigation brief: how much of our storage cost is inherent?

**Audience:** an investigating agent (or human) with access to this repo and the local scratch
data. **Deliverable:** a written report (`docs/storage-cost-findings.md`) answering the core
question with measurements, not estimates, wherever local data allows.

## Core question

The pipeline materializes TiB-scale per-pixel intermediates (see `docs/scale-out.md`
§"Storage, not compute"). **How much of that footprint is an inherent cost of the
architecture** (persisting per-pixel rasters at every stage, on a global 30 m grid, for
inspectability — see `architecture.md` §"Design ethos") **versus an artifact of the current
implementation** (default encodings, generous dtypes, redundant variables)? Rank the possible
savings by impact and effort; identify the quick wins.

## Established facts (verified 2026-09, local Honduras run)

- One 10° tile = 40000×40000 pixels at 0.00025° (~30 m), the GLAD native grid.
- Per tile, uncompressed: **harmonize ≈ 60 GiB** (10 float32 vars), **emit ≈ 119 GiB**
  (20 float32 vars). North America = 51 tiles → ~3 / ~6 TiB uncompressed.
- Local reference stores (tile `20N_090W`, mostly ocean, compresses ~135:1 — do NOT
  extrapolate its ratios to land-dense tiles):
  - harmonize: `<SCRATCH_ROOT>/5727f9b5df5b.zarr` (4.0 GiB on disk)
  - emit: `<SCRATCH_ROOT>/ca71c82005a9.zarr` (879 MiB on disk)
- Current encoding is **zarr v3 defaults, untouched by the code**
  (`storage.write_dask_dataset_to_zarr` passes no encoding): `zstd` level 0, no shuffle
  filter, 2048×2048 chunks, and **float32 for every variable** — including categorical ones
  like `land-class:<year>` and the GLAD land-class layers.

## Questions to answer

### 1. Encoding: what do zarr configuration changes buy?

Re-encode the two local stores under candidate configurations and measure on-disk size and
read/write time. At minimum: zstd at higher levels (3, 9, 19), blosc with bitshuffle/shuffle,
and 2–3 chunk shapes. Report a table: config × size × encode time × decode time. Note which
variables respond most (categorical vs continuous behave very differently under shuffle).

### 2. Dtypes: quick wins without information loss?

Audit all ~30 variables across both stores (`xarray.open_zarr` + the writers in
`harmonize.py` / `emit.py`):

- Which are **categorical/integer in disguise**? (`land-class:*` and `glad:glcluc:*` as
  float32 is the obvious smell — uint8 is a 4× raw reduction and compresses far better.)
- Which continuous variables tolerate **quantization** (e.g. float32 → uint16 with
  scale/offset, or float16)? For emissions-per-hectare-type fields, state what precision is
  physically meaningful given source-data uncertainty, and check what precision `attribute`'s
  rollup actually needs (errors sum over ~1.6e9 pixels — bound the aggregate error).
- Measure, don't just compute: re-encode with the proposed dtypes and report actual sizes.

### 3. Redundancy: does emit need 20 variables?

Trace which emit variables are actually **read downstream** by `attribute` /
`jurisdictional_direct` / `statistical` / `trace`, versus written only for
inspection/debugging. Several look derivable from others (per-period soil/vegetation
breakdowns vs totals). Proposal shape: a "lean" emit persisting only what downstream consumes,
with the rest either dropped, computed on demand, or written to a separate inspection-only
store with a short retention policy. Quantify the savings.

### 4. The architectural floor

The deliberate design choice is materializing inspectable per-pixel intermediates
(`architecture.md`). Estimate the floor if that choice were relaxed:

- Could **emit be fused into attribute** (compute per-pixel emissions lazily during the
  clip/rollup, never persisting the raster)? What would it cost in recompute time on reruns,
  and what would be lost for inspection/validation?
- Is **harmonize** worth persisting at all, given it is VRT-defined rescaling of ingested
  COGs (i.e. cheap to recompute)?
- Conclusion should state: X TiB is inherent to "persist per-pixel emissions at 30 m for a
  continent"; Y TiB is implementation slack recoverable by items 1–3; Z TiB more is available
  only by giving up inspectability/caching properties.

### 5. Mitigations that don't change the data

Lifecycle/retention: e.g. delete or downgrade (to coldline) emit/harmonize zarrs once the
attribute rollups exist and are validated; GCS lifecycle rules on the scratch prefix. Cost
this out at current GCS pricing for a continent-scale run.

### 6. Resolution sensitivity: 30 m → 10 m

First-order: 3× per axis = **9× pixels** (40000² → 120000² per tile), so ~9× uncompressed
(emit ≈ 1.07 TiB/tile; a continent moves from low-TiB to tens of TiB on disk). Verify and
refine:

- Do compression ratios shift at finer resolution (more spatial correlation vs more speckle
  in categorical layers)? A cheap proxy experiment: upsample a land-dense window of the local
  tile 3× and compare ratios.
- Second-order consequences worth flagging: per-tile working memory (observed ~39 GB peak at
  30 m) and whether one-tile-per-node still holds at 10 m, or the work unit must shrink
  (e.g. 5° tiles) / chunking must change.
- Note (out of scope to solve): 10 m implies different *source datasets* (GLAD GLCLUC is
  native 30 m), so this is not just a grid-config change.

## Method notes

- Work read-only against the local stores; write experiment outputs to a temp dir, not
  `SCRATCH_ROOT`.
- The ocean-heavy Honduras tile understates land compression ratios. For land-dense
  measurements, crop a fully-on-land window (e.g. a 10000² slice over mainland Honduras) and
  report ratios for that window separately.
- Cache-key note if regenerating anything: keys are
  `sha1("module|qualname|version|args")[:12]` (see `storage.get_cache_decorator`).

## Report format

Lead with a one-paragraph verdict on inherent-vs-implementation. Then a single ranked table:
**intervention × estimated saving (measured where possible) × effort × information/capability
lost**. Quick wins (config + dtype changes preserving all downstream behavior) clearly
separated from design changes (lean emit, fused attribute, retention).
