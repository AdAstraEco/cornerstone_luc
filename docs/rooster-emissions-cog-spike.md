# Rooster emissions layer from luc scratch — exploratory spike

> ⚠️ **Exploratory spike, not a committed design.** The goal is a *plausibility check*: produce a secondary output that carries the **same claimed data** as Rooster's emissions layer, generated from luc's intermediate scratch — even if the underlying methodology is deliberately misaligned. Numbers are not expected to match Rooster. Treat scope, schema, and naming as provisional.

This revision leads with **how the feature fits the existing pipeline**, which is the more important question than the precise raster content.

## Objective

Add a step that reads the `emit` phase's cached scratch output and writes a Cloud-Optimised GeoTIFF (COG) of per-pixel emissions — as a **cleanly separated, additive** feature that respects luc's existing architecture, layering, and orchestration conventions.

## Fit with the pipeline — summary verdict

**A separate step is the right call, and it is cleanly achievable.** The feature is purely *additive*: it reads an upstream cached artifact through the existing idiom and touches no existing stage logic. Three facts from the codebase make this clean:

1. **`emit`'s output is already the raster we want.** `emit.workflow(tile_id)` (`jdluc/emit.py:452`, `@storage.cache_to_zarr(version=0)`) returns an `xarray.Dataset` on the regular EPSG:4326 GLAD grid whose bands already include every pool we need: `vegetation-emissions:{span}` (biomass), `soil-emissions:{span}`, `peatland-occupation`, `emissions-per-hectare` (discounted total), and `hectares-per-pixel` (area scaling). **No change to `emit` is required.**
2. **Scratch is shared object storage, not per-pod.** `SCRATCH_ROOT` is a GCS bucket (`.env.example`, `infra/cloud.env.example`); the per-pod pd-ssd `/localtmp` is a *separate* volume for Dask spill / GDAL cache (`infra/run_aoi.py:93`). So a downstream pod reads emit's cached zarr from shared scratch **with no recompute**.
3. **Reading an upstream cache is an existing idiom.** A consumer just calls the upstream `workflow()`; the `@storage.cache_to_zarr` decorator returns the deserialised zarr on a cache hit. `attribute` already reads `emit` exactly this way. The cache key is derived from `(module, qualname, version, tile_id)` — so `tile_id` alone addresses the artifact.

## Architectural design

### Where it lives (module + import layering)

luc enforces a strict downward-only import contract (`pyproject.toml`, ETL layers): `trace → attribute → {statistical|jurisdictional_direct} → emit → harmonize → ingest`, over foundational utilities (`config`, `geo`, `storage`, `tiling`). A consumer of `emit` must sit **above** `emit`.

**Add a new module at the top of the layer stack** (above `trace`), e.g. `jdluc/export.py` (name TBD — `emissions_cog.py` also fits). It imports `emit` (band-name constants + the discount helpers `SPAN_TO_LINEAR_DISCOUNT_WEIGHT` / `get_linear_discounted_total`), plus `geo`, `storage`, `config`. **Nothing imports it** — it is a leaf that produces a terminal deliverable. The import-linter change is a single new top layer line.

### Where it runs (phase + orchestration)

`emit` is per-tile, so the export is naturally per-tile too (one COG per GLAD tile). Add a **dedicated per-tile phase** after `compute`:

- New `Phase.EXPORT = "export"` in `infra/run_phase.py` (Phase enum at `run_phase.py:71`), with `is_per_tile = True`.
- New dispatch case in the `match phase` block (`run_phase.py:332`) → a small `run_export(tile_id)` that calls the new module's `workflow(tile_id)`.
- New `PHASE_TO_SPEC` entry (`infra/run_aoi.py:104`): light resources — it reads one zarr and writes one COG, so modest memory, small `localtmp`, cheap node pool. Runs after the `compute` barrier, itself fanned out one pod per tile.

This mirrors the pipeline's existing shape (per-tile `compute` → 1-pod `reduce`) and is **additive only**: new module, new Phase member, new spec, new dispatch case, one import-linter line. **Zero edits to `emit`/`harmonize`/`compute`/ `attribute` logic.**

### Reading the upstream artifact (the dependency)

```python
dset = emit.workflow(tile_id=tile_id)   # cache hit on shared GCS scratch; no recompute
```

Dependency contract: the export phase must run **after** `compute` for the same AOI/version. Because scratch is shared and the cache key includes emit's `version=`, running after compute guarantees a matching, already-materialised key — no version handshake needed beyond ordering.

### Writing the output

The pipeline currently has **no published-artifact convention** — final outputs are cached parquet addressed by hash. A COG is a *terminal deliverable* that nothing downstream reads, so:

- **Do not** wrap it in `@storage.cache_to_*` (those exist so downstream stages can re-read by key). Write it to an explicit, human-readable path via `storage.put_file` / `storage.join_uri`.
- **Spike location:** `{scratch_root}/emissions-cog/{tile_id}.tif`.
- **Clean long-term home:** add an `EXPORT_ROOT` (or `OUTPUT_ROOT`) to `Config`, mirroring `ingest_root`/`scratch_root` — a small, principled addition once the spike proves out.
- **Reuse existing COG machinery:** `geo.convert_geotiff_to_cog` (`jdluc/geo.py:35`, DEFLATE profile, BIGTIFF) and the `watershed-*` provenance metadata tags from `jdluc/datasets/base.py` (version, processing-time, git-version, source-name).

### The one coupling to manage deliberately

The exporter depends on `emit`'s **output band schema** and, for discounted per-pool totals, on emit's discount weights. Keep this a one-directional dependency (exporter imports emit; never the reverse). Recommended hardening: promote emit's output band-name string literals (`"emissions-per-hectare"`, the `vegetation-emissions:{span}` pattern, etc.) to named constants that the exporter imports — turning a stringly-typed contract into an explicit one. This is the only place schema drift could bite.

### Alternative considered — fold into `compute`

Add one line after `emit.workflow(tile_id)` in `run_compute` (`run_phase.py:226`). Simpler orchestration and the emit zarr is still warm locally — but it couples the export into the `compute` phase contract, can't be run or re-run independently, and muddies the separation. **Rejected** given the priority on clean separation; kept as a fallback if a separate phase's per-pod overhead proves not worth it.

## Change surface — file-by-file (for the implementing agent)

The whole feature is additive. Concrete edit points:

- **New module** `jdluc/export.py` (name TBD) — top of the import layer stack. Public `workflow(tile_id: str) -> str` (returns the written COG URI). Reads `emit.workflow(tile_id)`; selects/stacks bands; writes via `geo.convert_geotiff_to_cog`.
- **`pyproject.toml`** — add the new module as a new top layer in the ETL import-linter contract (above `jdluc.trace`).
- **`infra/run_phase.py`** — add `Phase.EXPORT = "export"` to the Phase enum (`run_phase.py:71`), include it in `is_per_tile`; add a `run_export(tile_id)` and a `case Phase.EXPORT:` in the `match phase` dispatch (`run_phase.py:332`).
- **`infra/run_aoi.py`** — add a `PHASE_TO_SPEC[Phase.EXPORT]` entry (`run_aoi.py:104`) with light resources (small memory, small `localtmp`, cheap node pool); it fans out one pod per tile after the `compute` barrier.
- **Reused as-is (no edits):** `jdluc/geo.py:35` (`convert_geotiff_to_cog`, DEFLATE/BIGTIFF profile), `jdluc/storage.py` (`put_file`, `join_uri`), `jdluc/datasets/base.py` (`watershed-*` provenance metadata pattern), `jdluc/config.py` (`scratch_root`; add `EXPORT_ROOT` only when promoting past the spike).
- **Untouched by design:** `emit`, `harmonize`, `compute`, `attribute` logic.

Optional hardening (recommended before this stops being a spike): promote emit's output band-name literals to named constants in `jdluc/emit.py` and import them in the exporter, so the schema contract is explicit rather than stringly-typed.

## Claimed-data mapping (Rooster → luc emit bands)

| Rooster claimed pool     | luc `emit` output band                            |
| ------------------------ | ------------------------------------------------- |
| Biomass loss (AGB+BGB)   | `vegetation-emissions:{span}`                     |
| Grassland loss           | folded into vegetation carbon (grassland by zone) |
| Soil organic carbon loss | `soil-emissions:{span}`                           |
| Peat oxidation           | `peatland-occupation` (+ peat term inside soil)   |
| (Total, discounted)      | `emissions-per-hectare`                           |
| Area scaling             | `hectares-per-pixel`                              |

## Plan

### Slice 1 — single-tile exporter (the core spike)

- New top-layer module + `workflow(tile_id)` reading `emit.workflow(tile_id)`.
- Decide COG band schema: one band per pool (biomass/soil/peat) + total, and density (tCO2e/ha) vs absolute (× `hectares-per-pixel`). Document the choice. Per-pool discounted totals reuse emit's `get_linear_discounted_total`.
- Write via `geo.convert_geotiff_to_cog` to `{scratch_root}/emissions-cog/{tile_id}.tif`.
- Deliverable: a `tools/` script `emit tile → emissions COG` + one artifact to eyeball.

### Slice 2 — wire in as a phase

- Add `Phase.EXPORT`, the dispatch case, and the `PHASE_TO_SPEC` entry; extend the import-linter contract. Run it fanned out over tiles after `compute`.

### Slice 3 — AOI mosaic (optional)

- VRT / `gdal_merge` the per-tile COGs into an AOI-wide COG (a 1-pod step mirroring `reduce`, or a read-time VRT). Ivory Coast fixtures exist in Rooster for sanity checks.

## Open decisions

- **Module/phase name:** `export` vs `emissions_cog`.
- **Band schema:** split pools vs single total; density vs absolute.
- **Output home:** spike prefix under `scratch_root` now vs introducing `EXPORT_ROOT`.
- **Granularity:** per-tile COGs first; AOI mosaic deferred.

## Known semantic mismatches (accepted for the spike)

Different disturbance driver (GLAD land-cover transitions vs GFW tree-cover-loss), 2000–2020 five-year spans vs Rooster's 2024, different biomass/peat source datasets, and no per-permanency split. These move the numbers, not the structure, and are the substantive reconciliation work for any real follow-up.
