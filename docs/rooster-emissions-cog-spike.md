# Rooster emissions layer from luc scratch — exploratory spike

> ⚠️ **Exploratory spike, not a committed design.** The goal is a *plausibility
> check*: produce a secondary output that carries the **same claimed data** as
> Rooster's emissions layer, generated from luc's intermediate scratch — even if
> the underlying methodology is deliberately misaligned. Numbers are not expected
> to match Rooster. Nothing here is a specification; treat scope, schema, and
> naming as provisional.

## Objective

Generate an emissions raster (Cloud-Optimised GeoTIFF) from the intermediates
luc already writes to the scratch folder, containing the same *kinds* of claimed
quantities Rooster's emissions layer reports (biomass / soil / peat emissions),
without reimplementing Rooster's H3 pipeline or its GFW-driven methodology.

**Explicitly out of scope for the spike:** H3 output, matching Rooster's numbers,
per-permanency (annual/perennial/paddy/pasture) splits, and reconciling
disturbance semantics (GLAD transitions vs GFW tree-cover-loss).

## Why this is cheap

The hard part of "port Rooster" was the H3 re-indexing. Dropping it changes the
task from a port to a **serialization**:

- luc's `emit.workflow(tile_id)` (`jdluc/emit.py`) already returns an
  `xarray.Dataset` of **per-hectare emissions on a regular EPSG:4326 raster grid**
  (the GLAD 10° tile grid), cached to a `.zarr` in scratch. It is already a raster.
- luc already computes Rooster's claimed pools as separable arrays before summing:
  vegetation/biomass emissions, mineral-soil (SOC) emissions, and peat
  (conversion pulse + occupation) — with the GHGP 20-year linear discount applied.
- COG-writing machinery already exists in-repo: `jdluc/geo.py:51`
  (`rio_cogeo.cogeo.cog_translate`); `rio-cogeo` + `rioxarray` are already deps.

So the spike reuses existing computation and existing COG tooling; the new code
is a thin exporter.

## Claimed-data mapping (Rooster → luc emit)

| Rooster claimed pool        | luc `emit` source                                        |
| --------------------------- | -------------------------------------------------------- |
| Biomass loss (AGB+BGB)      | `span_to_vegetation_emissions` (forest AGB+BGB+DOM)      |
| Grassland loss              | folded into vegetation carbon (grassland carbon by zone) |
| Soil organic carbon loss    | `get_mineral_soil_emissions` (FLU-adjusted)              |
| Peat oxidation              | peat conversion pulse + `get_peatland_occupation_emissions` |

## Plan

### Slice 1 — single-tile exporter (the core spike)
- Read one `emit.workflow(tile_id)` result from scratch.
- Decide a small COG band schema representing the claim, e.g. one band each for
  biomass / soil / peat emissions plus a total. Decide **density (tCO2e/ha)** vs
  **absolute (× the existing `ha`/hectares-per-pixel band)** — pick one, document it.
- Write a multi-band COG via the existing `jdluc/geo.py` COG path.
- Deliverable: a `tools/` script `emit tile → emissions COG`, and one artifact to eyeball.

### Slice 2 — AOI mosaic (only if slice 1 looks right)
- VRT / `gdal_merge` the per-tile COGs into an AOI-wide COG (or lean on the
  existing `reduce` phase). Ivory Coast fixtures already exist in Rooster for
  side-by-side sanity checks.

### Slice 3 — spot comparison (optional, informational)
- Overlay against Rooster's Ivory Coast output to see whether magnitudes are in
  the same ballpark. Expect divergence; the point is a sniff test, not validation.

## Open questions / decisions to make
- **Band schema:** split pools vs single total; density vs absolute.
- **Source of truth:** export from `emit`'s already-computed emissions
  (fastest, recommended) vs rebuild from `harmonize`'s raw aligned bands with
  Rooster-style factors (more faithful to Rooster, much more work — not the spike).
- **Extent:** single tile for the spike; AOI mosaic deferred to slice 2.

## Known semantic mismatches (accepted for the spike)
Different disturbance driver (GLAD land-cover transitions vs GFW tree-cover-loss),
2000–2020 five-year spans vs Rooster's 2024, different biomass/peat source
datasets, and no per-permanency split. These move the numbers, not the
feasibility, and are the substantive reconciliation work for any real follow-up.
