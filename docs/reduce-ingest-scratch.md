# Reducing scratch-disk usage in raster tile ingest

**Status:** proposal / RFC. Two independent options, either of which can be adopted alone.
**Scope:** the raster ingest path (`jdluc/datasets/base.py`, `jdluc/datasets/glad_glcluc.py`,
`jdluc/geo.py`). No change to durable outputs, cache keys, or downstream stages.

---

## Summary

When we ingest a GLAD GLCLUC tile we materialise an **8.0 GB uncompressed intermediate
GeoTIFF** on local scratch, purely as a staging file between "download the five yearly
bands" and "write a Cloud-Optimized GeoTIFF (COG)". That intermediate is the single largest
consumer of local disk in the pipeline, and it is avoidable.

This document describes two ways to remove or shrink it:

- **Option A — give the intermediate a compressed, band-interleaved creation profile.**
  A few extra keys on one `rasterio.open(...)`, invisible outside one function, output
  bit-for-bit unchanged. **Measured on a real tile (HND `20N_090W`): 8.0 GB → 1.15 GB, a
  6.97× reduction, and the combine step is *faster* than the uncompressed default.** The
  decisive knob turns out to be **`interleave="band"`**, not the codec — see below.
- **Option B — eliminate the intermediate with a VRT.** Stack the five yearly bands as a
  GDAL virtual raster (VRT, ~1 KB of XML) and translate the COG directly from it, so the
  8 GB file is never written at all. Larger change; touches the generic ingest flow.

Both reduce the amount of data written to scratch. Option A is the low-risk first step and
is what we recommend adopting now; Option B is a fuller fix for later. They are compatible
(A can ship now, B later) but B makes A moot for GLAD.

### The counter-intuitive finding: interleave dominates, not the codec

We benchmarked several codecs/levels/layouts on a real tile (combine step only; the COG is
codec-independent and its per-band checksums are identical across every lossless variant,
so losslessness is proven separately). The result:

| profile | intermediate | reduction | combine time |
|---|---|---|---|
| uncompressed (today's default) | 8.00 GB | 1.0× | 34.5 s |
| ZSTD-1, **pixel** interleave (GTiff default) | 3.47 GB | 2.3× | 74.9 s |
| DEFLATE-6, pixel interleave | 3.24 GB | 2.5× | 917 s |
| ZSTD-6, pixel interleave | 3.20 GB | 2.5× | 205 s |
| **ZSTD-1, `interleave="band"`** | **1.15 GB** | **6.97×** | **21.1 s** |
| ZSTD-6, `interleave="band"` | 1.09 GB | 7.3× | 51.5 s |

Two things upstream reviewers should note, because they're surprising:

1. **`interleave="band"` is the lever.** Same codec (ZSTD-1), pixel → band, is 3.47 GB →
   1.15 GB. The GLAD bands are five yearly land-cover snapshots; band interleave keeps each
   year's large homogeneous regions (spatial run-length structure) contiguous, which
   compresses ~3× better than pixel interleave, which interleaves five dissimilar per-pixel
   values and shreds those runs. **Pixel is the GTiff default**, so simply "adding
   compression" without setting interleave only gets ~2.3×.
2. **Codec and level barely matter once band-interleaved.** ZSTD-1 (6.97×) vs ZSTD-6 (7.3×)
   is a 5 % size difference for 2.4× the CPU; DEFLATE-6 is no smaller than ZSTD and ~40×
   slower to encode. ZSTD level 1 is the sweet spot. A predictor makes it slightly *worse*
   (categorical data). So the recommended profile is deliberately the cheap one.

---

## Background: what the code does today

Raster ingest is generic across datasets. `RasterDataset.ingest_a_tile`
(`jdluc/datasets/base.py`) runs, per tile, inside a single `TemporaryDirectory` on local
disk:

1. `save_tile_id_to_local_path(path_to_geotiff, tile_id)` — dataset-specific staging that
   must leave a GDAL-readable raster at `path_to_geotiff`.
2. `geo.validate_geotiff(...)` — sanity checks (CRS, dimensions, transform sign).
3. `geo.set_band_names_for_geotiff(...)` — opens the file `r+` and writes band descriptions.
4. `geo.convert_geotiff_to_cog(...)` — `rio_cogeo.cog_translate(..., in_memory=False,
   use_cog_driver=True)` reads the staged raster and writes the COG (plus GDAL's own
   overview scratch).
5. `storage.put_file(...)` — uploads the COG to object storage. The intermediate is deleted
   with the `TemporaryDirectory`.

For **GLAD GLCLUC** the staging function (`jdluc/datasets/glad_glcluc.py`,
`_save_tile_id_to_local_path`) does:

```python
with tempfile.TemporaryDirectory() as tmpdir:
    year_to_geotiff = {}
    for year in YEARS:                       # YEARS = (2000, 2005, 2010, 2015, 2020)
        year_to_geotiff[year] = ...          # download each yearly single-band tile
        utils.save_remote_url_to_local_path(...)
    with rasterio.open(year_path) as dataset:
        profile = dataset.meta.copy()        # <-- meta carries NO compression/tiling keys
    profile.update(count=len(year_to_geotiff))
    with rasterio.open(fp=local_path, mode="w", **profile) as dataset:
        for idx, (_, year_path) in enumerate(sorted(year_to_geotiff.items()), start=1):
            with rasterio.open(fp=year_path) as source:
                dataset.write(source.read(1), idx)   # full band read into RAM, then written
```

The key fact: `rasterio`'s `dataset.meta` contains only `driver, dtype, nodata, width,
height, count, crs, transform`. It has **no compression, tiling, or blocksize keys**, so
`rasterio.open(mode="w", **profile)` writes the combined file with GTiff defaults:
**uncompressed and striped**.

### The size, as arithmetic (not estimate)

A GLAD tile is `TileResolution.GLAD = 40_000 × 40_000` pixels (`jdluc/tiling.py`), Byte
(8-bit) per band, five bands:

```
40000 × 40000 × 5 bands × 1 byte = 8_000_000_000 bytes = 8.0 GB
```

So each GLAD tile writes an 8.0 GB uncompressed intermediate, then reads all 8.0 GB back to
produce the (compressed, few-hundred-MB) COG. Peak scratch for one tile ≈ the yearly
downloads (~1 GB) + the 8.0 GB intermediate + the COG output + GDAL's overview temp.

## Why it matters

Land-use-change ingest is CPU-light but **write-heavy in bursts**, and the intermediate is
the burst. Two failure modes follow directly from writing 8 GB of uncompressed data to
local scratch:

1. **Write-bandwidth saturation.** On a shared node disk, two co-scheduled ingest pods each
   streaming an 8 GB uncompressed file can produce dirty pages faster than the device
   flushes them. The Linux writeback limiter (`balance_dirty_pages`) then throttles the
   writers — we have observed forward progress collapse to single-digit MB/s with the
   process pinned in uninterruptible-sleep (`D`) state and near-zero CPU. A tile that should
   take minutes hangs for the better part of an hour.
2. **Capacity exhaustion.** Two pods' intermediates plus COG temps can approach the node's
   ephemeral-storage ceiling, risking `DiskPressure` eviction that can take neighbouring
   pods down with it.

Both scale with **bytes written**. The uncompressed intermediate is the largest, least
necessary contributor to that number. Reducing it attacks the root cause on *any* storage
backend, and is cheaper and more robust than provisioning faster/larger scratch (e.g. a
dedicated local-SSD node pool) to absorb bytes we did not need to write.

### Why the intermediate exists at all

It is glue. `cog_translate` needs a single GDAL-readable source; the five yearly tiles
arrive as five single-band files. The current code combines them into one physical
multi-band GeoTIFF so it can hand `cog_translate` one path. Nothing downstream requires the
combined file to be a materialised, uncompressed, standalone raster — it lives and dies
inside one function call.

---

## Option A — compressed, band-interleaved intermediate

### What

Give the intermediate GeoTIFF a lossless-compressed, **band-interleaved**, tiled creation
profile instead of the GTiff default. The file still exists; it is just ~7× smaller on disk
and cheaper to flush — and, because band interleave matches the band-by-band write loop, it
is also *faster* to write than the uncompressed default.

### Implementation

In `_save_tile_id_to_local_path` (`jdluc/datasets/glad_glcluc.py`), extend the profile
before opening the output:

```python
profile.update(
    count=len(year_to_geotiff),
    compress="ZSTD",
    zstd_level=1,           # level barely matters once band-interleaved; keep it cheap
    interleave="band",      # THE lever: 2.3x -> 7x on this data (see Summary table)
    tiled=True,
    blockxsize=512,
    blockysize=512,
    BIGTIFF="IF_SAFER",
)
```

Notes:
- **`interleave="band"` is the decisive key**, not the codec. The GTiff default is *pixel*,
  which only reaches ~2.3×. See the Summary table for the measured comparison.
- **Lossless, guaranteed.** Interleave only changes *where* identical pixel values sit in
  the file, so read-back is bit-identical by construction; ZSTD is likewise lossless. We
  verified per-band COG checksums are identical across every variant.
- **Faster, not slower.** Measured combine dropped 34.5 s → 21.1 s: fewer bytes to write,
  cheap ZSTD-1 encode, and band interleave matches the existing band-by-band write order.
- **No predictor.** `predictor=2` made this categorical data slightly *larger*; leave it off.
- Optional: apply the same profile to other datasets that stage large intermediates via
  `save_tile_id_to_local_path`. Start with GLAD, the demonstrated hot spot.

### Blast radius

**Minimal.** The change is confined to the creation options of a temporary file that is
created and deleted inside `_save_tile_id_to_local_path`. It is never uploaded, never
cached, never read by any other stage. Specifically:

- **Durable output unchanged.** The COG in object storage is produced from the same pixel
  values; compression of the intermediate does not alter them.
- **No cache invalidation.** Ingest keys off object-store path existence
  (`RasterDataset.ingest_a_tile` checks `storage.path_exists(uri)`), not on intermediate
  bytes. Already-ingested tiles are not recomputed; only new ingests exercise the new path.
- **No API/contract change.** `save_tile_id_to_local_path` still returns one GDAL-readable
  GeoTIFF; `validate_geotiff`, `set_band_names_for_geotiff`, and `convert_geotiff_to_cog`
  see exactly the interface they see today.

### Risks

- **Read-side CPU.** `cog_translate` now decompresses the intermediate as it reads. Measured
  `cog_translate` time was essentially flat vs the uncompressed source (~110–116 s either
  way — it is dominated by overview building + output encode, not the source read), and the
  source is now 7× smaller, so this is not a concern in practice.
- **Compression ratio is data-dependent.** Ocean/edge tiles compress enormously; busy
  mixed-landscape tiles less so. The high-water mark is set by the *worst* tile, so the 6.97×
  figure (a land-heavy Honduras tile) is a realistic-to-conservative sample, not a best case.

### Confirmation

1. **Correctness (must be exact).** Ingest one tile before and after the change; compare the
   resulting COGs' per-band checksums — they must be identical:
   `gdalinfo -checksum <cog>` (or `rio info --checksum`). Identical checksums prove the
   compression was lossless end-to-end.
2. **Disk high-water mark.** During a single-tile ingest, watch the temp dir:
   `du -sh <tmpdir>` sampled, or the process's `write_bytes` from `/proc/<pid>/io`. Compare
   peak before vs after.
3. **In-cluster.** Re-run the fan-out phase with two pods per node; confirm the node's
   ephemeral-storage usage stays well below the ceiling and that no writer enters `D`-state
   on `balance_dirty_pages` (visible as high disk-io-wait with flat CPU). Compare phase
   wall-clock.

---

## Option B — eliminate the intermediate with a VRT

### What

A GDAL **VRT** (virtual raster) is a small XML file that references other rasters as bands
without copying their pixels. Instead of writing an 8 GB combined GeoTIFF, build a 5-band
VRT over the five yearly tiles and hand *that* to `cog_translate`. GDAL reads the yearly
bands through the VRT and writes the COG; the 8 GB physical intermediate is never created.

This is not a foreign technique here — `jdluc/harmonize.py` already assembles per-band VRTs
by hand (`iter_vrt_band_header` / `iter_vrt_band_content`) for the harmonize stage, so the
approach and the XML shape already exist in the codebase.

### Implementation

Two sub-variants, in increasing ambition:

**B1 — VRT over locally downloaded yearlies (recommended form).** Keep downloading the five
yearly tiles to scratch (~1 GB total, compressed as delivered), then build a `separate`-band
VRT over them and translate from it. Removes the 8 GB combined file; leaves only the modest
yearly downloads + COG output + overview temp.

```python
from osgeo import gdal   # if the GDAL Python bindings are present in the image
vrt_path = os.path.join(tmpdir, "stack.vrt")
gdal.BuildVRT(vrt_path, [year_to_geotiff[y] for y in YEARS], separate=True)
# band descriptions (years) set on the VRT, as harmonize.py already does for its VRTs
```

If the `osgeo.gdal` bindings are not in the image, hand-write the 5-band VRT XML exactly as
`harmonize.py` already does (one `<VRTRasterBand>` per year, each a `<SimpleSource>` onto
that year's file, band order 2000→2020).

The staging function then returns `vrt_path`, and `convert_geotiff_to_cog` translates the
COG straight from the VRT.

**B2 — VRT over remote `/vsicurl/` yearlies (further step).** Point the VRT sources at the
public HTTPS URLs via GDAL's `/vsicurl/` so the yearly tiles are never downloaded to disk at
all; GDAL streams the bytes it needs. Removes essentially all local *input*, leaving only
the COG output and overview temp. Trade-off: `cog_translate` may read the source more than
once (overview generation), turning one download into repeated ranged network reads —
possibly slower and more egress. Adopt only if B1's remaining footprint is still a problem,
and measure network cost.

### Blast radius

**Moderate — this one touches the generic flow.** The staging contract in
`base.py:ingest_a_tile` currently assumes `path_to_geotiff` is a real, writable GeoTIFF: it
is opened `r+` by `set_band_names_for_geotiff`, and read by `validate_geotiff` and
`convert_geotiff_to_cog`. A VRT is GDAL-readable and metadata-writable, but is not a
GeoTIFF, so this option requires one of:

- **(preferred) set band names at VRT-build time** (as `harmonize.py` does) and relax or
  bypass the separate `set_band_names_for_geotiff` step for VRT sources; or
- give GLAD a dataset-specific ingest path that constructs the VRT and calls
  `convert_geotiff_to_cog` directly, leaving `base.ingest_a_tile` untouched for other
  datasets.

Either way the change is larger than Option A and needs review of `validate_geotiff` /
`set_band_names_for_geotiff` against a VRT source. As with Option A: durable COG output is
unchanged, and there is no cache invalidation (path-existence keyed).

### Risks

- **Contract creep.** The generic `ingest_a_tile` assumes a materialised GeoTIFF; feeding it
  a VRT either loosens that assumption (affecting all raster datasets) or forks a
  GLAD-specific path (more code). Decide deliberately which.
- **Band order and descriptions** must be preserved exactly (bands 1..5 = years 2000..2020);
  downstream `harmonize` reads bands by year label. A VRT with the wrong source order would
  silently mislabel years. Covered by the checksum/label confirmation below.
- **Repeated source reads (B2 only).** Overview building may re-read sources; over
  `/vsicurl` that is repeated network I/O. B1 avoids this by keeping sources local.
- **`set_band_names_for_geotiff` on a VRT** — VRT supports band `<Description>`, but opening
  `r+` and setting descriptions through rasterio on a VRT should be verified, or avoided by
  setting descriptions at build time.

### Confirmation

1. **Correctness.** Same as Option A: per-band COG checksums (`gdalinfo -checksum`) must
   match the current pipeline's output for the same tile — this also proves band order and
   labelling are preserved.
2. **Band labels.** `gdalinfo <cog>` on the output; confirm band descriptions read
   `year=2000 … year=2020` in order.
3. **Disk high-water mark.** As Option A — the 8 GB file should be entirely absent from the
   temp dir; peak scratch drops to roughly the yearly downloads + COG + overview temp (B1),
   or to ~just the COG + overview temp (B2).
4. **In-cluster.** As Option A — two pods per node, watch ephemeral storage and writer state.

---

## Comparison and recommendation

| | Option A (band+compress) | Option B (VRT) |
|---|---|---|
| Change size | a few keys, one function | new VRT assembly; touches generic ingest contract |
| 8 GB intermediate | shrunk to ~1.15 GB (measured, 6.97×) | removed entirely |
| Combine speed | *faster* (34.5 s → 21.1 s, measured) | avoids the combine entirely |
| Yearly downloads on disk | unchanged (~1 GB) | unchanged (B1) / removed (B2) |
| Blast radius | confined to one temp file | generic flow or a GLAD-specific fork |
| Durable output | bit-identical | bit-identical |
| Cache invalidation | none | none |
| Main risk | none material (measured lossless, faster) | contract creep; band-order correctness |

**Recommendation.** Ship **Option A now**: it is nearly free, self-contained, provably
lossless, *faster* than today, and directly attacks both failure modes by cutting the bytes
written 7×. The remaining scratch (~1.15 GB intermediate + ~1 GB downloads + COG + overview
temp) is small enough that two co-scheduled pods should fit a modest disk comfortably —
confirm the high-water mark in-cluster. Pursue **Option B** later if we want to drive scratch
to its structural floor (COG + GDAL overview temp) or eliminate the intermediate entirely.

Whichever we adopt, the clean-failure guardrails are orthogonal and worth keeping
regardless: set `ephemeral-storage` requests/limits (and, if used, `emptyDir.sizeLimit`) so
a pod that *does* overrun its scratch fails by itself, deterministically, instead of hanging
or destabilising the node.

---

## Appendix: the numbers

- GLAD tile grid: `40_000 × 40_000` px (`jdluc/tiling.py`, `TileResolution.GLAD`).
- Per band: Byte / 8-bit (dataset `no_data = (1<<8) - 1`), one byte per pixel.
- Bands: 5 (`YEARS = 2000, 2005, 2010, 2015, 2020`).
- Uncompressed intermediate: `40000 × 40000 × 5 × 1 = 8.0 GB`.
- Written today with GTiff defaults (uncompressed, striped) because `rasterio`'s
  `dataset.meta` carries no compression/tiling keys.
- Observed on a node running two ingest pods: ephemeral-storage usage consistent with two
  such intermediates coexisting, with a writer throttled by `balance_dirty_pages` to
  single-digit MB/s.

### Measured on HND tile `20N_090W` (combine step; source read once, then COG)

| profile | intermediate | reduction | combine s |
|---|---|---|---|
| uncompressed (default) | 8.000 GB | 1.00× | 34.5 |
| ZSTD-1 pixel (GTiff default interleave) | 3.473 GB | 2.30× | 74.9 |
| ZSTD-6 pixel | 3.196 GB | 2.50× | 205.0 |
| DEFLATE-6 pixel | 3.235 GB | 2.47× | 916.9 |
| ZSTD-1 pixel + predictor=2 | 3.590 GB | 2.23× | 107.6 |
| **ZSTD-1 `interleave=band`** | **1.148 GB** | **6.97×** | **21.1** |
| ZSTD-6 `interleave=band` | 1.093 GB | 7.32× | 51.5 |

Losslessness: every variant's COG has identical per-band GDAL checksums
(`[44540, 5002, 34328, 31912, 27230]`) — the COG is codec/layout-independent. Reproduce with
`tools/bench_ingest_codecs.py` (codec/layout sweep) and `tools/bench_ingest_compression.py`
(combine + COG + checksum proof).
