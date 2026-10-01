"""Benchmark: compressing the GLAD ingest intermediate GeoTIFF (Option A).

Mirrors ``jdluc.datasets.glad_glcluc._save_tile_id_to_local_path`` +
``base.RasterDataset.ingest_a_tile`` but parameterises the *creation profile* of the staged
multi-band intermediate, so we can measure today's behaviour (no compression -- what
``rasterio``'s ``meta`` yields) against a compressed+tiled profile on a real tile.

For each variant it records the intermediate file size (the quantity we are shrinking), the
combine and cog-translate wall-clock, the resulting COG size, and the COG's per-band GDAL
checksums. Identical checksums across variants prove the compression is lossless end to end
(the durable COG is unchanged).

Usage:
    uv run python tools/bench_ingest_compression.py 20N_090W --out /tmp/bench.json
"""

import argparse
import json
import os
import tempfile
import time

import rasterio

from jdluc import geo, utils
from jdluc.datasets import glad_glcluc

# Per-band GDAL checksums of the COG from the baseline (uncompressed) run, per tile. The COG
# is codec-independent, so any lossless intermediate must reproduce these -- lets a
# compressed-only run self-validate without re-running the 8 GB baseline.
KNOWN_COG_CHECKSUMS: dict[str, list[int]] = {
    "20N_090W": [44540, 5002, 34328, 31912, 27230],
}

# Today's staged intermediate carries no compression/tiling keys (rasterio meta has none).
BASELINE_PROFILE: dict[str, object] = {}
# Option A: lossless compression + tiling for the scratch intermediate.
COMPRESSED_PROFILE: dict[str, object] = {
    "compress": "DEFLATE",  # zlib default level (6)
    "tiled": True,
    "blockxsize": 512,
    "blockysize": 512,
    "BIGTIFF": "IF_SAFER",
}


def download_years(cache_dir: str, tile_id: str) -> dict[int, str]:
    """Download the 5 yearly tiles into a persistent cache dir, reusing any already present."""
    os.makedirs(cache_dir, exist_ok=True)
    paths: dict[int, str] = {}
    for year in glad_glcluc.YEARS:
        path = os.path.join(cache_dir, f"{tile_id:s}.{year:d}.tif")
        if os.path.exists(path) and os.path.getsize(path) > 0:
            print(f"reuse cached {path}")
        else:
            utils.save_remote_url_to_local_path(
                local_path=path,
                params={},
                remote_url=f"https://storage.googleapis.com/earthenginepartners-hansen/GLCLU2000-2020/v2/{year:d}/{tile_id:s}.tif",
            )
        paths[year] = path
    return paths


def combine(year_paths: dict[int, str], out_path: str, extra_profile: dict) -> float:
    """Reproduce the real combine, with a parameterised creation profile. Returns seconds."""
    with rasterio.open(next(iter(year_paths.values()))) as dataset:
        profile = dataset.meta.copy()
    profile.update(count=len(year_paths), **extra_profile)
    start = time.monotonic()
    with rasterio.open(out_path, "w", **profile) as dst:
        for idx, (_, year_path) in enumerate(sorted(year_paths.items()), start=1):
            with rasterio.open(year_path) as src:
                dst.write(src.read(1), idx)
    return time.monotonic() - start


def band_checksums(path: str) -> list[int]:
    with rasterio.open(path) as dataset:
        return [dataset.checksum(i) for i in range(1, dataset.count + 1)]


def run_variant(name: str, year_paths: dict[int, str], tmpdir: str, profile: dict) -> dict:
    intermediate = os.path.join(tmpdir, f"{name}.intermediate.tif")
    combine_s = combine(year_paths, intermediate, profile)
    intermediate_bytes = os.path.getsize(intermediate)

    cog = os.path.join(tmpdir, f"{name}.cog.tif")
    start = time.monotonic()
    geo.convert_geotiff_to_cog(
        metadata={},
        no_data=glad_glcluc.DATASET.no_data,
        path_to_cog=cog,
        path_to_geotiff=intermediate,
    )
    cog_translate_s = time.monotonic() - start

    result = {
        "variant": name,
        "intermediate_bytes": intermediate_bytes,
        "intermediate_GB": round(intermediate_bytes / 1e9, 3),
        "combine_s": round(combine_s, 1),
        "cog_translate_s": round(cog_translate_s, 1),
        "cog_bytes": os.path.getsize(cog),
        "cog_checksums": band_checksums(cog),
    }
    os.remove(intermediate)
    os.remove(cog)
    return result


def main() -> int:
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tile_id")
    parser.add_argument("--out", default="/tmp/bench_ingest_compression.json")
    parser.add_argument(
        "--cache-dir",
        default=None,
        help="persistent dir for the yearly downloads (reused across runs); "
        "defaults to <tmp>/glad_<tile_id>",
    )
    parser.add_argument(
        "--variants",
        default="baseline,compressed",
        help="comma-separated subset of {baseline,compressed}",
    )
    args = parser.parse_args()

    profiles = {"baseline": BASELINE_PROFILE, "compressed": COMPRESSED_PROFILE}
    want = [v.strip() for v in args.variants.split(",") if v.strip()]
    cache_dir = args.cache_dir or os.path.join(
        tempfile.gettempdir(), f"glad_{args.tile_id}"
    )

    print(f"Yearly download cache: {cache_dir}")
    year_paths = download_years(cache_dir, args.tile_id)
    download_bytes = sum(os.path.getsize(p) for p in year_paths.values())

    results = []
    # Intermediates/COGs are large + disposable: keep them in an auto-cleaned work dir.
    with tempfile.TemporaryDirectory() as work:
        for name in want:
            print(f"--- variant: {name} ---")
            results.append(run_variant(name, year_paths, work, profiles[name]))

    by_name = {r["variant"]: r for r in results}
    report: dict = {
        "tile_id": args.tile_id,
        "yearly_download_bytes": download_bytes,
        "yearly_download_GB": round(download_bytes / 1e9, 3),
        "variants": results,
    }
    # Prove losslessness: any compressed variant's COG must match the baseline's COG.
    # Cross-run baseline reference (COG is codec-independent) so a compressed-only run
    # still self-validates.
    reference = KNOWN_COG_CHECKSUMS.get(args.tile_id)
    if "baseline" in by_name:
        reference = by_name["baseline"]["cog_checksums"]
    if "compressed" in by_name:
        comp = by_name["compressed"]
        report["checksums_match"] = reference is not None and comp["cog_checksums"] == reference
        report["checksum_reference"] = reference
        if "baseline" in by_name:
            report["intermediate_reduction_x"] = round(
                by_name["baseline"]["intermediate_bytes"] / comp["intermediate_bytes"], 2
            )

    with open(args.out, "w") as fp:
        json.dump(report, fp, indent=2)

    print("\n================ RESULT ================")
    print(json.dumps(report, indent=2))
    print(f"\nWrote {args.out}")
    if report.get("checksums_match") is False:
        print("!!! COG checksums differ -- compression was NOT lossless; investigate.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
