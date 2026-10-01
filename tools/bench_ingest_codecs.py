"""Codec sweep for the GLAD ingest intermediate (supports Option A's codec choice).

Downloads the five yearly tiles once, then writes the combined multi-band intermediate under
several lossless creation profiles, recording file size and combine wall-clock. The COG step
is skipped: all codecs here are lossless, so the durable COG is identical regardless (proven
separately by tools/bench_ingest_compression.py's checksum comparison). This only informs
*which* codec/level to stage the scratch intermediate with.

Usage:
    uv run python tools/bench_ingest_codecs.py 20N_090W --out /tmp/codecs.json
"""

import argparse
import json
import os
import tempfile
import time

import rasterio

from jdluc import utils
from jdluc.datasets import glad_glcluc

TILING = {"tiled": True, "blockxsize": 512, "blockysize": 512, "BIGTIFF": "IF_SAFER"}

# name -> extra creation profile (all lossless). Isolating the interleave axis: BAND
# compresses each year on spatial redundancy only; PIXEL puts all 5 years of a pixel
# adjacent, exploiting the huge year-to-year (temporal) redundancy of land cover.
CODECS: dict[str, dict] = {
    "zstd1_band": {"compress": "ZSTD", "zstd_level": 1, "interleave": "band", **TILING},
    "zstd6_band": {"compress": "ZSTD", "zstd_level": 6, "interleave": "band", **TILING},
    "zstd1_pixel": {"compress": "ZSTD", "zstd_level": 1, "interleave": "pixel", **TILING},
    "zstd6_pixel": {"compress": "ZSTD", "zstd_level": 6, "interleave": "pixel", **TILING},
    "zstd1_pixel_pred2": {"compress": "ZSTD", "zstd_level": 1, "interleave": "pixel", "predictor": 2, **TILING},
}


def download_years(cache_dir: str, tile_id: str) -> dict[int, str]:
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
    with rasterio.open(next(iter(year_paths.values()))) as dataset:
        profile = dataset.meta.copy()
    profile.update(count=len(year_paths), **extra_profile)
    start = time.monotonic()
    with rasterio.open(out_path, "w", **profile) as dst:
        for idx, (_, year_path) in enumerate(sorted(year_paths.items()), start=1):
            with rasterio.open(year_path) as src:
                dst.write(src.read(1), idx)
    return time.monotonic() - start


def main() -> int:
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tile_id")
    parser.add_argument("--out", default="/tmp/bench_ingest_codecs.json")
    parser.add_argument(
        "--cache-dir",
        default=None,
        help="persistent dir for the yearly downloads; defaults to <tmp>/glad_<tile_id>",
    )
    args = parser.parse_args()

    cache_dir = args.cache_dir or os.path.join(
        tempfile.gettempdir(), f"glad_{args.tile_id}"
    )
    print(f"Yearly download cache: {cache_dir}")
    year_paths = download_years(cache_dir, args.tile_id)

    rows = []
    with tempfile.TemporaryDirectory() as work:
        for name, profile in CODECS.items():
            out = os.path.join(work, f"{name}.tif")
            combine_s = combine(year_paths, out, profile)
            size = os.path.getsize(out)
            rows.append(
                {"codec": name, "bytes": size, "GB": round(size / 1e9, 3), "combine_s": round(combine_s, 1)}
            )
            print(f"{name:20s} {size/1e9:6.3f} GB  combine {combine_s:6.1f}s")
            os.remove(out)

    # 8.0 GB uncompressed baseline (40000x40000x5 bytes), measured separately.
    baseline = 8_000_480_544
    for r in rows:
        r["reduction_x"] = round(baseline / r["bytes"], 2)

    report = {"tile_id": args.tile_id, "baseline_bytes": baseline, "rows": rows}
    with open(args.out, "w") as fp:
        json.dump(report, fp, indent=2)
    print("\n" + json.dumps(report, indent=2))
    print(f"\nWrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
