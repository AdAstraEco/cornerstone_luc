"""cornerstone (docs/orbae/cornerstone-k8s.md): run ONE stage for ONE tile, in one pod.

This is the entire per-tile unit of work for the Job-per-tile fanout (approach B).
The k8s Job (infra/k8s/tile-job.yaml) sets this as its command with a tile id; the pod
runs the real pipeline stage, whose ``@storage.cache_to_zarr`` decorator materializes the
output straight to GCS as a side effect. There is no dask scheduler and no remote client:
``emit.workflow`` spins its own in-process ``LocalCluster`` (see ``jdluc.storage``) for the
per-tile threaded compute -- exactly as it does on a laptop. That "run the repo unmodified"
property is the point; the only new code is this thin CLI shim, which lives under infra/.

Run inside the container (the Job does this for you)::

    python infra/run_tile.py --stage emit 40N_090W

A failed stage propagates -- the process exits non-zero and the Job records the failure,
which is a datapoint (one tile's Job), not a whole-run crash.
"""

import argparse
import logging

from jdluc import tiling

logger = logging.getLogger(__name__)

STAGE_NAMES = ("harmonize", "emit")


def run_tile(tile_id: str, stage_name: str) -> None:
    if stage_name == "harmonize":
        from jdluc import harmonize

        harmonize.workflow(
            dataset_names=harmonize.LUC_AND_EMISSIONS_DATASET_NAMES,
            ignore_missing_tiles=True,
            skip_ingest=False,
            tile_id=tile_id,
            tile_resolution=tiling.TileResolution.GLAD,
        )
    elif stage_name == "emit":
        from jdluc import emit

        emit.workflow(tile_id=tile_id)
    else:
        raise ValueError(f"unknown {stage_name=:s}")


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tile_id", help="a 10-degree tile id, e.g. 40N_090W")
    parser.add_argument("--stage", choices=STAGE_NAMES, default="emit")
    args = parser.parse_args()

    logger.info(f"Running stage {args.stage!r} for {args.tile_id=:s}")
    run_tile(tile_id=args.tile_id, stage_name=args.stage)
    logger.info(f"Done: stage {args.stage!r} for {args.tile_id=:s}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
