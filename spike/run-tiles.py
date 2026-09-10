"""SPIKE (docs/spike-dask-k8s.md): fan tiles out as one k8s Job per tile (approach B).

Resolves the 10-degree tiles a set of countries' boundaries touch, then submits one k8s
Job per tile to the target namespace by rendering ``spike/k8s/tile-job.yaml``. The cluster
runs as many concurrently as the node pool's autoscaler provides -- that IS the scaling knob.
There is no dask scheduler to stand up and no change to ``jdluc``: each pod runs the real
stage via ``spike/run_tile.py``.

Infra identifiers (namespace, image, node pool, ...) are read from ``spike/infra.env`` (see
``spike/infra.env.example``), so nothing cluster-specific is hard-coded here.

    # one-time: copy the template and fill in your cluster's values
    cp spike/infra.env.example spike/infra.env    # then edit

    # M0: render the manifests and list what would run, but submit nothing
    uv run python spike/run-tiles.py HND --dry-run

    # M2: submit one emit Job per tile HND touches
    source spike/infra.env
    uv run python spike/run-tiles.py HND --stage emit

Watch:     kubectl -n "$K8S_NAMESPACE" get jobs -l app=jdluc-tile -w
Teardown:  kubectl -n "$K8S_NAMESPACE" delete jobs -l app=jdluc-tile
"""

import argparse
import logging
import os
import pathlib
import re
import string
import subprocess

from jdluc.datasets import worldbank_jurisdictions

logger = logging.getLogger(__name__)

STAGE_NAMES = ("harmonize", "emit")
INFRA_ENV_PATH = pathlib.Path(__file__).parent / "infra.env"
TEMPLATE_PATH = pathlib.Path(__file__).parent / "k8s" / "tile-job.yaml"

# Job-template placeholder -> the infra.env / environment variable that fills it.
TEMPLATE_TO_INFRA_VAR = {
    "NAMESPACE": "K8S_NAMESPACE",
    "SERVICE_ACCOUNT": "K8S_SERVICE_ACCOUNT",
    "NODE_POOL": "NODE_POOL",
    "SECRET_NAME": "K8S_SECRET",
    "IMAGE": "IMAGE",
}


def load_infra_env() -> dict[str, str]:
    """Read ``spike/infra.env`` (``KEY=value`` / ``export KEY=value``), overlaid by os.environ.

    The file is gitignored; the process environment wins, so ``source spike/infra.env`` (or
    CI-injected vars) overrides it. Returns only the keys the template needs.
    """
    values: dict[str, str] = {}
    if INFRA_ENV_PATH.exists():
        for line in INFRA_ENV_PATH.read_text().splitlines():
            line = line.strip().removeprefix("export ").strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, raw = line.partition("=")
            values[key.strip()] = raw.strip().strip("'\"")
    wanted = set(TEMPLATE_TO_INFRA_VAR.values())
    values.update({k: v for k, v in os.environ.items() if k in wanted})
    return {k: v for k, v in values.items() if k in wanted}


def job_name(stage: str, tile_id: str) -> str:
    """A DNS-1123-safe Job name, e.g. ('emit', '40N_090W') -> 'jdluc-tile-emit-40n-090w'."""
    safe_tile = re.sub(r"[^a-z0-9]+", "-", tile_id.lower()).strip("-")
    return f"jdluc-tile-{stage}-{safe_tile}"


def get_tile_ids(iso_3166s: list[str]) -> list[str]:
    return sorted(
        worldbank_jurisdictions.get_ten_degree_tile_ids_for_iso_3166s(
            iso_3166s=iso_3166s
        )
    )


def render_manifest(
    infra: dict[str, str], stage: str, tile_id: str, *, dry_run: bool
) -> str:
    mapping = {
        "STAGE": stage,
        "TILE_ID": tile_id,
        "JOB_NAME": job_name(stage=stage, tile_id=tile_id),
    }
    for placeholder, infra_var in TEMPLATE_TO_INFRA_VAR.items():
        value = infra.get(infra_var)
        if value is None:
            # On a dry-run, leave unfilled infra as a visible ${...} placeholder.
            if not dry_run:
                raise KeyError(infra_var)
            value = "${" + placeholder + "}"
        mapping[placeholder] = value
    return string.Template(TEMPLATE_PATH.read_text()).substitute(mapping)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "iso_3166s",
        nargs=argparse.ONE_OR_MORE,
        type=worldbank_jurisdictions.iso_3166_str,
        help="cover exactly the tiles these countries' boundaries touch (spike default: HND)",
    )
    parser.add_argument("--stage", choices=STAGE_NAMES, default="emit")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="render manifests and list tiles, but submit nothing (M0 smoke test)",
    )
    args = parser.parse_args()

    infra = load_infra_env()
    if not args.dry_run:
        missing = [v for v in TEMPLATE_TO_INFRA_VAR.values() if not infra.get(v)]
        if missing:
            parser.error(
                f"missing infra values {missing}; copy spike/infra.env.example to "
                "spike/infra.env and fill it in (or `source` it), or pass --dry-run"
            )

    tile_ids = get_tile_ids(iso_3166s=args.iso_3166s)
    logger.info(
        f"{len(tile_ids)} tile(s) for {args.iso_3166s} at stage {args.stage!r}: {tile_ids}"
    )

    namespace = infra.get("K8S_NAMESPACE")
    for tile_id in tile_ids:
        manifest = render_manifest(
            infra=infra, stage=args.stage, tile_id=tile_id, dry_run=args.dry_run
        )
        name = job_name(stage=args.stage, tile_id=tile_id)
        if args.dry_run:
            print(f"# --- {name} ---\n{manifest}")
            continue
        # Submit via kubectl so we inherit the caller's current context/credentials
        # rather than embedding a k8s client. check=True surfaces an apply failure loudly.
        assert namespace, "K8S_NAMESPACE is required to submit (validated above)"
        subprocess.run(
            ["kubectl", "-n", namespace, "apply", "-f", "-"],
            input=manifest,
            text=True,
            check=True,
        )
        logger.info(f"submitted {name}")

    if not args.dry_run:
        logger.info(
            f"submitted {len(tile_ids)} job(s). "
            f"Watch: kubectl -n {namespace} get jobs -l app=jdluc-tile -w"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
