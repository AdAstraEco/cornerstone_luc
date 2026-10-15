# syntax=docker/dockerfile:1.19
# cornerstone image (docs/orbae/argo-workflows.md): the jdluc pipeline, runnable one-tile-per-pod.
#
# The Argo workflow runs the real stages unchanged inside each pod, one pod per tile per
# phase, so this image just needs the resolved jdluc environment. We build from uv.lock so the
# image is pinned to exactly what a laptop run resolves -- the same "lean venv, no conda"
# choice the architecture doc makes. The workflow sets each step's command.
#
# Build + push (do NOT run without Dan's OK), from a clean commit, tagged with its short SHA.
# Build from the main checkout, not a git worktree: a worktree's .git is a file pointing outside
# the build context, so the image would carry no repository.
# IMAGE is the registry repository, without a tag:
#   TAG=$(git rev-parse --short HEAD)
#   docker build --platform=linux/amd64 -t "$IMAGE:$TAG" .
#   docker push "$IMAGE:$TAG"

FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim

WORKDIR /app

# rasterio/GDAL wheels vendor GDAL but still dynamically link a few base libs the slim image
# omits (libexpat.so.1, used by GDAL's XML parsing). git is for GitPython, which reads the
# copied .git for the provenance tagged on ingested COGs; safe.directory lets it read a repo
# owned by a different user from the one the pod runs as. Keep the layer minimal.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git libexpat1 \
    && rm -rf /var/lib/apt/lists/* \
    && git config --system --add safe.directory /app

# Resolve deps first for layer caching; --frozen makes the build fail loudly rather than
# silently re-resolving if uv.lock is stale, keeping the image pinned to the lockfile.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

# Then the source, without .git (.dockerignore keeps the rest lean). jdluc is pure-Python over
# wheels (rasterio/rioxarray ship GDAL), so no apt GDAL package is needed -- only the base
# shared lib installed above.
COPY --exclude=.git . .
RUN uv sync --frozen --no-dev

# .git last, so a commit that changes no source rebuilds only this layer.
COPY .git .git

ENV PATH="/app/.venv/bin:${PATH}"

# GDAL/rasterio tuning for /vsigs reads of COGs/VRTs over GCS (auth via Workload Identity).
# NB: do NOT set CPL_VSIL_CURL_ALLOWED_EXTENSIONS globally here -- with an allowlist GDAL
# treats any other extension (e.g. the .fgb boundary files) as "does not exist", breaking the
# vector reads. storage.write_dask_dataset_to_zarr scopes that allowlist to the raster
# zarr-write via rasterio.Env, which is the correct place for it.
ENV GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR" \
    GDAL_CACHEMAX="512"

# No ENTRYPOINT: each workflow step sets its own command, a CLI from the image.
