# SPIKE image (docs/spike-dask-k8s.md): the jdluc pipeline, runnable one-tile-per-pod.
#
# Approach B (one k8s Job per tile) runs the real stage unchanged inside the pod, so this
# image just needs the resolved jdluc environment. We build from uv.lock so the image is
# pinned to exactly what a laptop run resolves -- the same "lean venv, no conda" choice the
# architecture doc makes. The k8s Job sets the command (spike/run_tile.py <tile_id>).
#
# Build + push (do NOT run without Dan's OK). IMAGE comes from spike/infra.env:
#   source spike/infra.env
#   docker build -f spike/Dockerfile -t "$IMAGE" .
#   docker push "$IMAGE"

FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim

WORKDIR /app

# Resolve deps first for layer caching; --frozen makes the build fail loudly rather than
# silently re-resolving if uv.lock is stale, keeping the image pinned to the lockfile.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

# Then the source (.dockerignore keeps this lean; no .git needed -- the cache key uses
# func.__module__). jdluc is pure-Python over wheels (rasterio/rioxarray ship GDAL), so no
# apt GDAL is needed.
COPY . .
RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:${PATH}"

# GDAL/rasterio tuning for /vsigs reads of COGs/VRTs. storage.write_dask_dataset_to_zarr also
# sets these via rasterio.Env at compute time; setting them as pod env too is belt-and-braces
# so the pod behaves like a local run. Keep roughly in sync with storage.py.
ENV CPL_VSIL_CURL_ALLOWED_EXTENSIONS=".tif,.vrt" \
    GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR" \
    GDAL_CACHEMAX="512"

# Provenance baked at build time (the image has no .git). utils.get_git_* read these to tag
# ingested COGs; the build command passes them from the host repo. Default to "unknown".
ARG JDLUC_GIT_VERSION=unknown
ARG JDLUC_GIT_REMOTE_URL=unknown
ENV JDLUC_GIT_VERSION=${JDLUC_GIT_VERSION} \
    JDLUC_GIT_REMOTE_URL=${JDLUC_GIT_REMOTE_URL}

# No ENTRYPOINT: the Job sets `python spike/run_tile.py --stage <stage> <tile_id>`.
