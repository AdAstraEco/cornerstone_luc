# Running the pipeline on Kubernetes with kuber-job-tower

[kuber-job-tower](https://github.com/AdAstraEco/kuber-job-tower) plans a run, creates one Indexed Job per phase (one pod per tile), watches it, keeps status, logs and memory in a local SQLite file, removes the Jobs when they finish, and has a small web UI. It replaces `infra/run_aoi.py` and `infra/k8s/phase-job.yaml`.

## How it is wired here

| What                                                                                            | Where                                                                                                          |
| ----------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| The tower, as a development dependency (not in the Docker image)                                | `pyproject.toml`: dependency group `kjt`, source `kuber-job-tower` (a git branch; pin a tag once there is one) |
| The pipeline's description: phases, pod command and arguments, options, tile and country checks | `infra/kjt.py` (`PIPELINE`), registered as the entry point `kuberjobtower.pipelines` in `pyproject.toml`       |
| The pod program (unchanged)                                                                     | `infra/run_phase.py`, run as `python infra/run_phase.py --phase ...`                                           |
| Pod sizes, node pools, Secret names, retries                                                    | the tower's sizes file `.kjt/sizes.toml` (not this repo)                                                       |
| Cluster settings                                                                                | `.env` (`KJT_*`)                                                                                               |

`jdluc` never imports the tower, and the image never contains it. Only `infra/kjt.py` imports `kuberjobtower.spec`.

## Set up

```bash
uv sync                                    # installs the kjt group with the dev group
```

Add the cluster to `.env` (git-ignored); see the tower's `.env.example` for all settings:

```
KJT_GCP_PROJECT=...        KJT_GCP_ZONE=...        KJT_GKE_CLUSTER=...
KJT_KUBE_CONTEXT=...       KJT_NAMESPACE=...       KJT_SERVICE_ACCOUNT=...
KJT_ALLOWED_POOL_REGEX=^(your-prefix-.*)$
KJT_IMAGE_CORNERSTONE=<registry>/cornerstone:<tag>
```

Create the sizes file once (it names your node pools; sizes come from measured runs and can be edited):

```bash
uv run kjt sizes init --pipeline cornerstone --heavy <power-pool> --light <worker-pool>
uv run kjt doctor --pipeline cornerstone     # settings, sizes, database, service account, Secrets
```

The pods mount the Secrets `cornerstone-env` (source-API keys; ingest phases) and `cornerstone-env-nokeys` (the other phases) as `/app/.env`. Other names: `KJT_SECRET_KEYS`, `KJT_SECRET_NOKEYS`.

## Run

```bash
# plan only: every Job, its CPU and memory, node pool, Secret, the pinned image digest, the checks
uv run kjt submit --pipeline cornerstone --tile 20N_090W --country HND
# create the Jobs and watch the run through its phases
uv run kjt submit --pipeline cornerstone --tile 20N_090W --country HND --yes

uv run kjt status                  # runs;  add a run id for its phases and pods
uv run kjt logs <run-id> --phase compute --index 0
uv run kjt top                     # live memory and CPU
uv run kjt ui --pipeline cornerstone     # http://127.0.0.1:8090
```

Useful options: `--phase compute` (only some phases), `--opt methodology=JURISDICTIONAL_DIRECT`, `--opt skip_ingest=true` (compute over a warm `INGEST_ROOT`, no keys), `--set compute.memory=48Gi` (a size for this run), `--i-know` (a run above `KJT_CONFIRM_ITEMS` tiles). A failed run is never retried on its own: `kjt retry <run-id> --reason "..."`.

A country-only run (no `--tile`) resolves its tiles from the boundaries in `INGEST_ROOT`, which needs `ingest-world` to have run.

## The image

The tower runs the image you give it, pinned by digest at submit. Build for the nodes' architecture and push (needs your registry access):

```bash
docker buildx build --platform linux/amd64 -f infra/Dockerfile \
  --build-arg JDLUC_GIT_VERSION=$(git rev-parse --short HEAD) \
  -t <registry>/cornerstone:<tag> --push .
```

The image has to contain `infra/run_phase.py` with JSON logs (`LOG_FORMAT=json`), `--tile-ids`, and `infra/resource_monitor.py`. The `latest` tag is older than that; use a tag built from this branch.

## Changing the tower version

The tower is pinned in `pyproject.toml` (`[tool.uv.sources]`). To move to a newer commit of the branch: `uv lock --upgrade-package kuber-job-tower && uv sync`. To develop both repositories together: `uv pip install -e ../kuber-job-tower --no-deps` (do not commit a path source).

The tower's own docs: its README (architecture, settings), `docs/integrating-a-pipeline.md`, `docs/image-contract.md`.
