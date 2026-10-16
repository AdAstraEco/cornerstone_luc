"""Our deployment's glue between Argo and jdluc: one subcommand per workflow task.

What may live here: listing the units of work a run fans out over, and running exactly one
unit per call. What must not: anything that changes a result. Every argument that reaches a
cache key is passed straight through to jdluc, with jdluc's own defaults.

  python -m orchestration plan HND --methodology=STATISTICAL --output-dir=/plan
  python -m orchestration compute HND --methodology=STATISTICAL --tile-id=10N_090W
  python -m orchestration trace HND --methodology=STATISTICAL
  python -m orchestration export-tile --step=emit --deliverable=cie-emissions --format=cog \
      --tile-id=10N_090W
  python -m orchestration mosaic HND --deliverable=cie-emissions

Each subcommand that takes countries takes `--backfill` in their place, for every country and
GNW tile, as jdluc's CLIs do.

Ingest needs no wrapper: the workflow runs jdluc's own CLI for it.
`workflow.example.yaml` shows how the workflow calls each subcommand; the deployed copy,
which names our cluster, is kept outside the repo. See docs/orbae/argo-workflows.md.
"""
