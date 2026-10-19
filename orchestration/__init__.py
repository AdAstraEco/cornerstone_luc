"""Our deployment's glue between Argo and jdluc: one subcommand per workflow task.

What may live here: listing the units of work a run fans out over, and running exactly one
unit per call. What must not: anything that changes a result. Every argument that reaches a
cache key is passed straight through to jdluc, with jdluc's own defaults.

  python -m orchestration plan HND --methodology=STATISTICAL --output-dir=/plan
  python -m orchestration ingest-tile --dataset=GNW_TREE_COVER_LOSS --tile-id=10N_090W
  python -m orchestration compute HND --methodology=STATISTICAL --tile-id=10N_090W
  python -m orchestration trace HND --methodology=STATISTICAL
  python -m orchestration export-tile --step=emit --deliverable=cie-emissions --format=cog \
      --tile-id=10N_090W
  python -m orchestration mosaic HND --deliverable=cie-emissions

Each subcommand that takes countries takes `world` in their place, for every country and GNW
tile, as jdluc's `--backfill` covers.

Whole-world ingest needs no wrapper: the workflow runs jdluc's own CLI for it. A tile's ingest
has one, `ingest-tile`, so that its pod names the tile rather than the run's countries.
The examples in `workflows/` show how a workflow calls each subcommand; the deployed copies,
which name our cluster, are kept outside the repo. See docs/orbae/argo-workflows.md.
"""
