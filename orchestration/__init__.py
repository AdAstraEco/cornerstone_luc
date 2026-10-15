"""Our deployment's glue between Argo and jdluc: one subcommand per workflow task.

What may live here: listing the units of work a run fans out over, and running exactly one
unit per call. What must not: anything that changes a result. Every argument that reaches a
cache key is passed straight through to jdluc, with jdluc's own defaults.

  python -m orchestration plan HND --methodology=STATISTICAL --output-dir=/plan
  python -m orchestration compute HND --methodology=STATISTICAL --tile-id=10N_090W
  python -m orchestration reduce HND --methodology=STATISTICAL
  python -m orchestration export --tile-id=10N_090W
  python -m orchestration mosaic HND

Ingest and derive need no wrapper: the workflow runs jdluc's own CLIs for them.
`workflow.example.yaml` shows how the workflow calls each subcommand; the deployed copy,
which names our cluster, is kept outside the repo. See docs/orbae/argo-workflows.md.
"""
