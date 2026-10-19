"""The fork's additions to `orchestration`: per-tile exports and their mosaics.

`orchestration` runs plan, ingest, compute and trace, as upstream has it. This package wraps
it, so that a merge from upstream never touches the fork's code: it reuses
`orchestration.__main__.build_parser` and `orchestration.plan.get_task_to_items`, replaces
`plan` with one that also lists the exports, and adds two subcommands.

  python -m orchestration_ext plan HND --methodology=STATISTICAL --emit-export=cog \
      --derive-export=none --output-dir=/plan
  python -m orchestration_ext export-tile --step=emit --deliverable=cie-emissions \
      --format=cog --tile-id=10N_090W
  python -m orchestration_ext mosaic HND --deliverable=cie-emissions

Every other subcommand (ingest-tile, compute, trace) is `orchestration`'s, unchanged, and
can be called through either package. The examples in `workflows/` show how a workflow calls
each subcommand; the deployed copies, which name our cluster, are kept outside the repo. See
docs/orbae/argo-workflows.md.
"""
