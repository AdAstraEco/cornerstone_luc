"""The fork's additions to `orchestration`: per-tile exports of emit's and derive's output.

`orchestration` runs plan, ingest, compute and trace, as upstream has it. This package wraps
it, so that a merge from upstream never touches the fork's code: it reuses
`orchestration.__main__.build_parser` and `orchestration.plan.get_task_to_items`, replaces
`plan` with one that also lists the exports, and adds one subcommand.

  python -m orchestration_ext plan HND --methodology=STATISTICAL --emit-export=cog \
      --derive-export=zarr --derive-products=crop-class-emissions --output-dir=/plan
  python -m orchestration_ext export-tile --step=emit --product=cie-emissions \
      --format=cog --tile-id=10N_090W

Every other subcommand (ingest-tile, compute, trace) is `orchestration`'s, unchanged, and
can be called through either package. The examples in `workflows/` show how a workflow calls
each subcommand, alongside upstream's `cornerstone` WorkflowTemplate. See
docs/orchestration.md.
"""
