"""The fork's additions to `jdluc`: named deliverables over `export.write_tile`, and mosaics.

`jdluc` is upstream's, unchanged. This package builds on it and nothing in `jdluc` or
`orchestration` imports it, so that a merge from upstream never touches the fork's code.

- `deliverable`: a `Deliverable` names a step's variables and the values to keep on their
  extra dims; `write_deliverable` cuts a step's lazy dataset to them and writes one tile;
- `mosaic`: a read-time VRT over one deliverable's per-tile COGs, for an AOI-wide view.
"""
