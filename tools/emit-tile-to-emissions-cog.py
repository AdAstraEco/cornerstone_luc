"""Write one tile's `emit` deliverables (the CIE COG by default) from its cached scratch output.

    uv run tools/emit-tile-to-emissions-cog.py <tile_id> --deliverable cie-emissions \
        [--format zarr] [--output-root /tmp/exports]
"""

from jdluc import emit, export

if __name__ == "__main__":
    raise SystemExit(
        export.main(
            description=__doc__,
            export_workflow=emit.export_workflow,
            name_to_deliverable=emit.NAME_TO_DELIVERABLE,
        )
    )
