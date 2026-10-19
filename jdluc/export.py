"""Write a pipeline step's output as a deliverable under EXPORT_ROOT.

A utility, like `storage`: it knows nothing about the steps. A tabular deliverable (see
`trace.export_workflow`) is one parquet per set of countries, written by `write_table` to
`{EXPORT_ROOT}/{deliverable}/{name}.parquet`, with provenance in its `attrs`.

Unlike SCRATCH_ROOT, EXPORT_ROOT is not a cache that the pipeline reads back: what lands
there is a terminal output.
"""

import logging
import os
import tempfile

import pandas

from jdluc import config, storage, utils

logger = logging.getLogger(__name__)


def get_metadata(product_name: str, source_name: str) -> dict[str, str]:
    """Provenance, as `datasets.base` stamps on ingested COGs."""
    return {
        "cornerstone-processing-time": utils.get_utc_timestamp(),
        "cornerstone-processing-version": utils.get_git_version(),
        "cornerstone-product-name": product_name,
        "cornerstone-remote-url": utils.get_git_remote_url(),
        "cornerstone-source-name": source_name,
    }


def get_table_output_uri(
    deliverable_name: str, name: str, output_root: str | None = None
) -> str:
    """`{output_root}/{deliverable}/{name}.parquet`; `output_root` defaults to EXPORT_ROOT."""
    if output_root is None:
        output_root = config.Config.from_dot_env().export_root
    return storage.join_uri(
        root=output_root, prefix=f"{deliverable_name:s}/{name:s}.parquet"
    )


def write_table(
    df: pandas.DataFrame,
    deliverable_name: str,
    name: str,
    source_name: str,
    output_root: str | None = None,
) -> str:
    """Write a tabular deliverable as one parquet, provenance in its `attrs`; return the URI."""
    uri = get_table_output_uri(
        deliverable_name=deliverable_name, name=name, output_root=output_root
    )
    df = df.copy()
    df.attrs |= get_metadata(product_name=deliverable_name, source_name=source_name)
    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = os.path.join(tmpdir, os.path.basename(uri))
        df.to_parquet(path=local_path)
        storage.put_file(local_path=local_path, uri=uri)
    logger.info(f"Wrote {deliverable_name=:s} ({len(df):d} rows) to {uri=:s}")
    return uri
