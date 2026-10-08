"""The run history (plan 04): a write-once journal in a bucket, and a rebuildable SQLite read model.

The journal is the truth: typed records in objects that are only ever created, never rewritten. The
SQLite file is derived from it, can be deleted at any time, and is rebuilt when the schema changes.
Nothing here knows the pipeline or the cluster.
"""
