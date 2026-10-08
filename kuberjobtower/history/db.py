"""The SQLite read model: open, apply journal records, sync from the journal, rebuild.

Hand-written SQL on the standard library. The file is a cache of the journal: a newer schema
version in the code drops and rebuilds it, an older code refuses a newer file. WAL mode is only
sound on a local disk of one host, which is where the file lives (never on a synced folder).
"""

import collections.abc
import pathlib
import sqlite3
import time
import typing

from kuberjobtower.history import journal, records

SCHEMA_VERSION = 1
SCHEMA = (pathlib.Path(__file__).with_name("schema.sql")).read_text()

TERMINAL = {
    "runs": ("status", ("succeeded", "failed", "aborted")),
    "jobs": ("state", ("succeeded", "failed", "deleted")),
    "pods": ("phase", ("Succeeded", "Failed")),
}
# Terminal states stay put, and a later record fills in what an earlier one did not know, but
# never blanks it: a summary computed when a pod ended survives a late "Running" observation.
KEYS = {"runs": "run_uid", "jobs": "job_id", "pods": "pod_id"}
FIRST_WINS = {"finished_ms", "started_ms", "created_ms", "first_seen_ms"}


class SchemaTooNew(RuntimeError):
    pass


def _columns(db: sqlite3.Connection, table: str) -> list[str]:
    return [row[1] for row in db.execute(f"PRAGMA table_info({table})")]


def connect(path: str, *, read_only: bool = False) -> sqlite3.Connection:
    if path != ":memory:" and not read_only:
        pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
    uri = f"file:{path}?mode=ro" if read_only else f"file:{path}"
    db = sqlite3.connect(uri, uri=True, isolation_level=None)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=5000")
    if not read_only:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=NORMAL")
    return db


def open_store(path: str) -> sqlite3.Connection:
    """Open the store, creating it, or dropping and recreating it if its schema is older."""
    db = connect(path)
    version = db.execute("PRAGMA user_version").fetchone()[0]
    if version > SCHEMA_VERSION:
        db.close()
        raise SchemaTooNew(f"{path} has schema {version}; this code writes {SCHEMA_VERSION}")
    if version != SCHEMA_VERSION:
        for (name,) in db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall():
            db.execute(f"DROP TABLE IF EXISTS {name}")
        db.executescript(SCHEMA)
        db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
    return db


def _upsert(db: sqlite3.Connection, table: str, data: dict[str, typing.Any]) -> None:
    cols = [c for c in _columns(db, table) if c in data]
    key = KEYS[table]
    state_col, terminal = TERMINAL[table]
    sets = []
    for c in cols:
        if c == key:
            continue
        if c == state_col:
            marks = ",".join(f"'{t}'" for t in terminal)
            sets.append(f"{c} = CASE WHEN {table}.{c} IN ({marks}) THEN {table}.{c} ELSE excluded.{c} END")
        elif c in FIRST_WINS:
            sets.append(f"{c} = COALESCE({table}.{c}, excluded.{c})")
        elif c == "observed_ms":
            sets.append(f"{c} = MAX({table}.{c}, excluded.{c})")
        else:
            sets.append(
                f"{c} = CASE WHEN excluded.observed_ms >= {table}.observed_ms "
                f"THEN COALESCE(excluded.{c}, {table}.{c}) ELSE COALESCE({table}.{c}, excluded.{c}) END"
            )
    db.execute(
        f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join('?' * len(cols))}) "
        f"ON CONFLICT({key}) DO UPDATE SET {', '.join(sets)}",
        [data[c] for c in cols],
    )


def _insert_or_ignore(db: sqlite3.Connection, table: str, data: dict[str, typing.Any]) -> None:
    cols = [c for c in _columns(db, table) if c in data]
    db.execute(
        f"INSERT OR IGNORE INTO {table} ({','.join(cols)}) VALUES ({','.join('?' * len(cols))})",
        [data[c] for c in cols],
    )


def apply(db: sqlite3.Connection, batch: collections.abc.Iterable[records.Record]) -> int:
    """Apply records in one transaction; applying the same records again changes nothing."""
    n = 0
    db.execute("BEGIN")
    try:
        for rec in batch:
            d = dict(rec["d"])
            match rec["t"]:
                case "run" | "job" | "pod":
                    table = {"run": "runs", "job": "jobs", "pod": "pods"}[rec["t"]]
                    d.setdefault("observed_ms", records.now_ms())
                    _upsert(db, table, d)
                case "config":
                    d.setdefault("first_seen_ms", records.now_ms())
                    _insert_or_ignore(db, "configs", d)
                case "job_tiles":
                    for idx, tile in enumerate(d["tiles"]):
                        _insert_or_ignore(db, "job_tiles", {"job_id": d["job_id"], "idx": idx, "tile_id": tile})
                case "sample":
                    _insert_or_ignore(db, "samples", d)
                case "log_ref":
                    _insert_or_ignore(db, "log_objects", d)
                case "event":
                    db.execute(
                        "INSERT INTO events (event_id, run_uid, pod_name, type, reason, message, first_ms, last_ms, count) "
                        "VALUES (:event_id, :run_uid, :pod_name, :type, :reason, :message, :first_ms, :last_ms, :count) "
                        "ON CONFLICT(event_id) DO UPDATE SET count = MAX(count, excluded.count), "
                        "last_ms = MAX(last_ms, excluded.last_ms)",
                        {"pod_name": None, "message": None, "count": 1, "last_ms": d["first_ms"], **d},
                    )
            n += 1
        db.execute("COMMIT")
    except BaseException:
        db.execute("ROLLBACK")
        raise
    return n


def sync(db: sqlite3.Connection, root: str, run_uids: collections.abc.Iterable[str] | None = None) -> int:
    """Apply every journal chunk of the given runs (default: all) that this store has not seen."""
    applied = 0
    for run_uid in run_uids if run_uids is not None else journal.run_uids(root):
        for uri in journal.list_chunks(root, run_uid):
            if db.execute("SELECT 1 FROM ingested_objects WHERE uri = ?", [uri]).fetchone():
                continue
            apply(db, journal.read_chunk(uri))
            db.execute("INSERT OR IGNORE INTO ingested_objects VALUES (?, ?)", [uri, records.now_ms()])
            applied += 1
    return applied


def rebuild(path: str, root: str) -> int:
    """Delete the store and rebuild it from the journal."""
    pathlib.Path(path).unlink(missing_ok=True)
    for suffix in ("-wal", "-shm"):
        pathlib.Path(path + suffix).unlink(missing_ok=True)
    db = open_store(path)
    try:
        return sync(db, root)
    finally:
        db.close()


def prune_samples(db: sqlite3.Connection, older_than_days: int) -> int:
    """Drop old samples; the per-pod summaries stay."""
    cutoff = records.now_ms() - older_than_days * 86_400_000
    return db.execute("DELETE FROM samples WHERE ts_ms < ?", [cutoff]).rowcount


def verify(db: sqlite3.Connection) -> list[str]:
    """Rows that point at a parent the store does not have (a chunk not yet synced)."""
    checks = {
        "jobs without a run": "SELECT job_id FROM jobs WHERE run_uid NOT IN (SELECT run_uid FROM runs)",
        "pods without a job": "SELECT pod_id FROM pods WHERE job_id NOT IN (SELECT job_id FROM jobs)",
        "samples without a pod": "SELECT DISTINCT pod_id FROM samples WHERE pod_id NOT IN (SELECT pod_id FROM pods)",
    }
    return [f"{name}: {row[0]}" for name, sql in checks.items() for row in db.execute(sql)]
