"""Named read queries over the store, for the command line and later the UI."""

import sqlite3

Rows = list[sqlite3.Row]


def runs(db: sqlite3.Connection, limit: int = 20) -> Rows:
    return db.execute(
        "SELECT r.*, (SELECT COUNT(*) FROM jobs j WHERE j.run_uid = r.run_uid) AS n_jobs, "
        "(SELECT ROUND(SUM(p.cost_usd), 2) FROM pods p JOIN jobs j ON p.job_id = j.job_id "
        " WHERE j.run_uid = r.run_uid) AS cost_usd "
        "FROM runs r ORDER BY r.started_ms DESC LIMIT ?",
        [limit],
    ).fetchall()


def resolve_run(db: sqlite3.Connection, text: str) -> sqlite3.Row | None:
    """A run by exact ``run_uid``, by unique prefix of it, or by ``run_id`` (the latest such run)."""
    for sql, arg in (
        ("SELECT * FROM runs WHERE run_uid = ?", text),
        ("SELECT * FROM runs WHERE run_id = ? ORDER BY started_ms DESC LIMIT 1", text),
    ):
        if row := db.execute(sql, [arg]).fetchone():
            return row  # type: ignore[no-any-return]
    prefixed = db.execute("SELECT * FROM runs WHERE run_uid LIKE ? ESCAPE '\\'", [text.replace("%", "\\%") + "%"]).fetchall()
    return prefixed[0] if len(prefixed) == 1 else None


def jobs(db: sqlite3.Connection, run_uid: str) -> Rows:
    return db.execute("SELECT * FROM jobs WHERE run_uid = ? ORDER BY created_ms, job_name", [run_uid]).fetchall()


def pods(db: sqlite3.Connection, run_uid: str) -> Rows:
    return db.execute(
        "SELECT p.*, j.phase AS job_phase FROM pods p JOIN jobs j ON p.job_id = j.job_id "
        "WHERE j.run_uid = ? ORDER BY j.created_ms, p.idx, p.created_ms",
        [run_uid],
    ).fetchall()


def events(db: sqlite3.Connection, run_uid: str) -> Rows:
    return db.execute("SELECT * FROM events WHERE run_uid = ? ORDER BY first_ms", [run_uid]).fetchall()


def tile_history(db: sqlite3.Connection, tile_id: str) -> Rows:
    """Every pod that ran this tile, newest first: how long it took, and how much it used."""
    return db.execute(
        "SELECT r.run_uid, r.started_ms, j.phase, p.phase AS pod_phase, p.reason, p.duration_s, "
        "p.peak_anon_gib, p.peak_anon_pct, p.peak_mem_gib, p.cost_usd, p.verdicts, p.machine_type "
        "FROM pods p JOIN jobs j ON p.job_id = j.job_id JOIN runs r ON j.run_uid = r.run_uid "
        "WHERE p.tile_id = ? ORDER BY p.created_ms DESC",
        [tile_id],
    ).fetchall()


def near_limit(db: sqlite3.Connection, pct: float = 85.0) -> Rows:
    """Pods whose heap came within ``pct`` percent of the memory limit (page cache does not count)."""
    return db.execute(
        "SELECT r.run_uid, j.phase, p.tile_id, p.pod_name, p.peak_anon_gib, p.peak_anon_pct, p.reason "
        "FROM pods p JOIN jobs j ON p.job_id = j.job_id JOIN runs r ON j.run_uid = r.run_uid "
        "WHERE p.peak_anon_pct >= ? OR p.reason = 'OOMKilled' ORDER BY p.peak_anon_pct DESC",
        [pct],
    ).fetchall()


def peak_by_phase(db: sqlite3.Connection, run_uid: str) -> Rows:
    return db.execute(
        "SELECT j.phase, COUNT(*) AS pods, MAX(p.peak_anon_gib) AS peak_anon_gib, "
        "MAX(p.peak_anon_pct) AS peak_anon_pct, MAX(p.duration_s) AS longest_s, ROUND(SUM(p.cost_usd), 2) AS cost_usd "
        "FROM pods p JOIN jobs j ON p.job_id = j.job_id WHERE j.run_uid = ? GROUP BY j.phase ORDER BY MIN(j.created_ms)",
        [run_uid],
    ).fetchall()
