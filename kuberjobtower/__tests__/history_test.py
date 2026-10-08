import json
import pathlib

import pytest

from kuberjobtower.history import db as store
from kuberjobtower.history import journal, queries, records

DATA = pathlib.Path(__file__).parent / "data"
UID = "202610061800-ab12"


def sample_records() -> list[records.Record]:
    """A small run: one compute job, one pod, with the real Sri Lanka compute samples."""
    job = records.job_id(UID, "cornerstone-lka-compute-lka1")
    pod = records.pod_id(UID, "cornerstone-lka-compute-lka1", "cornerstone-lka-compute-lka1-0-zl6qw")
    samples = [json.loads(line) for line in (DATA / "lka1_compute_samples.ndjson").read_text().splitlines()]
    out = [
        records.make("run", run_uid=UID, run_id="lka1", aoi="LKA", status="running", started_ms=1000, observed_ms=1000),
        records.make("job", job_id=job, run_uid=UID, job_name="cornerstone-lka-compute-lka1", phase="compute",
                     state="running", created_ms=1000, observed_ms=1000),  # fmt: skip
        records.make("job_tiles", job_id=job, tiles=["10N_080E"]),
        records.make("pod", pod_id=pod, job_id=job, pod_name="p", idx=0, tile_id="10N_080E", phase="Running", observed_ms=1500),
    ]
    out += [
        records.make("sample", pod_id=pod, ts_ms=records.ms(s["time"]), mem_anon_gib=s["mem_anon_gib"], mem_anon_pct=s["mem_anon_pct"])
        for s in samples
    ]
    out += [
        records.make("pod", pod_id=pod, job_id=job, pod_name="p", phase="Succeeded", reason="Completed", duration_s=2490,
                     peak_anon_gib=53.5, peak_anon_pct=95.6, n_samples=len(samples), cost_usd=0.25, observed_ms=2000),  # fmt: skip
        records.make("job", job_id=job, run_uid=UID, job_name="cornerstone-lka-compute-lka1", phase="compute",
                     state="succeeded", finished_ms=2000, observed_ms=2000),  # fmt: skip
        records.make("run", run_uid=UID, run_id="lka1", status="succeeded", finished_ms=2000, observed_ms=2000),
    ]
    return out


def test_run_uids_sort_by_time_and_differ() -> None:
    import datetime

    when = datetime.datetime(2026, 10, 8, 15, 23, tzinfo=datetime.UTC)
    a, b = records.new_run_uid(when), records.new_run_uid(when)
    assert a.startswith("202610081523-") and len(a) == 17 and a != b


def test_decode_skips_damaged_lines_and_records_of_a_newer_format() -> None:
    text = '{"v":1,"t":"run","d":{"run_uid":"x"}}\nnot json\n{"v":2,"t":"run","d":{}}\n{"v":1,"t":"mystery","d":{}}\n'
    assert [r["d"]["run_uid"] for r in records.decode(text)] == ["x"]


def test_applying_the_same_records_twice_changes_nothing() -> None:
    db = store.open_store(":memory:")
    batch = sample_records()
    store.apply(db, batch)
    snapshot = [list(map(tuple, db.execute(f"SELECT * FROM {t}"))) for t in ("runs", "jobs", "pods", "samples", "job_tiles")]
    store.apply(db, batch)
    assert snapshot == [list(map(tuple, db.execute(f"SELECT * FROM {t}"))) for t in ("runs", "jobs", "pods", "samples", "job_tiles")]
    assert db.execute("SELECT COUNT(*) FROM samples").fetchone()[0] == 166


def test_a_late_running_observation_does_not_undo_a_finished_pod() -> None:
    db = store.open_store(":memory:")
    batch = sample_records()
    store.apply(db, batch)
    late = [r for r in batch if r["t"] == "pod"][0]
    store.apply(db, [{**late, "d": {**late["d"], "observed_ms": 9999}}])  # the old Running record, observed later
    row = db.execute("SELECT phase, reason, peak_anon_gib, duration_s FROM pods").fetchone()
    assert tuple(row) == ("Succeeded", "Completed", 53.5, 2490)  # terminal and summaries kept
    assert db.execute("SELECT status FROM runs").fetchone()[0] == "succeeded"


def test_events_keep_the_highest_count() -> None:
    db = store.open_store(":memory:")
    base = {"event_id": "e1", "run_uid": UID, "type": "Warning", "reason": "Evicted", "first_ms": 1, "last_ms": 5}
    store.apply(db, [records.make("event", **base, count=2), records.make("event", **{**base, "last_ms": 3}, count=1)])
    assert tuple(db.execute("SELECT count, last_ms FROM events").fetchone()) == (2, 5)


def test_journal_roundtrip_sync_and_rebuild(tmp_path: pathlib.Path) -> None:
    root = str(tmp_path / "bucket")
    journal.claim_run(root, UID, {"run_uid": UID, "run_id": "lka1"})
    with pytest.raises(journal.RunExists):
        journal.claim_run(root, UID, {"other": 1})  # a run uid is claimed once
    j = journal.Journal(root, "laptop-1")
    batch = sample_records()
    first = j.append(UID, batch[:50])
    second = j.append(UID, batch[50:])
    assert first and second and first != second and journal.run_uids(root) == [UID]
    assert len(journal.list_chunks(root, UID)) == 2
    assert j.append(UID, []) is None

    db_path = str(tmp_path / "history.db")
    db = store.open_store(db_path)
    assert store.sync(db, root) == 2
    assert store.sync(db, root) == 0  # nothing new: idempotent
    assert queries.resolve_run(db, "lka1")["status"] == "succeeded"  # type: ignore[index]
    assert store.verify(db) == []
    db.close()

    assert store.rebuild(db_path, root) == 2  # delete the file, rebuild from the journal alone
    again = store.open_store(db_path)
    assert queries.runs(again)[0]["run_uid"] == UID
    assert again.execute("SELECT COUNT(*) FROM samples").fetchone()[0] == 166


def test_a_second_observer_cannot_overwrite_the_first(tmp_path: pathlib.Path) -> None:
    root = str(tmp_path)
    a, b = journal.Journal(root, "a"), journal.Journal(root, "b")
    assert a.append(UID, sample_records()[:2]) != b.append(UID, sample_records()[:2])
    assert len(journal.list_chunks(root, UID)) == 2


def test_queries_answer_the_questions_the_plan_asks(tmp_path: pathlib.Path) -> None:
    db = store.open_store(":memory:")
    store.apply(db, sample_records())
    assert queries.resolve_run(db, UID[:10])["run_id"] == "lka1"  # type: ignore[index]
    assert queries.resolve_run(db, "nope") is None
    assert [r["phase"] for r in queries.peak_by_phase(db, UID)] == ["compute"]
    history = queries.tile_history(db, "10N_080E")
    assert [(r["phase"], r["peak_anon_pct"]) for r in history] == [("compute", 95.6)]
    assert [r["tile_id"] for r in queries.near_limit(db, 90)] == ["10N_080E"]
    assert queries.near_limit(db, 99) == []


def test_a_newer_schema_is_refused_and_an_older_one_is_rebuilt(tmp_path: pathlib.Path) -> None:
    path = str(tmp_path / "h.db")
    db = store.open_store(path)
    db.execute(f"PRAGMA user_version={store.SCHEMA_VERSION + 1}")
    db.close()
    with pytest.raises(store.SchemaTooNew):
        store.open_store(path)
    db = store.connect(path)
    db.execute("PRAGMA user_version=0")
    db.execute("INSERT INTO runs (run_uid, run_id, observed_ms) VALUES ('x', 'x', 1)")
    db.close()
    assert store.open_store(path).execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 0  # dropped and rebuilt


def test_the_first_chunk_of_a_run_on_a_bucket_without_folders(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Object stores raise FileNotFoundError when listing a folder nothing was written to yet."""
    real = journal._fs

    def bucket_like(uri: str):  # type: ignore[no-untyped-def]
        fs, path = real(uri)
        original = fs.ls
        fs.ls = lambda p, detail=False, **kw: original(p, detail=detail, **kw) if fs.exists(p) and any(
            n for n in original(p, detail=False)
        ) else (_ for _ in ()).throw(FileNotFoundError(p))
        return fs, path

    monkeypatch.setattr(journal, "_fs", bucket_like)
    assert journal.Journal(str(tmp_path), "o").append(UID, sample_records()[:2]) is not None
