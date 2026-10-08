import datetime
import json
import pathlib

import pytest

from kuberjobtower import run
from kuberjobtower.collect import events as collect_events
from kuberjobtower.collect import pods as collect_pods
from kuberjobtower.collect.recorder import Recorder, pod_summary
from kuberjobtower.history import db as store
from kuberjobtower.history import journal, queries
from kuberjobtower.models import Aoi, JobState, NodeInfo, PodEvent, PodState, RunSpec
from kuberjobtower.phases import Phase

DATA = pathlib.Path(__file__).parent / "data"
UTC = datetime.UTC
GIB = 1 << 30
UID = "202610081500-ab12"


def log_lines(name: str = "lka1_compute_samples.ndjson") -> list[str]:
    """The real Sri Lanka compute samples, as the pod printed them (severity first)."""
    out = [json.dumps({"severity": "INFO", "time": "2026-10-06T19:25:00+00:00", "message": "harmonize x", "logger": "__main__"})]
    for line in (DATA / name).read_text().splitlines():
        s = json.loads(line)
        out.append(json.dumps({"severity": "INFO", "message": "resource_sample", "logger": "resource_monitor", **s}))
    out.append(json.dumps({"severity": "INFO", "time": "2026-10-06T20:06:48+00:00", "kind": "resource_summary",
                           "message": "resource_summary", "total_write_gib": 0.08, "cpu_throttled_s": 0.0}))  # fmt: skip
    return out


class FakeSource:
    def __init__(self, reason: str | None = None, phase: str = "Succeeded", exit_code: int = 0) -> None:
        t0 = datetime.datetime(2026, 10, 6, 19, 25, tzinfo=UTC)
        self._pod = PodState(
            "job-r1-0-abcde", 0, phase, "node-1", reason, exit_code, t0,
            created=t0 - datetime.timedelta(seconds=5), finished=t0 + datetime.timedelta(minutes=41.5),
            cpu_request_m=6000, memory_request_bytes=56 * GIB, scratch_bytes=20 * GIB,
        )  # fmt: skip
        self.event_list = [PodEvent(t0, "Scheduled", "Normal", "job-r1-0-abcde", "assigned")]

    def pods(self, run_id: str, phase: str | None = None) -> list[PodState]:
        return [self._pod]

    def logs(self, pod: str, *, tail: int | None = None):  # type: ignore[no-untyped-def]
        return iter(log_lines())

    def events(self, run_id: str) -> list[PodEvent]:
        return self.event_list

    def node(self, name: str) -> NodeInfo | None:
        return NodeInfo(name, "e2-highmem-8", "yaroslav-power-node-pool", 7910, 57 * GIB)


def make_recorder(make_settings, tmp_path, source=None):  # type: ignore[no-untyped-def]
    settings = make_settings(ARCHIVE_ROOT=str(tmp_path / "bucket"), HISTORY_DB=str(tmp_path / "history.db"))
    spec = RunSpec(run_id="r1", aoi=Aoi(("10N_080E",), ("LKA",)), methodology="STATISTICAL", phases=(Phase.COMPUTE,), run_uid=UID)
    plan = run.plan(settings, spec)
    return Recorder(settings, source or FakeSource(), plan, UID), plan, settings


def test_a_phase_is_recorded_in_the_journal_the_archive_and_the_database(make_settings, tmp_path) -> None:  # type: ignore[no-untyped-def]
    recorder, plan, settings = make_recorder(make_settings, tmp_path)
    recorder.start()
    pp = plan.phases[0]
    notes = recorder.after_phase(pp, JobState(pp.job.name, "compute", "r1", 1, 0, 1, 0, "", None, frozenset({"Complete"}), UID))  # type: ignore[union-attr]
    recorder.finish(True)

    db = recorder.db
    run_row = queries.resolve_run(db, "r1")
    assert run_row is not None and (run_row["run_uid"], run_row["status"]) == (UID, "succeeded")
    pod = db.execute("SELECT * FROM pods").fetchone()
    assert (pod["tile_id"], pod["phase"], pod["machine_type"], pod["n_samples"]) == ("10N_080E", "Succeeded", "e2-highmem-8", 166)
    assert pod["peak_anon_pct"] == pytest.approx(95.6) and pod["verdicts"] == "memory_pressure"
    assert pod["cost_usd"] == pytest.approx(0.25, abs=0.01) and pod["pending_s"] == 5 and pod["duration_s"] == 2490
    assert db.execute("SELECT COUNT(*) FROM samples").fetchone()[0] == 166
    assert db.execute("SELECT state, n_succeeded FROM jobs").fetchone()["state"] == "succeeded"
    assert db.execute("SELECT tile_id FROM job_tiles").fetchone()[0] == "10N_080E"
    assert notes[-1].startswith("recorded 1 pod(s) of compute")

    assert store.sync(db, settings.archive_root) == 0  # this machine wrote the chunks: nothing new
    # the journal alone is enough to rebuild the same database elsewhere
    assert store.rebuild(str(tmp_path / "other.db"), settings.archive_root) == 3  # run start, the phase, run end
    other = store.open_store(str(tmp_path / "other.db"))
    assert other.execute("SELECT COUNT(*) FROM samples").fetchone()[0] == 166
    assert queries.tile_history(other, "10N_080E")[0]["peak_anon_pct"] == pytest.approx(95.6)
    # and the archive copies sit under the run uid, not the reusable run id
    assert collect_pods.read_archive(settings.archive_root, UID)["compute"][0]["verdicts"] == []
    assert len(collect_events.read_archive(settings.archive_root, UID)) == 1
    assert journal.read_run(settings.archive_root, UID)["run_id"] == "r1"  # type: ignore[index]


def test_a_failed_pod_is_recorded_with_its_reason_and_found_by_near_oom(make_settings, tmp_path) -> None:  # type: ignore[no-untyped-def]
    recorder, plan, _ = make_recorder(make_settings, tmp_path, FakeSource("OOMKilled", "Failed", 137))
    recorder.start()
    pp = plan.phases[0]
    recorder.after_phase(pp, JobState(pp.job.name, "compute", "r1", 1, 0, 0, 1, "0", None, frozenset({"Failed"}), UID))  # type: ignore[union-attr]
    recorder.finish(False)
    assert recorder.db.execute("SELECT status FROM runs").fetchone()[0] == "failed"
    near = queries.near_limit(recorder.db, 99)  # heap only 96%, but an OOM kill always counts
    assert [(r["reason"], r["tile_id"]) for r in near] == [("OOMKilled", "10N_080E")]
    assert "oom_killed" in recorder.db.execute("SELECT verdicts FROM pods").fetchone()[0]


def test_starting_a_resumed_run_keeps_its_original_start(make_settings, tmp_path) -> None:  # type: ignore[no-untyped-def]
    first, _, settings = make_recorder(make_settings, tmp_path)
    first.start()
    started = first.started_ms
    second = Recorder(settings, FakeSource(), first.plan, UID)
    second.start()  # the run uid is already claimed: this is a resume, not a collision
    assert second.started_ms == started


def test_the_pod_summary_reads_peaks_from_its_samples() -> None:
    samples, summary = pod_summary(log_lines("lka1_export_samples.ndjson"))
    assert len(samples) == 152 and summary["n_samples"] == 152
    assert summary["peak_anon_pct"] < 10 and summary["peak_mem_pct"] >= 90  # page cache, not heap
    assert summary["total_write_gib"] == 0.08
