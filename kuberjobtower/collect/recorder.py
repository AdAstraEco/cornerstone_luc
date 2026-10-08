"""Turns a run into history records as it goes: at each phase end, and at the run's end.

It reads what the cluster still knows (pods, events, node types, pod logs), writes the archive
copies, flushes one journal chunk, and applies the same records to the local database. One
recorder is the single writer for the run it drives; everyone else only reads.
"""

import collections.abc
import dataclasses
import getpass
import hashlib
import json
import os
import socket
import typing

from kuberjobtower import quantity, report
from kuberjobtower.collect import cost, verdicts
from kuberjobtower.collect import events as collect_events
from kuberjobtower.collect import logs as collect_logs
from kuberjobtower.collect import pods as collect_pods
from kuberjobtower.history import db as store
from kuberjobtower.history import journal, records
from kuberjobtower.models import JobState, NodeInfo, PhasePlan, PodEvent, PodState, RunPlan
from kuberjobtower.settings import Settings

GIB = 1 << 30
STATE = {"complete": "succeeded", "failed": "failed"}


class Source(typing.Protocol):
    def pods(self, run_id: str, phase: str | None = None) -> list[PodState]: ...
    def logs(self, pod: str, *, tail: int | None = None) -> collections.abc.Iterator[str]: ...
    def events(self, run_id: str) -> list[PodEvent]: ...
    def node(self, name: str) -> NodeInfo | None: ...


def _gib(text: str) -> float:
    return round(quantity.parse(text) / GIB, 3)


def pod_summary(lines: collections.abc.Sequence[str]) -> tuple[list[records.Record], dict[str, typing.Any]]:
    """The pod's resource samples as records' data, and the summary fields derived from them."""
    parsed = report.parse(lines)
    samples = [r for r in parsed if r.get("kind") == "resource_sample"]
    final = next((r for r in reversed(parsed) if r.get("kind") == "resource_summary"), {})

    def peak(key: str) -> float | None:
        values = [float(s[key]) for s in samples if key in s]
        return max(values) if values else None

    summary = {
        "peak_mem_gib": peak("mem_current_gib"), "peak_mem_pct": peak("mem_pct"),
        "peak_anon_gib": peak("mem_anon_gib"), "peak_anon_pct": peak("mem_anon_pct"),
        "peak_io_psi": peak("io_psi_full_avg10"), "peak_mem_psi": peak("mem_psi_full_avg10"),
        "peak_cpu_psi": peak("cpu_psi_full_avg10"), "peak_localtmp_pct": peak("localtmp_used_pct"),
        "total_write_gib": final.get("total_write_gib"), "cpu_throttled_s": final.get("cpu_throttled_s"),
        "oom_kill_count": int(peak("mem_events_oom_kill") or 0), "n_samples": len(samples),
    }  # fmt: skip
    return samples, summary


class Recorder:
    def __init__(self, settings: Settings, source: Source, plan: RunPlan, run_uid: str) -> None:
        self.settings, self.source, self.plan, self.run_uid = settings, source, plan, run_uid
        self.run_id = plan.spec.run_id
        self.journal = journal.Journal(
            settings.archive_root, f"{socket.gethostname()}-{os.getpid()}-{records.now_ms()}"
        )
        self.db = store.open_store(settings.history_db)
        self.started_ms = records.now_ms()
        self._phase_started_ms = self.started_ms
        self._nodes: dict[str, NodeInfo | None] = {}

    def _flush(self, batch: list[records.Record]) -> None:
        uri = self.journal.append(self.run_uid, batch)
        store.apply(self.db, batch)
        if uri:
            store.mark_ingested(self.db, uri)  # applied above: a later sync must not repeat it

    def _run_record(self, status: str, finished: bool = False) -> records.Record:
        spec = self.plan.spec
        spec_json = json.dumps(
            {
                "tiles": spec.aoi.tiles, "countries": spec.aoi.iso_3166s, "phases": [str(p) for p in spec.phases],
                "parallelism": spec.parallelism, "overrides": {str(k): v for k, v in spec.overrides.items()},
                "methodology": spec.methodology, "ttl_s": spec.ttl_s,
            },
            sort_keys=True,
        )  # fmt: skip
        return records.make(
            "run", run_uid=self.run_uid, run_id=self.run_id, aoi=spec.aoi.iso_3166s and "-".join(spec.aoi.iso_3166s) or None,
            methodology=spec.methodology, status=status, started_ms=self.started_ms,
            finished_ms=records.now_ms() if finished else None, image=spec.image or self.settings.image,
            cluster=self.settings.gke_cluster, namespace=self.settings.namespace,
            submitted_by=getpass.getuser(), spec_json=spec_json, observed_ms=records.now_ms(),
        )  # fmt: skip

    def start(self) -> None:
        """Claim the run uid (a resumed run already holds it) and record that the run began."""
        try:
            journal.claim_run(
                self.settings.archive_root, self.run_uid,
                {"run_uid": self.run_uid, "run_id": self.run_id, "started_ms": self.started_ms},
            )  # fmt: skip
        except journal.RunExists:
            existing = journal.read_run(self.settings.archive_root, self.run_uid) or {}
            self.started_ms = int(existing.get("started_ms", self.started_ms))
        self._flush([self._run_record("running")])

    def finish(self, ok: bool) -> None:
        self._flush([self._run_record("succeeded" if ok else "failed", finished=True)])

    def after_phase(self, pp: PhasePlan, final: JobState) -> list[str]:
        """Archive and record one ended phase; returns notes for the person watching."""
        assert pp.job is not None
        phase = str(pp.phase)
        job = records.job_id(self.run_uid, pp.job.name)
        now = records.now_ms()
        res = pp.job.resources
        config = {
            "image": pp.job.image, "node_pool": pp.job.node_pool, "secret": pp.job.secret,
            "cpu": [res.cpu_request, res.cpu_limit], "memory": [res.memory_request, res.memory_limit],
            "localtmp": res.localtmp, "deadline_s": pp.job.deadline_s, "retries": pp.job.retries_per_index,
            "fail_index_on_oom": pp.job.fail_index_on_oom, "phase": phase,
        }  # fmt: skip
        sha = records.config_sha(config)
        batch: list[records.Record] = [
            records.make("config", config_sha=sha, normalized_json=json.dumps(config, sort_keys=True), first_seen_ms=now),
            records.make(
                "job", job_id=job, run_uid=self.run_uid, job_name=pp.job.name, phase=phase, config_sha=sha,
                completions=pp.job.completions, parallelism=pp.job.parallelism, node_pool=pp.job.node_pool,
                cpu_request=res.cpu_request, cpu_limit=res.cpu_limit,
                mem_request_gib=_gib(res.memory_request), mem_limit_gib=_gib(res.memory_limit),
                localtmp_gib=_gib(res.localtmp), pod_deadline_s=pp.job.deadline_s,
                state=STATE.get(final.state, "running"), created_ms=self._phase_started_ms,
                finished_ms=now, n_succeeded=final.succeeded, n_failed=final.failed,
                failed_indexes=final.failed_indexes or None, observed_ms=now,
            ),
        ]  # fmt: skip
        if pp.phase.is_per_tile and pp.job.tiles:
            batch.append(records.make("job_tiles", job_id=job, tiles=list(pp.job.tiles)))

        events = self.source.events(self.run_id)
        pods = self.source.pods(self.run_id, phase)
        notes: list[str] = []
        for pod in pods:
            batch += self._pod_records(pp, job, pod, events, notes)
        for e in events:
            if f"-{self.run_id}-" in e.pod:
                eid = hashlib.sha1(f"{e.time.isoformat()}|{e.pod}|{e.reason}|{e.message}".encode()).hexdigest()
                t = records.ms(e.time)
                batch.append(records.make(
                    "event", event_id=eid, run_uid=self.run_uid, pod_name=e.pod, type=e.type,
                    reason=e.reason, message=e.message[:500], first_ms=t, last_ms=t,
                ))  # fmt: skip
        collect_events.archive(self.settings.archive_root, self.run_uid, events)
        nodes = {p.node: self._node(p.node) for p in pods if p.node}
        collect_pods.archive(self.settings.archive_root, self.run_uid, phase, pods, events, nodes)
        self._flush(batch)
        self._phase_started_ms = now
        notes.append(f"recorded {len(pods)} pod(s) of {phase} as run {self.run_uid}")
        return notes

    def _node(self, name: str | None) -> NodeInfo | None:
        if not name:
            return None
        if name not in self._nodes:
            self._nodes[name] = self.source.node(name)
        return self._nodes[name]

    def _pod_records(
        self, pp: PhasePlan, job: str, pod: PodState, events: list[PodEvent], notes: list[str]
    ) -> list[records.Record]:
        assert pp.job is not None
        pid = records.pod_id(self.run_uid, pp.job.name, pod.name)
        try:
            lines = list(self.source.logs(pod.name))
        except Exception as exc:  # a pod whose log cannot be read is still worth a record
            lines = []
            notes.append(f"no log for {pod.name}: {exc}")
        uri = collect_logs.write_archive(self.settings.archive_root, self.run_uid, str(pp.phase), pod.name, lines)
        samples, summary = pod_summary(lines)
        node = self._node(pod.node)
        rec = collect_pods.record(pod, events, node)
        pod_codes = typing.cast(list[str], rec["verdicts"])
        codes = list(dict.fromkeys([*pod_codes, *(v.code for v in verdicts.sample_verdicts(samples))]))
        tile = pp.job.tiles[pod.index] if pp.phase.is_per_tile and pod.index is not None and pod.index < len(pp.job.tiles) else None
        out = [records.make(
            "pod", pod_id=pid, job_id=job, pod_name=pod.name, idx=pod.index, tile_id=tile, node_name=pod.node,
            node_pool=rec["node_pool"], machine_type=rec["machine_type"], phase=pod.phase, reason=pod.reason,
            exit_code=pod.exit_code, created_ms=records.ms(pod.created), started_ms=records.ms(pod.started),
            finished_ms=records.ms(pod.finished), pending_s=rec["pending_s"], duration_s=rec["run_s"],
            cost_usd=rec["cost_usd"], verdicts=",".join(codes) or None, observed_ms=records.now_ms(), **summary,
        )]  # fmt: skip
        keys = {
            "mem_current_gib": "mem_current_gib", "mem_pct": "mem_pct", "mem_anon_gib": "mem_anon_gib",
            "mem_anon_pct": "mem_anon_pct", "io_psi": "io_psi_full_avg10", "mem_psi": "mem_psi_full_avg10",
            "cpu_psi": "cpu_psi_full_avg10", "cpu_usage_s": "cpu_usage_s", "cpu_throttled_s": "cpu_throttled_s",
            "write_bytes": "write_bytes", "localtmp_used_pct": "localtmp_used_pct",
        }  # fmt: skip
        for s in samples:
            out.append(records.make("sample", pod_id=pid, ts_ms=records.ms(s["time"]), **{col: s[key] for col, key in keys.items() if key in s}))
        if uri:
            out.append(records.make("log_ref", uri=uri, pod_id=pid, lines=len(lines)))
        return out
