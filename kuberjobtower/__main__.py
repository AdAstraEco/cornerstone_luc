"""``uv run python -m kuberjobtower ...``: argparse subcommands over the library."""

import argparse
import datetime
import sqlite3
import sys
import time

from kuberjobtower import aoi, manifest, report, run
from kuberjobtower import settings as settings_module
from kuberjobtower.cluster import Cluster, ClusterError
from kuberjobtower.collect import cost as collect_cost
from kuberjobtower.collect import events as collect_events
from kuberjobtower.collect import logs as collect_logs
from kuberjobtower.collect import monitoring, verdicts
from kuberjobtower.collect import pods as collect_pods
from kuberjobtower.collect.recorder import Recorder
from kuberjobtower.history import db as history_db
from kuberjobtower.history import queries as history_queries
from kuberjobtower.history import records as history_records
from kuberjobtower.models import Aoi, JobState, PhasePlan, PodEvent, RunPlan, RunSpec
from kuberjobtower.phases import Phase


def summary(plan: RunPlan, settings: settings_module.Settings) -> str:
    spec = plan.spec
    lines = [
        settings.banner(),
        f"run-id {spec.run_id}  methodology {spec.methodology}  image {spec.image or settings.image}",
        f"AOI    tiles {', '.join(spec.aoi.tiles) if spec.aoi.tiles else 'resolved from countries after ingest-world'}"
        f"  countries {', '.join(spec.aoi.iso_3166s) or '-'}",
        "",
        f"{'phase':13} {'idx':>4} {'par':>4}  {'cpu':7} {'mem(req=lim)':13} {'localtmp':9} {'pool':27} secret",
    ]
    for p in plan.phases:
        if p.job is None:
            lines.append(f"{p.phase!s:13} {p.note}")
            continue
        j, r = p.job, p.job.resources
        lines.append(
            f"{p.phase!s:13} {j.completions:>4} {j.parallelism:>4}  "
            f"{r.cpu_request + '/' + r.cpu_limit:7} "
            f"{r.memory_limit if r.memory_request == r.memory_limit else r.memory_request + '/' + r.memory_limit:13} "
            f"{r.localtmp:9} {j.node_pool:27} {j.secret}"
        )
    for c in plan.checks:
        lines.append(f"{c.severity.upper():8} {c.name}: {c.message}")
    return "\n".join(lines)


def cmd_doctor(_: argparse.Namespace) -> int:
    settings = settings_module.Settings.load()
    print(settings.banner())
    print(
        f"pools    heavy {settings.pool_heavy}  light {settings.pool_light}  allowed {settings.allowed_pool_regex}"
    )
    print(
        "settings ok. Cluster, Secret and bucket checks arrive with the cluster layer."
    )
    return 0


def build_plan(
    args: argparse.Namespace, settings: settings_module.Settings, run_uid: str = ""
) -> RunPlan:
    methodologies = aoi.methodology_names()
    if args.methodology_name not in methodologies:
        raise ValueError(
            f"--methodology-name must be one of {', '.join(methodologies)}"
        )
    spec = RunSpec(
        run_id=args.run_id,
        aoi=Aoi(
            tiles=aoi.validate_tiles(args.tile) if args.tile else None,
            iso_3166s=aoi.validate_countries(args.country),
        ),
        methodology=args.methodology_name,
        phases=tuple(Phase(p) for p in args.phases),
        parallelism=args.parallelism,
        ingest_concurrency=args.concurrency,
        skip_ingest=not args.no_skip_ingest,
        overrides=run.parse_overrides(
            [p.replace("=", ".pool=", 1) for p in args.pool] + args.set
        ),
        image=args.image,
        ttl_s=run.parse_duration(args.ttl),
        fail_fast=args.fail_fast,
        run_uid=run_uid,
    )
    return run.plan(settings, spec, allow_foreign_pool=args.allow_foreign_pool)


def cmd_submit(args: argparse.Namespace) -> int:
    settings = settings_module.Settings.load()
    plan = build_plan(args, settings)
    if not plan.spec.aoi.tiles and any(p.is_per_tile for p in plan.spec.phases):
        aoi.check_boundary_config()
    print(summary(plan, settings))
    if args.output == "yaml":
        for p in plan.phases:
            if p.job:
                print(
                    f"---\n# {p.job.name}\n{manifest.to_yaml(manifest.build_job(p.job))}",
                    end="",
                )
    if args.dry_run:
        return 1 if plan.errors else 0
    if plan.errors:
        return 1
    if not args.yes:
        print("\nnothing submitted: add --yes to create these Jobs")
        return 2
    tiles = len(plan.spec.aoi.tiles or ())
    if tiles >= settings.confirm_tiles and not args.i_know:
        print(f"{tiles} tiles is at or above {settings.confirm_tiles}: add --i-know")
        return 2
    print()
    cluster = Cluster(settings)
    # a resumed run keeps the uid its Jobs already carry; a new run gets a fresh one
    run_uid = (
        next((j.run_uid for j in cluster.jobs(args.run_id) if j.run_uid), "")
        or history_records.new_run_uid()
    )
    plan = build_plan(args, settings, run_uid)
    print(f"run {run_uid}  (history: {settings.archive_root}/runs/{run_uid})")
    recorder = Recorder(settings, cluster, plan, run_uid)
    recorder.start()

    def record(pp: PhasePlan, final: JobState) -> None:
        for note in recorder.after_phase(pp, final):
            print(f"  {note}")

    ok = False
    try:
        resolve = (
            None
            if plan.spec.aoi.tiles
            else run.lazy_tiles(
                settings, plan.spec, aoi.tiles_for_countries,
                allow_foreign_pool=args.allow_foreign_pool, i_know=args.i_know,
            )
        )  # fmt: skip
        ok = run.execute(
            plan, cluster, poll_s=args.poll, after_phase=record, resolve=resolve
        )
    except BaseException:
        recorder.finish(False)
        raise
    recorder.finish(ok)
    return 0 if ok else 1


GIB = 1 << 30


def cmd_pools(_: argparse.Namespace) -> int:
    settings = settings_module.Settings.load()
    roles = {settings.pool_heavy: "heavy", settings.pool_light: "light"}
    print(
        f"{'POOL':28} {'ROLE':5} {'MACHINE':14} {'NODES':>5} {'TARGET':>6} {'MIN-MAX':>8} {'ALLOCATABLE/NODE':>17}"
    )
    for p in Cluster(settings).pools(list(roles)):
        alloc = (
            f"{p.allocatable_cpu_m / 1000:g} cpu {p.allocatable_memory_bytes / GIB:.1f} GiB"
            if p.allocatable_cpu_m and p.allocatable_memory_bytes
            else "- (no node up)"
        )
        size = f"{_n(p.min_size, 'd')}-{_n(p.max_size, 'd')}"
        print(
            f"{p.name:28} {roles[p.name]:5} {p.machine_type or '-':14} {p.nodes:>5} {_n(p.target, 'd'):>6} {size:>8} {alloc:>17}"
        )
    print(
        "read-only: pools are resized with gcloud or the platform's tooling, never from here"
    )
    return 0


def _top_once(cluster: Cluster, run_ids: list[str]) -> int:
    usage = cluster.pod_usage()
    print(
        f"{'POD':36} {'PHASE':10} {'NODE':12} {'CPU m':>6} {'/ REQ':>6} {'MEM GiB':>8} {'/ REQ':>6} {'%':>4}"
    )
    shown = 0
    for run_id in run_ids:
        for pod in cluster.pods(run_id):
            if pod.phase != "Running":
                continue
            shown += 1
            u = usage.get(pod.name)
            if u is None:  # metrics-server is about a minute behind a new pod
                print(
                    f"{pod.name[-36:]:36} {pod.phase:10} {(pod.node or '-')[-12:]:12} {'-':>6} {'':>6} {'-':>8} {'':>6} {'':>4}"
                )
                continue
            req = pod.memory_request_bytes
            pct = f"{100 * u.memory_bytes / req:.0f}" if req else "-"
            print(
                f"{pod.name[-36:]:36} {pod.phase:10} {(pod.node or '-')[-12:]:12} {u.cpu_m:>6} {_n(pod.cpu_request_m, 'd'):>6} "
                f"{u.memory_bytes / GIB:>8.1f} {_n(req and req / GIB):>6} {pct:>4}"
            )
    if not shown:
        print("no running pods")
    return shown


def cmd_top(args: argparse.Namespace) -> int:
    settings = settings_module.Settings.load()
    cluster = Cluster(settings)
    while True:
        run_ids = (
            [_resolve(settings, args.run_id)[0]]
            if args.run_id
            else sorted({j.run_id for j in cluster.jobs() if j.state == "running"})
        )
        shown = _top_once(cluster, run_ids)
        if not args.watch:
            break
        if not shown and not args.run_id:
            break
        print()
        time.sleep(args.interval)
    print(
        "working set from metrics-server, the number the kubelet evicts on; see `report` for the heap alone"
    )
    return 0


def _resolve(settings: settings_module.Settings, text: str) -> tuple[str, str]:
    """(run id, history key) for a run id or run uid; runs from before the history store use the id."""
    db = history_db.open_store(settings.history_db)
    try:
        row = history_queries.resolve_run(db, text)
        if row is None:  # maybe recorded on another machine
            history_db.sync(db, settings.archive_root)
            row = history_queries.resolve_run(db, text)
    except Exception:
        row = None
    finally:
        db.close()
    return (row["run_id"], row["run_uid"]) if row else (text, text)


def cmd_status(args: argparse.Namespace) -> int:
    settings = settings_module.Settings.load()
    cluster = Cluster(settings)
    run_id = _resolve(settings, args.run_id)[0] if args.run_id else None
    jobs = cluster.jobs(run_id)
    if not jobs:
        print(
            "no kuberjobtower Jobs" + (f" for run {args.run_id}" if args.run_id else "")
        )
        return 0
    events_by_run: dict[str, list[PodEvent]] = {}
    print(f"{'JOB':52} {'STATE':9} {'DONE':>7} {'ACT':>3} {'FAIL':>4}")
    for j in jobs:
        print(
            f"{j.name:52} {j.state:9} {f'{j.succeeded}/{j.completions}':>7} {j.active:>3} {j.failed:>4}"
        )
        if args.pods:
            events = events_by_run.setdefault(j.run_id, cluster.events(j.run_id))
            for pod in cluster.pods(j.run_id, j.phase):
                print(
                    f"  idx {pod.index}  {pod.name}  {pod.phase}  node {pod.node}  exit {pod.exit_code} {pod.reason or ''}"
                )
                for v in verdicts.pod_verdicts(pod, events):
                    print(f"      {v.severity.upper()} {v.code}: {v.message}")
    return 0


def _lines(args: argparse.Namespace, tail: int | None = None) -> list[str]:
    """The pod's log lines from the first source that has them, naming it on stderr."""
    settings = settings_module.Settings.load()
    run_id, key = _resolve(settings, args.run_id)
    source, lines = collect_logs.lines_for(
        cluster=Cluster(settings),
        project=settings.gcp_project,
        namespace=settings.namespace,
        archive_root=settings.archive_root,
        run_id=run_id,
        archive_key=key,
        phase=args.phase,
        index=args.index,
        source=args.source,
        tail=tail,
    )
    print(f"(from {source}, {len(lines)} lines)", file=sys.stderr)
    return lines


def cmd_logs(args: argparse.Namespace) -> int:
    for line in _lines(args, args.tail):
        print(line)
    return 0


def cmd_report(args: argparse.Namespace) -> int:
    lines = _lines(args)
    print(report.table(lines))
    # the kubelet's view is the number eviction is decided on; a failed lookup only drops this line
    records = report.parse(lines)
    pod = next((r["pod"] for r in records if "pod" in r), None)
    if pod:
        settings = settings_module.Settings.load()
        try:
            view = monitoring.kubelet_view(
                settings.gcp_project,
                settings.namespace,
                pod,
                records[0]["_t"],
                records[-1]["_t"],
            )
            print(view.line())
        except Exception as exc:
            print(f"(kubelet view unavailable: {exc})", file=sys.stderr)
    return 0


def cmd_cost(args: argparse.Namespace) -> int:
    settings = settings_module.Settings.load()
    by_phase = collect_pods.read_archive(
        settings.archive_root, _resolve(settings, args.run_id)[1]
    )
    if not by_phase:
        print(
            f"no archived pod records for run {args.run_id} (only runs submitted with this version have them)"
        )
        return 1
    print(
        f"{'phase':14} {'pods':>4} {'pending':>8} {'ran':>8} {'machine':14} {'share':>5} {'USD':>7}"
    )
    grand, unknown = 0.0, 0
    for phase, pods in by_phase.items():
        costs = [float(p["cost_usd"]) for p in pods if p.get("cost_usd") is not None]  # type: ignore[arg-type]
        usd = sum(costs)
        unknown += sum(p.get("cost_usd") is None for p in pods)
        grand += usd
        first = pods[0]
        print(
            f"{phase:14} {len(pods):>4} {_minutes(first.get('pending_s')):>8} {_minutes(first.get('run_s')):>8} "
            f"{first.get('machine_type') or '-'!s:14} {first.get('node_share') or '-':>5} {usd:>7.2f}"
        )
    print(f"{'total':14} {'':>4} {'':>8} {'':>8} {'':14} {'':>5} {grand:>7.2f}")
    print(
        f"approximate: on-demand list price as of {collect_cost.RATES_AS_OF}, node share by request, "
        "no idle tail before a node is removed, no discounts"
        + (f"; {unknown} pod(s) without an estimate" if unknown else "")
    )
    return 0


def _minutes(seconds: object) -> str:
    return f"{float(seconds) / 60:.1f}m" if isinstance(seconds, int | float) else "-"  # type: ignore[arg-type]


def cmd_events(args: argparse.Namespace) -> int:
    settings = settings_module.Settings.load()
    run_id, key = _resolve(settings, args.run_id)
    source, found = collect_events.events_for(
        cluster=Cluster(settings),
        project=settings.gcp_project,
        namespace=settings.namespace,
        archive_root=settings.archive_root,
        run_id=run_id,
        archive_key=key,
        source=args.source,
    )
    print(f"(from {source}, {len(found)} events)", file=sys.stderr)
    for e in found:
        if args.phase and f"-{args.phase}-" not in e.pod:
            continue
        if args.warnings and e.type != "Warning":
            continue
        print(
            f"{e.time:%H:%M:%S} {e.type[:4]:4} {e.reason:18} {e.pod[-34:]:34} {e.message[:150]}"
        )
    return 0


def cmd_cleanup(args: argparse.Namespace) -> int:
    settings = settings_module.Settings.load()
    cluster = Cluster(settings)
    run_id = _resolve(settings, args.run_id)[0]
    names = [j.name for j in cluster.jobs(run_id)]
    if not args.yes:
        print(f"would delete {len(names)} Job(s): {', '.join(names) or '-'}; add --yes")
        return 0 if names else 1
    print(f"deleted {', '.join(cluster.delete_run(run_id)) or 'nothing'}")
    return 0


def _when(ms: int | None) -> str:
    if ms is None:
        return "-"
    return datetime.datetime.fromtimestamp(ms / 1000, tz=datetime.UTC).strftime(
        "%m-%d %H:%M"
    )


def _n(value: object, spec: str = ".1f") -> str:
    return "-" if value is None else format(value, spec)


def _history_db(
    settings: settings_module.Settings, *, sync: bool = True
) -> sqlite3.Connection:
    db = history_db.open_store(settings.history_db)
    if sync:
        try:
            applied = history_db.sync(db, settings.archive_root)
            if applied:
                print(f"(synced {applied} journal chunk(s))", file=sys.stderr)
        except Exception as exc:  # the local copy is still worth showing
            print(
                f"(could not sync from {settings.archive_root}: {exc})", file=sys.stderr
            )
    return db


def cmd_history(args: argparse.Namespace) -> int:
    settings = settings_module.Settings.load()
    if args.what == "rebuild":
        n = history_db.rebuild(settings.history_db, settings.archive_root)
        print(f"rebuilt {settings.history_db} from {n} journal chunk(s)")
        return 0
    db = _history_db(settings)
    q = history_queries
    match args.what:
        case "sync":
            print(
                f"{db.execute('SELECT COUNT(*) FROM ingested_objects').fetchone()[0]} journal chunk(s) applied in total"
            )
        case "runs":
            print(
                f"{'RUN_UID':18} {'RUN_ID':10} {'STATUS':10} {'STARTED':12} {'JOBS':>4} {'USD':>6}  AOI / BY"
            )
            for r in q.runs(db, args.limit):
                print(
                    f"{r['run_uid']:18} {r['run_id']:10} {r['status']:10} {_when(r['started_ms']):12} {r['n_jobs']:>4} {_n(r['cost_usd'], '.2f'):>6}  {r['aoi'] or '-'} / {r['submitted_by'] or '-'}"
                )
        case "show":
            run_row = q.resolve_run(db, args.run)
            if run_row is None:
                print(
                    f"no run {args.run!r} in the history (try `history sync`)",
                    file=sys.stderr,
                )
                return 1
            uid = run_row["run_uid"]
            print(
                f"run {uid} ({run_row['run_id']}): {run_row['status']}, {_when(run_row['started_ms'])} to {_when(run_row['finished_ms'])}"
            )
            print(
                f"  by {run_row['submitted_by']} on {run_row['cluster']}/{run_row['namespace']}, image {run_row['image']}"
            )
            print(
                f"{'PHASE':14} {'POD':>3} {'TILE':9} {'STATE':10} {'TIME':>7} {'HEAP GiB':>8} {'HEAP %':>6} {'USD':>6}  VERDICTS"
            )
            for p_ in q.pods(db, uid):
                dur = (
                    f"{p_['duration_s'] / 60:.1f}m"
                    if p_["duration_s"] is not None
                    else "-"
                )
                print(
                    f"{p_['job_phase']:14} {_n(p_['idx'], 'd'):>3} {p_['tile_id'] or '-':9} {(p_['reason'] or p_['phase']):10} {dur:>7} {_n(p_['peak_anon_gib']):>8} {_n(p_['peak_anon_pct'], '.0f'):>6} {_n(p_['cost_usd'], '.2f'):>6}  {p_['verdicts'] or ''}"
                )
            warnings = [
                e
                for e in q.events(db, uid)
                if e["type"] == "Warning" and e["reason"] != "FailedScheduling"
            ]
            for e in warnings[-5:]:
                print(f"  event {e['reason']}: {(e['message'] or '')[:150]}")
        case "tile":
            print(
                f"{'RUN_UID':18} {'PHASE':14} {'RESULT':10} {'TIME':>7} {'HEAP GiB':>8} {'HEAP %':>6} {'USD':>6}  MACHINE"
            )
            for r in q.tile_history(db, args.tile):
                dur = (
                    f"{r['duration_s'] / 60:.1f}m"
                    if r["duration_s"] is not None
                    else "-"
                )
                print(
                    f"{r['run_uid']:18} {r['phase']:14} {(r['reason'] or r['pod_phase']):10} {dur:>7} {_n(r['peak_anon_gib']):>8} {_n(r['peak_anon_pct'], '.0f'):>6} {_n(r['cost_usd'], '.2f'):>6}  {r['machine_type'] or '-'}"
                )
        case "near-oom":
            for r in q.near_limit(db, args.pct):
                print(
                    f"{r['run_uid']:18} {r['phase']:14} {r['tile_id'] or '-':9} heap {_n(r['peak_anon_gib'])} GiB = {_n(r['peak_anon_pct'], '.0f')}% of the limit {r['reason'] or ''}"
                )
        case "prune":
            print(
                f"dropped {history_db.prune_samples(db, args.days)} sample(s) older than {args.days} days"
            )
        case "verify":
            problems = history_db.verify(db)
            print("\n".join(problems) or "ok")
            return 1 if problems else 0
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="kuberjobtower", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser(
        "doctor", help="validate the settings and print the target"
    ).set_defaults(func=cmd_doctor)
    s = sub.add_parser(
        "submit", help="plan a run; with --yes, create its Jobs and wait for each phase"
    )
    s.add_argument(
        "--tile",
        action="append",
        default=[],
        metavar="ID",
        help="a ten-degree tile; repeatable",
    )
    s.add_argument(
        "--country", nargs="+", default=[], metavar="ISO", help="ISO-3166 alpha-3 codes"
    )
    s.add_argument(
        "--phases",
        nargs="+",
        choices=[str(p) for p in Phase],
        default=[str(p) for p in Phase],
    )
    s.add_argument("--methodology-name", default="STATISTICAL")
    s.add_argument("--parallelism", type=int, default=8)
    s.add_argument("--concurrency", type=int, default=4, help="in-pod ingest threads")
    s.add_argument(
        "--run-id", default=datetime.datetime.now(datetime.UTC).strftime("%m%d-%H%M")
    )
    s.add_argument("--image")
    s.add_argument("--ttl", default="30m")
    s.add_argument("--pool", action="append", default=[], metavar="PHASE=POOL")
    s.add_argument("--set", action="append", default=[], metavar="PHASE.FIELD=VALUE")
    s.add_argument("--no-skip-ingest", action="store_true")
    s.add_argument("--fail-fast", action="store_true")
    s.add_argument("--allow-foreign-pool", action="store_true")
    s.add_argument(
        "--dry-run", action="store_true", help="plan and print; touch nothing"
    )
    s.add_argument("--yes", action="store_true", help="create the Jobs")
    s.add_argument(
        "--i-know", action="store_true", help="also needed from CONFIRM_TILES tiles up"
    )
    s.add_argument(
        "--poll", type=float, default=15, help="seconds between status checks"
    )
    s.add_argument("-o", "--output", choices=("summary", "yaml"), default="summary")
    s.set_defaults(func=cmd_submit)
    st = sub.add_parser("status", help="Jobs of one run (or all kuberjobtower Jobs)")
    st.add_argument("run_id", nargs="?")
    st.add_argument("--pods", action="store_true")
    st.set_defaults(func=cmd_status)
    for name, func, help_ in (
        ("logs", cmd_logs, "a pod's log"),
        ("report", cmd_report, "per-step resource use of a pod"),
    ):
        p = sub.add_parser(name, help=help_)
        p.add_argument("run_id")
        p.add_argument("--phase", required=True, choices=[str(x) for x in Phase])
        p.add_argument("--index", type=int, default=0, help="pod index (default 0)")
        p.add_argument(
            "--source",
            choices=collect_logs.SOURCES,
            default="auto",
            help="auto tries the live pod, then the archive, then Cloud Logging",
        )
        if name == "logs":
            p.add_argument("--tail", type=int)
        p.set_defaults(func=func)
    h = sub.add_parser(
        "history", help="the run history: past runs, per-tile results, near-OOM pods"
    )
    hs = h.add_subparsers(dest="what", required=True)
    hs.add_parser("sync", help="apply journal chunks this machine has not seen")
    hs.add_parser(
        "rebuild", help="delete the local database and rebuild it from the journal"
    )
    hs.add_parser("verify", help="rows whose parent is missing (chunks not yet synced)")
    hr = hs.add_parser("runs")
    hr.add_argument("--limit", type=int, default=20)
    hh = hs.add_parser("show")
    hh.add_argument("run", help="a run uid, a prefix of one, or a run id")
    ht = hs.add_parser("tile")
    ht.add_argument("tile")
    hn = hs.add_parser("near-oom", help="pods whose heap came near the memory limit")
    hn.add_argument("--pct", type=float, default=85.0)
    hp = hs.add_parser("prune", help="drop old samples (pod summaries stay)")
    hp.add_argument("--days", type=int, default=180)
    h.set_defaults(func=cmd_history)
    sub.add_parser(
        "pools", help="the node pools this tool uses: size, range, machine"
    ).set_defaults(func=cmd_pools)
    tp = sub.add_parser(
        "top",
        help="live CPU and memory of running pods (one run, or every running run)",
    )
    tp.add_argument("run_id", nargs="?")
    tp.add_argument(
        "--watch", action="store_true", help="repeat until nothing runs (or Ctrl-C)"
    )
    tp.add_argument("--interval", type=float, default=30)
    tp.set_defaults(func=cmd_top)
    co = sub.add_parser(
        "cost", help="approximate cost of a run, from its archived pod records"
    )
    co.add_argument("run_id")
    co.set_defaults(func=cmd_cost)
    ev = sub.add_parser(
        "events", help="Kubernetes events about a run's pods (kept after the pods)"
    )
    ev.add_argument("run_id")
    ev.add_argument("--phase", choices=[str(x) for x in Phase])
    ev.add_argument("--warnings", action="store_true", help="only Warning events")
    ev.add_argument(
        "--source", choices=("auto", "pod", "archive", "cloud"), default="auto"
    )
    ev.set_defaults(func=cmd_events)
    c = sub.add_parser("cleanup", help="delete one run's Jobs and pods")
    c.add_argument("run_id")
    c.add_argument("--yes", action="store_true")
    c.set_defaults(func=cmd_cleanup)
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except (
        ValueError,
        settings_module.SettingsError,
        ClusterError,
        run.RunError,
        collect_logs.LogsUnavailable,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
