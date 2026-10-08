"""``uv run python -m kuberjobtower ...``: argparse subcommands over the library."""

import argparse
import datetime
import sys

from kuberjobtower import aoi, manifest, report, run
from kuberjobtower import settings as settings_module
from kuberjobtower.cluster import Cluster, ClusterError
from kuberjobtower.collect import logs as collect_logs
from kuberjobtower.models import Aoi, JobState, PhasePlan, RunPlan, RunSpec
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


def build_plan(args: argparse.Namespace, settings: settings_module.Settings) -> RunPlan:
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
    )
    return run.plan(settings, spec, allow_foreign_pool=args.allow_foreign_pool)


def cmd_submit(args: argparse.Namespace) -> int:
    settings = settings_module.Settings.load()
    plan = build_plan(args, settings)
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

    def archive(pp: PhasePlan, _: JobState) -> None:
        stored = collect_logs.archive_phase(
            cluster, settings.archive_root, plan.spec.run_id, str(pp.phase)
        )
        print(f"  archived {len(stored)} pod log(s) under {settings.archive_root}")

    return 0 if run.execute(plan, cluster, poll_s=args.poll, after_phase=archive) else 1


def cmd_status(args: argparse.Namespace) -> int:
    cluster = Cluster(settings_module.Settings.load())
    jobs = cluster.jobs(args.run_id)
    if not jobs:
        print("no kuberjobtower Jobs" + (f" for run {args.run_id}" if args.run_id else ""))
        return 0
    print(f"{'JOB':52} {'STATE':9} {'DONE':>7} {'ACT':>3} {'FAIL':>4}")
    for j in jobs:
        print(
            f"{j.name:52} {j.state:9} {f'{j.succeeded}/{j.completions}':>7} {j.active:>3} {j.failed:>4}"
        )
        if args.pods:
            for pod in cluster.pods(j.run_id, j.phase):
                print(
                    f"  idx {pod.index}  {pod.name}  {pod.phase}  node {pod.node}  exit {pod.exit_code} {pod.reason or ''}"
                )
    return 0


def _lines(args: argparse.Namespace, tail: int | None = None) -> list[str]:
    """The pod's log lines from the first source that has them, naming it on stderr."""
    settings = settings_module.Settings.load()
    source, lines = collect_logs.lines_for(
        cluster=Cluster(settings),
        project=settings.gcp_project,
        namespace=settings.namespace,
        archive_root=settings.archive_root,
        run_id=args.run_id,
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
    print(report.table(_lines(args)))
    return 0


def cmd_cleanup(args: argparse.Namespace) -> int:
    cluster = Cluster(settings_module.Settings.load())
    names = [j.name for j in cluster.jobs(args.run_id)]
    if not args.yes:
        print(f"would delete {len(names)} Job(s): {', '.join(names) or '-'}; add --yes")
        return 0 if names else 1
    print(f"deleted {', '.join(cluster.delete_run(args.run_id)) or 'nothing'}")
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
