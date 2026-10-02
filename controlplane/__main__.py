"""``uv run python -m controlplane ...``: argparse subcommands over the library."""

import argparse
import datetime
import sys

from controlplane import aoi, manifest, run
from controlplane import settings as settings_module
from controlplane.models import Aoi, RunPlan, RunSpec
from controlplane.phases import Phase


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


def cmd_submit(args: argparse.Namespace) -> int:
    if not args.dry_run:
        print(
            "error: only --dry-run exists so far; submitting needs the cluster layer",
            file=sys.stderr,
        )
        return 2
    if args.dry_run == "server":
        print("error: --dry-run=server needs the cluster layer", file=sys.stderr)
        return 2
    settings = settings_module.Settings.load()
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
    plan = run.plan(settings, spec, allow_foreign_pool=args.allow_foreign_pool)
    print(summary(plan, settings))
    if args.output == "yaml":
        for p in plan.phases:
            if p.job:
                print(
                    f"---\n# {p.job.name}\n{manifest.to_yaml(manifest.build_job(p.job))}",
                    end="",
                )
    return 1 if plan.errors else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="controlplane", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser(
        "doctor", help="validate the settings and print the target"
    ).set_defaults(func=cmd_doctor)
    s = sub.add_parser("submit", help="plan a run (only --dry-run so far)")
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
        "--dry-run", action="store_true", help="plan and print; nothing else exists yet"
    )
    s.add_argument("-o", "--output", choices=("summary", "yaml"), default="summary")
    s.set_defaults(func=cmd_submit)
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except (ValueError, settings_module.SettingsError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
