import json
import pathlib
import string

import pytest
import yaml

from kubejobs import manifest, run
from kubejobs.models import Aoi, JobSpec, RunSpec
from kubejobs.phases import Phase

TEMPLATE = pathlib.Path(__file__).parents[2] / "infra" / "k8s" / "phase-job.yaml"


def jobs(make_settings, **kw) -> dict[Phase, JobSpec]:  # type: ignore[no-untyped-def]
    spec = RunSpec(
        run_id="1002-1410",
        aoi=Aoi(tiles=("20N_090W", "20N_080W"), iso_3166s=("HND",)),
        methodology="STATISTICAL",
        phases=tuple(Phase),
        **kw,
    )
    return {p.phase: p.job for p in run.plan(make_settings(), spec).phases if p.job}  # type: ignore[misc]


def test_job_shape(make_settings) -> None:  # type: ignore[no-untyped-def]
    j = jobs(make_settings)
    body = manifest.build_job(j[Phase.COMPUTE]).to_dict()
    spec = body["spec"]
    assert spec["completionMode"] == "Indexed"
    assert (spec["completions"], spec["parallelism"]) == (2, 2)
    assert spec["maxFailedIndexes"] == 2  # a failed tile must not cancel the others
    actions = [r["action"] for r in spec["podFailurePolicy"]["rules"]]
    assert actions == ["FailIndex", "Ignore"]  # compute: OOM fails the index at once
    pod = spec["template"]["spec"]
    assert pod["containers"][0]["command"] == ["python", "infra/run_phase.py"]
    resources = pod["containers"][0]["resources"]
    assert resources["requests"]["memory"] == resources["limits"]["memory"]
    assert body["metadata"]["annotations"][manifest.TILES] == "20N_080W,20N_090W"
    export = manifest.build_job(j[Phase.EXPORT]).to_dict()
    assert [r["action"] for r in export["spec"]["podFailurePolicy"]["rules"]] == [
        "Ignore"
    ]


def test_fail_fast_stops_at_the_first_failed_index(make_settings) -> None:  # type: ignore[no-untyped-def]
    body = manifest.build_job(
        jobs(make_settings, fail_fast=True)[Phase.COMPUTE]
    ).to_dict()
    assert body["spec"]["maxFailedIndexes"] == 0


def test_a_shell_command_is_refused(make_settings) -> None:  # type: ignore[no-untyped-def]
    import dataclasses

    bad = dataclasses.replace(
        jobs(make_settings)[Phase.COMPUTE], command=("/bin/sh", "-lc", "x")
    )
    with pytest.raises(ValueError, match="exec-form"):
        manifest.build_job(bad)


def test_spec_hash_tracks_the_spec(make_settings) -> None:  # type: ignore[no-untyped-def]
    a = manifest.build_job(jobs(make_settings)[Phase.COMPUTE])
    again = manifest.build_job(jobs(make_settings)[Phase.COMPUTE])
    other = manifest.build_job(jobs(make_settings, parallelism=1)[Phase.COMPUTE])
    hash_of = lambda job: job.metadata.annotations[manifest.SPEC_HASH]
    assert hash_of(a) == hash_of(again) != hash_of(other)


def test_yaml_round_trips(make_settings) -> None:  # type: ignore[no-untyped-def]
    job = manifest.build_job(jobs(make_settings)[Phase.INGEST_TILES])
    assert yaml.safe_load(manifest.to_yaml(job)) == job.to_dict()


@pytest.mark.parametrize("phase", list(Phase))
def test_parity_with_the_template_it_replaces(make_settings, phase: Phase) -> None:  # type: ignore[no-untyped-def]
    """Everything the old ``phase-job.yaml`` set, the new builder sets identically.

    Temporary: delete with the template. The additions (failure policy, annotations, labels,
    three env vars) are deliberate and listed by their absence from the old side.
    """
    j = jobs(make_settings)[phase]
    r = j.resources
    old = yaml.safe_load(
        string.Template(TEMPLATE.read_text()).substitute(
            ARGS=json.dumps(list(j.args)),
            COMPLETIONS=j.completions,
            CPU_LIMIT=r.cpu_limit,
            CPU_REQUEST=r.cpu_request,
            IMAGE=j.image,
            JOB_NAME=j.name,
            LOCALTMP_SIZE=r.localtmp,
            MEMORY_LIMIT=r.memory_limit,
            MEMORY_REQUEST=r.memory_request,
            NAMESPACE=j.namespace,
            NODE_POOL=j.node_pool,
            PARALLELISM=j.parallelism,
            PHASE=str(j.phase),
            POD_DEADLINE_SECONDS=j.deadline_s,
            RUN_ID=j.run_id,
            SECRET_NAME=j.secret,
            SERVICE_ACCOUNT=j.service_account,
        )
    )
    new = manifest.build_job(j).to_dict()

    for key in (
        "completionMode", "completions", "parallelism", "ttlSecondsAfterFinished",
    ):  # fmt: skip
        assert new["spec"][key] == old["spec"][key], key
    # deliberate: ingest-tiles retries a flaky source twice, the template gave every phase one
    assert new["spec"]["backoffLimitPerIndex"] == j.retries_per_index
    assert new["metadata"]["name"] == old["metadata"]["name"]
    assert old["metadata"]["labels"].items() <= new["metadata"]["labels"].items()
    old_pod, new_pod = old["spec"]["template"]["spec"], new["spec"]["template"]["spec"]
    for key in (
        "restartPolicy", "activeDeadlineSeconds", "serviceAccountName", "nodeSelector",
        "tolerations", "volumes",
    ):  # fmt: skip
        assert new_pod[key] == old_pod[key], key
    old_c, new_c = old_pod["containers"][0], new_pod["containers"][0]
    for key in (
        "name", "image", "imagePullPolicy", "command", "args", "resources", "volumeMounts",
    ):  # fmt: skip
        assert new_c[key] == old_c[key], key
    assert new_c["env"][: len(old_c["env"])] == old_c["env"]
