"""``JobSpec`` -> Kubernetes Job. Pure: no I/O, no settings, no cluster.

One of the two modules that import the Kubernetes client (``cluster.py`` will be the other).
The shape is what ``infra/k8s/phase-job.yaml`` produced, so ``manifest_test.py`` can compare the
two; the additions are the failure policy, the labels and the annotations.
"""

import hashlib
import json
import typing

import yaml
from lightkube.models import batch_v1, core_v1, meta_v1
from lightkube.resources.batch_v1 import Job

from kuberjobtower.models import JobSpec

ANNOTATION_PREFIX = "cornerstone.adastra.eco/"
SPEC_HASH = ANNOTATION_PREFIX + "spec-hash"
TILES = ANNOTATION_PREFIX + "tiles"
IMAGE = ANNOTATION_PREFIX + "image"
RUN_UID = ANNOTATION_PREFIX + "run-uid"
CONTAINER = "phase"
NODEPOOL_LABEL = "cloud.google.com/gke-nodepool"


def check_exec_form(command: typing.Sequence[str]) -> None:
    """A shell wrapper (``sh -lc``) resets PATH and drops /app/.venv/bin: the 29 Sept failure."""
    if not command or command[0].rsplit("/", 1)[-1] in ("sh", "bash", "zsh"):
        raise ValueError(f"command must be exec-form, not a shell: {list(command)}")


def build_job(spec: JobSpec) -> Job:
    check_exec_form(spec.command)
    res = spec.resources
    container = core_v1.Container(
        name=CONTAINER,
        image=spec.image,
        # The tag is reused across rebuilds; a node must not run a stale cached image.
        imagePullPolicy="Always",
        command=list(spec.command),
        args=list(spec.args),
        env=[
            # all local temp (tempfile, GDAL) onto the per-pod pd-ssd mounted below
            core_v1.EnvVar(name="TMPDIR", value="/localtmp"),
            core_v1.EnvVar(name="CPL_TMPDIR", value="/localtmp"),
            core_v1.EnvVar(name="RUN_ID", value=spec.run_id),
            core_v1.EnvVar(
                name="POD_NAME",
                valueFrom=core_v1.EnvVarSource(
                    fieldRef=core_v1.ObjectFieldSelector(fieldPath="metadata.name")
                ),
            ),
            core_v1.EnvVar(name="LOG_FORMAT", value="json"),
        ],
        resources=core_v1.ResourceRequirements(
            # ephemeral-storage guards only the container's own overlay; the real scratch is /localtmp
            requests={
                "cpu": res.cpu_request,
                "memory": res.memory_request,
                "ephemeral-storage": "2Gi",
            },
            limits={
                "cpu": res.cpu_limit,
                "memory": res.memory_limit,
                "ephemeral-storage": "8Gi",
            },
        ),
        volumeMounts=[
            # Config.from_dot_env reads this file
            core_v1.VolumeMount(
                name="dotenv", mountPath="/app/.env", subPath=".env", readOnly=True
            ),
            core_v1.VolumeMount(name="localtmp", mountPath="/localtmp"),
        ],
    )
    pod = core_v1.PodSpec(
        restartPolicy="Never",
        # Per pod, not per Job: a Job-level deadline would cover every index.
        activeDeadlineSeconds=spec.deadline_s,
        serviceAccountName=spec.service_account,
        nodeSelector={NODEPOOL_LABEL: spec.node_pool},
        tolerations=[
            core_v1.Toleration(
                key="worker", operator="Equal", value="true", effect="NoSchedule"
            )
        ],
        containers=[container],
        volumes=[
            core_v1.Volume(
                name="dotenv", secret=core_v1.SecretVolumeSource(secretName=spec.secret)
            ),
            core_v1.Volume(
                name="localtmp",
                ephemeral=core_v1.EphemeralVolumeSource(
                    volumeClaimTemplate=core_v1.PersistentVolumeClaimTemplate(
                        spec=core_v1.PersistentVolumeClaimSpec(
                            accessModes=["ReadWriteOnce"],
                            storageClassName="premium-rwo",
                            resources=core_v1.VolumeResourceRequirements(
                                requests={"storage": res.localtmp}
                            ),
                        )
                    )
                ),
            ),
        ],
    )
    rules = []
    if spec.fail_index_on_oom:
        # An OOM retried on the same machine type fails identically.
        rules.append(
            batch_v1.PodFailurePolicyRule(
                action="FailIndex",
                onExitCodes=batch_v1.PodFailurePolicyOnExitCodesRequirement(
                    containerName=CONTAINER, operator="In", values=[137]
                ),
            )
        )
    # Scale-down and eviction are not the tile's fault: they must not burn its retry budget.
    rules.append(
        batch_v1.PodFailurePolicyRule(
            action="Ignore",
            onPodConditions=[
                batch_v1.PodFailurePolicyOnPodConditionsPattern(
                    type="DisruptionTarget", status="True"
                )
            ],
        )
    )
    job = Job(
        metadata=meta_v1.ObjectMeta(
            name=spec.name,
            namespace=spec.namespace,
            labels=dict(spec.labels),
            annotations={IMAGE: spec.image},
        ),
        spec=batch_v1.JobSpec(
            completionMode="Indexed",
            completions=spec.completions,
            parallelism=spec.parallelism,
            # Retries are per index, so one poisoned tile cannot burn the Job's budget.
            backoffLimitPerIndex=spec.retries_per_index,
            maxFailedIndexes=spec.max_failed_indexes,
            ttlSecondsAfterFinished=spec.ttl_s,
            podFailurePolicy=batch_v1.PodFailurePolicy(rules=rules),
            template=core_v1.PodTemplateSpec(
                metadata=meta_v1.ObjectMeta(labels=dict(spec.labels)),
                spec=pod,
            ),
        ),
    )
    assert job.metadata and job.metadata.annotations is not None
    if spec.run_uid:
        job.metadata.annotations[RUN_UID] = spec.run_uid
    if spec.tiles:
        job.metadata.annotations[TILES] = ",".join(spec.tiles)
    job.metadata.annotations[SPEC_HASH] = spec_hash(job)
    return job


def spec_hash(job: Job) -> str:
    """Hash of the Job minus its own hash annotation: equal specs hash equal."""
    body = job.to_dict()
    body["metadata"].get("annotations", {}).pop(SPEC_HASH, None)
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()[:16]


def to_yaml(job: Job) -> str:
    return yaml.safe_dump(job.to_dict(), sort_keys=False)
