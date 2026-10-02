"""Every cluster, bucket and path identifier is a setting; nothing else names a target.

Precedence: process environment, then the env file (``.env``, or ``CONTROLPLANE_ENV_FILE``),
then the default. A laptop has a file and sets nothing; a Cloud Run service sets variables and
has no file. See docs/control-plane/05-configuration-and-deployment.md.
"""

import dataclasses
import os
import pathlib
import re
import typing

import dotenv

PREFIX = "CONTROLPLANE_"


class SettingsError(ValueError):
    pass


@dataclasses.dataclass(frozen=True)
class Settings:
    # target cluster
    gcp_project: str
    gcp_zone: str
    gke_cluster: str
    namespace: str
    kube_context: (
        str  # the only context allowed to write; the cluster layer enforces it
    )
    # what the pods run as
    service_account: str
    secret: str
    secret_nokeys: str
    image: str
    # node pools
    pool_heavy: str
    pool_light: str
    allowed_pool_regex: str
    # storage
    bucket: str
    data_prefix: str
    # guardrails
    max_parallelism: int
    max_tiles: int
    max_completions_per_job: int
    confirm_tiles: int

    @classmethod
    def load(cls, environ: typing.Mapping[str, str] | None = None) -> typing.Self:
        """Read the settings; every problem is reported together, not one per run."""
        environ = os.environ if environ is None else environ
        env_file = environ.get(f"{PREFIX}ENV_FILE")
        if env_file and not pathlib.Path(env_file).is_file():
            raise SettingsError(f"{PREFIX}ENV_FILE={env_file} does not exist")
        file_values = dotenv.dotenv_values(
            env_file or dotenv.find_dotenv(usecwd=True) or None
        )

        def get(name: str, default: str | None = None) -> str | None:
            key = PREFIX + name
            value = environ.get(key, file_values.get(key))
            return value if value not in (None, "") else default

        errors: list[str] = []

        def required(name: str) -> str:
            value = get(name)
            if value is None:
                errors.append(f"{PREFIX}{name} is not set")
            return value or ""

        def number(name: str, default: int) -> int:
            raw = get(name, str(default)) or ""
            try:
                value = int(raw)
            except ValueError:
                errors.append(f"{PREFIX}{name}={raw!r} is not a whole number")
                return default
            if value <= 0:
                errors.append(f"{PREFIX}{name} must be positive")
            return value

        bucket = required("BUCKET")
        data_prefix = (get("DATA_PREFIX", "cornerstone") or "").strip("/")
        pool_heavy = required("POOL_HEAVY")
        pool_light = required("POOL_LIGHT")
        # Pool names are literal text, never regex; only the allow-list is a pattern.
        allowed = (
            get(
                "ALLOWED_POOL_REGEX",
                f"^({re.escape(pool_heavy)}|{re.escape(pool_light)})$",
            )
            or ""
        )
        try:
            re.compile(allowed)
        except re.error as exc:
            errors.append(
                f"{PREFIX}ALLOWED_POOL_REGEX is not a regular expression: {exc}"
            )
        settings = cls(
            gcp_project=required("GCP_PROJECT"),
            gcp_zone=required("GCP_ZONE"),
            gke_cluster=required("GKE_CLUSTER"),
            namespace=required("NAMESPACE"),
            kube_context=required("KUBE_CONTEXT"),
            service_account=required("SERVICE_ACCOUNT"),
            secret=required("SECRET"),
            secret_nokeys=required("SECRET_NOKEYS"),
            image=required("IMAGE"),
            pool_heavy=pool_heavy,
            pool_light=pool_light,
            allowed_pool_regex=allowed,
            bucket=bucket,
            data_prefix=data_prefix,
            max_parallelism=number("MAX_PARALLELISM", 16),
            max_tiles=number("MAX_TILES", 60),
            max_completions_per_job=number("MAX_COMPLETIONS_PER_JOB", 10000),
            confirm_tiles=number("CONFIRM_TILES", 20),
        )
        if errors:
            raise SettingsError("; ".join(errors))
        return settings

    def pool_allowed(self, pool: str) -> bool:
        return re.fullmatch(self.allowed_pool_regex, pool) is not None

    def banner(self) -> str:
        return (
            f"cluster {self.gke_cluster} ({self.gcp_project}, {self.gcp_zone})  "
            f"namespace {self.namespace}  bucket gs://{self.bucket}/{self.data_prefix}"
        )
