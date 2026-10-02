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
    kube_context: str | None
    auth: str
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
    history_root: str
    history_db: str
    # guardrails
    max_parallelism: int
    max_tiles: int
    max_completions_per_job: int
    confirm_tiles: int
    confirm_cost_usd: float
    # ui
    ui_host: str
    ui_port: int

    @classmethod
    def load(cls, environ: typing.Mapping[str, str] | None = None) -> typing.Self:
        """Read the settings; every problem is reported together, not one per run."""
        environ = os.environ if environ is None else environ
        file_values: dict[str, str | None] = {}
        env_file = environ.get(f"{PREFIX}ENV_FILE")
        path = env_file or dotenv.find_dotenv(usecwd=True)
        if env_file and not pathlib.Path(env_file).is_file():
            raise SettingsError(f"{PREFIX}ENV_FILE={env_file} does not exist")
        if path:
            file_values = dict(dotenv.dotenv_values(path))

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

        def number(
            name: str, default: str, kind: type[int] | type[float]
        ) -> typing.Any:
            raw = get(name, default) or default
            try:
                value = kind(raw)
            except ValueError:
                errors.append(f"{PREFIX}{name}={raw!r} is not a number")
                return kind(default)
            if value <= 0:
                errors.append(f"{PREFIX}{name} must be positive")
            return value

        bucket = required("BUCKET")
        data_prefix = (get("DATA_PREFIX", "cornerstone") or "").strip("/")
        pool_heavy = required("POOL_HEAVY")
        pool_light = required("POOL_LIGHT")
        auth = get("AUTH", "auto") or "auto"
        if auth not in ("auto", "kubeconfig", "incluster", "token"):
            errors.append(
                f"{PREFIX}AUTH={auth!r} is not auto, kubeconfig, incluster or token"
            )
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
            kube_context=get("KUBE_CONTEXT"),
            auth=auth,
            service_account=required("SERVICE_ACCOUNT"),
            secret=required("SECRET"),
            secret_nokeys=required("SECRET_NOKEYS"),
            image=required("IMAGE"),
            pool_heavy=pool_heavy,
            pool_light=pool_light,
            allowed_pool_regex=allowed,
            bucket=bucket,
            data_prefix=data_prefix,
            history_root=get("HISTORY_ROOT", f"gs://{bucket}/{data_prefix}/control")
            or "",
            history_db=get("HISTORY_DB", ".cache/controlplane/history.db") or "",
            max_parallelism=number("MAX_PARALLELISM", "16", int),
            max_tiles=number("MAX_TILES", "60", int),
            max_completions_per_job=number("MAX_COMPLETIONS_PER_JOB", "10000", int),
            confirm_tiles=number("CONFIRM_TILES", "20", int),
            confirm_cost_usd=number("CONFIRM_COST_USD", "5", float),
            ui_host=get("UI_HOST", "127.0.0.1") or "",
            ui_port=number("UI_PORT", "8090", int),
        )
        if auth in ("auto", "kubeconfig") and settings.kube_context is None:
            errors.append(f"{PREFIX}KUBE_CONTEXT is required to write from a laptop")
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
