import pathlib

import pytest

from controlplane.settings import Settings

VALUES = {
    "GCP_PROJECT": "proj",
    "GCP_ZONE": "us-central1-b",
    "GKE_CLUSTER": "cluster",
    "KUBE_CONTEXT": "ctx",
    "NAMESPACE": "ns",
    "SERVICE_ACCOUNT": "sa",
    "SECRET": "env",
    "SECRET_NOKEYS": "env-nokeys",
    "IMAGE": "registry/img:latest",
    "POOL_HEAVY": "ns-power-node-pool",
    "POOL_LIGHT": "ns-worker-node-pool",
    "BUCKET": "bucket",
}


@pytest.fixture
def make_settings(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
    """Settings from a throwaway env file, so a developer's real .env never leaks in."""
    for name in list(__import__("os").environ):
        if name.startswith("CONTROLPLANE_"):
            monkeypatch.delenv(name)

    def make(**overrides: str | None) -> Settings:
        values = {**VALUES, **overrides}
        path = tmp_path / "test.env"
        path.write_text(
            "".join(
                f"CONTROLPLANE_{k}='{v}'\n" for k, v in values.items() if v is not None
            )
        )
        return Settings.load({"CONTROLPLANE_ENV_FILE": str(path)})

    return make
