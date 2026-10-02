import pathlib

import pytest

from controlplane.settings import Settings, SettingsError


def test_defaults_and_derived_values(make_settings) -> None:  # type: ignore[no-untyped-def]
    s = make_settings()
    assert s.history_root == "gs://bucket/cornerstone/control"
    assert s.max_parallelism == 16
    assert s.pool_allowed("ns-power-node-pool")
    assert not s.pool_allowed(
        "standard-node-pool"
    )  # only the two role pools by default


def test_every_problem_is_reported_at_once(make_settings) -> None:  # type: ignore[no-untyped-def]
    with pytest.raises(SettingsError) as exc:
        make_settings(BUCKET=None, IMAGE=None, MAX_TILES="many")
    message = str(exc.value)
    assert "CONTROLPLANE_BUCKET" in message
    assert "CONTROLPLANE_IMAGE" in message
    assert "MAX_TILES" in message


def test_environment_beats_the_file(
    make_settings,  # type: ignore[no-untyped-def]
    tmp_path: pathlib.Path,
) -> None:
    make_settings()
    s = Settings.load(
        {
            "CONTROLPLANE_ENV_FILE": str(tmp_path / "test.env"),
            "CONTROLPLANE_NAMESPACE": "other",
        }
    )
    assert s.namespace == "other"


def test_a_laptop_needs_a_context_guard(make_settings) -> None:  # type: ignore[no-untyped-def]
    with pytest.raises(SettingsError, match="KUBE_CONTEXT"):
        make_settings(KUBE_CONTEXT=None)
    assert make_settings(KUBE_CONTEXT=None, AUTH="token").kube_context is None


def test_bad_pool_regex_is_rejected(make_settings) -> None:  # type: ignore[no-untyped-def]
    with pytest.raises(SettingsError, match="regular expression"):
        make_settings(ALLOWED_POOL_REGEX="(")
