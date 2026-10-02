import pathlib

import pytest

from jdluc import config

FIELDS = {
    "EXPORT_ROOT": "gs://b/export",
    "HARVARD_DATAVERSE_GUESTBOOK_JSON": "{}",
    "INGEST_ROOT": "gs://b/ingest",
    "NUMBER_OF_DASK_WORKERS": "4",
    "SCRATCH_ROOT": "gs://b/scratch",
    "USDA_NASS_API_KEY": "key",
}


@pytest.fixture(autouse=True)
def clean(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    for name in FIELDS:
        monkeypatch.delenv(name, raising=False)
    # find_dotenv walks up from jdluc/, so without this a developer's real .env would win
    monkeypatch.setattr(
        config.dotenv,
        "find_dotenv",
        lambda: str(tmp_path / ".env") if (tmp_path / ".env").exists() else "",
    )
    config.Config.from_dot_env.cache_clear()


def test_environment_alone_is_enough(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in FIELDS.items():
        monkeypatch.setenv(name, value)
    cfg = config.Config.from_dot_env()
    assert cfg.ingest_root == "gs://b/ingest"
    assert cfg.number_of_dask_workers == 4


def test_environment_beats_the_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    (tmp_path / ".env").write_text(
        "\n".join(f"{k} = '{v}'" for k, v in FIELDS.items()) + "\n"
    )
    monkeypatch.setenv("SCRATCH_ROOT", "gs://other/scratch")
    cfg = config.Config.from_dot_env()
    assert cfg.scratch_root == "gs://other/scratch"
    assert cfg.export_root == "gs://b/export"  # still from the file


def test_unresolved_field_without_a_file_is_an_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SCRATCH_ROOT", "gs://b/scratch")
    with pytest.raises(OSError, match="INGEST_ROOT"):
        config.Config.from_dot_env()
