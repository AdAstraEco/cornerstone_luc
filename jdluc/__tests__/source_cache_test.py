import contextlib
import dataclasses
import pathlib
import types

import dotenv
import pytest

from jdluc import config, utils

# Captured before conftest stubs it, to exercise the real .env parsing
FROM_DOT_ENV = config.Config.__dict__["from_dot_env"].__func__.__wrapped__

URL = "https://publisher.example/records/1/files/source.zip/content"


@pytest.fixture
def requested_urls(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Stub the publisher: every GET serves b"source"; return the URLs it was asked for."""
    urls: list[str] = []

    @contextlib.contextmanager
    def request(url: str, **kwargs: object):  # type: ignore[no-untyped-def]
        urls.append(url)
        yield types.SimpleNamespace(
            raise_for_status=lambda: None,
            iter_content=lambda chunk_size: iter([b"source"]),
        )

    monkeypatch.setattr(
        utils,
        "get_requests_session",
        lambda: types.SimpleNamespace(request=request),
    )
    return urls


def set_source_cache_root(monkeypatch: pytest.MonkeyPatch, root: str) -> None:
    configured = dataclasses.replace(
        config.Config.from_dot_env(), source_cache_root=root
    )
    monkeypatch.setattr(
        config.Config, "from_dot_env", classmethod(lambda cls: configured)
    )


def test_a_miss_downloads_and_stores_and_a_hit_skips_the_publisher(
    monkeypatch: pytest.MonkeyPatch,
    requested_urls: list[str],
    tmp_path: pathlib.Path,
) -> None:
    set_source_cache_root(monkeypatch, root=str(tmp_path / "cache"))
    for name in ("first", "second"):
        utils.save_remote_url_to_local_path(
            cache_key="zenodo/1/source.zip",
            local_path=str(tmp_path / name),
            params={},
            remote_url=URL,
        )
        assert (tmp_path / name).read_bytes() == b"source"
    assert requested_urls == [URL]
    assert (tmp_path / "cache/zenodo/1/source.zip").read_bytes() == b"source"


@pytest.mark.parametrize(
    ("root", "cache_key"),
    [
        pytest.param("", "zenodo/1/source.zip", id="root-unset"),
        pytest.param("cache", None, id="no-key"),
    ],
)
def test_without_a_root_or_a_key_every_call_downloads(
    monkeypatch: pytest.MonkeyPatch,
    requested_urls: list[str],
    tmp_path: pathlib.Path,
    root: str,
    cache_key: str | None,
) -> None:
    set_source_cache_root(monkeypatch, root=str(tmp_path / root) if root else "")
    for name in ("first", "second"):
        utils.save_remote_url_to_local_path(
            cache_key=cache_key,
            local_path=str(tmp_path / name),
            params={},
            remote_url=URL,
        )
    assert requested_urls == [URL, URL]
    assert not (tmp_path / "cache").exists()


REQUIRED_LINES = """\
EXPORT_ROOT = '/export'
HARVARD_DATAVERSE_GUESTBOOK_JSON = '{}'
INGEST_ROOT = '/ingest'
NUMBER_OF_DASK_WORKERS = 2
SCRATCH_ROOT = '/scratch'
USDA_NASS_API_KEY = 'key'
"""


@pytest.mark.parametrize(
    ("extra_line", "expected"),
    [
        pytest.param("", "", id="absent"),
        pytest.param("SOURCE_CACHE_ROOT = '/sources'\n", "/sources", id="set"),
    ],
)
def test_source_cache_root_may_be_left_out_of_dot_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    extra_line: str,
    expected: str,
) -> None:
    path_to_env = tmp_path / ".env"
    path_to_env.write_text(REQUIRED_LINES + extra_line)
    monkeypatch.setattr(dotenv, "find_dotenv", lambda **kwargs: str(path_to_env))
    assert FROM_DOT_ENV(config.Config).source_cache_root == expected


def test_a_required_key_may_not_be_left_out_of_dot_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    path_to_env = tmp_path / ".env"
    path_to_env.write_text(REQUIRED_LINES.replace("SCRATCH_ROOT = '/scratch'\n", ""))
    monkeypatch.setattr(dotenv, "find_dotenv", lambda **kwargs: str(path_to_env))
    with pytest.raises(KeyError, match="SCRATCH_ROOT"):
        FROM_DOT_ENV(config.Config)
