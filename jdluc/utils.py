import collections
import datetime
import enum
import functools
import logging
import os
import threading
import typing

import requests
import xarray

logger = logging.getLogger(__name__)


@functools.cache
def get_requests_session() -> requests.Session:
    logger.info("Creating a fresh session for requests")
    return requests.Session()


def save_remote_url_to_local_path(
    local_path: str,
    params: dict[str, str] | str,
    remote_url: str,
    # 4 MiB
    chunk_size: int = 1 << 22,
) -> None:
    logger.info(f"GET'ing from {remote_url=:s} with {params=:}")
    with get_requests_session().request(
        method="GET", params=params, stream=True, url=remote_url
    ) as response:
        response.raise_for_status()
        logger.info(f"Streaming data from {remote_url=:s} to {local_path=:s}")
        with open(file=local_path, mode="wb") as fp:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    fp.write(chunk)


# These stamp provenance into ingested COG metadata. In a container the source tree has no
# .git and the git binary may be absent, so import git lazily and fall back to build-time env
# (JDLUC_GIT_*) then a sentinel on ANY failure -- never crash ingest over a provenance tag.
# (GitPython's top-level import itself raises when the git executable is missing, so the
# import must live inside the try.)
@functools.cache
def get_git_version(default_branch_name: str = "main") -> str:
    try:
        import git

        repo = git.Repo(__file__, search_parent_directories=True)
        branch_name = repo.active_branch.name
        if branch_name == default_branch_name:
            return f"{branch_name:s}-{repo.head.commit.hexsha[:8]:s}"
        return branch_name
    except Exception:  # noqa: BLE001 -- provenance tag must never break ingest
        return os.environ.get("JDLUC_GIT_VERSION", "unknown")


@functools.cache
def get_git_remote_url(default_remote_name: str = "origin") -> str:
    try:
        import git

        repo = git.Repo(__file__, search_parent_directories=True)
        (remote,) = (r for r in repo.remotes if r.name == default_remote_name)
        return next(iter(remote.urls))
    except Exception:  # noqa: BLE001 -- provenance tag must never break ingest
        return os.environ.get("JDLUC_GIT_REMOTE_URL", "unknown")


def get_utc_timestamp() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat()


def threadsafe_cache[**P, R](func: typing.Callable[P, R]) -> typing.Callable[P, R]:
    cached = functools.cache(func)

    # each cached function has its own lock
    lock = threading.Lock()

    @functools.wraps(cached)
    def inner(*args: P.args, **kwargs: P.kwargs) -> R:
        with lock:
            return cached(*args, **kwargs)  # type: ignore

    inner.cache_clear = cached.cache_clear  # type: ignore
    inner.cache_info = cached.cache_info  # type: ignore
    inner.cache_parameters = cached.cache_parameters  # type: ignore
    return inner


def get_sum_totals[E: enum.Enum](
    enum_to_name_to_darray: dict[E, dict[str, xarray.DataArray]],
) -> dict[str, dict[str, float]]:
    enum_name_to_darray = {
        (e, name): darray
        for e, name_to_darray in enum_to_name_to_darray.items()
        for name, darray in name_to_darray.items()
    }
    batched_sum = xarray.Dataset(enum_name_to_darray).sum().compute()

    ret: dict[str, dict[str, float]] = collections.defaultdict(dict)
    for e, name in enum_name_to_darray:
        ret[e.name][name] = float(batched_sum[(e, name)])
    return ret
