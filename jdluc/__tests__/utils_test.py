import collections.abc
import concurrent.futures
import itertools
import re
import time

import git
import pytest

from jdluc.utils import (
    get_git_remote_url,
    get_git_version,
    iter_sharded,
    threadsafe_cache,
)


def test_threadsafe_cache_serial() -> None:
    @threadsafe_cache
    def now() -> float:
        time.sleep(0.2)
        return time.monotonic()

    result = now()
    assert now() == result
    assert now() == result
    assert now() == result
    assert now() == result


def test_threadsafe_cache_parallel() -> None:
    @threadsafe_cache
    def now() -> float:
        time.sleep(0.2)
        return time.monotonic()

    results: set[float] = set()
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures: list[concurrent.futures.Future[float]] = []
        for _ in range(25):
            futures.append(executor.submit(now))
        for future in concurrent.futures.as_completed(futures):
            results.add(future.result())
    assert len(results) == 1


@pytest.mark.parametrize(
    ("modulus", "residues", "values", "expected"),
    (
        (1, None, range(5), range(5)),
        (1, (0,), range(5), range(5)),
        (2, (0,), range(5), (0, 2, 4)),
        (2, (1,), range(5), (1, 3)),
        (2, (0, 1), range(5), range(5)),
        (3, (0,), range(5), (0, 3)),
        (3, (1,), range(5), (1, 4)),
        (3, (2,), range(5), (2,)),
    ),
)
def test_iter_sharded(
    modulus: int,
    residues: collections.abc.Sequence[int] | None,
    values: collections.abc.Sequence[int],
    expected: collections.abc.Sequence[int],
) -> None:
    result = iter_sharded(
        modulus=modulus,
        residues=residues,
        values=values,
    )
    assert list(result) == list(expected)


def test_iter_sharded_is_disjoint() -> None:
    shards = [
        list(iter_sharded(modulus=3, residues=(residue,), values=range(100)))
        for residue in range(3)
    ]
    for left, right in itertools.combinations(shards, r=2):
        assert set(left).isdisjoint(right)
    assert {value for shard in shards for value in shard} == set(range(100))


def test_iter_sharded_raises() -> None:
    with pytest.raises(
        AssertionError, match=re.escape("residues=(2,) exceed modulus=2")
    ):
        list(iter_sharded(modulus=2, residues=(2,), values=()))


@pytest.fixture
def without_a_git_repository(
    monkeypatch: pytest.MonkeyPatch,
) -> collections.abc.Iterator[None]:
    def no_repository(*args: object, **kwargs: object) -> git.Repo:
        raise git.InvalidGitRepositoryError

    monkeypatch.setattr(git, "Repo", no_repository)
    get_git_version.cache_clear()
    get_git_remote_url.cache_clear()
    yield
    get_git_version.cache_clear()
    get_git_remote_url.cache_clear()


@pytest.mark.usefixtures("without_a_git_repository")
def test_git_provenance_falls_back_to_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("JDLUC_GIT_VERSION", "main-0123abcd")
    monkeypatch.setenv("JDLUC_GIT_REMOTE_URL", "https://example.com/luc.git")
    assert get_git_version() == "main-0123abcd"
    assert get_git_remote_url() == "https://example.com/luc.git"


@pytest.mark.usefixtures("without_a_git_repository")
def test_git_provenance_falls_back_to_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("JDLUC_GIT_VERSION", raising=False)
    monkeypatch.delenv("JDLUC_GIT_REMOTE_URL", raising=False)
    assert get_git_version() == "unknown"
    assert get_git_remote_url() == "unknown"
