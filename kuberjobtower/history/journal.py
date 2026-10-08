"""The write-once journal: ``<root>/runs/<run_uid>/run.json`` and ``.../journal/<observer>/<seq>.jsonl.gz``.

Objects are created and never replaced. ``run.json`` is the only name two parties could race for,
so it is written create-only (a second claim of the same run id fails). Chunk names carry the
observer, so even two collectors on one run cannot overwrite each other. ``root`` is any fsspec URI.
"""

import collections.abc
import gzip
import json
import typing

from kuberjobtower.history import records


class RunExists(RuntimeError):
    pass


def _fs(uri: str) -> tuple[typing.Any, str]:
    import fsspec  # type: ignore[import-untyped]

    return fsspec.url_to_fs(uri)


def run_dir(root: str, run_uid: str) -> str:
    return f"{root.rstrip('/')}/runs/{run_uid}"


def claim_run(root: str, run_uid: str, data: collections.abc.Mapping[str, typing.Any]) -> str:
    """Create ``run.json``; raises ``RunExists`` if this run uid is already taken."""
    uri = f"{run_dir(root, run_uid)}/run.json"
    fs, path = _fs(uri)
    fs.makedirs(path.rsplit("/", 1)[0], exist_ok=True)
    try:
        fs.pipe_file(path, json.dumps(data, indent=1, sort_keys=True).encode(), mode="create")
    except FileExistsError as exc:
        raise RunExists(f"run {run_uid} already exists") from exc
    return uri


def read_run(root: str, run_uid: str) -> dict[str, typing.Any] | None:
    fs, path = _fs(f"{run_dir(root, run_uid)}/run.json")
    if not fs.exists(path):
        return None
    with fs.open(path, "r") as handle:
        return typing.cast(dict[str, typing.Any], json.loads(handle.read()))


class Journal:
    """One collector's view: it appends numbered chunks under its own observer name."""

    def __init__(self, root: str, observer: str) -> None:
        self.root, self.observer = root, observer
        self._seq: dict[str, int] = {}

    def append(self, run_uid: str, batch: collections.abc.Sequence[records.Record]) -> str | None:
        if not batch:
            return None
        directory = f"{run_dir(self.root, run_uid)}/journal/{self.observer}"
        fs, path = _fs(directory)
        fs.makedirs(path, exist_ok=True)
        seq = self._seq.get(run_uid, len(self._chunks(fs, path)))
        self._seq[run_uid] = seq + 1
        name = f"{directory}/{seq:06d}.jsonl.gz"
        fs.pipe_file(f"{path}/{seq:06d}.jsonl.gz", gzip.compress(records.encode(batch)), mode="create")
        return name

    @staticmethod
    def _chunks(fs: typing.Any, path: str) -> list[str]:
        try:
            return [n for n in fs.ls(path, detail=False) if n.endswith(".jsonl.gz")]
        except FileNotFoundError:  # a bucket has no folder until the first object is in it
            return []


def list_chunks(root: str, run_uid: str) -> list[str]:
    """Every journal object of the run, as URIs, oldest name first."""
    uri = f"{run_dir(root, run_uid)}/journal"
    fs, path = _fs(uri)
    if not fs.exists(path):
        return []
    scheme = uri.split("://", 1)[0] + "://" if "://" in uri else ""
    return sorted(scheme + n for n in fs.find(path) if n.endswith(".jsonl.gz"))


def read_chunk(uri: str) -> list[records.Record]:
    fs, path = _fs(uri)
    with fs.open(path, "rb") as handle:
        return records.decode(gzip.decompress(handle.read()).decode())


def run_uids(root: str) -> list[str]:
    """Every run that has a ``run.json`` under the root."""
    fs, path = _fs(f"{root.rstrip('/')}/runs")
    if not fs.exists(path):
        return []
    return sorted(
        n.rsplit("/", 1)[-1] for n in fs.ls(path, detail=False) if fs.exists(f"{n}/run.json")
    )
