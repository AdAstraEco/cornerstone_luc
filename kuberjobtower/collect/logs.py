"""Where a pod's log lines come from, in order: the live pod, our archive, Cloud Logging.

A Job's pods are deleted after its TTL, taking ``kubectl logs`` with them. Two copies survive:
the NDJSON archive we write when a phase ends, and Cloud Logging, which keeps container logs for
30 days. GKE stores a JSON log line as a structured entry and moves its ``severity`` and ``time``
out of the payload, so those two are put back to give the same line the pod printed.
"""

import collections.abc
import json
import re
import time
import typing

SCOPE = "https://www.googleapis.com/auth/logging.read"
ENTRIES_URL = "https://logging.googleapis.com/v2/entries:list"
SAFE = re.compile(r"[A-Za-z0-9._-]+")
SOURCES = ("auto", "pod", "archive", "cloud")
RETRIES = 4


class LogsUnavailable(RuntimeError):
    pass


class PodLogs(typing.Protocol):
    def pods(self, run_id: str, phase: str | None = None) -> list[typing.Any]: ...
    def logs(
        self, pod: str, *, tail: int | None = None
    ) -> collections.abc.Iterator[str]: ...


def clean(line: str) -> str | None:
    """Drop dask progress-bar redraws (hundreds of KB per pod); keep a JSON line that follows one."""
    start = line.find('{"severity"')
    if start >= 0:
        return line[start:]
    return None if "% Completed |" in line else line


# ---- archive: {root}/runs/{run_id}/logs/{phase}/{pod}.ndjson, written once ----------------------


def archive_dir(root: str, run_id: str, phase: str) -> str:
    return f"{root.rstrip('/')}/runs/{run_id}/logs/{phase}"


def write_archive(
    root: str, run_id: str, phase: str, pod: str, lines: collections.abc.Iterable[str]
) -> str | None:
    """Store the pod's cleaned lines; returns the URI, or None if one was already stored."""
    import fsspec  # type: ignore[import-untyped]

    uri = f"{archive_dir(root, run_id, phase)}/{pod}.ndjson"
    fs, path = fsspec.url_to_fs(uri)
    if fs.exists(path):
        return None
    fs.makedirs(path.rsplit("/", 1)[0], exist_ok=True)
    with fs.open(path, "w") as handle:
        for line in lines:
            if (kept := clean(line.rstrip("\n"))) is not None:
                handle.write(kept + "\n")
    return uri


def read_archive(
    root: str, run_id: str, phase: str, index: int
) -> collections.abc.Iterator[str]:
    import fsspec

    fs, path = fsspec.url_to_fs(archive_dir(root, run_id, phase))
    if not fs.exists(path):
        return
    pattern = re.compile(rf"-{index}-[a-z0-9]+\.ndjson$")
    for name in sorted(fs.ls(path, detail=False)):
        if pattern.search(name):
            with fs.open(name, "r") as handle:
                yield from (line.rstrip("\n") for line in handle)
            return


def archive_phase(cluster: PodLogs, root: str, run_id: str, phase: str) -> list[str]:
    """Archive every pod of the phase (the failed ones are the ones that matter most)."""
    stored = []
    for pod in cluster.pods(run_id, phase):
        uri = write_archive(root, run_id, phase, pod.name, cluster.logs(pod.name))
        if uri:
            stored.append(uri)
    return stored


# ---- Cloud Logging --------------------------------------------------------------------------


def cloud_filter(namespace: str, run_id: str, phase: str, index: int | None) -> str:
    for value in (namespace, run_id, phase):
        if not SAFE.fullmatch(value):
            raise ValueError(f"{value!r} cannot go into a log filter")
    clauses = [
        'resource.type="k8s_container"',
        f'resource.labels.namespace_name="{namespace}"',
        f'labels."k8s-pod/run-id"="{run_id}"',
        f'labels."k8s-pod/phase"="{phase}"',
    ]
    if index is not None:
        clauses.append(
            f'labels."k8s-pod/batch_kubernetes_io/job-completion-index"="{int(index)}"'
        )
    return " ".join(clauses)


def entry_line(entry: dict[str, typing.Any]) -> str | None:
    if "jsonPayload" in entry:
        return json.dumps(
            {
                "severity": entry.get("severity", "DEFAULT"),
                "time": entry["timestamp"],
                **entry["jsonPayload"],
            }
        )
    return entry.get("textPayload")


def cloud_lines(
    project: str,
    namespace: str,
    run_id: str,
    phase: str,
    index: int | None,
    *,
    session: typing.Any = None,
) -> collections.abc.Iterator[str]:
    if session is None:
        import google.auth
        from google.auth.transport.requests import AuthorizedSession

        credentials, _ = google.auth.default(scopes=[SCOPE])
        session = AuthorizedSession(credentials)
    body: dict[str, typing.Any] = {
        "resourceNames": [f"projects/{project}"],
        "filter": cloud_filter(namespace, run_id, phase, index),
        "orderBy": "timestamp asc",
        "pageSize": 1000,
    }
    while True:
        for attempt in range(RETRIES):
            response = session.post(ENTRIES_URL, json=body, timeout=60)
            # end-user credentials share a small default quota, so 429 is routine
            if response.status_code not in (429, 503) or attempt == RETRIES - 1:
                break
            time.sleep(2**attempt)
        if response.status_code != 200:
            raise LogsUnavailable(
                f"Cloud Logging answered {response.status_code}: {response.text[:200]}"
            )
        payload = response.json()
        for entry in payload.get("entries", []):
            if (line := entry_line(entry)) is not None:
                yield line
        if not (token := payload.get("nextPageToken")):
            return
        body["pageToken"] = token


# ---- the chain ------------------------------------------------------------------------------


def lines_for(
    *,
    cluster: PodLogs,
    project: str,
    namespace: str,
    archive_root: str,
    run_id: str,
    phase: str,
    index: int,
    source: str = "auto",
    tail: int | None = None,
    cloud_session: typing.Any = None,
) -> tuple[str, list[str]]:
    """The first source that has this pod's log, as (source name, lines)."""
    tried: list[str] = []
    if source in ("auto", "pod"):
        pods = [p for p in cluster.pods(run_id, phase) if p.index == index]
        if pods:
            try:
                return "pod", list(cluster.logs(pods[0].name, tail=tail))
            except Exception as exc:  # the pod may exist but not be readable yet
                tried.append(f"pod ({exc})")
        else:
            tried.append("pod (gone)")
    if source in ("auto", "archive"):
        lines = list(read_archive(archive_root, run_id, phase, index))
        if lines:
            return "archive", lines[-tail:] if tail else lines
        tried.append("archive (none)")
    if source in ("auto", "cloud"):
        lines = list(
            cloud_lines(project, namespace, run_id, phase, index, session=cloud_session)
        )
        if lines:
            return "cloud", lines[-tail:] if tail else lines
        tried.append("cloud (none)")
    raise LogsUnavailable(
        f"no log for run {run_id} phase {phase} index {index}: " + ", ".join(tried)
    )
