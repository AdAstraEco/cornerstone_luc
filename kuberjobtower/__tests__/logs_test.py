import json

import pytest

from kuberjobtower.collect import logs
from kuberjobtower.models import PodState


def test_clean_drops_progress_bars_but_keeps_a_json_line_after_one() -> None:
    bar = "\r[###     ] | 30% Completed |  3.1s\r[####    ] | 40% Completed |  4.1s"
    assert logs.clean(bar) is None
    assert logs.clean(bar + '{"severity": "INFO", "message": "x"}') == (
        '{"severity": "INFO", "message": "x"}'
    )
    assert logs.clean("plain text") == "plain text"


def test_a_structured_entry_gets_its_severity_and_time_back() -> None:
    entry = {
        "severity": "WARNING",
        "timestamp": "2026-10-06T20:06:45Z",
        "jsonPayload": {"message": "m", "run_id": "r1"},
    }
    assert json.loads(logs.entry_line(entry) or "") == {
        "severity": "WARNING",
        "time": "2026-10-06T20:06:45Z",
        "message": "m",
        "run_id": "r1",
    }
    assert logs.entry_line({"textPayload": "old text line"}) == "old text line"


def test_filter_pins_the_run_phase_and_pod_and_refuses_odd_values() -> None:
    f = logs.cloud_filter("ns", "lka1", "compute", 0)
    assert 'labels."k8s-pod/run-id"="lka1"' in f
    assert 'labels."k8s-pod/batch_kubernetes_io/job-completion-index"="0"' in f
    with pytest.raises(ValueError):
        logs.cloud_filter("ns", 'x" OR "1"="1', "compute", None)


class FakeResponse:
    def __init__(self, status: int, payload: dict[str, object]) -> None:
        self.status_code, self._payload, self.text = status, payload, str(payload)

    def json(self) -> dict[str, object]:
        return self._payload


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = responses
        self.bodies: list[dict[str, object]] = []

    def post(self, url: str, json: dict[str, object], timeout: int) -> FakeResponse:
        self.bodies.append(dict(json))
        return self.responses.pop(0)


def test_cloud_logging_is_paged_and_retries_a_429(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(logs.time, "sleep", lambda _: None)
    page = lambda text, token=None: FakeResponse(
        200,
        {
            "entries": [{"textPayload": text}],
            **({"nextPageToken": token} if token else {}),
        },
    )
    session = FakeSession([FakeResponse(429, {}), page("a", "t"), page("b")])
    assert list(logs.cloud_lines("p", "ns", "r1", "compute", 0, session=session)) == [
        "a",
        "b",
    ]
    assert session.bodies[2]["pageToken"] == "t"


def test_a_forbidden_cloud_logging_is_a_clear_error() -> None:
    session = FakeSession([FakeResponse(403, {"error": "no"})])
    with pytest.raises(logs.LogsUnavailable, match="403"):
        list(logs.cloud_lines("p", "ns", "r1", "compute", 0, session=session))


def test_archive_roundtrip_is_write_once_and_cleaned(tmp_path) -> None:  # type: ignore[no-untyped-def]
    root = str(tmp_path)
    uri = logs.write_archive(
        root, "r1", "compute", "job-r1-0-abcde", ["keep", "\r[#] | 5% Completed | 1s"]
    )
    assert uri and uri.endswith("runs/r1/logs/compute/job-r1-0-abcde.ndjson")
    assert (
        logs.write_archive(root, "r1", "compute", "job-r1-0-abcde", ["other"]) is None
    )
    assert list(logs.read_archive(root, "r1", "compute", 0)) == ["keep"]
    assert list(logs.read_archive(root, "r1", "compute", 1)) == []
    assert list(logs.read_archive(root, "nope", "compute", 0)) == []


class FakePods:
    def __init__(self, pods: list[PodState], lines: list[str]) -> None:
        self._pods, self._lines = pods, lines

    def pods(self, run_id: str, phase: str | None = None) -> list[PodState]:
        return self._pods

    def logs(self, pod: str, *, tail: int | None = None):  # type: ignore[no-untyped-def]
        return iter(self._lines)


def pod(index: int) -> PodState:
    return PodState(f"job-r1-{index}-abcde", index, "Succeeded", "n", None, 0, None)


def chain(cluster, tmp_path, session=None, **kw):  # type: ignore[no-untyped-def]
    return logs.lines_for(
        cluster=cluster, project="p", namespace="ns", archive_root=str(tmp_path),
        run_id="r1", phase="compute", index=0, cloud_session=session, **kw,
    )  # fmt: skip


def test_the_live_pod_wins_then_the_archive_then_cloud_logging(tmp_path) -> None:  # type: ignore[no-untyped-def]
    cloud = FakeSession(
        [FakeResponse(200, {"entries": [{"textPayload": "from cloud"}]})] * 3
    )
    assert chain(FakePods([pod(0)], ["live"]), tmp_path, cloud) == ("pod", ["live"])
    assert chain(FakePods([], []), tmp_path, cloud) == ("cloud", ["from cloud"])
    logs.write_archive(str(tmp_path), "r1", "compute", "job-r1-0-abcde", ["archived"])
    assert chain(FakePods([], []), tmp_path, cloud) == ("archive", ["archived"])
    assert chain(FakePods([], []), tmp_path, cloud, source="cloud")[0] == "cloud"


def test_nothing_anywhere_says_what_was_tried(tmp_path) -> None:  # type: ignore[no-untyped-def]
    empty = FakeSession([FakeResponse(200, {})])
    with pytest.raises(
        logs.LogsUnavailable,
        match="pod \\(gone\\), archive \\(none\\), cloud \\(none\\)",
    ):
        chain(FakePods([], []), tmp_path, empty)
