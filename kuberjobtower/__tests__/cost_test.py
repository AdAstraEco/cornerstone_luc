import datetime

import pytest

from kuberjobtower import quantity
from kuberjobtower.collect import cost, monitoring, pods
from kuberjobtower.models import NodeInfo, PodState

UTC = datetime.UTC
GIB = 1 << 30
T0 = datetime.datetime(2026, 10, 6, 19, 25, tzinfo=UTC)


@pytest.mark.parametrize(
    ("text", "value"),
    [("56Gi", 56 * GIB), ("6", 6), ("500m", 0.5), ("100Mi", 100 * 2**20), ("1G", 10**9), ("2.5Gi", 2.5 * GIB)],
)
def test_quantities(text: str, value: float) -> None:
    assert quantity.parse(text) == value
    assert quantity.millicores("6") == 6000
    with pytest.raises(ValueError):
        quantity.parse("lots")


def node(instance: str = "e2-highmem-8", mem_gib: float = 57.0) -> NodeInfo:
    return NodeInfo("n", instance, "pool", 7910, int(mem_gib * GIB))


def pod(minutes: float = 41.5, mem_gib: float = 56.0, scratch_gib: float = 20.0) -> PodState:
    return PodState(
        "p", 0, "Succeeded", "n", None, 0, T0,
        created=T0 - datetime.timedelta(seconds=30), finished=T0 + datetime.timedelta(minutes=minutes),
        cpu_request_m=6000, memory_request_bytes=int(mem_gib * GIB), scratch_bytes=int(scratch_gib * GIB),
    )  # fmt: skip


def test_the_sri_lanka_compute_pod_costs_about_a_quarter_of_a_dollar() -> None:
    c = cost.pod_cost(pod(), node())
    assert c is not None and c.instance_type == "e2-highmem-8"
    assert c.share == pytest.approx(56 / 57)  # it fills the node, so it pays for almost all of it
    assert c.node_usd == pytest.approx(0.3616 * 41.5 / 60 * 56 / 57, rel=1e-6)
    assert 0.24 < c.usd < 0.26


def test_a_small_pod_pays_its_share_of_the_node_and_the_disk_for_its_lifetime() -> None:
    small = cost.pod_cost(pod(minutes=60, mem_gib=7, scratch_gib=300), node("e2-standard-8", 27.6))
    assert small is not None
    assert small.share == pytest.approx(max(7 / 27.6, 6000 / 7910))  # cpu is the bigger share
    disk_h = (60 * 60 + 30) / 3600
    assert small.disk_usd == pytest.approx(300 * 0.17 / 730 * disk_h)


def test_no_estimate_without_a_known_node_rate_or_finished_pod() -> None:
    assert cost.pod_cost(pod(), None) is None
    assert cost.pod_cost(pod(), node("n2-standard-4")) is None
    unfinished = pod()
    assert cost.pod_cost(PodState(**{**unfinished.__dict__, "finished": None}), node()) is None
    assert cost.total([cost.pod_cost(pod(), node()), None])[1] == 1


def test_the_pod_record_carries_the_machine_and_the_cost() -> None:
    rec = pods.record(pod(), [], node())
    assert rec["machine_type"] == "e2-highmem-8" and rec["cost_usd"] == pytest.approx(0.25, abs=0.01)
    assert pods.record(pod(), [], None)["cost_usd"] is None


def test_pod_records_are_archived_and_read_back(tmp_path) -> None:  # type: ignore[no-untyped-def]
    pods.archive(str(tmp_path), "r1", "compute", [pod()], [], {"n": node()})
    pods.archive(str(tmp_path), "r1", "export", [pod(minutes=10)], [], {"n": node()})
    stored = pods.read_archive(str(tmp_path), "r1")
    assert list(stored) == ["compute", "export"]
    assert stored["compute"][0]["run_s"] == 2490.0
    assert pods.read_archive(str(tmp_path), "none") == {}


class Response:
    def __init__(self, status: int, payload: dict[str, object]) -> None:
        self.status_code, self._payload, self.text = status, payload, ""

    def json(self) -> dict[str, object]:
        return self._payload


class MonitoringSession:
    """Answers each metric the way Cloud Monitoring did for the Sri Lanka compute pod."""

    def __init__(self) -> None:
        self.filters: list[str] = []

    def get(self, url: str, params: dict[str, str], timeout: int) -> Response:
        self.filters.append(params["filter"])
        f = params["filter"]

        def series(value: float, **labels: str) -> dict[str, object]:
            return {"metric": {"labels": labels}, "points": [{"value": {"doubleValue": value}}, {"value": {"doubleValue": value / 2}}]}

        if "memory/used_bytes" in f:
            body = [series(0.3 * GIB, memory_type="evictable"), series(53.1 * GIB, memory_type="non-evictable")]
        elif "memory/request_bytes" in f or "memory/limit_bytes" in f:
            body = [series(56 * GIB)]
        else:
            body = [series(6.35)]
        return Response(200, {"timeSeries": body})


def test_the_kubelet_view_reads_the_non_evictable_working_set() -> None:
    session = MonitoringSession()
    view = monitoring.kubelet_view("p", "ns", "pod-1", T0, T0 + datetime.timedelta(hours=1), session=session)
    assert view.peak_working_set_gib == pytest.approx(53.1)
    assert (view.memory_request_gib, view.peak_cpu_cores) == (56.0, 6.35)
    assert "peak working set 53.1 GiB of a 56 GiB request (95%)" in view.line()
    assert all('resource.labels.pod_name="pod-1"' in f for f in session.filters)


def test_monitoring_refuses_odd_filter_values_and_reports_an_error() -> None:
    with pytest.raises(ValueError):
        monitoring.kubelet_view("p", 'x" OR "1"="1', "pod", T0, T0, session=object())
    bad = type("S", (), {"get": lambda self, *a, **k: Response(403, {})})()
    with pytest.raises(Exception, match="403"):
        monitoring.kubelet_view("p", "ns", "pod", T0, T0, session=bad)
