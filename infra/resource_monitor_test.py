import pathlib

import pytest
import resource_monitor


@pytest.fixture
def cgroup(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> pathlib.Path:
    monkeypatch.setattr(resource_monitor, "CGROUP_ROOT", str(tmp_path))
    return tmp_path


def test_sample_reports_anon_events_cpu_and_pod_wide_writes(
    cgroup: pathlib.Path,
) -> None:
    gib = 1 << 30
    (cgroup / "memory.current").write_text(str(40 * gib))
    (cgroup / "memory.max").write_text(str(60 * gib))
    (cgroup / "memory.stat").write_text(f"anon {30 * gib}\nfile {10 * gib}\nother 1\n")
    (cgroup / "memory.events").write_text("low 0\nhigh 3\nmax 2\noom 1\noom_kill 1\n")
    (cgroup / "cpu.stat").write_text(
        "usage_usec 5000000\nnr_throttled 7\nthrottled_usec 2500000\n"
    )
    (cgroup / "io.stat").write_text("8:0 rbytes=1 wbytes=100 rios=1\n8:16 wbytes=50\n")

    sample = resource_monitor._sample(temp_dir=str(cgroup))

    assert sample["mem_pct"] == pytest.approx(66.7)
    assert sample["mem_anon_gib"] == 30.0
    assert sample["mem_anon_pct"] == 50.0
    assert sample["mem_limit_gib"] == 60.0
    assert sample["mem_events_oom_kill"] == 1
    assert "mem_events_low" not in sample
    assert sample["cpu_usage_s"] == 5.0
    assert sample["cpu_throttled_s"] == 2.5
    assert sample["cpu_nr_throttled"] == 7
    assert sample["write_bytes"] == 150  # summed over devices, not /proc/self/io


def test_unreadable_cgroup_is_omitted_not_an_error(cgroup: pathlib.Path) -> None:
    assert "mem_pct" not in resource_monitor._sample(temp_dir=str(cgroup))
