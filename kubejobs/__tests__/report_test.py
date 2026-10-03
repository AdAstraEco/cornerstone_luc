import json

from kubejobs import report

GIB = 1 << 30


def line(t: float, message: str = "", logger: str = "x", **fields: object) -> str:
    stamp = f"2026-10-02T10:{int(t) // 60:02d}:{int(t) % 60:02d}+00:00"
    return json.dumps(
        {"severity": "INFO", "time": stamp, "message": message, "logger": logger, **fields}
    )


def sample(t: float, mem: float, anon: float, cpu_s: float, wrote: int, io: float = 0.0) -> str:
    return line(t, "resource_sample", "resource_monitor", kind="resource_sample", mem_current_gib=mem,
                mem_anon_gib=anon, cpu_usage_s=cpu_s, write_bytes=wrote, io_psi_full_avg10=io,
                localtmp_used_pct=10.0)  # fmt: skip


LOG = [
    line(0, "phase compute for ['HND']", "__main__"),
    line(1, "harmonize LUC tile_id=20N_090W", "__main__"),
    sample(5, 4.0, 3.0, 10, 0),
    line(10, "Saving to path_to_zarr=gs://b/s/abc123.zarr with num_workers=8", "jdluc.storage"),
    sample(20, 30.0, 25.0, 50, 2 * GIB, io=40.0),
    sample(35, 33.0, 28.0, 110, 5 * GIB, io=10.0),
    line(40, "Finished writing to path_to_zarr=gs://b/s/abc123.zarr", "jdluc.storage"),
    line(50, "emit tile_id=20N_090W", "__main__"),
    sample(55, 12.0, 9.0, 130, 5 * GIB),
    sample(70, 15.0, 11.0, 160, 6 * GIB),
    "2026-09-29 18:23:50 - plain text lines are skipped",
    "[##      ] | 5% Completed | 78.03 s" + sample(75, 16.0, 12.0, 170, 6 * GIB),  # progress bar first
    line(80, "Done", "__main__"),
]


def test_steps_are_the_marked_ones_with_their_zarr_writes_inside() -> None:
    labels = [(s.label, s.parent is not None) for s in report.steps(report.parse(LOG))]
    assert labels == [
        ("harmonize LUC tile_id=20N_090W", False),
        ("write abc123", True),
        ("emit tile_id=20N_090W", False),
    ]


def test_usage_joins_samples_to_the_step_window() -> None:
    records = report.parse(LOG)
    write = report.usage(records, report.steps(records)[1])
    assert (write.samples, write.peak_mem_gib, write.peak_anon_gib) == (2, 33.0, 28.0)
    assert write.written_gib == 3.0  # 5 GiB - 2 GiB, over this window only
    assert write.cpu_cores == 4.0  # (110 - 50) cpu-seconds over 15 s
    assert write.peak_io_psi == 40.0


def test_a_sample_after_a_dask_progress_bar_is_still_read() -> None:
    records = report.parse(LOG)
    emit = report.usage(records, report.steps(records)[2])
    assert emit.samples == 3 and emit.peak_mem_gib == 16.0


def test_table_has_a_row_per_step_and_survives_a_log_without_json() -> None:
    text = report.table(LOG)
    assert text.count("\n") == 3 and "write abc123" in text
    assert "no JSON log lines" in report.table(["plain text only"])


def test_export_marks_its_own_steps_and_a_pod_without_marks_is_one_row() -> None:
    export = [
        line(0, "Reading emit scratch output for tile_id=20N_090W", "jdluc.export"),
        sample(5, 1.0, 1.0, 5, 0),
        line(30, "Writing staged GeoTIFF to path_to_geotiff=/localtmp/x.tif", "jdluc.export"),
        sample(40, 2.0, 1.5, 20, GIB),
    ]
    assert [s.label for s in report.steps(report.parse(export))][:2] == [
        "Reading emit scratch output for tile_id=20N_090W",
        "Writing staged GeoTIFF to path_to_geotiff=/localtmp/x.tif",
    ]
    quiet = [sample(0, 1.0, 1.0, 0, 0), sample(15, 1.0, 1.0, 15, 0)]
    assert [s.label for s in report.steps(report.parse(quiet))] == ["whole pod (no marked steps)"]
