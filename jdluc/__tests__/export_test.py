import dataclasses

import numpy
import pytest
import rasterio
import rasterio.transform
import xarray

from jdluc import export

CIE_BAND_NAMES = (
    "cie-climate-zone",
    "cie-conversion-source",
    "cie-vegetation-emissions-undiscounted:tco2e-per-ha",
)


def _darray(value: float) -> xarray.DataArray:
    return xarray.DataArray(
        coords={"y": [1.5, 0.5], "x": [0.5, 1.5, 2.5]},  # north up, as a raster
        data=numpy.full((2, 3), value, dtype=numpy.float32),
        dims=("y", "x"),
    )


def _dataset() -> xarray.Dataset:
    """A step's output: a plain (y, x) variable, and one that varies by `scheme`."""
    by_scheme = xarray.concat(
        [_darray(1), _darray(2), _darray(3)], dim="scheme"
    ).assign_coords(scheme=["a", "b", "c"])
    return xarray.Dataset(
        {"area:ha": _darray(7).astype(numpy.int16), "total:t": by_scheme}
    ).chunk({"x": 2})


DELIVERABLE = export.Deliverable(
    name="example",
    bands=(
        export.Band("total:t", select={"scheme": ("c", "a")}),
        export.Band("area:ha"),
    ),
)


def test_get_selections_collects_every_selected_value_in_order() -> None:
    deliverable = export.Deliverable(
        name="two",
        bands=(
            export.Band("total:t", select={"scheme": ("c",)}),
            export.Band("other", select={"scheme": ("a", "c")}),
        ),
    )
    assert export.get_selections(deliverable) == {"scheme": ["c", "a"]}


def test_band_names_append_the_selected_values() -> None:
    assert export.get_band_names(DELIVERABLE) == ["total:t:c", "total:t:a", "area:ha"]


@pytest.mark.parametrize(
    ("band", "match"),
    (
        (export.Band("missing"), "no variable"),
        (export.Band("total:t"), "varies by"),
        (export.Band("area:ha", select={"scheme": ("a",)}), "varies by"),
    ),
)
def test_check_deliverable_rejects_bands_that_do_not_fit(
    band: export.Band, match: str
) -> None:
    with pytest.raises(AssertionError, match=match):
        export.check_deliverable(
            export.Deliverable(name="bad", bands=(band,)), _dataset()
        )


def test_check_deliverable_rejects_a_repeated_band() -> None:
    band = export.Band("area:ha")
    with pytest.raises(AssertionError, match="repeats a band"):
        export.check_deliverable(
            export.Deliverable(name="bad", bands=(band, band)), _dataset()
        )


def test_to_cog_dataset_writes_one_float32_band_per_selection() -> None:
    result = export.to_cog_dataset(_dataset(), DELIVERABLE).compute()
    assert list(result) == export.get_band_names(DELIVERABLE)
    for name, value in zip(result, (3, 1, 7), strict=True):
        assert result[name].dims == ("y", "x")
        assert result[name].dtype == numpy.float32
        numpy.testing.assert_array_equal(result[name].values, value)


def test_to_zarr_dataset_keeps_the_selected_dimension() -> None:
    result = export.to_zarr_dataset(_dataset(), DELIVERABLE).compute()
    assert result["total:t"].dims == ("scheme", "y", "x")
    assert list(result["scheme"].values) == ["c", "a"]
    numpy.testing.assert_array_equal(result["total:t"].sel(scheme="a").values, 1)
    assert result["area:ha"].dtype == numpy.float32


@pytest.mark.parametrize("format", list(export.Format))
def test_write_deliverable_writes_the_format_under_the_deliverables_name(
    format: export.Format, tmp_path
) -> None:
    uri = export.write_deliverable(
        dset=_dataset(),
        deliverable=DELIVERABLE,
        format=format,
        output_root=str(tmp_path),
        source_name="test",
        tile_id="00N_000E",
    )
    assert uri == str(
        tmp_path / "example" / f"00N_000E{export.FORMAT_TO_EXTENSION[format]:s}"
    )
    match format:
        case export.Format.COG:
            with rasterio.open(uri) as dataset:
                assert list(dataset.descriptions) == export.get_band_names(DELIVERABLE)
                assert dataset.tags()["watershed-product-name"] == "example"
        case export.Format.ZARR:
            written = xarray.open_zarr(uri, consolidated=False)
            assert set(written) == {"total:t", "area:ha"}
            assert written.attrs["watershed-product-name"] == "example"


def test_write_deliverable_takes_the_deliverables_format_by_default(tmp_path) -> None:
    uri = export.write_deliverable(
        dset=_dataset(),
        deliverable=dataclasses.replace(DELIVERABLE, format=export.Format.ZARR),
        output_root=str(tmp_path),
        source_name="test",
        tile_id="00N_000E",
    )
    assert uri.endswith(".zarr")


def _write_tile(
    path: str,
    *,
    west: float,
    north: float,
    pixel: float,
    values: list[float],
    size: int = 4,
    band_names: tuple[str, ...] = CIE_BAND_NAMES,
    nodata: float = export.NO_DATA,
) -> None:
    """A tiny multi-band GeoTIFF: band i filled with values[i-1], on the given EPSG:4326 grid."""
    data = numpy.stack(
        [numpy.full((size, size), value, dtype=numpy.float32) for value in values]
    )
    transform = rasterio.transform.from_origin(west, north, pixel, pixel)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=size,
        width=size,
        count=len(values),
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
        nodata=nodata,
    ) as dataset:
        dataset.write(data)
        for index, name in enumerate(band_names, start=1):
            dataset.set_band_description(index, name)


def test_mosaic_builds_a_readable_union_vrt(tmp_path) -> None:
    values_a = [float(i) for i in range(1, 4)]
    values_b = [100.0 + i for i in range(1, 4)]
    _write_tile(str(tmp_path / "a.tif"), west=0, north=10, pixel=1, values=values_a)
    _write_tile(
        str(tmp_path / "b.tif"), west=4, north=10, pixel=1, values=values_b
    )  # east of a
    out = str(tmp_path / "mosaic.vrt")

    export.mosaic([str(tmp_path / "a.tif"), str(tmp_path / "b.tif")], out)

    with rasterio.open(out) as dataset:
        assert dataset.count == len(CIE_BAND_NAMES)
        assert dataset.descriptions == CIE_BAND_NAMES
        assert dataset.crs.to_epsg() == 4326
        assert (dataset.width, dataset.height) == (8, 4)  # union of the two 4x4 tiles
        assert dataset.transform.c == 0 and dataset.transform.f == 10
        band1 = dataset.read(1)
        numpy.testing.assert_array_equal(
            band1[:, :4], values_a[0]
        )  # tile a on the left
        numpy.testing.assert_array_equal(
            band1[:, 4:], values_b[0]
        )  # tile b on the right


def test_mosaic_skips_absent_tiles(tmp_path) -> None:
    _write_tile(str(tmp_path / "a.tif"), west=0, north=10, pixel=1, values=[1.0] * 3)
    out = str(tmp_path / "mosaic.vrt")

    export.mosaic([str(tmp_path / "a.tif"), str(tmp_path / "missing.tif")], out)

    with rasterio.open(out) as dataset:
        assert (dataset.width, dataset.height) == (4, 4)  # only the present tile


def test_mosaic_rejects_grid_mismatch(tmp_path) -> None:
    _write_tile(str(tmp_path / "a.tif"), west=0, north=10, pixel=1, values=[1.0] * 3)
    _write_tile(
        str(tmp_path / "b.tif"), west=4, north=10, pixel=2, values=[1.0] * 3
    )  # coarser
    with pytest.raises(ValueError, match="does not share the grid"):
        export.mosaic(
            [str(tmp_path / "a.tif"), str(tmp_path / "b.tif")], str(tmp_path / "m.vrt")
        )


def test_mosaic_raises_when_nothing_exists(tmp_path) -> None:
    with pytest.raises(FileNotFoundError):
        export.mosaic([str(tmp_path / "nope.tif")], str(tmp_path / "m.vrt"))


def test_mosaic_workflow_reads_the_deliverables_tiles(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        export,
        "get_output_uri",
        lambda deliverable_name, format, tile_id: str(
            tmp_path / deliverable_name / f"{tile_id:s}.tif"
        ),
    )
    (tmp_path / "example").mkdir()
    _write_tile(
        str(tmp_path / "example" / "a.tif"), west=0, north=10, pixel=1, values=[1.0] * 3
    )
    out = str(tmp_path / "m.vrt")
    assert (
        export.mosaic_workflow(
            deliverable_name="example", tile_ids=["a"], name="aoi", output_uri=out
        )
        == out
    )
    with rasterio.open(out) as dataset:
        assert (dataset.width, dataset.height) == (4, 4)
