import pathlib

import numpy
import pytest
import rasterio
import xarray

from jdluc import export
from jdluc_ext import deliverable

TILE_ID = "00N_000E"


def get_darray(value: float) -> xarray.DataArray:
    return xarray.DataArray(
        coords={"y": [1.5, 0.5], "x": [0.5, 1.5, 2.5]},  # north up, as a raster
        data=numpy.full((2, 3), value, dtype=numpy.float32),
        dims=("y", "x"),
    )


def get_dset() -> xarray.Dataset:
    """A step's output: a plain (y, x) variable, and one that varies by `scheme`."""
    by_scheme = xarray.concat(
        [get_darray(value=1), get_darray(value=2), get_darray(value=3)], dim="scheme"
    ).assign_coords(scheme=["a", "b", "c"])
    return xarray.Dataset(
        {
            "area:ha": get_darray(value=7).astype(numpy.int16),
            "other": get_darray(value=9),
            "total:t": by_scheme,
        }
    ).chunk({"x": 2})


EXAMPLE = deliverable.Deliverable(
    name="example",
    select={"scheme": ("c", "a")},
    variables=("total:t", "area:ha"),
)


def test_select_keeps_the_selected_values_in_order() -> None:
    selected = deliverable.select(dset=get_dset(), deliverable=EXAMPLE)
    assert list(selected["scheme"].values) == ["c", "a"]
    numpy.testing.assert_array_equal(selected["total:t"].sel(scheme="a").values, 1)


def test_select_rejects_a_dim_the_dataset_lacks() -> None:
    bad = deliverable.Deliverable(
        name="bad", select={"missing": ("a",)}, variables=("area:ha",)
    )
    with pytest.raises(ValueError, match="missing"):
        deliverable.select(dset=get_dset(), deliverable=bad)


def test_write_deliverable_as_cog_writes_its_bands_in_order(
    tmp_path: pathlib.Path,
) -> None:
    uri = deliverable.write_deliverable(
        dset=get_dset(),
        deliverable=EXAMPLE,
        format=export.Format.COG,
        output_root=str(tmp_path),
        source_name="test",
        tile_id=TILE_ID,
    )
    assert uri == str(tmp_path / "example" / f"{TILE_ID:s}.tif")
    with rasterio.open(uri) as dataset:
        assert list(dataset.descriptions) == ["total:t:c", "total:t:a", "area:ha"]
        numpy.testing.assert_array_equal(dataset.read()[:, 0, 0], [3, 1, 7])
        assert dataset.tags()["cornerstone-source-name"] == "test"


def test_write_deliverable_as_zarr_keeps_only_the_selection(
    tmp_path: pathlib.Path,
) -> None:
    uri = deliverable.write_deliverable(
        dset=get_dset(),
        deliverable=EXAMPLE,
        format=export.Format.ZARR,
        output_root=str(tmp_path),
        source_name="test",
        tile_id=TILE_ID,
        version="v1",
    )
    assert uri == str(tmp_path / "v1" / "example" / f"{TILE_ID:s}.zarr")
    written = xarray.open_zarr(uri, consolidated=False)
    assert set(written.data_vars) == {"total:t", "area:ha"}
    assert list(written["scheme"].values) == ["c", "a"]
    assert written["area:ha"].dtype == numpy.int16
