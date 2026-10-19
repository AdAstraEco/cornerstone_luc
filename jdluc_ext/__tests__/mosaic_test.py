import pathlib

import numpy
import pytest
import rasterio
import rasterio.transform

from jdluc import export
from jdluc_ext import mosaic

BAND_NAMES = ("climate-zone", "conversion-source", "emissions:tco2e-per-ha")


def write_cog(
    path: pathlib.Path,
    north: float,
    pixel: float,
    values: list[float],
    west: float,
    size: int = 4,
) -> str:
    """A tiny GeoTIFF on an EPSG:4326 grid, band i filled with values[i - 1]."""
    path.parent.mkdir(exist_ok=True, parents=True)
    with rasterio.open(
        path,
        "w",
        count=len(values),
        crs="EPSG:4326",
        driver="GTiff",
        dtype="float32",
        height=size,
        nodata=float("nan"),
        transform=rasterio.transform.from_origin(west, north, pixel, pixel),
        width=size,
    ) as dataset:
        dataset.write(
            numpy.stack(
                [
                    numpy.full((size, size), value, dtype=numpy.float32)
                    for value in values
                ]
            )
        )
        for index, name in enumerate(BAND_NAMES, start=1):
            dataset.set_band_description(index, name)
    return str(path)


def test_mosaic_spans_the_union_of_its_tiles(tmp_path: pathlib.Path) -> None:
    a = write_cog(path=tmp_path / "a.tif", north=10, pixel=1, values=[1, 2, 3], west=0)
    # East of a
    b = write_cog(path=tmp_path / "b.tif", north=10, pixel=1, values=[4, 5, 6], west=4)
    uri = mosaic.mosaic(output_uri=str(tmp_path / "m.vrt"), source_uris=[a, b])

    with rasterio.open(uri) as dataset:
        assert dataset.descriptions == BAND_NAMES
        assert dataset.crs.to_epsg() == 4326
        assert (dataset.width, dataset.height) == (8, 4)
        assert (dataset.transform.c, dataset.transform.f) == (0, 10)
        band = dataset.read(1)
    numpy.testing.assert_array_equal(band[:, :4], 1)
    numpy.testing.assert_array_equal(band[:, 4:], 4)


def test_mosaic_skips_absent_tiles(tmp_path: pathlib.Path) -> None:
    a = write_cog(path=tmp_path / "a.tif", north=10, pixel=1, values=[1] * 3, west=0)
    uri = mosaic.mosaic(
        output_uri=str(tmp_path / "m.vrt"),
        source_uris=[a, str(tmp_path / "absent.tif")],
    )
    with rasterio.open(uri) as dataset:
        assert (dataset.width, dataset.height) == (4, 4)


def test_mosaic_raises_when_no_tile_exists(tmp_path: pathlib.Path) -> None:
    with pytest.raises(FileNotFoundError):
        mosaic.mosaic(
            output_uri=str(tmp_path / "m.vrt"),
            source_uris=[str(tmp_path / "absent.tif")],
        )


def test_mosaic_rejects_another_grid(tmp_path: pathlib.Path) -> None:
    a = write_cog(path=tmp_path / "a.tif", north=10, pixel=1, values=[1] * 3, west=0)
    b = write_cog(path=tmp_path / "b.tif", north=10, pixel=2, values=[1] * 3, west=4)
    with pytest.raises(ValueError, match="does not share the grid"):
        mosaic.mosaic(output_uri=str(tmp_path / "m.vrt"), source_uris=[a, b])


def test_mosaic_accepts_pixel_sizes_one_rounding_apart(tmp_path: pathlib.Path) -> None:
    # As exported for a tile whose north edge is the equator and for its neighbour
    pixel, rounded = 0.00025, 0.00025000000000000006
    assert pixel != rounded
    a = write_cog(path=tmp_path / "a.tif", north=0, pixel=pixel, values=[1] * 3, west=0)
    # South of a
    b = write_cog(
        path=tmp_path / "b.tif", north=-0.001, pixel=rounded, values=[2] * 3, west=0
    )
    uri = mosaic.mosaic(output_uri=str(tmp_path / "m.vrt"), source_uris=[a, b])

    with rasterio.open(uri) as dataset:
        assert (dataset.width, dataset.height) == (4, 8)
        band = dataset.read(1)
    numpy.testing.assert_array_equal(band[:4], 1)
    numpy.testing.assert_array_equal(band[4:], 2)


def test_mosaic_rejects_pixel_sizes_beyond_rounding(tmp_path: pathlib.Path) -> None:
    a = write_cog(
        path=tmp_path / "a.tif", north=0, pixel=0.00025, values=[1] * 3, west=0
    )
    b = write_cog(
        path=tmp_path / "b.tif",
        north=-0.001,
        pixel=0.00025 * (1 + 1e-6),
        values=[1] * 3,
        west=0,
    )
    with pytest.raises(ValueError, match="does not share the grid"):
        mosaic.mosaic(output_uri=str(tmp_path / "m.vrt"), source_uris=[a, b])


def test_mosaic_workflow_mosaics_a_deliverables_tiles_beside_them(
    tmp_path: pathlib.Path,
) -> None:
    for tile_id, west in (("a", 0), ("b", 4)):
        write_cog(
            path=pathlib.Path(
                export.get_tile_output_uri(
                    format=export.Format.COG,
                    name="example",
                    output_root=str(tmp_path),
                    tile_id=tile_id,
                    version="v1",
                )
            ),
            north=10,
            pixel=1,
            values=[1] * 3,
            west=west,
        )
    uri = mosaic.mosaic_workflow(
        aoi_name="aoi",
        deliverable_name="example",
        output_root=str(tmp_path),
        tile_ids=["a", "b", "c"],
        version="v1",
    )
    assert uri == str(tmp_path / "v1" / "mosaics" / "example-aoi.vrt")
    with rasterio.open(uri) as dataset:
        assert (dataset.width, dataset.height) == (8, 4)
