import pytest

from kubejobs import aoi


def test_explicit_tiles_are_validated_without_reading_a_boundary() -> None:
    assert aoi.validate_tiles(["20N_090W", "20N_080W", "20N_090W"]) == (
        "20N_080W",
        "20N_090W",
    )
    with pytest.raises(ValueError, match="not tiles of the pipeline's grid"):
        aoi.validate_tiles(["99N_999E"])


def test_methodology_names_match_the_pipeline() -> None:
    assert "STATISTICAL" in aoi.methodology_names()
