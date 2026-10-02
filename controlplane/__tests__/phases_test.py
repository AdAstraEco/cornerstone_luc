import importlib
import pathlib
import sys

from controlplane.phases import DEFAULT_SPECS, Phase

sys.path.insert(0, str(pathlib.Path(__file__).parents[2] / "infra"))


def test_phases_equal_the_pods_phase_argument() -> None:
    # the pod's entry point cannot import controlplane, so compare here
    run_phase = importlib.import_module("run_phase")

    assert {p.value for p in Phase} == {p.value for p in run_phase.Phase}
    assert {p.value for p in Phase if p.is_per_tile} == {
        p.value for p in run_phase.Phase if p.is_per_tile
    }


def test_every_phase_has_a_spec() -> None:
    assert set(DEFAULT_SPECS) == set(Phase)
