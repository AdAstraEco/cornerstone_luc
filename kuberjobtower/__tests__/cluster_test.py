from kuberjobtower import quantity
from kuberjobtower.cluster import _group_of


def test_gke_cuts_the_pool_name_to_sixteen_characters_in_its_instance_group_names() -> (
    None
):
    url = "https://www.googleapis.com/compute/v1/projects/p/zones/z/instanceGroups/gke-nonprod-shared-c-yaroslav-power-n-81341ed6-grp"
    assert _group_of("yaroslav-power-node-pool", url)
    assert not _group_of("yaroslav-worker-node-pool", url)
    worker = url.replace("power-n-81341ed6", "worker--731e69b9")
    assert _group_of("yaroslav-worker-node-pool", worker)
    assert not _group_of("standard-node-pool", worker)


def test_metrics_server_reports_cpu_in_nanocores() -> None:
    assert quantity.millicores("300412n") == 0
    assert quantity.millicores("7850000000n") == 7850
    assert quantity.millicores("250m") == 250
    assert quantity.bytes_("268692Ki") == 268692 * 1024
