from jdluc import cache_key


def test_cache_key_is_stable() -> None:
    # emit.workflow(tile_id="20N_090W") at version=2, as existing scratch roots hold it.
    assert (
        cache_key.cache_key(
            module="jdluc.emit",
            qualname="workflow",
            version=2,
            arguments=[("tile_id", "20N_090W")],
        )
        == "94cbe56c057a"
    )


def test_cache_key_depends_on_version_and_arguments() -> None:
    keys = {
        cache_key.cache_key("jdluc.emit", "workflow", version, arguments)
        for version in (1, 2)
        for arguments in ([("tile_id", "20N_090W")], [("tile_id", "20N_080W")])
    }
    assert len(keys) == 4
