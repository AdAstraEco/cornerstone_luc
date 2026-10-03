from jdluc import cache_key


def test_cache_key_matches_a_key_that_exists_in_the_bucket() -> None:
    # emit.workflow(tile_id="20N_090W") at version=2: the scratch store the 2 Oct 2026 run wrote.
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
