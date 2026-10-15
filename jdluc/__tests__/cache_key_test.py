from jdluc import cache_key


def test_cache_key_is_unchanged_from_the_git_path_recipe() -> None:
    # emit.workflow(tile_id="20N_090W") at version=1, as keyed when the module name was a file
    # path relative to the git working tree. A different value would orphan every cached artifact.
    assert (
        cache_key.cache_key(
            module="jdluc.emit",
            qualname="workflow",
            version=1,
            arguments=[("tile_id", "20N_090W")],
        )
        == "71e2b639de7a"
    )


def test_cache_key_depends_on_version_and_arguments() -> None:
    keys = {
        cache_key.cache_key("jdluc.emit", "workflow", version, arguments)
        for version in (1, 2)
        for arguments in ([("tile_id", "20N_090W")], [("tile_id", "20N_080W")])
    }
    assert len(keys) == 4
