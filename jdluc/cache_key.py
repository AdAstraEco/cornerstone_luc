"""The scratch-cache key: the recipe ``storage`` hashes an artifact's identity with.

A pure function with no third-party imports, kept out of ``storage`` (which pulls in pandas and
xarray) so it stays cheap to import and easy to test. ``cache_key_test.py`` pins a key that
exists in the bucket: changing this recipe changes every key and so orphans every cached
artifact.
"""

import collections.abc
import hashlib
import typing


def cache_key(
    module: str,
    qualname: str,
    version: int,
    arguments: collections.abc.Iterable[tuple[str, typing.Any]],
) -> str:
    """12 hex chars identifying ``module.qualname`` at ``version`` called with ``arguments``.

    ``arguments`` are the bound ``(name, value)`` pairs with defaults applied and ignored
    arguments already removed, in signature order. Each value is hashed through ``str()``, and
    the key does not cover the code of the function: a changed body keeps its key unless
    ``version`` is bumped.
    """
    data = "|".join(map(str, (module, qualname, version, *arguments))).encode()
    return hashlib.sha1(data=data).hexdigest()[:12]
