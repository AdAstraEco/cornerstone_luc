"""The scratch-cache key, as a pure function with no third-party imports.

Kept apart from ``storage`` (which pulls in pandas and xarray) so tools that only need to
*name* a cache artifact, such as the control plane's tile-status check, can import it cheaply.
Changing this changes every key and so orphans every cached artifact: ``cache_key_test.py``
pins a key that exists in the bucket.
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
    arguments already removed, in signature order.
    """
    data = "|".join(map(str, (module, qualname, version, *arguments))).encode()
    return hashlib.sha1(data=data).hexdigest()[:12]
