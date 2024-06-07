from __future__ import annotations

from typing import Awaitable

import pluggy

project_name = "redis_canal.adapter"
"""
The entry-point name of this extension.

Should be used in ``pyproject.toml`` as ``[project.entry-points."redis_canal.adapter.bridge"]``
"""
hookimpl = pluggy.HookimplMarker(project_name)
"""
Hookimpl marker for this extension, extension module should use this marker

Example:

    .. code-block:: python

        @hookimpl
        def register(manager):
            ...
"""

hookspec = pluggy.HookspecMarker(project_name)


@hookspec
def register(manager):
    """
    For more information about this function, please check the :ref:`manager`

    We provided an example package for you in ``{project_root}/example/extension/custom-adapter``.

    Example:

    .. code-block:: python

        class CustomAdapter(Adapter):
            register_name = "example"

        from redis_canal.adapter.plugin import hookimpl

        @hookimpl
        def register(manager):
            manager.register(CustomAdapter)


    Config ``project.entry-points`` so that we can find it

    .. code-block:: toml

        [project.entry-points."redis_canal.adapter"]
        {whatever-name} = "{package}.{path}.{to}.{file-with-hookimpl-function}"


    You can verify it by `redis_canal-adapter list-plugins`.
    """


class Adapter:
    """
    Adapter is used to adapt queue service and redis stream
    """

    register_name: str

    def __init__(self, *args, **kwargs):
        pass
