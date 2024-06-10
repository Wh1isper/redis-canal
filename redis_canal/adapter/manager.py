from __future__ import annotations

import glob
import importlib
from os.path import basename, dirname, isfile, join
from typing import Any

import pluggy

from redis_canal.adapter import impl, plugin
from redis_canal.exceptions import PluginInitializationError, PluginNotFoundError
from redis_canal.log import logger
from redis_canal.utils import Singleton


class AdapterManager(metaclass=Singleton):
    def __init__(self):
        self.pm = pluggy.PluginManager(plugin.project_name)
        self.pm.add_hookspecs(plugin)
        self._registed_cls: dict[str, type[plugin.Adapter]] = {}

        self.pm.load_setuptools_entrypoints(plugin.project_name)
        self.load_all_local_model()

    def load_all_local_model(self):
        """
        Import all implementation in ./impl
        """
        logger.debug(f"Loading all local model from {impl}")

        return self._load_dir(impl)

    @property
    def registed_cls(self) -> dict[str, type]:
        """
        Access all registed class.

        Lazy load, only load once.
        """
        if self._registed_cls:
            return self._registed_cls
        for f in self.pm.hook.register(manager=self):
            try:
                f()
            except Exception as e:
                logger.error(f"One bridge registration failed: {e}")
                logger.exception(e)
                continue
        return self._registed_cls

    def _load_dir(self, module):
        """
        Import all python files in a submodule.
        """
        modules = glob.glob(join(dirname(module.__file__), "*.py"))
        sub_packages = (
            basename(f)[:-3] for f in modules if isfile(f) and not f.endswith("__init__.py")
        )
        packages = (str(module.__package__) + "." + i for i in sub_packages)
        for p in packages:
            logger.debug(f"Loading {p}")
            self.pm.register(importlib.import_module(p))

    def _normalize_name(self, name: str) -> str:
        return name.strip().lower()

    def register(self, cls: type[plugin.Adapter]):
        """
        Register a new model, if the model is already registed, skip it.
        """

        cls_name = self._normalize_name(cls.register_name)
        logger.debug(f"Register for new model: {cls_name}")
        if cls in self._registed_cls.values():
            logger.warning(
                f"SKIP: trying to register {cls} as '{cls_name}', but {cls_name} is already registed."
            )
            return
        if not issubclass(cls, plugin.Adapter):
            logger.error(f"SKIP: {cls_name} is not a subclass of {plugin.Adapter}")
            return
        self._registed_cls[cls_name] = cls

    def init(self, c, *args, **kwargs: dict[str, Any]) -> plugin.Adapter:
        """
        Init a new subclass of plugin.Adapter.

        Raises:
            NotFoundError: if cls_name is not registered
            InitializationError: if failed to initialize
        """
        if isinstance(c, plugin.Adapter):
            return c

        if isinstance(c, type):
            cls_type = c
        else:
            c = self._normalize_name(c)

            if not c in self.registed_cls:
                raise PluginNotFoundError(f"{c} is not registed.")
            cls_type = self.registed_cls[c]
        try:
            return cls_type(*args, **kwargs)
        except Exception as e:
            raise PluginInitializationError(e)
