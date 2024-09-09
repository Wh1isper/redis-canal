import asyncio
from functools import cached_property
from typing import Awaitable

from redis_canal.adapter.plugin import Adapter, hookimpl
from redis_canal.log import logger
from redis_canal.models import Message
from redis_canal.tools import run_in_threadpool


class PubSubAdapter(Adapter):
    pass


@hookimpl
def register(manager):
    manager.register(PubSubAdapter)
