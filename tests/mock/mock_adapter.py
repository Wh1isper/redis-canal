from __future__ import annotations

from typing import TYPE_CHECKING, Awaitable

from redis_canal.adapter.plugin import Adapter, hookimpl
from redis_canal.models import Message


class MockAdapter(Adapter):
    register_name = "mock"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._emitted = {}
        self._polled = {
            "test:polled": Message(
                redis_key="test:polled",
                message_id="123-345",
                message_content={"f1": "v1"},
            ),
        }

    async def emit(self, message: Message) -> None:
        self._emitted[message.redis_key] = message

    async def poll(self, process_func: Awaitable[Message], *args, **kwargs) -> None:
        for message in self._polled.values():
            await process_func(message)


@hookimpl
def register(manager):
    manager.register(MockAdapter)
