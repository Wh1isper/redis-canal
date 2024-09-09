from __future__ import annotations

import asyncio
from functools import cached_property
from typing import TYPE_CHECKING, Awaitable

from redis_canal.adapter.plugin import Adapter, hookimpl
from redis_canal.log import logger
from redis_canal.models import Message
from redis_canal.tools import run_in_threadpool

if TYPE_CHECKING:
    from google.cloud import pubsub_v1


class PubSubAdapter(Adapter):
    """
    PubSub Adapter for Google Cloud Pub/Sub.

    Args:
        queue_url (str): The topic path, e.g. projects/{project}/topics/{topic}
        poll_time (float): The time to poll the queue
        poll_size (int): The number of messages to poll at a time
    """

    register_name = "pubsub"

    def __init__(self, queue_url: str, poll_time: float, poll_size: int, *args, **kwargs):
        super().__init__(queue_url, poll_time, poll_size, *args, **kwargs)

        if self.poll_time < 1:
            self.poll_time = 1
        if self.poll_size > 10:
            self.poll_size = 10

        self.poll_time = self.poll_time
        self.ensure_queue_exists()

    @property
    def topic_path(self) -> str:
        return self.queue_url

    @property
    def subscription_path(self) -> str:
        return f"projects/{self.project_id}/subscriptions/{self.subscribe_id}"

    @property
    def project_id(self) -> str:
        return self.topic_path.split("/")[1]

    @property
    def subscribe_id(self) -> str:
        return "redis_cannal_pubsub"

    @cached_property
    def publisher(self) -> "pubsub_v1.PublisherClient":
        try:
            from google.cloud import pubsub_v1
        except ImportError:
            raise RuntimeError(
                "Google Cloud Pub/Sub SDK is required to use PubSub Adapter, try install redis_canal with `pip install redis_canal[all]` for all components or `pip install redis_canal[pubsub]` for pubsub"
            )

        return pubsub_v1.PublisherClient()

    @cached_property
    def subscriber(self) -> "pubsub_v1.SubscriberClient":
        try:
            from google.cloud import pubsub_v1
        except ImportError:
            raise RuntimeError(
                "Google Cloud Pub/Sub SDK is required to use PubSub Adapter, try install redis_canal with `pip install redis_canal[all]` for all components or `pip install redis_canal[pubsub]` for pubsub"
            )

        return pubsub_v1.SubscriberClient()

    def ensure_queue_exists(self):
        from google.api_core import exceptions

        try:
            self.publisher.create_topic(
                name=self.topic_path,
            )
        except exceptions.AlreadyExists:
            pass

        try:
            self.subscriber.create_subscription(
                name=self.subscription_path,
                topic=self.topic_path,
            )
        except exceptions.AlreadyExists:
            pass

    async def emit(self, message: Message) -> None:
        def _():
            response = self.publisher.publish(
                topic=self.topic_path,
                data=message.model_dump_json().encode("utf-8"),
            )

            logger.debug(f"Published message {response.result()} to {self.topic_path}")

        await run_in_threadpool(_)

    async def poll(self, process_func: Awaitable[Message], *args, **kwargs) -> None:
        pubsub_messages = await self._poll_message()
        await asyncio.gather(
            *[
                self._process_messages(process_func, message, ack_id)
                for message, ack_id in pubsub_messages
            ]
        )

    async def _poll_message(self) -> list[tuple[Message, str]]:
        def _():
            response = self.subscriber.pull(
                request={
                    "subscription": self.subscription_path,
                    "max_messages": self.poll_size,
                }
            )
            return [
                (
                    Message.model_validate_json(message.message.data.decode("utf-8")),
                    message.ack_id,
                )
                for message in response.received_messages
            ]

        return await run_in_threadpool(_)

    async def _process_messages(
        self, process_func: Awaitable[Message], message: Message, ack_id: str
    ):
        try:
            await process_func(message)
        except Exception as e:
            logger.error(f"Error processing message {message}: {e}")
        else:
            await run_in_threadpool(
                self.subscriber.acknowledge,
                request={
                    "subscription": self.subscription_path,
                    "ack_ids": [ack_id],
                },
            )


@hookimpl
def register(manager):
    manager.register(PubSubAdapter)
