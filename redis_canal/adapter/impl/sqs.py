from functools import cached_property
from typing import Awaitable

from redis_canal.adapter.plugin import Adapter, hookimpl
from redis_canal.log import logger
from redis_canal.models import Message


class SQSAdapter(Adapter):
    register_name = "sqs"

    def __init__(
        self,
        queue_url: str,
        poll_time: float,
        poll_size: int,
        *args,
        **kwargs,
    ):
        super().__init__(queue_url, poll_time, poll_size, *args, **kwargs)

        if self.poll_time < 1:
            self.poll_time = 1
        if self.poll_size > 10:
            self.poll_size = 10

        self.poll_time = self.poll_time
        self.ensure_queue_exists()

    @cached_property
    def client(self):
        try:
            import boto3
        except ImportError:
            raise RuntimeError(
                "boto3 is not installed, try install moriarty with `pip install moriarty[matrix]` for all components or `pip install moriarty[sqs]` for sqs only"
            )
        return boto3.client("sqs")

    def ensure_queue_exists(self):
        queue_name = self.queue_url.split("/")[-1]
        try:
            if self.client.get_queue_url(QueueName=queue_name):
                return
        except self.client.exceptions.QueueDoesNotExist:
            logger.error(f"Queue {self.queue_url} does not exist")
            raise

    async def emit(self, message: Message) -> None:
        pass

    async def poll(self, process_func: Awaitable[Message], *args, **kwargs) -> None:
        pass


@hookimpl
def register(manager):
    manager.register(SQSAdapter)
