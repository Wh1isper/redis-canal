import asyncio
from functools import cached_property
from typing import Awaitable

from redis_canal.adapter.plugin import Adapter, hookimpl
from redis_canal.log import logger
from redis_canal.models import Message
from redis_canal.tools import run_in_threadpool


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
        await run_in_threadpool(
            self.client.send_message,
            QueueUrl=self.queue_url,
            MessageBody=message.model_dump_json(),
        )

    def _poll_message(self) -> list[tuple[Message, str]]:
        valid_messages = []
        response = self.client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=self.poll_size,
            WaitTimeSeconds=self.poll_time,
        )
        if "Messages" not in response:
            return valid_messages

        for message in response["Messages"]:
            receipt_handle = message["ReceiptHandle"]
            try:
                body = message["Body"]
                message = Message.model_validate_json(body)
            except Exception as e:
                logger.error(f"Error parsing message {body}: {e}")
                logger.exception(e)
            else:
                valid_messages.append((message, receipt_handle))
        return valid_messages

    async def _process_messages(
        self, process_func: Awaitable[Message], message: Message, receipt_handle: str
    ):
        try:
            await process_func(message)
        except Exception as e:
            logger.error(f"Error processing message {message}: {e}")
        else:
            await run_in_threadpool(
                self.client.delete_message,
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle,
            )

    async def poll(self, process_func: Awaitable[Message], *args, **kwargs) -> None:
        response_and_receipt_handles = await run_in_threadpool(self._poll_message)
        await asyncio.gather(
            *[
                self._process_messages(process_func, message, receipt_handle)
                for message, receipt_handle in response_and_receipt_handles
            ]
        )


@hookimpl
def register(manager):
    manager.register(SQSAdapter)
