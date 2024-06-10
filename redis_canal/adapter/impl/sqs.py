from functools import cached_property

from redis_canal.adapter.plugin import Adapter, hookimpl


class SQSAdapter(Adapter):
    register_name = "sqs"

    def __init__(
        self,
        queue_url: str,
        poll_interval: int,
        poll_size: int,
        *args,
        **kwargs,
    ):
        self.queue_url = queue_url
        self.poll_interval = poll_interval
        self.poll_size = poll_size

    @cached_property
    def client(self):
        try:
            import boto3
        except ImportError:
            raise RuntimeError(
                "boto3 is not installed, try install moriarty with `pip install moriarty[matrix]` for all components or `pip install moriarty[sqs]` for sqs only"
            )
        return boto3.client("sqs")


@hookimpl
def register(manager):
    manager.register(SQSAdapter)
