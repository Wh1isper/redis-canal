from functools import cached_property

from redis_canal.adapter.plugin import Adapter, hookimpl


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
        self.create_queue_if_not_exists()

    @cached_property
    def client(self):
        try:
            import boto3
        except ImportError:
            raise RuntimeError(
                "boto3 is not installed, try install moriarty with `pip install moriarty[matrix]` for all components or `pip install moriarty[sqs]` for sqs only"
            )
        return boto3.client("sqs")

    def create_queue_if_not_exists(self):
        pass


@hookimpl
def register(manager):
    manager.register(SQSAdapter)
