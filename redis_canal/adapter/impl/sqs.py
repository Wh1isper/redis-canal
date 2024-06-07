from redis_canal.adapter.plugin import Adapter, hookimpl


class SQSAdapter(Adapter):
    register_name = "sqs"

    def __init__(
        self,
        sqs_url: str,
        *args,
        **kwargs,
    ):
        pass

    @property
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
