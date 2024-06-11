import pytest

from redis_canal.adapter.impl.sqs import SQSAdapter


@pytest.fixture
def sqs_adapter(case_id):
    try:
        import boto3

        sqs = boto3.client("sqs")
    except ImportError:
        pytest.skip("boto3 is not installed")
    queue_name = f"redis-canal-test-{case_id}"
    try:
        queue_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
    except sqs.exceptions.QueueAlreadyExists:
        pass
    except Exception:
        pytest.skip("boto3 is not configured")

    adapter = SQSAdapter(
        queue_url=queue_url,
        poll_time=1,
        poll_size=10,
    )
    yield adapter
    try:
        sqs.delete_queue(QueueUrl=queue_url)
    except sqs.exceptions.QueueDoesNotExist:
        pass


async def test_sqs_adapter(sqs_adapter):
    pass
