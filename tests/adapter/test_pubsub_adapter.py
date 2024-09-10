import os

import pytest

from redis_canal.adapter.impl.pubsub import PubSubAdapter
from redis_canal.models import Message


@pytest.fixture
def pubsub_adapter(case_id):
    try:
        import google.api_core.exceptions as exceptions
        import google.cloud.pubsub_v1

    except ImportError:
        pytest.skip("Google Cloud Pub/Sub SDK is not installed")

    project_id = os.getenv("TEST_PUBSUB_PROJECT_ID")
    if not project_id:
        pytest.skip("TEST_PUBSUB_PROJECT_ID is not configured")

    queue_url = f"projects/{project_id}/topics/test-topic"
    try:
        adapter = PubSubAdapter(
            queue_url=queue_url,
            poll_time=1,
            poll_size=10,
        )
    except Exception as e:
        pytest.skip(f"Google Cloud Pub/Sub SDK is not configured correctly: {e}")

    yield adapter
    try:
        adapter.publisher.delete_topic(request={"topic": queue_url})
    except exceptions.NotFound:
        pass


async def test_pubsub_adapter(pubsub_adapter):

    message_input = Message(
        redis_key="test",
        message_id="123-345",
        message_content={"f1": "v1"},
    )

    async def validate(message):
        assert message == message_input
        print("validated!")

    await pubsub_adapter.emit(message_input)
    await pubsub_adapter.poll(
        process_func=validate,
    )
