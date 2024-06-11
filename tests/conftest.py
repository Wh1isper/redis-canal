import os

os.environ["REDIS_CANAL_LOG_LEVEL"] = "DEBUG"

import socket
import time
from uuid import uuid4

import pytest
import redis
import redis.asyncio

import docker
from redis_canal.adapter.manager import AdapterManager
from redis_canal.agent import QueueToStream, StreamToQueue
from redis_canal.tools import get_redis_url


def get_port():
    # Get an unoccupied port
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def docker_client():
    try:
        client = docker.from_env()
        client.ping()
        return client
    except:
        pytest.skip("Docker is not available")


@pytest.fixture(scope="session")
def redis_port(docker_client):
    """
    Start a redis container and return the port
    """
    redis_port = get_port()
    container = None
    try:
        container = docker_client.containers.run(
            f"redis:{os.getenv('REDIS_VERSION', '7')}",
            detach=True,
            ports={"6379": redis_port},
            remove=True,
        )
        time.sleep(1)  # Wait for the server to start
        while True:
            try:
                # Ping redis
                redis_client = redis.Redis(host="localhost", port=redis_port)
                redis_client.ping()
            except:
                time.sleep(0.5)
            else:
                break
        yield redis_port
    finally:
        if container:
            container.stop()


@pytest.fixture
async def async_redis_client(redis_port):
    redis_url = get_redis_url(port=redis_port)
    redis_client = redis.asyncio.from_url(redis_url, decode_responses=True)
    try:
        await redis_client.flushall()
        yield redis_client
    finally:
        await redis_client.aclose()


@pytest.fixture
def case_id():
    return uuid4().hex


@pytest.fixture(scope="session")
def mock_adapter_manager():
    AdapterManager._instances.clear()
    a = AdapterManager()

    from mock import mock_adapter

    a._load_dir(mock_adapter)
    yield a
    AdapterManager._instances.clear()


@pytest.fixture
def redis_key(case_id):
    return f"test:{case_id}"


@pytest.fixture
async def stream_key(async_redis_client, redis_key):
    await async_redis_client.xadd(name=redis_key, fields={"f1": "v1", "f2": "v2"})
    await async_redis_client.xadd(name="test:other", fields={"f1": "v1", "f2": "v2"})
    yield redis_key
    await async_redis_client.delete(redis_key)


@pytest.fixture(params=[True, False])
def dynamic(request):
    yield request.param


@pytest.fixture
def stream_to_queue(mock_adapter_manager, async_redis_client, stream_key, dynamic):
    s2q = StreamToQueue(
        async_redis_client,
        queue_type="mock",
        queue_url="mock",
        poll_interval=0.1,
        poll_time=0.1,
        poll_size=10,
        redis_stream_key=stream_key,
        redis_stream_key_prefix="test:",
        remove_if_enqueued=True,
        dynamic=dynamic,
    )
    s2q.adapter_manager = mock_adapter_manager
    return s2q


@pytest.fixture
def queue_to_stream(mock_adapter_manager, async_redis_client):
    q2s = QueueToStream(
        async_redis_client,
        queue_type="mock",
        queue_url="mock",
        poll_interval=0.1,
        poll_time=0.1,
        poll_size=10,
        maxlen=10,
    )
    q2s.adapter_manager = mock_adapter_manager
    return q2s
