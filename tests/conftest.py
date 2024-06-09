import os

os.environ["REDIS_CANAL_LOG_LEVEL"] = "DEBUG"

import socket
import time
from uuid import uuid4

import pytest
import redis
import redis.asyncio

import docker
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
