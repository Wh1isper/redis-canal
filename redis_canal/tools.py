import asyncio
import contextlib
import functools
import inspect
import typing
from contextlib import asynccontextmanager
from datetime import datetime
from functools import wraps
from typing import Any, AsyncGenerator, ParamSpec, TypeVar

import anyio
import redis.asyncio as redis

from redis_canal.log import logger

P = ParamSpec("P")
T = TypeVar("T")


async def run_in_threadpool(func: typing.Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
    """
    From fastapi.concurrency
    """
    if kwargs:  # pragma: no cover
        # run_sync doesn't accept 'kwargs', so bind them in here
        func = functools.partial(func, **kwargs)
    return await anyio.to_thread.run_sync(func, *args)


def coro(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


async def get_current_timestamp(
    redis_client: redis.Redis | redis.RedisCluster,
) -> int:
    return int((await redis_client.time())[0])


async def get_current_timestamp_ms(
    redis_client: redis.Redis | redis.RedisCluster,
) -> int:
    return int((await redis_client.time())[0] * 1000 + (await redis_client.time())[1] / 1000)


def timestamp_to_datetime(timestamp: int, tz=None) -> datetime:
    return datetime.fromtimestamp(timestamp, tz=tz)


def timestamp_ms_to_datetime(timestamp_ms: int) -> datetime:
    return datetime.fromtimestamp(timestamp_ms / 1000)


async def event_wait(evt, timeout):
    # suppress TimeoutError because we'll return False in case of timeout
    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(evt.wait(), timeout)
    return evt.is_set()


def get_redis_url(
    host: str = "localhost",
    port: int = 6379,
    db: int = 0,
    cluster: bool = False,
    tls: bool = False,
    username: str = "",
    password: str = "",
):
    if not cluster:
        url = f"{host}:{port}/{db}"
    else:
        logger.debug("Cluster mode, no need to specify db")
        url = f"{host}:{port}"

    if username or password:
        url = f"{username}:{password}@{url}"

    if tls:
        return f"rediss://{url}"
    else:
        return f"redis://{url}"


@asynccontextmanager
async def get_redis_client(
    redis_url,
    is_cluster: bool = False,
) -> AsyncGenerator[Any, redis.Redis | redis.RedisCluster]:
    if is_cluster:
        redis_client = redis.RedisCluster.from_url(redis_url, decode_responses=True)
    else:
        redis_client = redis.from_url(redis_url, decode_responses=True)
    try:
        await redis_client.ping()
        yield redis_client
    finally:
        await redis_client.aclose()


def ensure_awaitable(func):
    if inspect.iscoroutinefunction(func):
        return func

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        nonlocal func
        if kwargs:
            func = functools.partial(func, **kwargs)
        return await anyio.to_thread.run_sync(func, *args)

    return wrapper
