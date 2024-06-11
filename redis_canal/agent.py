import asyncio
import signal
from uuid import uuid4

import redis.asyncio as redis

from redis_canal.adapter.manager import AdapterManager
from redis_canal.log import logger
from redis_canal.models import Message
from redis_canal.tools import event_wait


class Agent:
    def __init__(
        self,
        redis_client: redis.Redis | redis.RedisCluster,
        queue_type: str,
        queue_url: str,
        poll_interval: float,
        poll_time: float,
        poll_size: int,
        *args,
        **kwargs,
    ):
        self._stop_event = asyncio.Event()
        self._task: None | asyncio.Task = None

        self.adapter_manager = AdapterManager()
        self.redis_client = redis_client

        self.queue_type = queue_type
        self.queue_url = queue_url
        self.poll_interval = poll_interval
        self.poll_time = poll_time
        self.poll_size = poll_size

        self.adapter = self.adapter_manager.init(
            queue_type,
            queue_url=queue_url,
            poll_time=poll_time,
            poll_size=poll_size,
        )

    async def start(self):
        if self._task:
            raise RuntimeError("Consumer already started.")
        self._stop_event.clear()
        self._task = asyncio.create_task(self._job())

    async def _job(self):
        await self.initialize()
        while not await event_wait(self._stop_event, self.poll_interval):
            if self._stop_event.is_set():
                break
            try:
                await self.run()
            except Exception as e:
                logger.exception(e)

        await self.cleanup()

    async def stop(self):
        self._stop_event.set()
        if self._task:
            logger.info("Waiting for consumer finish...")
            await self._task

    async def run_forever(self, stop_signals: list = [signal.SIGINT, signal.SIGTERM]):
        loop = asyncio.get_event_loop()

        stop_event = asyncio.Event()

        async def _stop():
            logger.debug("Signal received")
            stop_event.set()

        for sig in stop_signals:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(_stop()))

        await self.start()
        logger.info(f"Consumer started, waiting for signals {stop_signals}...")
        await stop_event.wait()

        logger.info(f"Terminating consumer...")
        await self.stop()
        logger.info(f"Consumer terminated")

    async def initialize(self):
        """
        Initialize, implement in subclass
        """
        pass

    async def run(self):
        """
        Run, implement in subclass
        """

    async def cleanup(self):
        """
        Cleanup, implement in subclass
        """
        pass


class StreamToQueue(Agent):
    def __init__(
        self,
        redis_client: redis.Redis | redis.RedisCluster,
        queue_type: str,
        queue_url: str,
        poll_interval: float,
        poll_time: float,
        poll_size: int,
        redis_stream_key: str,
        redis_stream_key_prefix: str,
        remove_if_enqueued: bool,
        dynamic: bool = True,
        *args,
        **kwargs,
    ):
        super().__init__(
            redis_client,
            queue_type,
            queue_url,
            poll_interval,
            poll_time,
            poll_size,
            *args,
            **kwargs,
        )

        self.redis_stream_key = redis_stream_key
        self.redis_stream_key_prefix = redis_stream_key_prefix
        self.remove_if_enqueued = remove_if_enqueued
        self._stream_keys = None
        self.dynamic = dynamic

        self.group_name = f"redis-canal-{queue_type}"
        self.min_idle_time = (
            self.poll_interval
        )  # If a message is not been acked in min_idle_time ms, will retry in next poll
        self.consumer_id = uuid4().hex

    async def run(self):
        keys = self._stream_keys or await self._get_redis_stream_keys()
        if not self.dynamic and self._stream_keys is None:
            logger.info(f"Disable dynamic mode, cached {len(keys)} stream keys")
            self._stream_keys = keys

        if not keys:
            logger.debug(
                f"No stream to poll for specified key `{self.redis_stream_key}` and prefix `{self.redis_stream_key_prefix}`, waiting {self.poll_interval}s..."
            )
            await event_wait(self._stop_event, self.poll_interval)
            return
        for key in keys:
            logger.debug(f"Polling and emitting stream {key}")
            await self._poll_stream_and_emit(key)

    async def _get_redis_stream_keys(self) -> list[str]:
        keys = []

        if self.redis_stream_key:
            # Check is stream
            try:
                await self.redis_client.xinfo_stream(self.redis_stream_key)
            except redis.ResponseError:
                logger.error(f"{self.redis_stream_key} is not a stream or does not exist")
                raise
            keys.append(self.redis_stream_key)

        if self.redis_stream_key_prefix:
            if self.redis_stream_key_prefix == "*":
                scan_key = "*"
            else:
                scan_key = f"{self.redis_stream_key_prefix}*"
            keys.extend(
                [k async for k in self.redis_client.scan_iter(match=scan_key, _type="stream")]
            )
        return keys

    async def _poll_stream_and_emit(self, key) -> None:
        # We use xautoclaim to process all unacked messages
        while True:
            messgae_id = "0-0"
            claimed_messages = (
                await self.redis_client.xautoclaim(
                    key,
                    self.group_name,
                    self.consumer_id,
                    min_idle_time=int(self.min_idle_time * 1000),
                    start_id=messgae_id,
                    count=self.poll_size,
                )
            )[1]
            if not claimed_messages:
                break

            asyncio.gather(
                *[
                    self._emit_one(key, message_id, message_content)
                    for message_id, message_content in claimed_messages
                ]
            )

    async def _emit_one(self, key, message_id, message_content):
        if message_id == message_content == None:
            # Fix (None, None) for redis 6.x
            return
        try:
            await self.adapter.emit(
                Message.from_redis(
                    key,
                    message_id,
                    message_content,
                )
            )
        except Exception as e:
            logger.exception(e)
        else:
            await self.redis_client.xack(key, self.group_name, message_id)
            if self.remove_if_enqueued:
                await self.redis_client.xdel(key, message_id)


class QueueToStream(Agent):
    def __init__(
        self,
        redis_client: redis.Redis | redis.RedisCluster,
        queue_type: str,
        queue_url: str,
        poll_interval: float,
        poll_time: float,
        poll_size: int,
        maxlen: int,
        *args,
        **kwargs,
    ):
        super().__init__(
            redis_client,
            queue_type,
            queue_url,
            poll_interval,
            poll_time,
            poll_size,
            *args,
            **kwargs,
        )
        self.maxlen = maxlen

    async def _xadd_to_redis(self, message: Message):
        main_id = message.message_id.split("-")[0]
        try:
            await self.redis_client.xadd(
                name=message.redis_key,
                fields=message.message_content,
                id=f"{main_id}-*",
                maxlen=self.maxlen,
            )
        except Exception as e:
            logger.exception(e)

    async def run(self):
        await self.adapter.poll(
            self._xadd_to_redis,
        )
