import asyncio
import signal

from redis_canal.adapter.manager import AdapterManager
from redis_canal.adapter.plugin import Adapter
from redis_canal.log import logger
from redis_canal.models import Message
from redis_canal.tools import event_wait


class Agent:
    def __init__(
        self,
        redis_client,
        queue_type,
        queue_url,
        poll_interval,
        poll_size,
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
        self.poll_size = poll_size

        self.adapter = self.adapter_manager.init(
            queue_type,
            queue_url=queue_url,
            poll_interval=poll_interval,
            poll_size=poll_size,
        )

    async def start(self):
        if self._task:
            raise RuntimeError("Consumer already started.")
        self._stop_event.clear()
        self._task = asyncio.create_task(self._job())

    async def _job(self):
        await self.initialize()
        while not await event_wait(self._stop_event, 0.1):
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
        redis_client,
        queue_type,
        queue_url,
        poll_interval,
        poll_size,
        redis_stream_key,
        redis_stream_key_prefix,
        remove_if_enqueued,
        *args,
        **kwargs,
    ):
        super().__init__(
            redis_client,
            queue_type,
            queue_url,
            poll_interval,
            poll_size,
            *args,
            **kwargs,
        )

        self.redis_stream_key = redis_stream_key
        self.redis_stream_key_prefix = redis_stream_key_prefix
        self.remove_if_enqueued = remove_if_enqueued

    async def run(self):
        keys = await self._get_redis_stream_keys()
        for key in keys:
            await self._poll_stream_and_emit(key)

    async def _get_redis_stream_keys(self) -> list[str]:
        pass

    async def _poll_stream_and_emit(self, key):
        pass


class QueueToStream(Agent):
    def __init__(
        self,
        redis_client,
        queue_type,
        queue_url,
        poll_interval,
        poll_size,
        *args,
        **kwargs,
    ):
        super().__init__(
            redis_client,
            queue_type,
            queue_url,
            poll_interval,
            poll_size,
            *args,
            **kwargs,
        )

    async def _xadd_to_redis(self, message: Message):
        pass

    async def run(self):
        await self.adapter.poll(
            self._xadd_to_redis,
        )
