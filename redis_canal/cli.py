import click

from redis_canal.agent import QueueToStream, StreamToQueue
from redis_canal.tools import coro, get_redis_client, get_redis_url

from .envs import *


@click.command()
@click.option(
    "--redis-host",
    default="localhost",
    help="Redis host",
    type=str,
    envvar=REDIS_HOST_ENV,
)
@click.option(
    "--redis-port",
    default=6379,
    help="Redis port",
    type=int,
    envvar=REDIS_PORT_ENV,
)
@click.option(
    "--redis-db",
    default=0,
    help="Redis db",
    type=int,
    envvar=REDIS_DB_ENV,
)
@click.option(
    "--redis-cluster",
    default=False,
    help="Redis cluster",
    type=bool,
    envvar=REDIS_CLUSTER_ENV,
)
@click.option(
    "--redis-tls",
    default=False,
    help="Redis TLS",
    type=bool,
    envvar=REDIS_TLS_ENV,
)
@click.option(
    "--redis-username",
    default="",
    help="Redis username",
    type=str,
    envvar=REDIS_USERNAME_ENV,
)
@click.option(
    "--redis-password",
    default="",
    help="Redis password",
    type=str,
    envvar=REDIS_PASSWORD_ENV,
)
@click.option("--redis-url", required=False, help="Redis URL", type=str, envvar=REDIS_URL_ENV)
@click.option(
    "--queue-type",
    default="sqs",
    help="Queue type",
    type=str,
    envvar=QUEUE_TYPE_ENV,
)
@click.option(
    "--queue-url",
    default="",
    help="Queue URL",
    type=str,
    envvar=QUEUE_URL_ENV,
)
@click.option(
    "--redis-stream-key",
    default="",
    help="Redis stream key",
    type=str,
    envvar=REDIS_STREAM_KEY_ENV,
)
@click.option(
    "--redis-stream-key-prefix",
    default="",
    help="Redis stream key prefix",
    type=str,
    envvar=REDIS_STREAM_KEY_PREFIX_ENV,
)
@click.option(
    "--poll-interval",
    default=1,
    help="Poll interval",
    type=int,
    envvar=POLL_INTERVAL_ENV,
)
@click.option(
    "--poll-time",
    default=1,
    help="Poll time",
    type=int,
    envvar=POLL_TIME_ENV,
)
@click.option(
    "--poll-size",
    default=10,
    help="Poll size",
    type=int,
    envvar=POLL_SIZE_ENV,
)
@click.option(
    "--remove-if-enqueued",
    default=False,
    help="Remove if enqueued",
    type=bool,
    envvar=REMOVE_IF_ENQUEUED_ENV,
)
@click.option(
    "--dynamic",
    default=True,
    help="Get stream keys dynamically",
    type=bool,
    envvar=DYNAMIC_STREAM_KEY_ENV,
)
@coro
async def stream_to_queue(
    redis_host,
    redis_port,
    redis_db,
    redis_cluster,
    redis_tls,
    redis_username,
    redis_password,
    redis_url,
    queue_type,
    queue_url,
    redis_stream_key,
    redis_stream_key_prefix,
    poll_interval,
    poll_time,
    poll_size,
    remove_if_enqueued,
    dynamic,
):
    redis_url = redis_url or get_redis_url(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        cluster=redis_cluster,
        tls=redis_tls,
        username=redis_username,
        password=redis_password,
    )
    async with get_redis_client(redis_url, is_cluster=redis_cluster) as redis_client:
        await StreamToQueue(
            redis_client=redis_client,
            queue_type=queue_type,
            queue_url=queue_url,
            redis_stream_key=redis_stream_key,
            redis_stream_key_prefix=redis_stream_key_prefix,
            poll_interval=poll_interval,
            poll_time=poll_time,
            poll_size=poll_size,
            remove_if_enqueued=remove_if_enqueued,
            dynamic=dynamic,
        ).run_forever()


@click.command()
@click.option(
    "--redis-host",
    default="localhost",
    help="Redis host",
    type=str,
    envvar=REDIS_HOST_ENV,
)
@click.option(
    "--redis-port",
    default=6379,
    help="Redis port",
    type=int,
    envvar=REDIS_PORT_ENV,
)
@click.option(
    "--redis-db",
    default=0,
    help="Redis db",
    type=int,
    envvar=REDIS_DB_ENV,
)
@click.option(
    "--redis-cluster",
    default=False,
    help="Redis cluster",
    type=bool,
    envvar=REDIS_CLUSTER_ENV,
)
@click.option(
    "--redis-tls",
    default=False,
    help="Redis TLS",
    type=bool,
    envvar=REDIS_TLS_ENV,
)
@click.option(
    "--redis-username",
    default="",
    help="Redis username",
    type=str,
    envvar=REDIS_USERNAME_ENV,
)
@click.option(
    "--redis-password",
    default="",
    help="Redis password",
    type=str,
    envvar=REDIS_PASSWORD_ENV,
)
@click.option("--redis-url", required=False, help="Redis URL", type=str, envvar=REDIS_URL_ENV)
@click.option(
    "--queue-type",
    default="sqs",
    help="Queue type",
    type=str,
    envvar=QUEUE_TYPE_ENV,
)
@click.option(
    "--queue-url",
    default="",
    help="Queue URL",
    type=str,
    envvar=QUEUE_URL_ENV,
)
@click.option(
    "--poll-interval",
    default=0.1,
    help="Poll interval",
    type=int,
    envvar=POLL_INTERVAL_ENV,
)
@click.option(
    "--poll-time",
    default=1,
    help="Poll time",
    type=int,
    envvar=POLL_TIME_ENV,
)
@click.option(
    "--poll-size",
    default=10,
    help="Poll size",
    type=int,
    envvar=POLL_SIZE_ENV,
)
@click.option(
    "--maxlen",
    default=1000,
    help="Maxlen of stream",
    type=int,
    envvar=MAXLEN_ENV,
)
@coro
async def queue_to_stream(
    redis_host,
    redis_port,
    redis_db,
    redis_cluster,
    redis_tls,
    redis_username,
    redis_password,
    redis_url,
    queue_type,
    queue_url,
    poll_interval,
    poll_time,
    poll_size,
    maxlen,
):
    redis_url = redis_url or get_redis_url(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        cluster=redis_cluster,
        tls=redis_tls,
        username=redis_username,
        password=redis_password,
    )

    async with get_redis_client(redis_url, is_cluster=redis_cluster) as redis_client:
        await QueueToStream(
            redis_client=redis_client,
            queue_type=queue_type,
            queue_url=queue_url,
            poll_interval=poll_interval,
            poll_time=poll_time,
            poll_size=poll_size,
            maxlen=maxlen,
        ).run_forever()


@click.group()
def cli():
    pass


cli.add_command(stream_to_queue)
cli.add_command(queue_to_stream)
