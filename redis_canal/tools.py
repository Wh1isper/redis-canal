from redis_canal.log import logger


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
