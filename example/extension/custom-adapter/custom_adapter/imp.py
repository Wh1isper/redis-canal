from redis_canal.adapter.plugin import Adapter, hookimpl


class CustomAdapter(Adapter):
    register_name = "example"


@hookimpl
def register(manager):
    manager.register(CustomAdapter)
