from custom_adapter.imp import CustomAdapter

from redis_canal.adapter.manager import AdapterManager


def test_registed():
    manager = AdapterManager()

    assert CustomAdapter.register_name in manager.registed_cls.keys()
