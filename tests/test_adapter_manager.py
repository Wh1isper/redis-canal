from redis_canal.adapter.manager import AdapterManager


def test_bridge_manager():
    registed = ["sqs"]
    manager = AdapterManager()

    assert sorted(registed) == sorted(manager.registed_cls.keys())
