from redis_canal.adapter.manager import AdapterManager


def test_bridge_manager():
    registed = ["sqs", "pubsub"]
    manager = AdapterManager()

    assert sorted(registed) == sorted(manager.registed_cls.keys())
