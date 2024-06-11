async def test_s2q_and_q2s(stream_to_queue, queue_to_stream, redis_key):
    await stream_to_queue.run()

    assert redis_key in stream_to_queue.adapter._emitted
    assert "test:other" in stream_to_queue.adapter._emitted

    await queue_to_stream.run()

    await stream_to_queue.run()
    if stream_to_queue.dynamic:
        assert "test:polled" in stream_to_queue.adapter._emitted
    else:
        assert "test:polled" not in stream_to_queue.adapter._emitted
