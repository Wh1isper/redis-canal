from __future__ import annotations

from pydantic import BaseModel


class Message(BaseModel):
    redis_key: str
    message_id: str
    message_content: dict

    @classmethod
    def from_redis(cls, redis_key: str, message_id: str, message_content: dict) -> Message:
        return cls(redis_key=redis_key, message_id=message_id, message_content=message_content)
