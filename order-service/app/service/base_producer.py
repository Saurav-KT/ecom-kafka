import json
from aiokafka import AIOKafkaProducer
from aiokafka.errors import (
    KafkaTimeoutError,
    KafkaConnectionError,
    LeaderNotAvailableError,
    NotLeaderForPartitionError,
    RequestTimedOutError,
)
from  utils.retry import retry_with_backoff, RetryConfig


RETRYABLE_PRODUCER_ERRORS = (
    KafkaTimeoutError,
    KafkaConnectionError,
    LeaderNotAvailableError,
    NotLeaderForPartitionError,
    RequestTimedOutError,
)


class BaseKafkaProducer:
    def __init__(self, bootstrap_servers: str):
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda k: k.encode() if k else None,
            value_serializer=lambda v: json.dumps(v).encode(),
            acks="all",
            enable_idempotence=True,
        )

        self._retry_config = RetryConfig()

    async def start(self):
        await self._producer.start()

    async def stop(self):
        await self._producer.stop()

    async def _send(self, topic, key, value):
        return await self._producer.send_and_wait(
            topic=topic,
            key=key,
            value=value,
        )

    async def send(self, topic, key, value):
        return await retry_with_backoff(
            self._send,
            RETRYABLE_PRODUCER_ERRORS,
            self._retry_config,
            topic,
            key,
            value,
        )
