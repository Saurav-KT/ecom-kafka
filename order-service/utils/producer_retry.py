import asyncio
import random
from typing import Callable, Tuple, Type
from aiokafka.errors import KafkaError


class RetryConfig:
    def __init__(
        self,
        retries: int = 5,
        base_delay: float = 0.5,
        max_delay: float = 30.0,
        jitter: bool = True,
    ):
        self.retries = retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.jitter = jitter


async def retry_with_backoff(
    func: Callable,
    retry_on: Tuple[Type[Exception], ...],
    retry_config: RetryConfig,
    *args,
    **kwargs,
):
    attempt = 0

    while True:
        try:
            return await func(*args, **kwargs)
        except retry_on as e:
            attempt += 1
            if attempt > retry_config.retries:
                raise

            delay = min(
                retry_config.base_delay * (2 ** (attempt - 1)),
                retry_config.max_delay,
            )

            if retry_config.jitter:
                delay *= random.uniform(0.7, 1.3)

            await asyncio.sleep(delay)
        except KafkaError:
            # Non-retryable Kafka error
            raise
