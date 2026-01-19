from aiokafka.errors import (
    KafkaError,

    # Authorization
    GroupAuthorizationFailedError,

    # Consumer group
    CommitFailedError,
    RebalanceInProgressError,
    IllegalGenerationError,
    UnknownMemberIdError,

    # Offset related
    OffsetOutOfRangeError,
    InvalidCommitOffsetSizeError, CoordinatorNotAvailableError,

)

from typing import Optional, Dict, Any
# =====================================================
# Base Exception
# =====================================================

class BaseKafkaException(Exception):
    """
    Base Kafka exception for application-level handling
    """

    def __init__(
        self,
        message: str,
        *,
        retryable: bool = False,
        fatal: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
    ):
        super().__init__(message)
        self.retryable = retryable
        self.fatal = fatal
        self.metadata = metadata or {}
        self.__cause__ = cause

    def __str__(self) -> str:
        meta = f" metadata={self.metadata}" if self.metadata else ""
        return f"{self.__class__.__name__}: {self.args[0]}{meta}"

# =====================================================
# Consumer Exceptions
# =====================================================

class KafkaConsumerException(BaseKafkaException):
    """Base class for Consumer-related exceptions"""


class ConsumerCommitException(KafkaConsumerException):
    pass


class ConsumerRebalanceException(KafkaConsumerException):
    pass


class ConsumerOffsetOutOfRangeException(KafkaConsumerException):
    pass


class ConsumerAuthorizationException(KafkaConsumerException):
    pass


class ConsumerCoordinatorException(KafkaConsumerException):
    pass

def map_consumer_exception(
    exc: Exception,
    *,
    metadata: Optional[Dict[str, Any]] = None
) -> BaseKafkaException:
    """
    Convert aiokafka consumer exceptions to application exceptions
    """

    if isinstance(exc, CommitFailedError):
        return ConsumerCommitException(
            "Offset commit failed",
            retryable=True,
            metadata=metadata,
            cause=exc,
        )

    if isinstance(exc, RebalanceInProgressError):
        return ConsumerRebalanceException(
            "Rebalance in progress",
            retryable=True,
            metadata=metadata,
            cause=exc,
        )

    if isinstance(exc, OffsetOutOfRangeError):
        return ConsumerOffsetOutOfRangeException(
            "Consumer offset out of range",
            fatal=True,
            metadata=metadata,
            cause=exc,
        )

    if isinstance(exc, GroupAuthorizationFailedError):
        return ConsumerAuthorizationException(
            "Consumer group authorization failed",
            fatal=True,
            metadata=metadata,
            cause=exc,
        )

    if isinstance(exc, CoordinatorNotAvailableError):
        return ConsumerCoordinatorException(
            "Group coordinator unavailable",
            retryable=True,
            metadata=metadata,
            cause=exc,
        )

    if isinstance(exc, KafkaError):
        return KafkaConsumerException(
            "Unhandled Kafka consumer error",
            retryable=True,
            metadata=metadata,
            cause=exc,
        )

    return BaseKafkaException(
        "Unexpected consumer exception",
        fatal=True,
        metadata=metadata,
        cause=exc,
    )