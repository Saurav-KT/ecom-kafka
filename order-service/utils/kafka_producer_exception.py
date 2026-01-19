from aiokafka.errors import (
    KafkaError,

    # Connection & network
    KafkaConnectionError,
    KafkaTimeoutError,
    RequestTimedOutError,
    BrokerNotAvailableError,

   # Topic / partition
    UnknownTopicOrPartitionError,
    NotLeaderForPartitionError,
    LeaderNotAvailableError,

    # Authorization
    TopicAuthorizationFailedError,
    GroupAuthorizationFailedError,
    ClusterAuthorizationFailedError,


    # Message / request
    RecordTooLargeError,
    InvalidTopicError,

    # Protocol / state
    IllegalStateError,
    UnsupportedVersionError,


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
# Producer Exceptions
# =====================================================


class KafkaProducerException(BaseKafkaException):
    """Base class for Producer-related exceptions"""

class ProducerBrokerException(KafkaProducerException):
    pass

class ProducerTimeoutException(KafkaProducerException):
    pass


class ProducerConnectionException(KafkaProducerException):
    pass


class ProducerAuthorizationException(KafkaProducerException):
    pass


class ProducerLeaderNotAvailableException(KafkaProducerException):
    pass


class ProducerUnknownTopicException(KafkaProducerException):
    pass


# =====================================================
# Exception Mappers
# =====================================================

def map_producer_exception(
    exc: Exception,
    *,
    metadata: Optional[Dict[str, Any]] = None
) -> BaseKafkaException:
    """
    Convert aiokafka producer exceptions to application exceptions
    """

    if isinstance(exc, (KafkaTimeoutError, RequestTimedOutError)):
        return ProducerTimeoutException(
            "Kafka producer request timed out",
            retryable=True,
            metadata=metadata,
            cause=exc,
        )

    if isinstance(exc, BrokerNotAvailableError):
        return ProducerBrokerException(message="Kafka broker not available",
                                       retryable=True,
                                       metadata=metadata,
                                       cause=exc,
                                       )

    if isinstance(exc, KafkaConnectionError):
        return ProducerConnectionException(
            "Kafka producer connection failed",
            retryable=True,
            metadata=metadata,
            cause=exc,
        )

    if isinstance(exc, TopicAuthorizationFailedError):
        return ProducerAuthorizationException(
            "Producer not authorized for topic",
            fatal=True,
            metadata=metadata,
            cause=exc,
        )

    if isinstance(exc, NotLeaderForPartitionError):
        return ProducerLeaderNotAvailableException(
            "Partition leader not available",
            retryable=True,
            metadata=metadata,
            cause=exc,
        )

    if isinstance(exc, UnknownTopicOrPartitionError):
        return ProducerUnknownTopicException(
            "Unknown topic or partition",
            fatal=True,
            metadata=metadata,
            cause=exc,
        )

    if isinstance(exc, KafkaError):
        return KafkaProducerException(
            "Unhandled Kafka producer error",
            retryable=True,
            metadata=metadata,
            cause=exc,
        )

    return BaseKafkaException(
        "Unexpected producer exception",
        fatal=True,
        metadata=metadata,
        cause=exc,
    )