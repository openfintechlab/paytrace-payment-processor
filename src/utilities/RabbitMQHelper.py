# -*- coding: utf-8 -*-
"""
RabbitMQ utility with persistent listener support for payment request queues.
"""

from __future__ import annotations

import atexit
import json
import os
import time
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from threading import Event, Lock, Thread
from typing import Any, Callable

from .ConfigLoader import ConfigLoader
from .Logging import Logging

try:
    import pika
except Exception:  # pragma: no cover
    pika = None


class RabbitMQConnectionError(RuntimeError):
    """Raised when RabbitMQ connection retries are exhausted."""


class RabbitMQHelper:
    """Manage a persistent RabbitMQ consumer for domestic and cross-border queues."""

    _connection = None
    _channel = None
    _listener_thread: Thread | None = None
    _stop_event: Event = Event()
    _lock: Lock = Lock()
    _message_handler: Callable[[Any, Any, Any, bytes], Any] | None = None

    @classmethod
    def _require_pika(cls) -> Any:
        if pika is None:
            raise RuntimeError("RabbitMQ client is unavailable. Install dependency: pika")
        return pika

    @classmethod
    def _as_bool(cls, value: Any, default: bool) -> bool:
        if value is None:
            return default
        return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}

    @classmethod
    def _build_connection_parameters(cls) -> Any:
        pika_module = cls._require_pika()

        host = str(ConfigLoader.get("OFTL_RABITMQ_HOST", "localhost"))
        port = int(ConfigLoader.get("OFTL_RABITMQ_PORT", 5672))
        username = str(ConfigLoader.get("OFTL_RABITMQ_USERNAME", "guest"))
        password = str(ConfigLoader.get("OFTL_RABITMQ_PASSWORD_SECRET", "guest"))
        virtual_host = str(ConfigLoader.get("OFTL_RABITMQ_VHOST", "/"))
        heartbeat = int(ConfigLoader.get("OFTL_RABITMQ_HEARTBEAT", 60))
        blocked_timeout = float(ConfigLoader.get("OFTL_RABITMQ_BLOCKED_CONNECTION_TIMEOUT", 30))
        connection_attempts = int(ConfigLoader.get("OFTL_RABITMQ_CONNECTION_ATTEMPTS", 3))
        retry_delay = float(ConfigLoader.get("OFTL_RABITMQ_RETRY_DELAY", 2))
        socket_timeout = float(ConfigLoader.get("OFTL_RABITMQ_SOCKET_TIMEOUT", 5))

        credentials = pika_module.PlainCredentials(username=username, password=password)
        return pika_module.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=virtual_host,
            heartbeat=heartbeat,
            blocked_connection_timeout=blocked_timeout,
            connection_attempts=connection_attempts,
            retry_delay=retry_delay,
            socket_timeout=socket_timeout,
            credentials=credentials,
        )

    @classmethod
    def _queue_names(cls) -> tuple[str, str]:
        domestic_queue = str(
            ConfigLoader.get(
                "OFTL_RABITMQ_DOEMSTIC_REQUEST_QUEUE",
                "CSV.PAYMENTS.DOMESTIC.REQ",
            )
        )
        cross_border_queue = str(
            ConfigLoader.get(
                "OFTL_RABITMQ_CROSS_BORDER_REQUEST_QUEUE",
                "CSV.PAYMENTS.CROSS_BORDER.REQ",
            )
        )
        return domestic_queue, cross_border_queue

    @classmethod
    def _connection_retry_count(cls) -> int:
        retry_count = int(ConfigLoader.get("OFTL_RABITMQ_CONN_RETRYCOUNT", 3))
        return max(retry_count, 1)

    @classmethod
    def _connect_consumer_with_retry(cls) -> Any:
        retry_count = cls._connection_retry_count()
        retry_delay = float(ConfigLoader.get("OFTL_RABITMQ_RETRY_DELAY", 2))
        last_error: Exception | None = None

        for attempt in range(1, retry_count + 1):
            try:
                cls._connect_consumer()
                return cls._register_consumers()
            except Exception as exc:
                last_error = exc
                cls._cleanup_connection()
                Logging.error(
                    "RabbitMQ connection attempt %s of %s failed: %s",
                    attempt,
                    retry_count,
                    exc,
                )
                if attempt < retry_count:
                    time.sleep(retry_delay)

        raise RabbitMQConnectionError(
            f"Unable to connect to RabbitMQ after {retry_count} attempts."
        ) from last_error

    @classmethod
    def _exit_application(cls) -> None:
        Logging.error("RabbitMQ connection failed after configured retries. Exiting application with code 99.")
        os._exit(99)

    @classmethod
    def initialize_connection(
        cls,
        message_handler: Callable[[Any, Any, Any, bytes], Any] | None = None,
    ) -> None:
        """Validate RabbitMQ client availability and connection configuration."""
        cls._message_handler = message_handler
        cls._require_pika()
        cls._build_connection_parameters()
        cls._connect_consumer_with_retry()
        cls._cleanup_connection()

    @classmethod
    def start_listener(cls) -> None:
        """Start a background listener for payment request queues."""
        with cls._lock:
            if cls._listener_thread and cls._listener_thread.is_alive():
                Logging.info("RabbitMQ listener already running.")
                return

            cls._stop_event.clear()
            cls._listener_thread = Thread(
                target=cls._consume_forever,
                name="rabbitmq-payment-listener",
                daemon=True,
            )
            cls._listener_thread.start()
            Logging.info("RabbitMQ listener thread started.")

    @classmethod
    def _consume_forever(cls) -> None:
        while not cls._stop_event.is_set():
            try:
                channel = cls._connect_consumer_with_retry()
                Logging.info("RabbitMQ listener is consuming payment request queues.")
                channel.start_consuming()
            except RabbitMQConnectionError:
                if cls._stop_event.is_set():
                    break
                cls._exit_application()
            except Exception as exc:
                if cls._stop_event.is_set():
                    break
                Logging.error("RabbitMQ listener error: %s", exc)
                cls._cleanup_connection()
            finally:
                cls._cleanup_connection()

    @classmethod
    def _connect_consumer(cls) -> None:
        pika_module = cls._require_pika()
        params = cls._build_connection_parameters()
        cls._connection = pika_module.BlockingConnection(params)
        cls._channel = cls._connection.channel()
        Logging.info("RabbitMQ connection established for payment listener.")

    @classmethod
    def _register_consumers(cls) -> Any:
        channel = cls._channel
        if channel is None:  # pragma: no cover
            raise RuntimeError("RabbitMQ channel is not initialized.")

        queue_durable = cls._as_bool(ConfigLoader.get("OFTL_RABITMQ_QUEUE_DURABLE", "true"), True)
        prefetch_count = int(ConfigLoader.get("OFTL_RABITMQ_PREFETCH_COUNT", 1))
        domestic_queue, cross_border_queue = cls._queue_names()

        channel.basic_qos(prefetch_count=prefetch_count)
        for queue_name in (domestic_queue, cross_border_queue):
            channel.queue_declare(queue=queue_name, durable=queue_durable)
            channel.basic_consume(
                queue=queue_name,
                on_message_callback=cls._on_message,
                auto_ack=cls._as_bool(ConfigLoader.get("OFTL_RABITMQ_AUTOACK", "true"), True),
            )
            Logging.info("RabbitMQ listener bound to queue: %s", queue_name)
        return channel

    @classmethod
    def publish_message(
        cls,
        exchange_name: str,
        routing_key: str,
        message: Any,
        exchange_type: str | None = None,
        *,
        correlation_id: str | None = None,
        message_id: str | None = None,
        headers: dict[str, Any] | None = None,
    ) -> bool:
        """Publish a message to an exchange and routing key."""
        pika_module = cls._require_pika()
        resolved_exchange_type = exchange_type or str(ConfigLoader.get("OFTL_RABITMQ_EXCHANGE_TYPE", "direct"))
        durable_exchange = cls._as_bool(ConfigLoader.get("OFTL_RABITMQ_EXCHANGE_DURABLE", "true"), True)
        persistent_message = cls._as_bool(ConfigLoader.get("OFTL_RABITMQ_MESSAGE_PERSISTENT", "true"), True)

        close_after_publish = False
        if cls._channel is None or getattr(cls._channel, "is_closed", False):
            cls._connect_consumer()
            close_after_publish = True

        if cls._channel is None:  # pragma: no cover
            raise RuntimeError("RabbitMQ channel is not initialized.")

        try:
            cls._channel.exchange_declare(
                exchange=exchange_name,
                exchange_type=resolved_exchange_type,
                durable=durable_exchange,
            )
            payload, content_type = cls._build_payload(message)
            properties = pika_module.BasicProperties(
                content_type=content_type,
                content_encoding="utf-8",
                delivery_mode=2 if persistent_message else 1,
                correlation_id=correlation_id,
                message_id=message_id,
                headers=headers,
            )
            return bool(
                cls._channel.basic_publish(
                    exchange=exchange_name,
                    routing_key=routing_key,
                    body=payload,
                    mandatory=cls._as_bool(ConfigLoader.get("OFTL_RABITMQ_PUBLISH_MANDATORY", "false"), False),
                    properties=properties,
                )
            )
        finally:
            if close_after_publish:
                cls._cleanup_connection()

    @classmethod
    def _build_payload(cls, message: Any) -> tuple[bytes, str]:
        if isinstance(message, bytes):
            return message, "application/octet-stream"
        if isinstance(message, str):
            return message.encode("utf-8"), "text/plain"
        return (
            json.dumps(
                message,
                separators=(",", ":"),
                ensure_ascii=False,
                default=cls._json_default,
            ).encode("utf-8"),
            "application/json",
        )

    @staticmethod
    def _json_default(value: Any) -> Any:
        if is_dataclass(value) and not isinstance(value, type):
            return asdict(value)
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, Decimal):
            return str(value)
        raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")

    @classmethod
    def _on_message(cls, channel: Any, method: Any, properties: Any, body: bytes) -> None:
        queue_name = getattr(method, "routing_key", "unknown")
        correlation_id = getattr(properties, "correlation_id", None)

        try:
            payload = cls._decode_body(body)
            Logging.info("Received RabbitMQ request from queue=%s correlation_id=%s",queue_name,correlation_id or "N/A")
            if cls._message_handler is not None:
                Logging.info(
                    "Calling RabbitMQ message handler for queue=%s correlation_id=%s",
                    queue_name,
                    correlation_id or "N/A",
                )
                cls._message_handler(channel, method, properties, body)
                Logging.info(
                    "Completed RabbitMQ message handler for queue=%s correlation_id=%s",
                    queue_name,
                    correlation_id or "N/A",
                )
            if not cls._as_bool(ConfigLoader.get("OFTL_RABITMQ_AUTOACK", "true"), True):
                Logging.info("Acknowledging RabbitMQ message for queue=%s correlation_id=%s", queue_name, correlation_id or "N/A")
                channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as exc:
            Logging.error(
                "Failed to process RabbitMQ payment request from queue=%s: %s",
                queue_name,
                exc,
            )
            if not cls._as_bool(ConfigLoader.get("OFTL_RABITMQ_AUTOACK", "true"), True):
                Logging.info("NACKing RabbitMQ message for queue=%s correlation_id=%s with requeue=True", queue_name, correlation_id or "N/A")
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            

    @classmethod
    def _decode_body(cls, body: bytes) -> Any:
        text = body.decode("utf-8")
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text

    @classmethod
    def stop_listener(cls) -> None:
        """Stop the background RabbitMQ listener."""
        cls._stop_event.set()

        if cls._channel is not None:
            try:
                cls._channel.stop_consuming()
            except Exception:
                pass

        if cls._listener_thread is not None and cls._listener_thread.is_alive():
            cls._listener_thread.join(timeout=5)

        cls._listener_thread = None
        cls._cleanup_connection()

    @classmethod
    def _cleanup_connection(cls) -> None:
        if cls._channel is not None:
            try:
                cls._channel.close()
            except Exception:
                pass

        if cls._connection is not None:
            try:
                cls._connection.close()
            except Exception:
                pass

        cls._channel = None
        cls._connection = None


atexit.register(RabbitMQHelper.stop_listener)
