from types import SimpleNamespace

import pytest

from src.utilities.ConfigLoader import ConfigLoader
from src.utilities.RabbitMQHelper import RabbitMQConnectionError, RabbitMQHelper


@pytest.fixture(autouse=True)
def reset_rabbitmq_helper_state(monkeypatch):
    monkeypatch.setattr(ConfigLoader._env, "read_env", lambda *args, **kwargs: None)
    ConfigLoader.configurations = {}
    RabbitMQHelper._connection = None
    RabbitMQHelper._channel = None
    RabbitMQHelper._listener_thread = None
    RabbitMQHelper._stop_event.clear()
    yield
    ConfigLoader.configurations = {}
    RabbitMQHelper._connection = None
    RabbitMQHelper._channel = None
    RabbitMQHelper._listener_thread = None
    RabbitMQHelper._stop_event.clear()


def test_queue_names_use_expected_defaults():
    queue_names = RabbitMQHelper._queue_names()

    assert queue_names == (
        "CSV.PAYMENTS.DOMESTIC.REQ",
        "CSV.PAYMENTS.CROSS_BORDER.REQ",
    )


def test_queue_names_use_environment_variables(monkeypatch):
    monkeypatch.setenv("OFTL_RABITMQ_DOEMSTIC_REQUEST_QUEUE", "custom.domestic.queue")
    monkeypatch.setenv("OFTL_RABITMQ_CROSS_BORDER_REQUEST_QUEUE", "custom.cross.border.queue")

    queue_names = RabbitMQHelper._queue_names()

    assert queue_names == ("custom.domestic.queue", "custom.cross.border.queue")


def test_on_message_acknowledges_json_payload(monkeypatch):
    monkeypatch.setenv("OFTL_RABITMQ_AUTOACK", "false")
    ack_calls: list[int] = []
    nack_calls: list[tuple[int, bool]] = []
    channel = SimpleNamespace(
        basic_ack=lambda delivery_tag: ack_calls.append(delivery_tag),
        basic_nack=lambda delivery_tag, requeue: nack_calls.append((delivery_tag, requeue)),
    )
    method = SimpleNamespace(delivery_tag=7, routing_key="CSV.PAYMENTS.DOMESTIC.REQ")
    properties = SimpleNamespace(correlation_id="abc-123")

    RabbitMQHelper._on_message(channel, method, properties, b'{"payment_id":"123"}')

    assert ack_calls == [7]
    assert nack_calls == []


def test_on_message_calls_registered_handler_before_ack(monkeypatch):
    monkeypatch.setenv("OFTL_RABITMQ_AUTOACK", "false")
    observed: list[tuple[object, object, object, bytes]] = []
    ack_calls: list[int] = []
    channel = SimpleNamespace(
        basic_ack=lambda delivery_tag: ack_calls.append(delivery_tag),
        basic_nack=lambda delivery_tag, requeue: None,
    )
    method = SimpleNamespace(delivery_tag=8, routing_key="CSV.PAYMENTS.DOMESTIC.REQ")
    properties = SimpleNamespace(correlation_id="def-456")

    RabbitMQHelper._message_handler = lambda ch, met, props, body: observed.append((ch, met, props, body))

    RabbitMQHelper._on_message(channel, method, properties, b'{"payment_id":"456"}')

    assert len(observed) == 1
    assert observed[0] == (channel, method, properties, b'{"payment_id":"456"}')
    assert ack_calls == [8]


def test_connection_retry_count_uses_default():
    assert RabbitMQHelper._connection_retry_count() == 3


def test_connect_consumer_with_retry_raises_after_configured_attempts(monkeypatch):
    monkeypatch.setenv("OFTL_RABITMQ_CONN_RETRYCOUNT", "2")
    attempts: list[int] = []

    def failing_connect():
        attempts.append(1)
        raise RuntimeError("auth failed")

    monkeypatch.setattr(RabbitMQHelper, "_connect_consumer", classmethod(lambda cls: failing_connect()))
    monkeypatch.setattr(RabbitMQHelper, "_register_consumers", classmethod(lambda cls: object()))
    monkeypatch.setattr(RabbitMQHelper, "_cleanup_connection", classmethod(lambda cls: None))
    monkeypatch.setattr("src.utilities.RabbitMQHelper.time.sleep", lambda _: None)

    with pytest.raises(RabbitMQConnectionError):
        RabbitMQHelper._connect_consumer_with_retry()

    assert len(attempts) == 2


def test_publish_message_declares_exchange_and_publishes_json(monkeypatch):
    calls: list[tuple[str, object]] = []

    class FakePikaModule:
        class PlainCredentials:
            def __init__(self, username, password):
                self.username = username
                self.password = password

        class ConnectionParameters:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        class BasicProperties:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

    class FakeChannel:
        is_closed = False

        def exchange_declare(self, exchange, exchange_type, durable):
            calls.append(("exchange_declare", (exchange, exchange_type, durable)))

        def basic_publish(self, **kwargs):
            calls.append(("basic_publish", kwargs))
            return True

    monkeypatch.setattr("src.utilities.RabbitMQHelper.pika", FakePikaModule)
    RabbitMQHelper._channel = FakeChannel()

    assert RabbitMQHelper.publish_message(
        "paytrace.events",
        "payment.row.processed",
        {"event_code": "EV003"},
        exchange_type="topic",
        correlation_id="corr-1",
        message_id="evt-1",
        headers={"event_code": "EV003"},
    ) is True

    assert calls[0] == ("exchange_declare", ("paytrace.events", "topic", True))
    publish_call = calls[1][1]
    assert publish_call["exchange"] == "paytrace.events"
    assert publish_call["routing_key"] == "payment.row.processed"
    assert publish_call["body"] == b'{"event_code":"EV003"}'
    assert publish_call["properties"].kwargs["correlation_id"] == "corr-1"
    assert publish_call["properties"].kwargs["message_id"] == "evt-1"
    assert publish_call["properties"].kwargs["headers"] == {"event_code": "EV003"}
