import importlib
from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.domain.PaymentRequestHandler import PaymentRequestHandler, PaymentRequestProcessingError
@pytest.fixture(autouse=True)
def reset_handler_caches(monkeypatch):
    PaymentRequestHandler._load_schema.cache_clear()
    PaymentRequestHandler._validator.cache_clear()
    monkeypatch.setattr(
        payment_request_handler_module.RabbitMQHelper,
        "publish_message",
        lambda *args, **kwargs: True,
    )
    yield
    PaymentRequestHandler._load_schema.cache_clear()
    PaymentRequestHandler._validator.cache_clear()


payment_request_handler_module = importlib.import_module("src.domain.PaymentRequestHandler")


def _valid_payload() -> dict[str, object]:
    return {
        "transfer_id": "tx-123",
        "transfer_type": "DOMESTIC",
        "transaction_datetime": "2026-03-19T10:15:30Z",
        "requested_execution_date": "2026-03-20",
        "amount": Decimal("10.50"),
        "currency": "USD",
        "debtor_name": "John Debtor",
        "debtor_country": "US",
        "debtor_account_scheme": "IBAN",
        "debtor_account_id": "US00TEST00000000000000000000000001",
        "debtor_bank_id_scheme": "BIC",
        "debtor_bank_id": "DEBTUSB1",
        "creditor_name": "Jane Creditor",
        "creditor_country": "US",
        "creditor_account_scheme": "IBAN",
        "creditor_account_id": "US00TEST00000000000000000000000002",
        "creditor_bank_id_scheme": "BIC",
        "creditor_bank_id": "CREDUSB1",
    }


def test_handle_message_marks_processed_for_valid_message(monkeypatch):
    calls: list[tuple[str, dict[str, object]]] = []
    post_calls: list[tuple[str, dict[str, object]]] = []
    published_events: list[dict[str, object]] = []
    method = SimpleNamespace(routing_key="CSV.PAYMENTS.DOMESTIC.REQ")
    properties = SimpleNamespace(correlation_id="corr-1")

    monkeypatch.setattr(
        payment_request_handler_module.DBHelper,
        "execute_update",
        lambda query, params=None: calls.append((query, params or {})) or 1,
    )
    monkeypatch.setattr(
        payment_request_handler_module.Iso20022AdapterPoster,
        "post_domestic",
        lambda payload, *, correlation_id: post_calls.append(
            ("domestic", {"payload": payload, "correlation_id": correlation_id})
        ) or (
            True,
            "ACTC",
            "Accepted",
            {"success": True, "status_code": "ACTC", "status_description": "Accepted"},
        ),
    )
    monkeypatch.setattr(
        payment_request_handler_module.RabbitMQHelper,
        "publish_message",
        lambda exchange_name, routing_key, message, **kwargs: published_events.append(
            {
                "exchange_name": exchange_name,
                "routing_key": routing_key,
                "message": message,
                "kwargs": kwargs,
            }
        )
        or True,
    )

    result = PaymentRequestHandler.handle_message(
        channel=None,
        method=method,
        properties=properties,
        body=(
            b'{"transfer_id":"tx-123","transfer_type":"DOMESTIC","transaction_datetime":"2026-03-19T10:15:30Z",'
            b'"requested_execution_date":"2026-03-20","amount":10.50,"currency":"USD","debtor_name":"John Debtor",'
            b'"debtor_country":"US","debtor_account_scheme":"IBAN","debtor_account_id":"US00TEST00000000000000000000000001",'
            b'"debtor_bank_id_scheme":"BIC","debtor_bank_id":"DEBTUSB1","creditor_name":"Jane Creditor",'
            b'"creditor_country":"US","creditor_account_scheme":"IBAN","creditor_account_id":"US00TEST00000000000000000000000002",'
            b'"creditor_bank_id_scheme":"BIC","creditor_bank_id":"CREDUSB1"}'
        ),
    )

    payload = result["payload"]
    assert payload["transfer_id"] == "tx-123"
    assert result["adapter_response"]["status_code"] == "ACTC"
    assert post_calls == [("domestic", {"payload": payload, "correlation_id": "corr-1"})]
    assert len(calls) == 1
    assert calls[0][1]["transfer_id"] == "tx-123"
    assert calls[0][1]["status"] == "processed"
    assert calls[0][1]["request_queue"] == "CSV.PAYMENTS.DOMESTIC.REQ"
    assert calls[0][1]["error_message"] is None
    assert len(published_events) == 1
    event = published_events[0]["message"]
    assert published_events[0]["exchange_name"] == "paytrace.events"
    assert published_events[0]["routing_key"] == "payment.row.processed"
    assert event["event_code"] == "EV003"
    assert event["event_type"] == "payment.row.processed"
    assert event["source"] == "paytrace-payment-processor"
    assert event["correlation_id"] == "corr-1"
    assert event["causation_id"] == "tx-123"
    assert event["payload"]["processing_status"] == "processed"
    assert event["payload"]["message_payload"] == payload
    assert event["payload"]["adapter_response"]["status_code"] == "ACTC"
    assert published_events[0]["kwargs"]["headers"]["event_code"] == "EV003"


def test_handle_message_marks_failed_for_invalid_message(monkeypatch):
    calls: list[dict[str, object]] = []
    method = SimpleNamespace(routing_key="CSV.PAYMENTS.CROSS_BORDER.REQ")
    properties = SimpleNamespace(correlation_id="corr-2")

    monkeypatch.setattr(
        payment_request_handler_module.DBHelper,
        "execute_update",
        lambda query, params=None: calls.append(params or {}) or 1,
    )

    with pytest.raises(PaymentRequestProcessingError):
        PaymentRequestHandler.handle_message(
            channel=None,
            method=method,
            properties=properties,
            body=b'{"transfer_id":"tx-999","transfer_type":"DOMESTIC"}',
        )

    assert len(calls) == 1
    assert calls[0]["transfer_id"] == "tx-999"
    assert calls[0]["status"] == "failed"
    assert calls[0]["request_queue"] == "CSV.PAYMENTS.CROSS_BORDER.REQ"
    assert "required property" in str(calls[0]["error_message"])


def test_handle_message_marks_failed_for_non_json_message(monkeypatch):
    calls: list[dict[str, object]] = []
    method = SimpleNamespace(routing_key="CSV.PAYMENTS.DOMESTIC.REQ")
    properties = SimpleNamespace(correlation_id="corr-3")

    monkeypatch.setattr(
        payment_request_handler_module.DBHelper,
        "execute_update",
        lambda query, params=None: calls.append(params or {}) or 1,
    )

    with pytest.raises(PaymentRequestProcessingError, match="valid JSON"):
        PaymentRequestHandler.handle_message(
            channel=None,
            method=method,
            properties=properties,
            body=b"not-json",
        )

    assert calls[0]["transfer_id"] == "unknown"
    assert calls[0]["status"] == "failed"


def test_handle_message_accepts_string_numbers_and_optional_nulls(monkeypatch):
    calls: list[tuple[str, dict[str, object]]] = []
    post_calls: list[tuple[str, dict[str, object]]] = []
    method = SimpleNamespace(routing_key="CSV.PAYMENTS.CROSS_BORDER.REQ")
    properties = SimpleNamespace(correlation_id="PTX-0000002")

    monkeypatch.setattr(
        payment_request_handler_module.DBHelper,
        "execute_update",
        lambda query, params=None: calls.append((query, params or {})) or 1,
    )
    monkeypatch.setattr(
        payment_request_handler_module.Iso20022AdapterPoster,
        "post_crossborder",
        lambda payload, *, correlation_id: post_calls.append(
            ("crossborder", {"payload": payload, "correlation_id": correlation_id})
        ) or (
            True,
            "ACSP",
            "Accepted settlement in process",
            {
                "success": True,
                "status_code": "ACSP",
                "status_description": "Accepted settlement in process",
            },
        ),
    )

    result = PaymentRequestHandler.handle_message(
        channel=None,
        method=method,
        properties=properties,
        body=(
            b'{"transfer_id":"PTX-0000002","transfer_type":"CROSS_BORDER","transaction_datetime":"2026-03-03T10:20:00+00:00",'
            b'"requested_execution_date":"2026-03-04","amount":"1200.00","currency":"USD","purpose_code":"INVC",'
            b'"charge_bearer":"SHAR","exchange_rate":"1.0000","debtor_name":"Sharjah Trading LLC","debtor_country":"AE",'
            b'"debtor_account_scheme":"IBAN","debtor_account_id":"AE070331234567890123456","debtor_bank_id_scheme":"BIC",'
            b'"debtor_bank_id":"SIBUAEAD","creditor_name":"Global Parts Ltd","creditor_country":"GB",'
            b'"creditor_account_scheme":"IBAN","creditor_account_id":"GB33BUKB20201555555555",'
            b'"creditor_bank_id_scheme":"BIC","creditor_bank_id":"BUKBGB22","intermediary_bank_bic":null,'
            b'"remittance_unstructured":"Invoice 9912 - spare parts","remittance_reference":"INV-9912"}'
        ),
    )

    payload = result["payload"]
    assert payload["amount"] == Decimal("1200.00")
    assert payload["exchange_rate"] == Decimal("1.0000")
    assert "intermediary_bank_bic" not in payload
    assert result["adapter_response"]["status_code"] == "ACSP"
    assert post_calls == [("crossborder", {"payload": payload, "correlation_id": "PTX-0000002"})]
    assert calls[0][1]["transfer_id"] == "PTX-0000002"
    assert calls[0][1]["status"] == "processed"


def test_handle_message_marks_failed_for_unsupported_queue(monkeypatch):
    calls: list[dict[str, object]] = []
    method = SimpleNamespace(routing_key="CSV.PAYMENTS.UNKNOWN.REQ")
    properties = SimpleNamespace(correlation_id="corr-4")

    monkeypatch.setattr(
        payment_request_handler_module.DBHelper,
        "execute_update",
        lambda query, params=None: calls.append(params or {}) or 1,
    )

    with pytest.raises(PaymentRequestProcessingError, match="Unsupported queue"):
        PaymentRequestHandler.handle_message(
            channel=None,
            method=method,
            properties=properties,
            body=(
                b'{"transfer_id":"tx-124","transfer_type":"DOMESTIC","transaction_datetime":"2026-03-19T10:15:30Z",'
                b'"requested_execution_date":"2026-03-20","amount":10.50,"currency":"USD","debtor_name":"John Debtor",'
                b'"debtor_country":"US","debtor_account_scheme":"IBAN","debtor_account_id":"US00TEST00000000000000000000000001",'
                b'"debtor_bank_id_scheme":"BIC","debtor_bank_id":"DEBTUSB1","creditor_name":"Jane Creditor",'
                b'"creditor_country":"US","creditor_account_scheme":"IBAN","creditor_account_id":"US00TEST00000000000000000000000002",'
                b'"creditor_bank_id_scheme":"BIC","creditor_bank_id":"CREDUSB1"}'
            ),
        )

    assert calls[0]["transfer_id"] == "tx-124"
    assert calls[0]["status"] == "failed"
    assert calls[0]["request_queue"] == "CSV.PAYMENTS.UNKNOWN.REQ"


def test_handle_message_marks_failed_when_domestic_posting_returns_false(monkeypatch):
    calls: list[dict[str, object]] = []
    published_events: list[dict[str, object]] = []
    method = SimpleNamespace(routing_key="CSV.PAYMENTS.DOMESTIC.REQ")
    properties = SimpleNamespace(correlation_id="corr-5")

    monkeypatch.setattr(
        payment_request_handler_module.DBHelper,
        "execute_update",
        lambda query, params=None: calls.append(params or {}) or 1,
    )
    monkeypatch.setattr(
        payment_request_handler_module.Iso20022AdapterPoster,
        "post_domestic",
        lambda payload, *, correlation_id: (
            False,
            "RJCT",
            "Adapter validation failed",
            {
                "success": False,
                "status_code": "RJCT",
                "status_description": "Adapter validation failed",
            },
        ),
    )
    monkeypatch.setattr(
        payment_request_handler_module.RabbitMQHelper,
        "publish_message",
        lambda exchange_name, routing_key, message, **kwargs: published_events.append(
            {
                "exchange_name": exchange_name,
                "routing_key": routing_key,
                "message": message,
                "kwargs": kwargs,
            }
        )
        or True,
    )

    with pytest.raises(
        PaymentRequestProcessingError,
        match="RJCT:Adapter validation failed",
    ):
        PaymentRequestHandler.handle_message(
            channel=None,
            method=method,
            properties=properties,
            body=(
                b'{"transfer_id":"tx-125","transfer_type":"DOMESTIC","transaction_datetime":"2026-03-19T10:15:30Z",'
                b'"requested_execution_date":"2026-03-20","amount":10.50,"currency":"USD","debtor_name":"John Debtor",'
                b'"debtor_country":"US","debtor_account_scheme":"IBAN","debtor_account_id":"US00TEST00000000000000000000000001",'
                b'"debtor_bank_id_scheme":"BIC","debtor_bank_id":"DEBTUSB1","creditor_name":"Jane Creditor",'
                b'"creditor_country":"US","creditor_account_scheme":"IBAN","creditor_account_id":"US00TEST00000000000000000000000002",'
                b'"creditor_bank_id_scheme":"BIC","creditor_bank_id":"CREDUSB1"}'
            ),
        )

    assert calls[0]["transfer_id"] == "tx-125"
    assert calls[0]["status"] == "failed"
    assert calls[0]["request_queue"] == "CSV.PAYMENTS.DOMESTIC.REQ"
    assert calls[0]["error_message"] == "RJCT:Adapter validation failed"
    assert len(published_events) == 1
    event = published_events[0]["message"]
    assert published_events[0]["exchange_name"] == "paytrace.events"
    assert published_events[0]["routing_key"] == "payment.row.processed"
    assert event["event_code"] == "EV003"
    assert event["payload"]["processing_status"] == "failed"
    assert event["payload"]["error_message"] == "RJCT:Adapter validation failed"
    assert event["payload"]["message_payload"]["transfer_id"] == "tx-125"
    assert event["payload"]["adapter_response"]["status_code"] == "RJCT"
