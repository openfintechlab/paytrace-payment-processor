# -*- coding: utf-8 -*-
"""Payment request message handling for RabbitMQ consumers."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping

from jsonschema import Draft202012Validator, FormatChecker

try:
    from domain.Iso20022AdapterPoster import Iso20022AdapterPoster
    from utilities.DBHelper import DBHelper
    from utilities.ConfigLoader import ConfigLoader
    from utilities.Logging import Logging
    from utilities.RabbitMQHelper import RabbitMQHelper
except ImportError:  # pragma: no cover - package import fallback
    from src.domain.Iso20022AdapterPoster import Iso20022AdapterPoster
    from src.utilities.ConfigLoader import ConfigLoader
    from src.utilities.DBHelper import DBHelper
    from src.utilities.Logging import Logging
    from src.utilities.RabbitMQHelper import RabbitMQHelper


class PaymentRequestProcessingError(ValueError):
    """Raised when a payment request cannot be parsed or validated."""


AdapterProcessingResult = tuple[bool, str, str, dict[str, Any]]


class PaymentRequestHandler:
    """Validate inbound payment messages and persist processing status."""

    _SCHEMA_PATH = Path(__file__).resolve().with_name("payment_instruction.schema.json")
    _STATUS_PROCESSED = "PROCESSED"
    _STATUS_FAILED = "FAILED"
    _DOMESTIC_QUEUE = "CSV.PAYMENTS.DOMESTIC.REQ"
    _CROSS_BORDER_QUEUE = "CSV.PAYMENTS.CROSS_BORDER.REQ"
    _EV003_CODE = "EV003"
    _EV003_DEFAULT_TOPIC = "payment.row.processed"
    _EVENT_VERSION = "1.0"
    _EVENT_SOURCE = "paytrace-payment-processor"
    _DEFAULT_EVENT_EXCHANGE = "paytrace.events"

    @classmethod
    def handle_message(cls, channel: Any, method: Any, properties: Any, body: bytes) -> dict[str, Any]:
        queue_name = getattr(method, "routing_key", "unknown")
        correlation_id = getattr(properties, "correlation_id", None) or "N/A"
        transfer_id = "unknown"
        adapter_response: dict[str, Any] | None = None
        event_emitted = False
        payload: dict[str, Any] = {}

        try:
            payload = cls._build_payload(cls._decode_message(body))
            transfer_id = str(payload.get("transfer_id", "")).strip() or "unknown"
            cls._validate_payload(payload)
            processing_status, status_code, status_description, adapter_response = cls._post_to_iso20022_adapter(
                queue_name=queue_name,
                payload=payload,
                correlation_id=correlation_id,
            )
            cls._safe_emit_payment_row_processed_event(
                message_payload=payload,
                processing_status=cls._STATUS_PROCESSED if processing_status else cls._STATUS_FAILED,
                correlation_id=correlation_id,
                causation_id=transfer_id,
                adapter_response=adapter_response,
                error_message=None if processing_status else cls._format_processing_error(status_code, status_description),
            )
            event_emitted = True

            if not processing_status:
                raise PaymentRequestProcessingError(
                    cls._format_processing_error(status_code, status_description)
                )
            cls._mark_status(
                transfer_id=transfer_id,
                status=cls._STATUS_PROCESSED,
                request_queue=queue_name,
                error_message=None,
            )
            Logging.info(
                "Payment request processed successfully transfer_id=%s queue=%s correlation_id=%s",
                transfer_id,
                queue_name,
                correlation_id,
            )
            return {
                "payload": payload,
                "adapter_response": adapter_response,
            }
        except Exception as exc:
            if transfer_id == "unknown":
                transfer_id = cls._extract_transfer_id(body)
            if not event_emitted:
                cls._safe_emit_payment_row_processed_event(
                    message_payload=payload or cls._raw_message_payload(body),
                    processing_status=cls._STATUS_FAILED,
                    correlation_id=correlation_id,
                    causation_id=transfer_id,
                    adapter_response=adapter_response,
                    error_message=str(exc),
                )
            cls._mark_status(
                transfer_id=transfer_id,
                status=cls._STATUS_FAILED,
                request_queue=queue_name,
                error_message=str(exc),
            )
            Logging.error(
                "Payment request processing failed transfer_id=%s queue=%s correlation_id=%s error=%s",
                transfer_id,
                queue_name,
                correlation_id,
                exc,
            )
            raise

    @classmethod
    def _safe_emit_payment_row_processed_event(
        cls,
        *,
        message_payload: dict[str, Any],
        processing_status: str,
        correlation_id: str,
        causation_id: str,
        adapter_response: dict[str, Any] | None,
        error_message: str | None,
    ) -> None:
        try:
            cls._emit_payment_row_processed_event(
                message_payload=message_payload,
                processing_status=processing_status,
                correlation_id=correlation_id,
                causation_id=causation_id,
                adapter_response=adapter_response,
                error_message=error_message,
            )
        except Exception as event_exc:
            Logging.error(
                "Failed to publish EV003 event transfer_id=%s correlation_id=%s error=%s",
                causation_id,
                correlation_id,
                event_exc,
            )

    @classmethod
    def _emit_payment_row_processed_event(
        cls,
        *,
        message_payload: dict[str, Any],
        processing_status: str,
        correlation_id: str,
        causation_id: str,
        adapter_response: dict[str, Any] | None,
        error_message: str | None,
    ) -> None:
        routing_key = str(ConfigLoader.get("OFTL_RABITMQ_PUBEVENT_EV003", cls._EV003_DEFAULT_TOPIC)).strip()
        if not routing_key:
            raise ValueError("OFTL_RABITMQ_PUBEVENT_EV003 must be configured for EV003 publishing.")

        exchange_name = str(ConfigLoader.get("OFTL_RABITMQ_SAGA_EXCHANGE", cls._DEFAULT_EVENT_EXCHANGE)).strip()
        if not exchange_name:
            raise ValueError("OFTL_RABITMQ_SAGA_EXCHANGE must be configured for EV003 publishing.")

        event_id = str(uuid.uuid4())
        resolved_correlation_id = str(correlation_id).strip() if correlation_id else str(uuid.uuid4())
        resolved_causation_id = str(causation_id).strip() if causation_id else resolved_correlation_id
        event = {
            "event_id": event_id,
            "event_code": cls._EV003_CODE,
            "event_type": routing_key,
            "event_version": cls._EVENT_VERSION,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": cls._EVENT_SOURCE,
            "correlation_id": resolved_correlation_id,
            "causation_id": resolved_causation_id,
            "payload": {
                "processing_status": processing_status,
                "message_payload": message_payload,
                "adapter_response": adapter_response or {},
                "error_message": error_message,
            },
        }

        RabbitMQHelper.publish_message(
            exchange_name,
            routing_key,
            event,
            exchange_type="topic",
            correlation_id=resolved_correlation_id,
            message_id=event_id,
            headers={
                "event_code": cls._EV003_CODE,
                "transfer_id": str(message_payload.get("transfer_id", "")),
                "processing_status": processing_status,
            },
        )
        Logging.info(
            "Event with ID: %s and code: %s published to topic: %s",
            event_id,
            cls._EV003_CODE,
            routing_key,
        )

    @classmethod
    def _raw_message_payload(cls, body: bytes) -> dict[str, Any]:
        try:
            decoded = cls._decode_message(body)
        except Exception:
            return {"raw_body": body.decode("utf-8", errors="replace")}
        return decoded

    @classmethod
    def _post_to_iso20022_adapter(
        cls,
        *,
        queue_name: str,
        payload: dict[str, Any],
        correlation_id: str,
    ) -> AdapterProcessingResult:
        transfer_id = str(payload.get("transfer_id", "")).strip() or "unknown"

        if queue_name == cls._DOMESTIC_QUEUE:
            Logging.info(
                "Processing domestic payment request transfer_id=%s correlation_id=%s",
                transfer_id,
                correlation_id,
            )
            return Iso20022AdapterPoster.post_domestic(payload, correlation_id=correlation_id)

        if queue_name == cls._CROSS_BORDER_QUEUE:
            Logging.info(
                "Processing cross-border payment request transfer_id=%s correlation_id=%s",
                transfer_id,
                correlation_id,
            )
            return Iso20022AdapterPoster.post_crossborder(payload, correlation_id=correlation_id)

        raise PaymentRequestProcessingError(
            f"Miss Configuration detected. Unsupported queue: {queue_name}"
        )

    @classmethod
    def _format_processing_error(cls, status_code: str, status_description: str) -> str:
        safe_status_code = str(status_code).strip() or "UNKNOWN"
        safe_status_description = str(status_description).strip() or "Unknown processing error"
        return f"{safe_status_code}:{safe_status_description}"

    @classmethod
    def _build_payload(cls, data: Mapping[str, Any]) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        schema_properties = cls._schema_properties()
        unknown_fields = sorted(set(data.keys()) - set(schema_properties.keys()))
        if unknown_fields:
            raise PaymentRequestProcessingError(
                f"Unexpected payment fields: {', '.join(unknown_fields)}"
            )

        for field_name, field_schema in schema_properties.items():
            if field_name not in data:
                continue
            coerced_value = cls._coerce_input_value(
                data[field_name],
                field_schema,
                field_name=field_name,
            )
            if coerced_value is not None:
                payload[field_name] = coerced_value

        return payload

    @classmethod
    def _decode_message(cls, body: bytes) -> dict[str, Any]:
        try:
            text = body.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise PaymentRequestProcessingError("Message body must be valid UTF-8.") from exc

        try:
            payload = json.loads(text, parse_float=Decimal)
        except json.JSONDecodeError as exc:
            raise PaymentRequestProcessingError("Message body must be valid JSON.") from exc

        if not isinstance(payload, dict):
            raise PaymentRequestProcessingError("Message body must be a JSON object.")

        return payload

    @classmethod
    def _coerce_input_value(
        cls,
        raw_value: Any,
        field_schema: Mapping[str, Any],
        *,
        field_name: str,
    ) -> Any:
        if raw_value is None:
            return None

        if isinstance(raw_value, str):
            raw_value = raw_value.strip()
            if raw_value == "":
                return None

        field_type = cls._non_null_type(field_schema)
        if field_type == "number":
            return cls._parse_decimal(raw_value, field_name=field_name)

        return raw_value

    @classmethod
    def _validate_payload(cls, payload: dict[str, Any]) -> None:
        errors = sorted(
            cls._validator().iter_errors(payload),
            key=lambda error: list(error.absolute_path),
        )
        if not errors:
            return

        messages: list[str] = []
        for error in errors:
            field_path = ".".join(str(part) for part in error.absolute_path) or "root"
            messages.append(f"{field_path}: {error.message}")

        raise PaymentRequestProcessingError("; ".join(messages))

    @classmethod
    def _mark_status(
        cls,
        *,
        transfer_id: str,
        status: str,
        request_queue: str,
        error_message: str | None,
    ) -> None:
        safe_transfer_id = transfer_id[:36] if transfer_id else "unknown"
        safe_queue = request_queue[:255] if request_queue else "unknown"
        truncated_error = error_message[:2000] if error_message else None

        DBHelper.execute_update(
            """
            INSERT INTO oftl_fwcsv_row_dispatch (
                transfer_id, file_id, row_number, request_queue, status, published_at, updated_at, error_message
            ) VALUES (
                :transfer_id,
                COALESCE(
                    (SELECT file_id FROM oftl_fwcsv_row_dispatch WHERE transfer_id = :transfer_id),
                    'payment-processor'
                ),
                COALESCE(
                    (SELECT row_number FROM oftl_fwcsv_row_dispatch WHERE transfer_id = :transfer_id),
                    0
                ),
                :request_queue,
                :status,
                CASE WHEN :status = 'PROCESSED' THEN NOW() ELSE NULL END,
                NOW(),
                :error_message
            )
            ON CONFLICT (transfer_id) DO UPDATE
            SET
                request_queue = EXCLUDED.request_queue,
                status = EXCLUDED.status,
                published_at = CASE
                    WHEN EXCLUDED.status = 'PROCESSED' THEN COALESCE(oftl_fwcsv_row_dispatch.published_at, NOW())
                    ELSE oftl_fwcsv_row_dispatch.published_at
                END,
                updated_at = NOW(),
                error_message = EXCLUDED.error_message
            """,
            {
                "transfer_id": safe_transfer_id,
                "request_queue": safe_queue,
                "status": status,
                "error_message": truncated_error,
            },
        )

    @classmethod
    def _extract_transfer_id(cls, body: bytes) -> str:
        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception:
            return "unknown"

        if not isinstance(payload, dict):
            return "unknown"

        transfer_id = str(payload.get("transfer_id", "")).strip()
        return transfer_id[:36] if transfer_id else "unknown"

    @classmethod
    def _parse_decimal(cls, value: Any, *, field_name: str) -> Decimal:
        if isinstance(value, Decimal):
            return value

        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise PaymentRequestProcessingError(f"{field_name} must be a valid number") from exc

    @classmethod
    def _schema_properties(cls) -> dict[str, Any]:
        return cls._load_schema()["properties"]

    @classmethod
    def _type_names(cls, field_schema: Mapping[str, Any]) -> tuple[str, ...]:
        field_type = field_schema.get("type")
        if isinstance(field_type, list):
            return tuple(str(type_name) for type_name in field_type)
        if isinstance(field_type, str):
            return (field_type,)
        return ()

    @classmethod
    def _non_null_type(cls, field_schema: Mapping[str, Any]) -> str | None:
        for type_name in cls._type_names(field_schema):
            if type_name != "null":
                return type_name
        return None

    @classmethod
    @lru_cache(maxsize=1)
    def _load_schema(cls) -> dict[str, Any]:
        with cls._SCHEMA_PATH.open("r", encoding="utf-8") as schema_file:
            return json.load(schema_file, parse_float=Decimal)

    @classmethod
    @lru_cache(maxsize=1)
    def _validator(cls) -> Draft202012Validator:
        return Draft202012Validator(cls._load_schema(), format_checker=FormatChecker())
