# -*- coding: utf-8 -*-
"""Payment request message handling for RabbitMQ consumers."""

from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping

from jsonschema import Draft202012Validator, FormatChecker

try:
    from utilities.DBHelper import DBHelper
    from utilities.Logging import Logging
except ImportError:  # pragma: no cover - package import fallback
    from src.utilities.DBHelper import DBHelper
    from src.utilities.Logging import Logging


class PaymentRequestProcessingError(ValueError):
    """Raised when a payment request cannot be parsed or validated."""


class PaymentRequestHandler:
    """Validate inbound payment messages and persist processing status."""

    _SCHEMA_PATH = Path(__file__).resolve().with_name("payment_instruction.schema.json")
    _STATUS_PROCESSED = "processed"
    _STATUS_FAILED = "failed"

    @classmethod
    def handle_message(cls, channel: Any, method: Any, properties: Any, body: bytes) -> dict[str, Any]:
        queue_name = getattr(method, "routing_key", "unknown")
        correlation_id = getattr(properties, "correlation_id", None) or "N/A"
        transfer_id = "unknown"

        try:
            payload = cls._build_payload(cls._decode_message(body))
            transfer_id = str(payload.get("transfer_id", "")).strip() or "unknown"
            cls._validate_payload(payload)
            # TODO: Add mapping logic to convert the message to ISO20022 and forward it to message adapter for further processing
            # END;
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
            return payload
        except Exception as exc:
            if transfer_id == "unknown":
                transfer_id = cls._extract_transfer_id(body)
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
                CASE WHEN :status = 'processed' THEN NOW() ELSE NULL END,
                NOW(),
                :error_message
            )
            ON CONFLICT (transfer_id) DO UPDATE
            SET
                request_queue = EXCLUDED.request_queue,
                status = EXCLUDED.status,
                published_at = CASE
                    WHEN EXCLUDED.status = 'processed' THEN COALESCE(oftl_fwcsv_row_dispatch.published_at, NOW())
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
