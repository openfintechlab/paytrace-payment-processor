# -*- coding: utf-8 -*-
"""Posting helpers for forwarding payment requests to the ISO 20022 adapter."""

from __future__ import annotations

from urllib import error, request
from typing import Any

try:
    from domain.Iso20022MessageMapper import Iso20022AdapterResult, Iso20022MessageMapper
    from utilities.ConfigLoader import ConfigLoader
    from utilities.Logging import Logging
except ImportError:  # pragma: no cover - package import fallback
    from src.domain.Iso20022MessageMapper import Iso20022AdapterResult, Iso20022MessageMapper
    from src.utilities.ConfigLoader import ConfigLoader
    from src.utilities.Logging import Logging


class Iso20022AdapterPoster:
    """Encapsulate queue-specific posting behavior for ISO 20022 adapter messages."""

    _DEFAULT_TIMEOUT_SECONDS = 5.0    

    @classmethod
    def post_domestic(
        cls,
        payload: dict[str, Any],
        *,
        correlation_id: str,
    ) -> tuple[bool, str, str, dict[str, Any]]:
        transfer_id = str(payload.get("transfer_id", "")).strip() or "unknown"
        Logging.info(
            "Posting domestic payment message to ISO20022 adapter transfer_id=%s correlation_id=%s",
            transfer_id,
            correlation_id,
        )
        result = cls._post_payment(payload, correlation_id=correlation_id)
        return result.success, result.status_code, result.status_description, result.as_dict()

    @classmethod
    def post_crossborder(
        cls,
        payload: dict[str, Any],
        *,
        correlation_id: str,
    ) -> tuple[bool, str, str, dict[str, Any]]:
        transfer_id = str(payload.get("transfer_id", "")).strip() or "unknown"
        Logging.info(
            "Posting cross-border payment message to ISO20022 adapter transfer_id=%s correlation_id=%s",
            transfer_id,
            correlation_id,
        )
        result = cls._post_payment(payload, correlation_id=correlation_id)
        return result.success, result.status_code, result.status_description, result.as_dict()

    @classmethod
    def _post_payment(
        cls,
        payload: dict[str, Any],
        *,
        correlation_id: str,
    ) -> Iso20022AdapterResult:
        transfer_id = str(payload.get("transfer_id", "")).strip() or "unknown"
        endpoint = cls._adapter_url()
        timeout_seconds = cls._timeout_seconds()
        xml_payload = Iso20022MessageMapper.build_pain_001(payload)

        request_headers = {
            "Content-Type": "application/xml",
            "Accept": "application/xml",
            "X-Correlation-Id": correlation_id,
        }
        http_request = request.Request(
            endpoint,
            data=xml_payload.encode("utf-8"),
            headers=request_headers,
            method="POST",
        )

        try:
            with request.urlopen(http_request, timeout=timeout_seconds) as response:
                response_body = response.read().decode("utf-8")
        except error.HTTPError as exc:
            response_body = exc.read().decode("utf-8", errors="replace")
            Logging.error(
                "ISO20022 adapter HTTP error transfer_id=%s correlation_id=%s status=%s body=%s",
                transfer_id,
                correlation_id,
                exc.code,
                response_body,
            )
            parsed_error = cls._parse_response_or_fallback(
                response_body,
                fallback_code=f"HTTP_{exc.code}",
                fallback_description=response_body or str(exc),
            )
            return Iso20022AdapterResult(
                success=False,
                status_code=parsed_error.status_code,
                status_description=parsed_error.status_description,
                raw_message=parsed_error.raw_message,
                message_id=parsed_error.message_id,
                transaction_status=parsed_error.transaction_status,
                reason_code=parsed_error.reason_code,
            )
        except error.URLError as exc:
            raise RuntimeError(
                f"Unable to reach ISO20022 adapter endpoint {endpoint}: {exc.reason}"
            ) from exc

        Logging.info(
            "ISO20022 adapter response received transfer_id=%s correlation_id=%s",
            transfer_id,
            correlation_id,
        )
        return cls._parse_response_or_fallback(
            response_body,
            fallback_code="UNKNOWN",
            fallback_description="Adapter response could not be parsed",
        )

    @classmethod
    def _parse_response_or_fallback(
        cls,
        response_body: str,
        *,
        fallback_code: str,
        fallback_description: str,
    ) -> Iso20022AdapterResult:
        try:
            return Iso20022MessageMapper.parse_pacs_002(response_body)
        except ValueError:
            return Iso20022AdapterResult(
                success=False,
                status_code=fallback_code,
                status_description=fallback_description,
                raw_message=response_body,
            )

    @classmethod
    def _adapter_url(cls) -> str:
        raw_value = ConfigLoader.get("OFTL_HTTPURL_ISO20022ADAP")
        value = str(raw_value).strip() if raw_value is not None else ""
        if value:
            return value
        raise RuntimeError(
            "Missing ISO20022 adapter URL. Set OFTL_HTTPURL_ISO20022ADAP or OFTL_HTTPURL_ISO2022ADAP."
        )

    @classmethod
    def _timeout_seconds(cls) -> float:
        raw_timeout = ConfigLoader.get("OFTL_HTTPURL_ISO20022TIMEOUT", cls._DEFAULT_TIMEOUT_SECONDS)
        try:
            timeout = float(raw_timeout)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("OFTL_HTTPURL_ISO20022TIMEOUT must be a valid number of seconds.") from exc

        if timeout <= 0:
            raise RuntimeError("OFTL_HTTPURL_ISO20022TIMEOUT must be greater than zero.")
        return timeout
