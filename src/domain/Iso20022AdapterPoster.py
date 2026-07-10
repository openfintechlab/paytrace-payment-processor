# -*- coding: utf-8 -*-
"""Posting helpers for forwarding payment requests to the ISO 20022 adapter."""

from __future__ import annotations

from urllib.parse import urljoin
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
    _REDIRECT_STATUS_CODES = {307, 308}

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
        request_headers = cls._request_headers(
            transfer_id=transfer_id,
            correlation_id=correlation_id,
        )
        request_body = xml_payload.encode("utf-8")

        try:
            with cls._open_request(
                endpoint=endpoint,
                request_body=request_body,
                request_headers=request_headers,
                timeout_seconds=timeout_seconds,
                transfer_id=transfer_id,
                correlation_id=correlation_id,
            ) as response:
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
    def _open_request(
        cls,
        *,
        endpoint: str,
        request_body: bytes,
        request_headers: dict[str, str],
        timeout_seconds: float,
        transfer_id: str,
        correlation_id: str,
    ):
        current_endpoint = endpoint

        for redirect_count in range(2):
            http_request = request.Request(
                current_endpoint,
                data=request_body,
                headers=request_headers,
                method="POST",
            )
            try:
                return request.urlopen(http_request, timeout=timeout_seconds)
            except error.HTTPError as exc:
                if exc.code not in cls._REDIRECT_STATUS_CODES:
                    raise

                redirected_endpoint = cls._redirect_location(exc, current_endpoint)
                if not redirected_endpoint:
                    raise

                Logging.info(
                    "Following ISO20022 adapter redirect transfer_id=%s correlation_id=%s status=%s location=%s",
                    transfer_id,
                    correlation_id,
                    exc.code,
                    redirected_endpoint,
                )
                current_endpoint = redirected_endpoint

        raise RuntimeError(
            f"ISO20022 adapter redirect loop detected for endpoint {endpoint}"
        )

    @classmethod
    def _redirect_location(cls, http_error: error.HTTPError, current_endpoint: str) -> str:
        location = http_error.headers.get("Location")
        if not location:
            return ""
        return urljoin(current_endpoint, location.strip())

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

    @classmethod
    def _request_headers(
        cls,
        *,
        transfer_id: str,
        correlation_id: str,
    ) -> dict[str, str]:
        content_type = cls._config_value("OFTL_HTTP_CONTENT_TYPE") or "application/xml"
        authorization = cls._config_value("OFTL_HTTP_AUTHORIZATION_SECRET")
        accept_language = cls._config_value("OFTL_HTTP_ACCEPT_LANGUAGE")
        transaction_id = cls._config_value("OFTL_HTTP_X_TRANSACTION_ID") or transfer_id
        outbound_correlation_id = cls._config_value("OFTL_HTTP_X_CORRELATION_ID") or correlation_id
        idempotency_key = cls._config_value("OFTL_HTTP_IDEMPOTENCY_KEY") or transfer_id

        headers = {
            "Content-Type": content_type,
            "Accept": "application/xml",
            "x-transaction-id": transaction_id,
            "x-correlation-id": outbound_correlation_id,
            "idempotency-key": idempotency_key,
        }
        if authorization:
            headers["Authorization"] = authorization
        if accept_language:
            headers["Accept-Language"] = accept_language
        return headers

    @classmethod
    def _config_value(cls, key: str) -> str:
        raw_value = ConfigLoader.get(key)
        return str(raw_value).strip() if raw_value is not None else ""
