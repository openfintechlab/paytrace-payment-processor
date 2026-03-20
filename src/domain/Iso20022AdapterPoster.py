# -*- coding: utf-8 -*-
"""Posting helpers for forwarding payment requests to the ISO 20022 adapter."""

from __future__ import annotations

from typing import Any

try:
    from utilities.Logging import Logging
except ImportError:  # pragma: no cover - package import fallback
    from src.utilities.Logging import Logging


class Iso20022AdapterPoster:
    """Encapsulate queue-specific posting behavior for ISO 20022 adapter messages."""

    @classmethod
    def post_domestic(
        cls,
        payload: dict[str, Any],
        *,
        correlation_id: str,
    ) -> tuple[bool, str, str]:
        transfer_id = str(payload.get("transfer_id", "")).strip() or "unknown"
        Logging.info(
            "Posting domestic payment message to ISO20022 adapter transfer_id=%s correlation_id=%s",
            transfer_id,
            correlation_id,
        )
        # TODO: Map the domestic payload to ISO 20022 and send it to the adapter endpoint.
        return True, "SUCCESS", "Domestic payment message posted successfully"

    @classmethod
    def post_crossborder(
        cls,
        payload: dict[str, Any],
        *,
        correlation_id: str,
    ) -> tuple[bool, str, str]:
        transfer_id = str(payload.get("transfer_id", "")).strip() or "unknown"
        Logging.info(
            "Posting cross-border payment message to ISO20022 adapter transfer_id=%s correlation_id=%s",
            transfer_id,
            correlation_id,
        )
        # TODO: Map the cross-border payload to ISO 20022 and send it to the adapter endpoint.
        return True, "SUCCESS", "Cross-border payment message posted successfully"
