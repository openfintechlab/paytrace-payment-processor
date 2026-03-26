# -*- coding: utf-8 -*-
"""ISO 20022 message mapping helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any
import xml.etree.ElementTree as ET

try:
    from utilities.Logging import Logging
except ImportError:  # pragma: no cover - package import fallback
    from src.utilities.Logging import Logging



@dataclass(frozen=True)
class Iso20022AdapterResult:
    """Normalized pacs.002 response details."""

    success: bool
    status_code: str
    status_description: str
    raw_message: str
    message_id: str | None = None
    transaction_status: str | None = None
    reason_code: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "status_code": self.status_code,
            "status_description": self.status_description,
            "raw_message": self.raw_message,
            "message_id": self.message_id,
            "transaction_status": self.transaction_status,
            "reason_code": self.reason_code,
        }


class Iso20022MessageMapper:
    """Build pain.001 requests and parse pacs.002 responses."""

    _PAIN_NAMESPACE = "urn:iso:std:iso:20022:tech:xsd:pain.001.001.03"
    _ACCEPTED_STATUSES = {"ACTC", "ACCP", "ACFC", "ACSP", "ACSC", "ACWC", "RCVD"}

    @classmethod
    def build_pain_001(cls, payload: dict[str, Any]) -> str:
        ET.register_namespace("", cls._PAIN_NAMESPACE)
        document = ET.Element(cls._tag("Document"))
        customer_credit_transfer = ET.SubElement(document, cls._tag("CstmrCdtTrfInitn"))

        cls._append_group_header(customer_credit_transfer, payload)
        cls._append_payment_information(customer_credit_transfer, payload)

        return ET.tostring(document, encoding="unicode", xml_declaration=True)

    @classmethod
    def parse_pacs_002(cls, xml_message: str) -> Iso20022AdapterResult:
        try:
            normalized_message, root = cls._parse_xml_root(xml_message)
        except ET.ParseError as exc:
            Logging.error("Failed to parse ISO20022 adapter response as XML: %s", exc)
            raise ValueError("Adapter response is not valid XML.") from exc

        message_id = cls._find_text(root, "MsgId")
        transaction_status = (
            cls._find_text(root, "TxSts")
            or cls._find_text(root, "GrpSts")
            or cls._find_text(root, "PmtInfSts")
            or "UNKNOWN"
        )
        reason_code = cls._find_text(root, "Cd")
        additional_info = cls._find_text(root, "AddtlInf")

        description_parts = [part for part in (reason_code, additional_info) if part]
        status_description = " - ".join(description_parts) if description_parts else transaction_status

        return Iso20022AdapterResult(
            success=transaction_status in cls._ACCEPTED_STATUSES,
            status_code=transaction_status,
            status_description=status_description,
            raw_message=normalized_message,
            message_id=message_id,
            transaction_status=transaction_status,
            reason_code=reason_code,
        )

    @classmethod
    def _parse_xml_root(cls, xml_message: str) -> tuple[str, ET.Element]:
        parse_errors: list[ET.ParseError] = []

        for candidate in cls._xml_parse_candidates(xml_message):
            try:
                return candidate, ET.fromstring(candidate)
            except ET.ParseError as exc:
                parse_errors.append(exc)

        raise parse_errors[-1]

    @classmethod
    def _xml_parse_candidates(cls, xml_message: str) -> list[str]:
        text = xml_message.strip()
        candidates: list[str] = [text]

        if text.startswith('"') and text.endswith('"'):
            try:
                decoded = json.loads(text)
            except json.JSONDecodeError:
                decoded = None
            if isinstance(decoded, str):
                candidates.append(decoded.strip())

        if '\\"' in text:
            candidates.append(text.replace('\\"', '"'))

        unique_candidates: list[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            if candidate and candidate not in seen:
                unique_candidates.append(candidate)
                seen.add(candidate)
        return unique_candidates

    @classmethod
    def _append_group_header(cls, parent: ET.Element, payload: dict[str, Any]) -> None:
        group_header = ET.SubElement(parent, cls._tag("GrpHdr"))
        cls._append_text(group_header, "MsgId", str(payload["transfer_id"]))
        cls._append_text(group_header, "CreDtTm", cls._normalize_datetime(payload["transaction_datetime"]))
        cls._append_text(group_header, "NbOfTxs", "1")
        cls._append_text(group_header, "CtrlSum", cls._format_amount(payload["amount"]))

        initiating_party = ET.SubElement(group_header, cls._tag("InitgPty"))
        cls._append_text(initiating_party, "Nm", str(payload["debtor_name"]))

    @classmethod
    def _append_payment_information(cls, parent: ET.Element, payload: dict[str, Any]) -> None:
        payment_information = ET.SubElement(parent, cls._tag("PmtInf"))
        cls._append_text(payment_information, "PmtInfId", str(payload["transfer_id"]))
        cls._append_text(payment_information, "PmtMtd", "TRF")
        cls._append_text(payment_information, "BtchBookg", "false")
        cls._append_text(payment_information, "NbOfTxs", "1")
        cls._append_text(payment_information, "CtrlSum", cls._format_amount(payload["amount"]))
        cls._append_text(
            payment_information,
            "ReqdExctnDt",
            cls._normalize_date(payload["requested_execution_date"]),
        )

        debtor = ET.SubElement(payment_information, cls._tag("Dbtr"))
        cls._append_text(debtor, "Nm", str(payload["debtor_name"]))
        cls._append_country(debtor, str(payload["debtor_country"]))

        debtor_account = ET.SubElement(payment_information, cls._tag("DbtrAcct"))
        cls._append_account_identifier(
            debtor_account,
            scheme=str(payload["debtor_account_scheme"]),
            identifier=str(payload["debtor_account_id"]),
        )

        debtor_agent = ET.SubElement(payment_information, cls._tag("DbtrAgt"))
        cls._append_agent_identifier(
            debtor_agent,
            scheme=str(payload["debtor_bank_id_scheme"]),
            identifier=str(payload["debtor_bank_id"]),
        )

        if payload.get("charge_bearer"):
            cls._append_text(payment_information, "ChrgBr", str(payload["charge_bearer"]))

        credit_transfer = ET.SubElement(payment_information, cls._tag("CdtTrfTxInf"))
        payment_identification = ET.SubElement(credit_transfer, cls._tag("PmtId"))
        cls._append_text(payment_identification, "InstrId", str(payload["transfer_id"]))
        cls._append_text(payment_identification, "EndToEndId", str(payload["transfer_id"]))

        amount = ET.SubElement(credit_transfer, cls._tag("Amt"))
        instructed_amount = ET.SubElement(
            amount,
            cls._tag("InstdAmt"),
            {"Ccy": str(payload["currency"])},
        )
        instructed_amount.text = cls._format_amount(payload["amount"])

        creditor_agent = ET.SubElement(credit_transfer, cls._tag("CdtrAgt"))
        cls._append_agent_identifier(
            creditor_agent,
            scheme=str(payload["creditor_bank_id_scheme"]),
            identifier=str(payload["creditor_bank_id"]),
        )

        creditor = ET.SubElement(credit_transfer, cls._tag("Cdtr"))
        cls._append_text(creditor, "Nm", str(payload["creditor_name"]))
        cls._append_country(creditor, str(payload["creditor_country"]))

        creditor_account = ET.SubElement(credit_transfer, cls._tag("CdtrAcct"))
        cls._append_account_identifier(
            creditor_account,
            scheme=str(payload["creditor_account_scheme"]),
            identifier=str(payload["creditor_account_id"]),
        )

        if payload.get("purpose_code"):
            purpose = ET.SubElement(credit_transfer, cls._tag("Purp"))
            cls._append_text(purpose, "Cd", str(payload["purpose_code"]))

        if payload.get("intermediary_bank_bic"):
            intermediary_agent = ET.SubElement(credit_transfer, cls._tag("IntrmyAgt1"))
            cls._append_agent_identifier(
                intermediary_agent,
                scheme="BIC",
                identifier=str(payload["intermediary_bank_bic"]),
            )

        cls._append_remittance_information(credit_transfer, payload)

    @classmethod
    def _append_remittance_information(cls, parent: ET.Element, payload: dict[str, Any]) -> None:
        remittance_unstructured = payload.get("remittance_unstructured")
        remittance_reference = payload.get("remittance_reference")
        if not remittance_unstructured and not remittance_reference:
            return

        remittance = ET.SubElement(parent, cls._tag("RmtInf"))
        if remittance_unstructured:
            cls._append_text(remittance, "Ustrd", str(remittance_unstructured))

        if remittance_reference:
            structured = ET.SubElement(remittance, cls._tag("Strd"))
            creditor_reference_info = ET.SubElement(structured, cls._tag("CdtrRefInf"))
            creditor_reference_type = ET.SubElement(creditor_reference_info, cls._tag("Tp"))
            code_or_proprietary = ET.SubElement(creditor_reference_type, cls._tag("CdOrPrtry"))
            cls._append_text(code_or_proprietary, "Cd", "SCOR")
            cls._append_text(creditor_reference_info, "Ref", str(remittance_reference))

    @classmethod
    def _append_account_identifier(cls, parent: ET.Element, *, scheme: str, identifier: str) -> None:
        account_id = ET.SubElement(parent, cls._tag("Id"))
        if scheme == "IBAN":
            cls._append_text(account_id, "IBAN", identifier)
            return

        other = ET.SubElement(account_id, cls._tag("Othr"))
        cls._append_text(other, "Id", identifier)

    @classmethod
    def _append_agent_identifier(cls, parent: ET.Element, *, scheme: str, identifier: str) -> None:
        institution = ET.SubElement(parent, cls._tag("FinInstnId"))
        if scheme == "BIC":
            cls._append_text(institution, "BICFI", identifier)
            return

        clearing_system_member = ET.SubElement(institution, cls._tag("ClrSysMmbId"))
        cls._append_text(clearing_system_member, "MmbId", identifier)

    @classmethod
    def _append_country(cls, parent: ET.Element, country_code: str) -> None:
        postal_address = ET.SubElement(parent, cls._tag("PstlAdr"))
        cls._append_text(postal_address, "Ctry", country_code)

    @classmethod
    def _append_text(cls, parent: ET.Element, tag_name: str, value: str) -> ET.Element:
        element = ET.SubElement(parent, cls._tag(tag_name))
        element.text = value
        return element

    @classmethod
    def _normalize_datetime(cls, value: Any) -> str:
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value)

    @classmethod
    def _normalize_date(cls, value: Any) -> str:
        if isinstance(value, date):
            return value.isoformat()
        return str(value)

    @classmethod
    def _format_amount(cls, value: Any) -> str:
        amount = value if isinstance(value, Decimal) else Decimal(str(value))
        return f"{amount:.2f}"

    @classmethod
    def _find_text(cls, root: ET.Element, local_name: str) -> str | None:
        for element in root.iter():
            if cls._local_name(element.tag) == local_name and element.text:
                return element.text.strip()
        return None

    @classmethod
    def _local_name(cls, tag_name: str) -> str:
        return tag_name.rsplit("}", 1)[-1]

    @classmethod
    def _tag(cls, local_name: str) -> str:
        return f"{{{cls._PAIN_NAMESPACE}}}{local_name}"
