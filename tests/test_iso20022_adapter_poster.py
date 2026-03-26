from decimal import Decimal
import importlib

import pytest

from src.domain.Iso20022AdapterPoster import Iso20022AdapterPoster
from src.domain.Iso20022MessageMapper import Iso20022MessageMapper

iso20022_adapter_poster_module = importlib.import_module("src.domain.Iso20022AdapterPoster")


def _valid_payload() -> dict[str, object]:
    return {
        "transfer_id": "PTX-0000001",
        "transfer_type": "DOMESTIC",
        "transaction_datetime": "2026-03-03T10:15:30+00:00",
        "requested_execution_date": "2026-03-04",
        "amount": Decimal("2500.00"),
        "currency": "AED",
        "purpose_code": "SUPP",
        "charge_bearer": "SHAR",
        "debtor_name": "Sharjah Trading LLC",
        "debtor_country": "AE",
        "debtor_account_scheme": "IBAN",
        "debtor_account_id": "AE070331234567890123456",
        "debtor_bank_id_scheme": "BIC",
        "debtor_bank_id": "SIBUAEAD",
        "creditor_name": "Desert Supplies FZC",
        "creditor_country": "AE",
        "creditor_account_scheme": "IBAN",
        "creditor_account_id": "AE170540123456789012345",
        "creditor_bank_id_scheme": "LOCAL",
        "creditor_bank_id": "EBILAEAD",
        "remittance_unstructured": "Invoice 7843 - office supplies",
        "remittance_reference": "INV-7843",
    }


def test_build_pain_001_maps_core_fields():
    xml_message = Iso20022MessageMapper.build_pain_001(_valid_payload())

    assert "pain.001.001.03" in xml_message
    assert "<MsgId>PTX-0000001</MsgId>" in xml_message
    assert '<InstdAmt Ccy="AED">2500.00</InstdAmt>' in xml_message
    assert "<IBAN>AE070331234567890123456</IBAN>" in xml_message
    assert "<MmbId>EBILAEAD</MmbId>" in xml_message
    assert "<Cd>SUPP</Cd>" in xml_message
    assert "<Ref>INV-7843</Ref>" in xml_message


def test_parse_pacs_002_returns_normalized_result():
    response_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.002.001.10">
      <FIToFIPmtStsRpt>
        <GrpHdr>
          <MsgId>RESP-123</MsgId>
        </GrpHdr>
        <TxInfAndSts>
          <TxSts>ACSP</TxSts>
        </TxInfAndSts>
      </FIToFIPmtStsRpt>
    </Document>
    """

    result = Iso20022MessageMapper.parse_pacs_002(response_xml)

    assert result.success is True
    assert result.status_code == "ACSP"
    assert result.message_id == "RESP-123"
    assert result.as_dict()["transaction_status"] == "ACSP"


def test_parse_pacs_002_accepts_escaped_xml_string_payload():
    response_xml = r'"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pain.002.001.03\"><CstmrPmtStsRpt><GrpHdr><MsgId>RESP-20260326-0001</MsgId><CreDtTm>2026-03-26T16:30:00Z</CreDtTm><InitgPty><Nm>Sample Bank</Nm></InitgPty></GrpHdr><OrgnlGrpInfAndSts><OrgnlMsgId>PAYTRACE-20260326-0001</OrgnlMsgId><OrgnlMsgNmId>pain.001.001.03</OrgnlMsgNmId><GrpSts>ACCP</GrpSts></OrgnlGrpInfAndSts><OrgnlPmtInfAndSts><OrgnlPmtInfId>PMT-INF-001</OrgnlPmtInfId><PmtInfSts>ACCP</PmtInfSts><TxInfAndSts><OrgnlInstrId>TXN-10001</OrgnlInstrId><OrgnlEndToEndId>E2E-10001</OrgnlEndToEndId><TxSts>ACSP</TxSts><AccptncDtTm>2026-03-26T16:30:05Z</AccptncDtTm></TxInfAndSts></OrgnlPmtInfAndSts></CstmrPmtStsRpt></Document>"'

    result = Iso20022MessageMapper.parse_pacs_002(response_xml)

    assert result.success is True
    assert result.status_code == "ACSP"
    assert result.message_id == "RESP-20260326-0001"
    assert result.raw_message.startswith("<?xml version=\"1.0\" encoding=\"UTF-8\"?>")


def test_post_domestic_posts_xml_and_parses_response(monkeypatch):
    captured: dict[str, object] = {}
    response_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.002.001.10">
      <FIToFIPmtStsRpt>
        <TxInfAndSts>
          <TxSts>ACTC</TxSts>
        </TxInfAndSts>
      </FIToFIPmtStsRpt>
    </Document>
    """

    class DummyResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def read(self) -> bytes:
            return response_xml.encode("utf-8")

    def fake_urlopen(http_request, timeout):
        captured["url"] = http_request.full_url
        captured["timeout"] = timeout
        captured["body"] = http_request.data.decode("utf-8")
        captured["headers"] = dict(http_request.header_items())
        return DummyResponse()

    monkeypatch.setattr(
        iso20022_adapter_poster_module.ConfigLoader,
        "get",
        lambda key, default=None: {
            "OFTL_HTTPURL_ISO20022ADAP": "http://localhost:8082/iso20022",
            "OFTL_HTTPURL_ISO20022TIMEOUT": "7",
        }.get(key, default),
    )
    monkeypatch.setattr(iso20022_adapter_poster_module.request, "urlopen", fake_urlopen)

    success, status_code, status_description, adapter_response = Iso20022AdapterPoster.post_domestic(
        _valid_payload(),
        correlation_id="corr-123",
    )

    assert success is True
    assert status_code == "ACTC"
    assert status_description == "ACTC"
    assert adapter_response["status_code"] == "ACTC"
    assert captured["url"] == "http://localhost:8082/iso20022"
    assert captured["timeout"] == 7.0
    assert "application/xml" in captured["headers"]["Content-type"]
    assert captured["headers"]["X-correlation-id"] == "corr-123"
    assert "<Document" in captured["body"]


def test_post_domestic_requires_adapter_url(monkeypatch):
    monkeypatch.setattr(iso20022_adapter_poster_module.ConfigLoader, "get", lambda key, default=None: default)

    with pytest.raises(RuntimeError, match="Missing ISO20022 adapter URL"):
        Iso20022AdapterPoster.post_domestic(_valid_payload(), correlation_id="corr-123")
