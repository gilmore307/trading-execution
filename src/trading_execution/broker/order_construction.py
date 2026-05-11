"""Gated broker order-intent construction.

This module is the first formal order-construction boundary. It can construct a
broker-shaped order intent after an explicit construction approval and a valid
``trade_risk_cap``. It does not submit the order, call broker APIs, inspect or
mutate accounts, or activate production model configs.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, Mapping, Sequence

from trading_execution.risk_cap import validate_trade_risk_cap

ORDER_CONSTRUCTION_APPROVAL_CONTRACT = "execution_order_construction_approval_v1"
ORDER_CONSTRUCTION_SCOPE = "broker_order_construction_only"
SUPPORTED_BROKERS = ("okx",)
SUPPORTED_ORDER_TYPES = ("market", "limit")
SUPPORTED_SIDES = ("buy", "sell")


@dataclass(frozen=True)
class OrderConstructionApprovalValidation:
    """Validation result for order-construction approval."""

    contract_type: str
    approval_id: str | None
    valid: bool
    missing_fields: tuple[str, ...]
    invalid_fields: tuple[str, ...]
    unapproved_instruments: tuple[str, ...]
    expired: bool
    construct_order_allowed: bool
    broker_execution_allowed: bool
    account_mutation_allowed: bool

    def summary_row(self) -> dict[str, Any]:
        row = asdict(self)
        row["missing_fields"] = list(self.missing_fields)
        row["invalid_fields"] = list(self.invalid_fields)
        row["unapproved_instruments"] = list(self.unapproved_instruments)
        return row


@dataclass(frozen=True)
class BrokerOrderIntent:
    """Broker-shaped order intent that has not been submitted."""

    contract_type: str
    order_intent_id: str
    approval_id: str
    decision_record_id: str
    broker_id: str
    instrument_ref: str
    side: str
    order_type: str
    quantity: str
    limit_price: str | None
    idempotency_key: str
    broker_order_payload: Mapping[str, Any]
    risk_cap_validation: Mapping[str, Any]
    intent_status: str
    broker_order_construction_performed: bool
    broker_calls_performed: int
    account_mutation_performed: bool
    production_decision_activation_performed: bool

    def summary_row(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        return [str(item) for item in value]
    return []


def _stable_id(prefix: str, payload: Mapping[str, Any]) -> str:
    digest = sha256(repr(sorted(payload.items())).encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def validate_order_construction_approval(
    approval: Mapping[str, Any],
    *,
    decision_record: Mapping[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Validate approval before constructing an order intent."""

    required = (
        "contract_type",
        "approval_id",
        "approval_scope",
        "broker_id",
        "approved_instrument_refs",
        "approved_sides",
        "approved_order_types",
        "approved_at_utc",
        "expires_at_utc",
    )
    missing = [field for field in required if approval.get(field) in (None, "", [], {})]
    invalid: list[str] = []
    if approval.get("contract_type") != ORDER_CONSTRUCTION_APPROVAL_CONTRACT:
        invalid.append("contract_type")
    if approval.get("approval_scope") != ORDER_CONSTRUCTION_SCOPE:
        invalid.append("approval_scope")
    if approval.get("broker_id") not in SUPPORTED_BROKERS:
        invalid.append("broker_id")

    broker_execution_allowed = bool(approval.get("broker_execution_allowed"))
    account_mutation_allowed = bool(approval.get("account_mutation_allowed"))
    construct_order_allowed = bool(approval.get("construct_order_allowed"))
    if not construct_order_allowed:
        invalid.append("construct_order_allowed")
    if broker_execution_allowed:
        invalid.append("broker_execution_allowed_must_be_false")
    if account_mutation_allowed:
        invalid.append("account_mutation_allowed_must_be_false")

    expires_at = _parse_time(approval.get("expires_at_utc"))
    if expires_at is None:
        invalid.append("expires_at_utc")
        expired = True
    else:
        expired = (now or _now()) > expires_at

    unapproved_instruments: list[str] = []
    if decision_record:
        instrument = str(decision_record.get("instrument_ref") or "").strip()
        approved_instruments = set(_list(approval.get("approved_instrument_refs")))
        if instrument and "*" not in approved_instruments and instrument not in approved_instruments:
            unapproved_instruments.append(instrument)
        if decision_record.get("broker_id") and decision_record.get("broker_id") != approval.get("broker_id"):
            invalid.append("decision_broker_id_not_approved")
        side = str(decision_record.get("side") or "").strip()
        if side and side not in _list(approval.get("approved_sides")):
            invalid.append("decision_side_not_approved")
        order_type = str(decision_record.get("order_type") or "").strip()
        if order_type and order_type not in _list(approval.get("approved_order_types")):
            invalid.append("decision_order_type_not_approved")

    valid = not missing and not invalid and not unapproved_instruments and not expired
    return OrderConstructionApprovalValidation(
        contract_type="execution_order_construction_approval_validation_v1",
        approval_id=str(approval.get("approval_id")) if approval.get("approval_id") else None,
        valid=valid,
        missing_fields=tuple(missing),
        invalid_fields=tuple(sorted(set(invalid))),
        unapproved_instruments=tuple(unapproved_instruments),
        expired=expired,
        construct_order_allowed=construct_order_allowed,
        broker_execution_allowed=broker_execution_allowed,
        account_mutation_allowed=account_mutation_allowed,
    ).summary_row()


def _required_decision_fields(decision_record: Mapping[str, Any]) -> list[str]:
    required = ("decision_record_id", "broker_id", "instrument_ref", "side", "order_type", "quantity")
    return [field for field in required if decision_record.get(field) in (None, "", [], {})]


def _okx_payload(decision_record: Mapping[str, Any]) -> dict[str, Any]:
    order_type = str(decision_record.get("order_type"))
    payload: dict[str, Any] = {
        "instId": str(decision_record.get("instrument_ref")),
        "tdMode": str(decision_record.get("td_mode") or "cash"),
        "side": str(decision_record.get("side")),
        "ordType": order_type,
        "sz": str(decision_record.get("quantity")),
    }
    if order_type == "limit":
        payload["px"] = str(decision_record.get("limit_price"))
    return payload


def build_broker_order_intent(
    decision_record: Mapping[str, Any],
    *,
    approval: Mapping[str, Any],
    construct_order: bool = False,
) -> dict[str, Any]:
    """Construct a broker order intent after approval and risk-cap validation."""

    approval_validation = validate_order_construction_approval(approval, decision_record=decision_record)
    risk_validation = validate_trade_risk_cap(decision_record)
    missing_decision_fields = _required_decision_fields(decision_record)
    side = str(decision_record.get("side") or "")
    order_type = str(decision_record.get("order_type") or "")
    invalid_decision_fields: list[str] = []
    if side and side not in SUPPORTED_SIDES:
        invalid_decision_fields.append("side")
    if order_type and order_type not in SUPPORTED_ORDER_TYPES:
        invalid_decision_fields.append("order_type")
    if order_type == "limit" and not decision_record.get("limit_price"):
        invalid_decision_fields.append("limit_price_required_for_limit_order")

    ready = approval_validation["valid"] and risk_validation["valid"] and not missing_decision_fields and not invalid_decision_fields
    if not construct_order or not ready:
        return {
            "contract_type": "execution_broker_order_intent_result_v1",
            "approval_validation": approval_validation,
            "risk_cap_validation": risk_validation,
            "missing_decision_fields": missing_decision_fields,
            "invalid_decision_fields": invalid_decision_fields,
            "order_intent": None,
            "order_construction_status": "ready_requires_construct_order_flag" if ready else "blocked_order_construction_validation_failed",
            "broker_order_construction_performed": False,
            "broker_calls_performed": 0,
            "account_mutation_performed": False,
            "production_decision_activation_performed": False,
        }

    broker_id = str(decision_record.get("broker_id"))
    if broker_id != "okx":
        raise ValueError("only okx order-intent construction is currently supported")
    payload = _okx_payload(decision_record)
    idempotency_key = str(
        decision_record.get("idempotency_key")
        or _stable_id(
            "idem",
            {
                "decision_record_id": decision_record.get("decision_record_id"),
                "broker_id": broker_id,
                "instrument_ref": decision_record.get("instrument_ref"),
                "side": decision_record.get("side"),
                "quantity": decision_record.get("quantity"),
                "order_type": decision_record.get("order_type"),
                "limit_price": decision_record.get("limit_price"),
            },
        )
    )
    intent_id = _stable_id("ordintent", {"idempotency_key": idempotency_key, "approval_id": approval.get("approval_id")})
    intent = BrokerOrderIntent(
        contract_type="execution_broker_order_intent_v1",
        order_intent_id=intent_id,
        approval_id=str(approval.get("approval_id")),
        decision_record_id=str(decision_record.get("decision_record_id")),
        broker_id=broker_id,
        instrument_ref=str(decision_record.get("instrument_ref")),
        side=str(decision_record.get("side")),
        order_type=str(decision_record.get("order_type")),
        quantity=str(decision_record.get("quantity")),
        limit_price=str(decision_record.get("limit_price")) if decision_record.get("limit_price") is not None else None,
        idempotency_key=idempotency_key,
        broker_order_payload=payload,
        risk_cap_validation=risk_validation,
        intent_status="constructed_not_submitted",
        broker_order_construction_performed=True,
        broker_calls_performed=0,
        account_mutation_performed=False,
        production_decision_activation_performed=False,
    ).summary_row()
    return {
        "contract_type": "execution_broker_order_intent_result_v1",
        "approval_validation": approval_validation,
        "risk_cap_validation": risk_validation,
        "missing_decision_fields": [],
        "invalid_decision_fields": [],
        "order_intent": intent,
        "order_construction_status": "constructed_not_submitted",
        "broker_order_construction_performed": True,
        "broker_calls_performed": 0,
        "account_mutation_performed": False,
        "production_decision_activation_performed": False,
    }


__all__ = [
    "ORDER_CONSTRUCTION_APPROVAL_CONTRACT",
    "ORDER_CONSTRUCTION_SCOPE",
    "BrokerOrderIntent",
    "OrderConstructionApprovalValidation",
    "build_broker_order_intent",
    "validate_order_construction_approval",
]
