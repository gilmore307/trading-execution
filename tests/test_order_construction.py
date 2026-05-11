from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from trading_execution.broker import build_broker_order_intent, validate_order_construction_approval


def _decision_record() -> dict[str, object]:
    return {
        "decision_record_id": "decision_order_unit",
        "broker_id": "okx",
        "instrument_ref": "BTC-USDT",
        "side": "buy",
        "order_type": "limit",
        "quantity": "0.01",
        "limit_price": "65000",
        "trade_risk_cap": {
            "max_loss_usd": 25.0,
            "max_loss_pct": 0.01,
            "time_stop_at": "2026-05-11T20:00:00+00:00",
            "cap_enforcement_mode": "broker_native_stop",
            "cap_failure_action": "reject_order",
            "model_invalidation_price": 64000.0,
            "hard_stop_price": 63900.0,
        },
    }


def _approval() -> dict[str, object]:
    return {
        "contract_type": "execution_order_construction_approval_v1",
        "approval_id": "ordapproval_unit",
        "approval_scope": "broker_order_construction_only",
        "broker_id": "okx",
        "approved_instrument_refs": ["BTC-USDT"],
        "approved_sides": ["buy"],
        "approved_order_types": ["limit"],
        "approved_at_utc": "2026-05-11T13:00:00+00:00",
        "expires_at_utc": "2099-05-11T14:00:00+00:00",
        "construct_order_allowed": True,
        "broker_execution_allowed": False,
        "account_mutation_allowed": False,
    }


class OrderConstructionTests(unittest.TestCase):
    def test_approval_rejects_broker_execution_flag(self) -> None:
        approval = _approval()
        approval["broker_execution_allowed"] = True
        validation = validate_order_construction_approval(approval, decision_record=_decision_record())

        self.assertFalse(validation["valid"])
        self.assertIn("broker_execution_allowed_must_be_false", validation["invalid_fields"])

    def test_plan_only_does_not_construct_order(self) -> None:
        result = build_broker_order_intent(_decision_record(), approval=_approval(), construct_order=False)

        self.assertEqual(result["order_construction_status"], "ready_requires_construct_order_flag")
        self.assertFalse(result["broker_order_construction_performed"])
        self.assertEqual(result["broker_calls_performed"], 0)

    def test_constructs_okx_order_intent_without_submission(self) -> None:
        result = build_broker_order_intent(_decision_record(), approval=_approval(), construct_order=True)

        self.assertEqual(result["order_construction_status"], "constructed_not_submitted")
        self.assertTrue(result["broker_order_construction_performed"])
        self.assertEqual(result["broker_calls_performed"], 0)
        self.assertFalse(result["account_mutation_performed"])
        intent = result["order_intent"]
        self.assertEqual(intent["contract_type"], "execution_broker_order_intent_v1")
        self.assertEqual(intent["intent_status"], "constructed_not_submitted")
        self.assertEqual(intent["broker_order_payload"]["instId"], "BTC-USDT")
        self.assertEqual(intent["broker_order_payload"]["ordType"], "limit")
        self.assertEqual(intent["broker_order_payload"]["px"], "65000")

    def test_invalid_risk_cap_blocks_construction(self) -> None:
        decision = _decision_record()
        decision.pop("trade_risk_cap")
        result = build_broker_order_intent(decision, approval=_approval(), construct_order=True)

        self.assertEqual(result["order_construction_status"], "blocked_order_construction_validation_failed")
        self.assertFalse(result["broker_order_construction_performed"])
        self.assertIn("missing_trade_risk_cap", result["risk_cap_validation"]["reason_codes"])

    def test_cli_constructs_order_intent(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            decision_path = Path(temp_dir) / "decision.json"
            approval_path = Path(temp_dir) / "approval.json"
            decision_path.write_text(json.dumps(_decision_record()), encoding="utf-8")
            approval_path.write_text(json.dumps(_approval()), encoding="utf-8")
            result = subprocess.run(
                [
                    sys.executable,
                    "scripts/execution/build_broker_order_intent.py",
                    "--decision-record",
                    str(decision_path),
                    "--approval",
                    str(approval_path),
                    "--construct-order",
                ],
                check=True,
                cwd="/root/projects/trading-execution",
                env={"PYTHONPATH": "src"},
                text=True,
                capture_output=True,
            )
        payload = json.loads(result.stdout)
        self.assertEqual(payload["order_construction_status"], "constructed_not_submitted")


if __name__ == "__main__":
    unittest.main()
