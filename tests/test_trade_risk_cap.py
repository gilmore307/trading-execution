from __future__ import annotations

import unittest

from trading_execution.risk_cap import validate_trade_risk_cap


class TradeRiskCapTests(unittest.TestCase):
    def test_missing_trade_risk_cap_rejects_order(self) -> None:
        result = validate_trade_risk_cap({"decision_record_id": "decision_001"})

        self.assertFalse(result["valid"])
        self.assertTrue(result["reject_order"])
        self.assertEqual(result["reason_codes"], ["missing_trade_risk_cap"])

    def test_direct_underlying_cap_requires_invalidation_and_stop(self) -> None:
        result = validate_trade_risk_cap(
            {
                "trade_risk_cap": {
                    "max_loss_usd": 125.0,
                    "max_loss_pct": 0.0125,
                    "time_stop_at": "2026-05-08T15:55:00-04:00",
                    "cap_enforcement_mode": "risk_monitor_synthetic_stop",
                    "cap_failure_action": "reject_order",
                    "model_invalidation_price": 178.6,
                    "hard_stop_price": 179.2,
                }
            }
        )

        self.assertTrue(result["valid"])
        self.assertFalse(result["reject_order"])
        self.assertEqual(result["reason_codes"], [])

    def test_invalid_cap_failure_action_rejects_order(self) -> None:
        result = validate_trade_risk_cap(
            {
                "trade_risk_cap": {
                    "max_loss_usd": 125.0,
                    "max_loss_pct": 0.0125,
                    "time_stop_at": "2026-05-08T15:55:00-04:00",
                    "cap_enforcement_mode": "broker_native_stop",
                    "cap_failure_action": "warn_only",
                    "model_invalidation_price": 178.6,
                    "hard_stop_price": 179.2,
                }
            }
        )

        self.assertFalse(result["valid"])
        self.assertIn("cap_failure_action_must_reject_order", result["reason_codes"])

    def test_long_option_premium_defined_risk_cap(self) -> None:
        result = validate_trade_risk_cap(
            {
                "trade_risk_cap": {
                    "max_loss_usd": 240.0,
                    "max_loss_pct": 0.006,
                    "planned_max_premium_at_risk_usd": 240.0,
                    "max_loss_is_premium_paid_flag": True,
                    "time_stop_at": "2026-05-08T15:55:00-04:00",
                    "cap_enforcement_mode": "long_option_premium_defined_risk",
                    "cap_failure_action": "reject_order",
                }
            }
        )

        self.assertTrue(result["valid"])
        self.assertEqual(result["cap_enforcement_mode"], "long_option_premium_defined_risk")

    def test_long_option_cap_requires_premium_paid_flag(self) -> None:
        result = validate_trade_risk_cap(
            {
                "trade_risk_cap": {
                    "max_loss_usd": 240.0,
                    "max_loss_pct": 0.006,
                    "time_stop_at": "2026-05-08T15:55:00-04:00",
                    "cap_enforcement_mode": "long_option_premium_defined_risk",
                    "cap_failure_action": "reject_order",
                }
            }
        )

        self.assertFalse(result["valid"])
        self.assertIn("premium_defined_risk_requires_premium_paid_flag", result["reason_codes"])


if __name__ == "__main__":
    unittest.main()
