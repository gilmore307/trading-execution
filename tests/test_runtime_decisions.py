import unittest

from trading_execution.runtime import (
    CRYPTO_SPOT_ACCOUNT_SLEEVE,
    EQUITY_OPTIONS_ACCOUNT_SLEEVE,
    build_entry_decision,
    build_execution_order_intent,
    build_position_lifecycle_decision,
    build_target_allocation_snapshot,
    validate_entry_decision,
    validate_execution_order_intent,
    validate_position_lifecycle_decision,
    validate_target_allocation_snapshot,
)


VALID_STOCK_RISK_CAP = {
    "max_loss_usd": 125.0,
    "max_loss_pct": 0.02,
    "time_stop_at": "2026-01-05T20:00:00Z",
    "cap_enforcement_mode": "broker_native_stop",
    "cap_failure_action": "reject_order",
    "model_invalidation_price": 94.0,
    "hard_stop_price": 93.5,
    "planned_quantity": 3,
    "planned_limit_price": 101.25,
}


class RuntimeDecisionTests(unittest.TestCase):
    def test_crypto_allocation_uses_fixed_three_asset_pool(self) -> None:
        snapshot = build_target_allocation_snapshot(
            account_sleeve_id=CRYPTO_SPOT_ACCOUNT_SLEEVE,
            market_universe={"targets": [{"symbol": "BTC"}, {"symbol": "DOGE"}]},
            generated_at_utc="2026-01-01T00:00:00Z",
        )

        self.assertEqual(snapshot["contract_type"], "target_allocation_snapshot")
        self.assertEqual(snapshot["account_sleeve_id"], CRYPTO_SPOT_ACCOUNT_SLEEVE)
        self.assertEqual([row["target_ref"] for row in snapshot["selected_targets"]], ["BTC"])
        self.assertEqual(snapshot["blocked_targets"], [{"target_ref": "DOGE", "reason_codes": ["outside_fixed_crypto_candidate_pool"]}])
        self.assertEqual(validate_target_allocation_snapshot(snapshot)["validation_status"], "passed")
        self.assertEqual(snapshot["safety"]["broker_calls_performed"], 0)

    def test_equity_options_entry_can_open_option_when_allocated(self) -> None:
        allocation = build_target_allocation_snapshot(
            account_sleeve_id=EQUITY_OPTIONS_ACCOUNT_SLEEVE,
            target_context_rows=[
                {"target_ref": "AAPL", "instrument_ref": "AAPL", "asset_class": "us_equity"},
            ],
            generated_at_utc="2026-01-01T00:00:00Z",
        )

        decision = build_entry_decision(
            target_allocation_snapshot=allocation,
            target_ref="AAPL",
            alpha_confidence_vector={"alpha_confidence_score": 0.80},
            event_failure_risk_vector={"risk_level": "low"},
            dynamic_risk_policy_state={"minimum_entry_alpha_confidence": 0.55},
            option_expression_plan={"preferred_expression": "long_call", "instrument_ref": "AAPL_20260220_120C"},
            generated_at_utc="2026-01-01T00:01:00Z",
        )

        self.assertEqual(decision["contract_type"], "entry_decision")
        self.assertEqual(decision["decision_status"], "accepted")
        self.assertEqual(decision["decision_action"], "open_option")
        self.assertEqual(decision["asset_class"], "us_option")
        self.assertEqual(decision["instrument_ref"], "AAPL_20260220_120C")
        self.assertEqual(validate_entry_decision(decision)["validation_status"], "passed")

    def test_crypto_entry_blocks_options(self) -> None:
        allocation = build_target_allocation_snapshot(
            account_sleeve_id=CRYPTO_SPOT_ACCOUNT_SLEEVE,
            generated_at_utc="2026-01-01T00:00:00Z",
        )

        decision = build_entry_decision(
            target_allocation_snapshot=allocation,
            target_ref="BTC",
            alpha_confidence_vector={"alpha_confidence_score": 0.95},
            option_expression_plan={"preferred_expression": "long_call", "instrument_ref": "BTC_OPTION"},
            generated_at_utc="2026-01-01T00:01:00Z",
        )

        self.assertEqual(decision["decision_status"], "blocked")
        self.assertEqual(decision["decision_action"], "block_entry")
        self.assertIn("options_not_allowed_for_account_sleeve", decision["reason_codes"])
        self.assertEqual(validate_entry_decision(decision)["validation_status"], "passed")

    def test_position_lifecycle_reduces_on_high_event_risk(self) -> None:
        decision = build_position_lifecycle_decision(
            position_state={
                "position_ref": "pos-aapl-1",
                "account_sleeve_id": EQUITY_OPTIONS_ACCOUNT_SLEEVE,
                "target_ref": "AAPL",
                "instrument_ref": "AAPL",
                "quantity": 10,
            },
            event_failure_risk_vector={"risk_level": "high"},
            account_sleeve_risk_budget={"max_position_loss_pct": 0.05},
            generated_at_utc="2026-01-01T00:02:00Z",
        )

        self.assertEqual(decision["contract_type"], "position_lifecycle_decision")
        self.assertEqual(decision["decision_status"], "accepted")
        self.assertEqual(decision["decision_action"], "reduce")
        self.assertIn("event_failure_risk_requires_reduction", decision["reason_codes"])
        self.assertEqual(validate_position_lifecycle_decision(decision)["validation_status"], "passed")

    def test_order_intent_is_broker_neutral_and_requires_valid_risk_cap(self) -> None:
        allocation = build_target_allocation_snapshot(
            account_sleeve_id=EQUITY_OPTIONS_ACCOUNT_SLEEVE,
            target_context_rows=[{"target_ref": "MSFT", "asset_class": "us_equity"}],
            generated_at_utc="2026-01-01T00:00:00Z",
        )
        decision = build_entry_decision(
            target_allocation_snapshot=allocation,
            target_ref="MSFT",
            alpha_confidence_vector={"alpha_confidence_score": 0.80},
            generated_at_utc="2026-01-01T00:01:00Z",
        )

        intent = build_execution_order_intent(
            decision_record=decision,
            trade_risk_cap=VALID_STOCK_RISK_CAP,
            execution_policy_snapshot={"default_order_type": "limit", "time_in_force": "day"},
            generated_at_utc="2026-01-01T00:03:00Z",
        )

        self.assertEqual(intent["contract_type"], "execution_order_intent")
        self.assertEqual(intent["intent_status"], "ready_for_execution_gate_not_submitted")
        self.assertEqual(intent["broker_neutral_order"]["instrument_ref"], "MSFT")
        self.assertEqual(intent["broker_neutral_order"]["side"], "buy")
        self.assertEqual(intent["risk_cap_validation"]["valid"], True)
        self.assertEqual(intent["safety"]["broker_calls_performed"], 0)
        self.assertFalse(intent["safety"]["account_mutation_performed"])
        self.assertEqual(validate_execution_order_intent(intent)["validation_status"], "passed")

    def test_order_intent_blocks_missing_risk_cap(self) -> None:
        allocation = build_target_allocation_snapshot(
            account_sleeve_id=CRYPTO_SPOT_ACCOUNT_SLEEVE,
            generated_at_utc="2026-01-01T00:00:00Z",
        )
        decision = build_entry_decision(
            target_allocation_snapshot=allocation,
            target_ref="BTC",
            alpha_confidence_vector={"alpha_confidence_score": 0.85},
            generated_at_utc="2026-01-01T00:01:00Z",
        )

        intent = build_execution_order_intent(
            decision_record=decision,
            trade_risk_cap={},
            generated_at_utc="2026-01-01T00:03:00Z",
        )

        self.assertEqual(intent["intent_status"], "blocked_order_intent")
        self.assertIn("missing_max_loss_usd", intent["reason_codes"])
        self.assertTrue(intent["risk_cap_validation"]["reject_order"])
        self.assertEqual(validate_execution_order_intent(intent)["validation_status"], "passed")


if __name__ == "__main__":
    unittest.main()
