import unittest

from trading_execution.runtime import (
    CRYPTO_SPOT_ACCOUNT_SLEEVE,
    EQUITY_OPTIONS_ACCOUNT_SLEEVE,
    build_entry_decision,
    build_execution_gate_result,
    build_execution_order_intent,
    build_failure_explanation_packet,
    build_option_reexpression_decision,
    build_position_lifecycle_decision,
    build_simulated_fill_event,
    build_execution_intake_snapshot,
    validate_entry_decision,
    validate_execution_gate_result,
    validate_execution_order_intent,
    validate_failure_explanation_packet,
    validate_option_reexpression_decision,
    validate_position_lifecycle_decision,
    validate_simulated_fill_event,
    validate_execution_intake_snapshot,
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

VALID_UNDERLYING_ENTRY_PLAN = {
    "entry_direction": "long",
    "entry_price_min": 100.0,
    "entry_price_max": 103.0,
    "target_price": 112.0,
    "model_invalidation_price": 94.0,
    "hard_stop_price": 93.5,
    "expected_horizon": "swing",
    "entry_thesis_score": 0.82,
}


class RuntimeDecisionTests(unittest.TestCase):
    def test_crypto_intake_uses_fixed_three_asset_pool(self) -> None:
        snapshot = build_execution_intake_snapshot(
            account_sleeve_id=CRYPTO_SPOT_ACCOUNT_SLEEVE,
            account_sleeve_state={"available_cash_usd": 1000.0},
            market_universe={"targets": [{"symbol": "BTC"}, {"symbol": "DOGE"}]},
            generated_at_utc="2026-01-01T00:00:00Z",
        )

        self.assertEqual(snapshot["contract_type"], "execution_intake_snapshot")
        self.assertEqual(snapshot["account_sleeve_id"], CRYPTO_SPOT_ACCOUNT_SLEEVE)
        self.assertEqual([row["target_ref"] for row in snapshot["watch_targets"]], ["BTC"])
        self.assertEqual(snapshot["blocked_targets"], [{"target_ref": "DOGE", "reason_codes": ["outside_fixed_crypto_candidate_pool"]}])
        self.assertEqual(snapshot["new_position_balance_status"], "has_balance")
        self.assertEqual(validate_execution_intake_snapshot(snapshot)["validation_status"], "passed")
        self.assertEqual(snapshot["safety"]["broker_calls_performed"], 0)

    def test_intake_marks_no_available_balance_without_managing_risk(self) -> None:
        snapshot = build_execution_intake_snapshot(
            account_sleeve_id=EQUITY_OPTIONS_ACCOUNT_SLEEVE,
            account_sleeve_state={"available_cash_usd": 0.0},
            target_context_rows=[{"target_ref": "AAPL", "asset_class": "us_equity"}],
            generated_at_utc="2026-01-01T00:00:00Z",
        )
        self.assertEqual(snapshot["new_position_balance_status"], "no_available_balance")
        self.assertNotIn("risk_budget", snapshot)

    def test_intake_builds_sector_opportunity_mix_from_strong_sectors(self) -> None:
        snapshot = build_execution_intake_snapshot(
            account_sleeve_id=EQUITY_OPTIONS_ACCOUNT_SLEEVE,
            account_sleeve_state={"available_cash_usd": 1000.0},
            sector_context_state={
                "strong_sector_threshold": 0.70,
                "sector_scores": [
                    {"sector_ref": "software", "sector_strength_score": 0.80},
                    {"sector_ref": "semiconductors", "sector_strength_score": 0.80},
                    {"sector_ref": "healthcare", "sector_strength_score": 0.40},
                    {"sector_ref": "financials", "sector_strength_score": 0.72},
                ],
            },
            target_context_rows=[{"target_ref": "MSFT", "asset_class": "us_equity"}],
            generated_at_utc="2026-01-01T00:00:00Z",
        )

        self.assertEqual(
            snapshot["sector_opportunity_mix"],
            [
                {
                    "sector_ref": "semiconductors",
                    "opportunity_strength_score": 0.8,
                    "target_mix_weight": 0.344828,
                    "current_mix_weight": 0.0,
                    "remaining_mix_weight": 0.344828,
                    "opportunity_mix_weight": 0.344828,
                },
                {
                    "sector_ref": "software",
                    "opportunity_strength_score": 0.8,
                    "target_mix_weight": 0.344828,
                    "current_mix_weight": 0.0,
                    "remaining_mix_weight": 0.344828,
                    "opportunity_mix_weight": 0.344828,
                },
                {
                    "sector_ref": "financials",
                    "opportunity_strength_score": 0.72,
                    "target_mix_weight": 0.310345,
                    "current_mix_weight": 0.0,
                    "remaining_mix_weight": 0.310345,
                    "opportunity_mix_weight": 0.310345,
                },
            ],
        )

    def test_intake_removes_sector_when_current_mix_already_filled(self) -> None:
        snapshot = build_execution_intake_snapshot(
            account_sleeve_id=EQUITY_OPTIONS_ACCOUNT_SLEEVE,
            account_sleeve_state={"available_cash_usd": 1000.0},
            position_state=[
                {"position_ref": "pos-nvda", "sector_ref": "semiconductors", "portfolio_weight": 0.35},
            ],
            sector_context_state={
                "strong_sector_threshold": 0.50,
                "sector_scores": [
                    {"sector_ref": "software", "sector_strength_score": 0.70},
                    {"sector_ref": "semiconductors", "sector_strength_score": 0.70},
                    {"sector_ref": "healthcare", "sector_strength_score": 0.60},
                ],
            },
            target_context_rows=[{"target_ref": "MSFT", "asset_class": "us_equity", "sector_ref": "software"}],
            generated_at_utc="2026-01-01T00:00:00Z",
        )

        self.assertEqual(
            snapshot["sector_opportunity_mix"],
            [
                {
                    "sector_ref": "software",
                    "opportunity_strength_score": 0.7,
                    "target_mix_weight": 0.35,
                    "current_mix_weight": 0.0,
                    "remaining_mix_weight": 0.35,
                    "opportunity_mix_weight": 0.538462,
                },
                {
                    "sector_ref": "healthcare",
                    "opportunity_strength_score": 0.6,
                    "target_mix_weight": 0.3,
                    "current_mix_weight": 0.0,
                    "remaining_mix_weight": 0.3,
                    "opportunity_mix_weight": 0.461538,
                },
            ],
        )

    def test_intake_pool_keeps_filled_sector_target_with_independent_signal(self) -> None:
        snapshot = build_execution_intake_snapshot(
            account_sleeve_id=EQUITY_OPTIONS_ACCOUNT_SLEEVE,
            account_sleeve_state={"available_cash_usd": 1000.0},
            position_state=[
                {"position_ref": "pos-nvda", "sector_ref": "semiconductors", "portfolio_weight": 0.35},
            ],
            sector_context_state={
                "strong_sector_threshold": 0.50,
                "sector_scores": [
                    {"sector_ref": "software", "sector_strength_score": 0.70},
                    {"sector_ref": "semiconductors", "sector_strength_score": 0.70},
                    {"sector_ref": "healthcare", "sector_strength_score": 0.60},
                ],
            },
            target_context_rows=[
                {"target_ref": "NVDA", "asset_class": "us_equity", "sector_ref": "semiconductors", "volume_score": 0.90},
                {"target_ref": "AMD", "asset_class": "us_equity", "sector_ref": "semiconductors"},
                {"target_ref": "MSFT", "asset_class": "us_equity", "sector_ref": "software"},
                {"target_ref": "LLY", "asset_class": "us_equity", "sector_ref": "healthcare"},
                {"target_ref": "TSLA", "asset_class": "us_equity", "relative_volume": 2.4},
                {"target_ref": "AAPL", "asset_class": "us_equity", "news_catalyst_score": 0.80},
                {"target_ref": "LOWQ", "asset_class": "us_equity"},
            ],
            generated_at_utc="2026-01-01T00:00:00Z",
        )

        self.assertEqual([row["target_ref"] for row in snapshot["watch_targets"]], ["NVDA", "MSFT", "LLY", "TSLA", "AAPL"])
        self.assertEqual(
            snapshot["blocked_targets"],
            [
                {"target_ref": "AMD", "reason_codes": ["sector_opportunity_already_filled"]},
                {"target_ref": "LOWQ", "reason_codes": ["not_in_c01_candidate_source_pool"]},
            ],
        )
        self.assertEqual(snapshot["watch_targets"][0]["candidate_reasons"], ["recent_high_trading_volume"])
        self.assertEqual(snapshot["watch_targets"][1]["candidate_reasons"], ["remaining_strong_sector_opportunity"])
        self.assertEqual(snapshot["watch_targets"][3]["candidate_reasons"], ["recent_abnormal_volume"])
        self.assertEqual(snapshot["watch_targets"][4]["candidate_reasons"], ["recent_news_catalyst"])

    def test_equity_options_entry_outputs_underlying_thesis_only(self) -> None:
        allocation = build_execution_intake_snapshot(
            account_sleeve_id=EQUITY_OPTIONS_ACCOUNT_SLEEVE,
            account_sleeve_state={"available_cash_usd": 1000.0},
            target_context_rows=[
                {"target_ref": "AAPL", "instrument_ref": "AAPL", "asset_class": "us_equity"},
            ],
            generated_at_utc="2026-01-01T00:00:00Z",
        )

        decision = build_entry_decision(
            execution_intake_snapshot=allocation,
            target_ref="AAPL",
            alpha_confidence_vector={"alpha_confidence_score": 0.80},
            event_failure_risk_vector={"risk_level": "low"},
            dynamic_risk_policy_state={"minimum_entry_alpha_confidence": 0.55},
            underlying_action_plan=VALID_UNDERLYING_ENTRY_PLAN,
            option_expression_plan={"preferred_expression": "long_call", "instrument_ref": "AAPL_20260220_120C"},
            target_context_state={"current_price": 101.0},
            generated_at_utc="2026-01-01T00:01:00Z",
        )

        self.assertEqual(decision["contract_type"], "entry_decision")
        self.assertEqual(decision["decision_status"], "suitable")
        self.assertEqual(decision["decision_action"], "continue_to_option_review")
        self.assertEqual(decision["entry_thesis_status"], "suitable")
        self.assertEqual(decision["entry_direction"], "long")
        self.assertEqual(decision["asset_class"], "us_equity")
        self.assertEqual(decision["instrument_ref"], "AAPL")
        self.assertEqual(decision["entry_zone"], {"low": 100.0, "high": 103.0})
        self.assertEqual(decision["target_price"], 112.0)
        self.assertEqual(decision["model_invalidation_price"], 94.0)
        self.assertEqual(decision["hard_stop_price"], 93.5)
        self.assertNotIn("option_expression_plan", decision["model_layer_refs"])
        self.assertEqual(validate_entry_decision(decision)["validation_status"], "passed")

    def test_entry_rejects_missing_underlying_thesis_without_handling_options(self) -> None:
        allocation = build_execution_intake_snapshot(
            account_sleeve_id=CRYPTO_SPOT_ACCOUNT_SLEEVE,
            account_sleeve_state={"available_cash_usd": 1000.0},
            generated_at_utc="2026-01-01T00:00:00Z",
        )

        decision = build_entry_decision(
            execution_intake_snapshot=allocation,
            target_ref="BTC",
            alpha_confidence_vector={"alpha_confidence_score": 0.95},
            option_expression_plan={"preferred_expression": "long_call", "instrument_ref": "BTC_OPTION"},
            generated_at_utc="2026-01-01T00:01:00Z",
        )

        self.assertEqual(decision["decision_status"], "rejected")
        self.assertEqual(decision["decision_action"], "reject_entry_thesis")
        self.assertIn("missing_underlying_entry_direction", decision["reason_codes"])
        self.assertNotIn("options_not_allowed_for_account_sleeve", decision["reason_codes"])
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

    def test_position_lifecycle_uses_underlying_stop_not_fixed_loss_pct(self) -> None:
        decision = build_position_lifecycle_decision(
            position_state={
                "position_ref": "pos-nvda-1",
                "account_sleeve_id": EQUITY_OPTIONS_ACCOUNT_SLEEVE,
                "target_ref": "NVDA",
                "instrument_ref": "NVDA",
                "quantity": 1,
                "unrealized_loss_pct": 0.80,
                "current_underlying_price": 101.0,
                "position_side": "long",
            },
            account_sleeve_risk_budget={"max_position_loss_pct": 0.05},
            underlying_action_plan={
                "hard_stop_price": 94.0,
                "model_invalidation_price": 95.0,
                "target_price": 120.0,
            },
            alpha_confidence_vector={"alpha_confidence_score": 0.80},
            generated_at_utc="2026-01-01T00:02:00Z",
        )

        self.assertEqual(decision["decision_status"], "accepted")
        self.assertEqual(decision["decision_action"], "hold")
        self.assertIn("position_thesis_still_valid", decision["reason_codes"])
        self.assertIn("fixed_percentage_loss_not_lifecycle_stop", decision["reason_codes"])
        self.assertNotIn("max_position_loss_pct_reached", decision["reason_codes"])

    def test_position_lifecycle_stops_on_model_underlying_hard_stop(self) -> None:
        decision = build_position_lifecycle_decision(
            position_state={
                "position_ref": "pos-nvda-2",
                "account_sleeve_id": EQUITY_OPTIONS_ACCOUNT_SLEEVE,
                "target_ref": "NVDA",
                "instrument_ref": "NVDA",
                "quantity": 1,
                "current_underlying_price": 93.5,
                "position_side": "long",
            },
            underlying_action_plan={"hard_stop_price": 94.0, "model_invalidation_price": 95.0},
            generated_at_utc="2026-01-01T00:02:00Z",
        )

        self.assertEqual(decision["decision_status"], "accepted")
        self.assertEqual(decision["decision_action"], "stop")
        self.assertIn("model_underlying_hard_stop_reached", decision["reason_codes"])
        self.assertEqual(validate_position_lifecycle_decision(decision)["validation_status"], "passed")

    def test_position_lifecycle_blocks_add_when_sector_mix_is_filled(self) -> None:
        decision = build_position_lifecycle_decision(
            position_state={
                "position_ref": "pos-msft-1",
                "account_sleeve_id": EQUITY_OPTIONS_ACCOUNT_SLEEVE,
                "target_ref": "MSFT",
                "instrument_ref": "MSFT",
                "quantity": 1,
                "current_underlying_price": 105.0,
            },
            alpha_confidence_vector={"alpha_confidence_score": 0.90},
            dynamic_risk_policy_state={"minimum_add_alpha_confidence": 0.70},
            position_projection_vector={
                "add_allowed": True,
                "sector_mix_add_allowed": False,
                "sector_opportunity_remaining_weight": 0.0,
            },
            generated_at_utc="2026-01-01T00:02:00Z",
        )

        self.assertEqual(decision["decision_status"], "accepted")
        self.assertEqual(decision["decision_action"], "hold")
        self.assertTrue(decision["portfolio_constraint_checks"]["add_blocked"])
        self.assertIn("sector_opportunity_mix_blocks_add", decision["portfolio_constraint_checks"]["reason_codes"])
        self.assertIn("add_blocked_by_portfolio_constraints", decision["reason_codes"])

    def test_order_intent_is_broker_neutral_and_requires_valid_risk_cap(self) -> None:
        decision = build_position_lifecycle_decision(
            position_state={
                "position_ref": "pos-msft-1",
                "account_sleeve_id": EQUITY_OPTIONS_ACCOUNT_SLEEVE,
                "target_ref": "MSFT",
                "instrument_ref": "MSFT",
                "quantity": 5,
            },
            event_failure_risk_vector={"risk_level": "high"},
            generated_at_utc="2026-01-01T00:02:00Z",
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
        self.assertEqual(intent["broker_neutral_order"]["side"], "sell")
        self.assertEqual(intent["broker_neutral_order"]["quantity"], VALID_STOCK_RISK_CAP["planned_quantity"])
        self.assertEqual(intent["sizing_plan"]["position_management_owner"], "component_05_order_intent")
        self.assertEqual(intent["sizing_plan"]["quantity"], VALID_STOCK_RISK_CAP["planned_quantity"])
        self.assertEqual(intent["sizing_plan"]["quantity_source"], "trade_risk_cap.planned_quantity")
        self.assertFalse(intent["sizing_plan"]["execution_gate_may_change_quantity"])
        self.assertEqual(intent["risk_cap_validation"]["valid"], True)
        self.assertTrue(intent["required_execution_gate_reviews"]["agent_final_review_required"])
        self.assertEqual(
            intent["required_execution_gate_reviews"]["agent_final_review_status"],
            "required_before_live_submission",
        )
        self.assertEqual(intent["safety"]["broker_calls_performed"], 0)
        self.assertFalse(intent["safety"]["account_mutation_performed"])
        self.assertEqual(validate_execution_order_intent(intent)["validation_status"], "passed")

    def test_order_intent_blocks_missing_risk_cap(self) -> None:
        decision = build_position_lifecycle_decision(
            position_state={
                "position_ref": "pos-btc-1",
                "account_sleeve_id": CRYPTO_SPOT_ACCOUNT_SLEEVE,
                "target_ref": "BTC",
                "instrument_ref": "BTC-USDT",
                "quantity": 0.5,
            },
            event_failure_risk_vector={"risk_level": "high"},
            generated_at_utc="2026-01-01T00:02:00Z",
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

    def test_execution_gate_result_approves_replay_without_changing_quantity(self) -> None:
        decision = build_position_lifecycle_decision(
            position_state={
                "position_ref": "pos-msft-1",
                "account_sleeve_id": EQUITY_OPTIONS_ACCOUNT_SLEEVE,
                "target_ref": "MSFT",
                "instrument_ref": "MSFT",
                "quantity": 5,
            },
            event_failure_risk_vector={"risk_level": "high"},
            generated_at_utc="2026-01-01T00:02:00Z",
        )
        intent = build_execution_order_intent(
            decision_record=decision,
            trade_risk_cap=VALID_STOCK_RISK_CAP,
            generated_at_utc="2026-01-01T00:03:00Z",
        )

        gate = build_execution_gate_result(
            execution_order_intent=intent,
            mode="replay",
            execution_hard_block_checks={"reason_codes": []},
            generated_at_utc="2026-01-01T00:03:30Z",
        )

        self.assertEqual(gate["contract_type"], "execution_gate_result")
        self.assertEqual(gate["execution_gate_status"], "approved_for_simulated_fill")
        self.assertEqual(gate["execution_action"], "simulate_fill")
        self.assertTrue(gate["quantity_unchanged_by_execution_gate"])
        self.assertEqual(gate["source_order_quantity"], VALID_STOCK_RISK_CAP["planned_quantity"])
        self.assertEqual(gate["sizing_plan_quantity"], VALID_STOCK_RISK_CAP["planned_quantity"])
        self.assertEqual(gate["safety"]["broker_calls_performed"], 0)
        self.assertEqual(validate_execution_gate_result(gate)["validation_status"], "passed")

    def test_execution_gate_result_rejects_live_without_agent_review(self) -> None:
        decision = build_position_lifecycle_decision(
            position_state={
                "position_ref": "pos-msft-1",
                "account_sleeve_id": EQUITY_OPTIONS_ACCOUNT_SLEEVE,
                "target_ref": "MSFT",
                "instrument_ref": "MSFT",
                "quantity": 5,
            },
            event_failure_risk_vector={"risk_level": "high"},
            generated_at_utc="2026-01-01T00:02:00Z",
        )
        intent = build_execution_order_intent(
            decision_record=decision,
            trade_risk_cap=VALID_STOCK_RISK_CAP,
            generated_at_utc="2026-01-01T00:03:00Z",
        )

        gate = build_execution_gate_result(
            execution_order_intent=intent,
            mode="live",
            agent_final_review={"review_status": "required_before_live_submission"},
            broker_submit_enabled=False,
            generated_at_utc="2026-01-01T00:03:30Z",
        )

        self.assertEqual(gate["execution_gate_status"], "rejected_execution_gate")
        self.assertEqual(gate["execution_action"], "reject")
        self.assertIn("agent_final_review_not_approved", gate["reason_codes"])
        self.assertIn("missing_agent_final_review_ref", gate["reason_codes"])
        self.assertIn("broker_submit_disabled", gate["reason_codes"])
        self.assertTrue(gate["quantity_unchanged_by_execution_gate"])
        self.assertEqual(validate_execution_gate_result(gate)["validation_status"], "passed")

    def test_execution_gate_result_rejects_quantity_mismatch(self) -> None:
        decision = build_position_lifecycle_decision(
            position_state={
                "position_ref": "pos-msft-1",
                "account_sleeve_id": EQUITY_OPTIONS_ACCOUNT_SLEEVE,
                "target_ref": "MSFT",
                "instrument_ref": "MSFT",
                "quantity": 5,
            },
            event_failure_risk_vector={"risk_level": "high"},
            generated_at_utc="2026-01-01T00:02:00Z",
        )
        intent = build_execution_order_intent(
            decision_record=decision,
            trade_risk_cap=VALID_STOCK_RISK_CAP,
            generated_at_utc="2026-01-01T00:03:00Z",
        )
        intent["broker_neutral_order"]["quantity"] = 4

        gate = build_execution_gate_result(
            execution_order_intent=intent,
            mode="replay",
            generated_at_utc="2026-01-01T00:03:30Z",
        )

        self.assertEqual(gate["execution_gate_status"], "rejected_execution_gate")
        self.assertIn("c06_quantity_mismatch_with_c05_sizing_plan", gate["reason_codes"])
        self.assertFalse(gate["quantity_unchanged_by_execution_gate"])
        self.assertEqual(validate_execution_gate_result(gate)["validation_status"], "passed")

    def test_option_reexpression_rolls_only_for_equity_options_sleeve(self) -> None:
        decision = build_option_reexpression_decision(
            option_position_state={
                "position_ref": "opt-aapl-1",
                "account_sleeve_id": EQUITY_OPTIONS_ACCOUNT_SLEEVE,
                "underlying_symbol": "AAPL",
                "instrument_ref": "AAPL_20260220_110C",
                "quantity": 1,
                "contract_quality_score": 0.40,
            },
            dynamic_risk_policy_state={"minimum_roll_quality_improvement": 0.15, "max_roll_cost_pct": 0.10},
            candidate_option_contracts=[
                {"instrument_ref": "AAPL_20260320_115C", "contract_quality_score": 0.62, "roll_cost_pct": 0.04},
                {"instrument_ref": "AAPL_20260320_120C", "contract_quality_score": 0.50, "roll_cost_pct": 0.03},
            ],
            generated_at_utc="2026-01-01T00:04:00Z",
        )

        self.assertEqual(decision["contract_type"], "option_reexpression_decision")
        self.assertEqual(decision["decision_status"], "accepted")
        self.assertEqual(decision["decision_action"], "roll_option")
        self.assertEqual(decision["replacement_instrument_ref"], "AAPL_20260320_115C")
        self.assertEqual(validate_option_reexpression_decision(decision)["validation_status"], "passed")

    def test_failure_explanation_only_uses_events_before_failure(self) -> None:
        packet = build_failure_explanation_packet(
            failure_observation={
                "failure_ref": "failure-aapl-1",
                "account_sleeve_id": EQUITY_OPTIONS_ACCOUNT_SLEEVE,
                "observed_at_utc": "2026-01-03T15:00:00Z",
            },
            unscreened_event_evidence=[
                {
                    "event_ref": "event-before",
                    "event_time_utc": "2026-01-03T14:00:00Z",
                    "event_family": "regulatory_probe",
                    "severity_score": 0.7,
                    "match_score": 0.8,
                },
                {
                    "event_ref": "event-after",
                    "event_time_utc": "2026-01-03T16:00:00Z",
                    "event_family": "late_news",
                    "severity_score": 0.9,
                    "match_score": 0.9,
                },
            ],
            generated_at_utc="2026-01-03T15:01:00Z",
        )

        self.assertEqual(packet["contract_type"], "failure_explanation_packet")
        self.assertEqual(packet["explanation_status"], "candidate_causes_found")
        self.assertEqual(packet["ranked_possible_causes"][0]["event_ref"], "event-before")
        self.assertEqual(packet["ignored_events"], [{"event_ref": "event-after", "reason_codes": ["event_after_failure_time"]}])
        self.assertEqual(validate_failure_explanation_packet(packet)["validation_status"], "passed")

    def test_simulated_fill_event_consumes_ready_order_intent(self) -> None:
        decision = build_position_lifecycle_decision(
            position_state={
                "position_ref": "pos-eth-1",
                "account_sleeve_id": CRYPTO_SPOT_ACCOUNT_SLEEVE,
                "target_ref": "ETH",
                "instrument_ref": "ETH-USDT",
                "quantity": 0.5,
            },
            event_failure_risk_vector={"risk_level": "high"},
            generated_at_utc="2026-01-01T00:02:00Z",
        )
        intent = build_execution_order_intent(
            decision_record=decision,
            trade_risk_cap={
                "max_loss_usd": 50.0,
                "max_loss_pct": 0.02,
                "time_stop_at": "2026-01-05T20:00:00Z",
                "cap_enforcement_mode": "broker_native_stop",
                "cap_failure_action": "reject_order",
                "model_invalidation_price": 3000.0,
                "hard_stop_price": 2995.0,
                "planned_quantity": 0.2,
                "planned_limit_price": 3200.0,
            },
            generated_at_utc="2026-01-01T00:03:00Z",
        )
        gate = build_execution_gate_result(
            execution_order_intent=intent,
            mode="replay",
            execution_hard_block_checks={"reason_codes": []},
            generated_at_utc="2026-01-01T00:03:30Z",
        )
        fill = build_simulated_fill_event(
            execution_order_intent=intent,
            execution_gate_result=gate,
            replay_fill_policy={"slippage_bps": 5, "fee_bps": 10, "replay_fill_policy_ref": "policy://fixture"},
            market_snapshot={"reference_price": 3190.0, "market_snapshot_ref": "snapshot://eth"},
            generated_at_utc="2026-01-01T00:04:00Z",
        )

        self.assertEqual(fill["contract_type"], "simulated_fill_event")
        self.assertEqual(fill["fill_status"], "simulated_filled")
        self.assertEqual(fill["source_execution_gate_result_id"], gate["execution_gate_result_id"])
        self.assertEqual(fill["instrument_ref"], "ETH-USDT")
        self.assertAlmostEqual(fill["simulated_fill_price"], 3188.405)
        self.assertEqual(fill["safety"]["broker_calls_performed"], 0)
        self.assertEqual(validate_simulated_fill_event(fill)["validation_status"], "passed")


if __name__ == "__main__":
    unittest.main()
